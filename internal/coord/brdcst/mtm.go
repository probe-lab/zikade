package brdcst

import (
	"context"
	"fmt"
	"time"

	"github.com/plprobelab/go-libdht/kad"
	"github.com/plprobelab/go-libdht/kad/key"
	"github.com/plprobelab/go-libdht/kad/trie"
	"go.opentelemetry.io/otel/trace"

	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/tele"
)

// ManyToMany is a [Broadcast] state machine and encapsulates the logic around
// doing a put operation to a static set of nodes. That static set of nodes
// is given by the list of seed nodes in the [EventBroadcastStart] event.
type ManyToMany[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	// the unique ID for this broadcast operation
	queryID coordt.QueryID

	// a struct holding configuration options
	cfg *ConfigManyToMany[K]

	// TODO
	keyReports map[string]*report

	// TODO
	unprocessedNodes map[string]*nodeState[K, N]

	// TODO
	inflightWithCapacity map[string]*nodeState[K, N]

	// TODO
	inflightAtCapacity map[string]*nodeState[K, N]

	// TODO
	processedNodes map[string]*nodeState[K, N]

	// TODO
	msgFunc func(K) M
}

type brdcstManyMapVal[K kad.Key[K], N kad.NodeID[K]] struct {
	target K
	node   N
}

type nodeState[K kad.Key[K], N kad.NodeID[K]] struct {
	node     N
	todo     []K
	inflight map[string]K
	done     []K
}

type report struct {
	successes   int
	failures    int
	lastSuccess time.Time
}

// NewManyToMany initializes a new [ManyToMany] struct.
func NewManyToMany[K kad.Key[K], N kad.NodeID[K], M coordt.Message](qid coordt.QueryID, msgFunc func(K) M, seed []N, cfg *ConfigManyToMany[K]) *ManyToMany[K, N, M] {
	t := trie.New[K, N]()
	for _, s := range seed {
		t.Add(s.Key(), s)
	}

	// find out which seed nodes are responsible to hold the provider/put
	// record for which target key.
	keyReports := make(map[string]*report, len(cfg.Targets))
	mappings := map[string]map[string]*brdcstManyMapVal[K, N]{} // map from node -> map of target keys -> target key
	for _, target := range cfg.Targets {
		entries := trie.Closest(t, target, 20) // TODO: make configurable
		targetMapKey := key.HexString(target)
		keyReports[targetMapKey] = &report{}
		for _, entry := range entries {
			node := entry.Data
			nodeMapKey := node.String()
			if _, found := mappings[nodeMapKey]; !found {
				mappings[nodeMapKey] = map[string]*brdcstManyMapVal[K, N]{}
			}

			mappings[nodeMapKey][targetMapKey] = &brdcstManyMapVal[K, N]{target: target, node: node}
		}
	}

	unprocessedNodes := make(map[string]*nodeState[K, N], len(mappings))
	for node, mapVals := range mappings {
		if len(mapVals) == 0 {
			continue
		}

		unprocessedNodes[node] = &nodeState[K, N]{
			todo:     make([]K, 0, len(mapVals)),
			done:     make([]K, 0, len(mapVals)),
			inflight: map[string]K{},
		}
		for _, val := range mapVals {
			unprocessedNodes[node].todo = append(unprocessedNodes[node].todo, val.target)
			unprocessedNodes[node].node = val.node // actually, this needs to only be done once
		}
	}

	return &ManyToMany[K, N, M]{
		queryID:              qid,
		cfg:                  cfg,
		keyReports:           keyReports,
		unprocessedNodes:     unprocessedNodes,
		inflightWithCapacity: map[string]*nodeState[K, N]{},
		inflightAtCapacity:   map[string]*nodeState[K, N]{},
		processedNodes:       map[string]*nodeState[K, N]{},
		msgFunc:              msgFunc,
	}
}

// Advance advances the state of the [ManyToMany] [Broadcast] state machine.
func (otm *ManyToMany[K, N, M]) Advance(ctx context.Context, ev BroadcastEvent) (out BroadcastState) {
	_, span := tele.StartSpan(ctx, "ManyToMany.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer span.End()

	switch ev := ev.(type) {
	case *EventBroadcastStop:
	case *EventBroadcastStoreRecordSuccess[K, N, M]:
		if nstate, found := otm.inflightAtCapacity[ev.NodeID.String()]; found {
			delete(nstate.inflight, key.HexString(ev.Target))
			nstate.done = append(nstate.done, ev.Target)

			delete(otm.inflightAtCapacity, ev.NodeID.String())
			if len(nstate.todo) == 0 && len(nstate.inflight) == 0 {
				otm.processedNodes[ev.NodeID.String()] = nstate
			} else {
				otm.inflightWithCapacity[ev.NodeID.String()] = nstate
			}
		} else if nstate, found := otm.inflightWithCapacity[ev.NodeID.String()]; found {
			delete(nstate.inflight, key.HexString(ev.Target))
			nstate.done = append(nstate.done, ev.Target)

			if len(nstate.todo) == 0 && len(nstate.inflight) == 0 {
				otm.processedNodes[ev.NodeID.String()] = nstate
			}
		}

	case *EventBroadcastStoreRecordFailure[K, N, M]:
	case *EventBroadcastPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	if len(otm.inflightWithCapacity)+len(otm.inflightAtCapacity) == otm.cfg.NodeConcurrency {
		for node, nstate := range otm.inflightWithCapacity {
			var popped K
			popped, nstate.todo = nstate.todo[0], nstate.todo[1:]

			nstate.inflight[key.HexString(popped)] = popped

			if len(nstate.todo) == 0 || len(nstate.inflight) == otm.cfg.StreamConcurrency {
				delete(otm.inflightWithCapacity, node)
				otm.inflightAtCapacity[nstate.node.String()] = nstate
			}

			return &StateBroadcastStoreRecord[K, N, M]{
				QueryID: otm.queryID,
				NodeID:  nstate.node,
				Target:  popped,
				Message: otm.msgFunc(popped),
			}
		}

		return &StateBroadcastWaiting{
			QueryID: otm.queryID,
		}
	}

	for nodeStr, nstate := range otm.unprocessedNodes {
		delete(otm.unprocessedNodes, nodeStr)

		var popped K
		popped, nstate.todo = nstate.todo[0], nstate.todo[1:]
		nstate.inflight[key.HexString(popped)] = popped

		if len(nstate.todo) == 0 {
			otm.inflightAtCapacity[nstate.node.String()] = nstate
		} else {
			otm.inflightWithCapacity[nstate.node.String()] = nstate
		}

		return &StateBroadcastStoreRecord[K, N, M]{
			QueryID: otm.queryID,
			NodeID:  nstate.node,
			Target:  popped,
			Message: otm.msgFunc(popped),
		}
	}

	return &StateBroadcastIdle{}
}
