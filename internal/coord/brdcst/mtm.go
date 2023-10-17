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

	// keyReports tracks for each key this [ManyToMany] state machine should
	// broadcast the number of successes and failures.
	// TODO: perhaps this is better tracked outside of this state machine?
	keyReports map[string]*report

	// unprocessedNodes is a map from a node's ID to its [NodeState]. The
	// [NodeState] contains information about all the keys that should be
	// stored with that node, as well as, a map of all inflight requests and
	// all keys that have already been tried to store with that node.
	unprocessedNodes map[string]*NodeState[K, N]

	// inflightWithCapacity holds information about nodes that we are currently
	// contacting but still have capacity to receive more requests from us. The
	// term capacity refers to the number of concurrent streams we can open to
	// a single node based on [ConfigManyToMany.StreamConcurrency].
	inflightWithCapacity map[string]*NodeState[K, N]

	// inflightWithCapacity holds information about nodes that we are currently
	// contacting with no capacity to receive more concurrent streams. The
	// term capacity refers to the number of concurrent streams we can open
	// to a single node based on [ConfigManyToMany.StreamConcurrency].
	inflightAtCapacity map[string]*NodeState[K, N]

	// processedNodes is a map from a node's ID to its [NodeState]. All nodes
	// in this map have been fully processed. This means that all keys we wanted
	// to store with a node have been attempted to be stored with it.
	processedNodes map[string]*NodeState[K, N]

	// msgFunc takes a key and returns the corresponding message that we will
	// need to send to the remote node to store said key.
	msgFunc func(K) M
}

type brdcstManyMapVal[K kad.Key[K], N kad.NodeID[K]] struct {
	target K
	node   N
}

type NodeState[K kad.Key[K], N kad.NodeID[K]] struct {
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

	// TODO: the below is quite expensive for many keys. It's probably worth doing this outside of the event loop

	// find out which seed nodes are responsible to hold the provider/put
	// record for which target key.
	keyReports := make(map[string]*report, len(cfg.Targets))
	mappings := map[string]map[string]*brdcstManyMapVal[K, N]{} // map from node -> map of target keys -> target key
	for _, target := range cfg.Targets {
		entries := trie.Closest(t, target, 20) // TODO: make configurable
		targetMapKey := key.HexString(target)

		if len(entries) > 0 {
			keyReports[targetMapKey] = &report{}
		}

		for _, entry := range entries {
			node := entry.Data
			nodeMapKey := node.String()
			if _, found := mappings[nodeMapKey]; !found {
				mappings[nodeMapKey] = map[string]*brdcstManyMapVal[K, N]{}
			}

			mappings[nodeMapKey][targetMapKey] = &brdcstManyMapVal[K, N]{target: target, node: node}
		}
	}

	unprocessedNodes := make(map[string]*NodeState[K, N], len(mappings))
	for node, mapVals := range mappings {
		if len(mapVals) == 0 {
			continue
		}

		unprocessedNodes[node] = &NodeState[K, N]{
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
		inflightWithCapacity: map[string]*NodeState[K, N]{},
		inflightAtCapacity:   map[string]*NodeState[K, N]{},
		processedNodes:       map[string]*NodeState[K, N]{},
		msgFunc:              msgFunc,
	}
}

// Advance advances the state of the [ManyToMany] [Broadcast] state machine.
func (mtm *ManyToMany[K, N, M]) Advance(ctx context.Context, ev BroadcastEvent) (out BroadcastState) {
	_, span := tele.StartSpan(ctx, "ManyToMany.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventBroadcastStop:
	case *EventBroadcastStoreRecordSuccess[K, N, M]:
		mtm.handleStoreRecordResult(ev.NodeID, ev.Target)

		targetMapKey := key.HexString(ev.Target)
		mtm.keyReports[targetMapKey].successes += 1
		mtm.keyReports[targetMapKey].lastSuccess = time.Now()

	case *EventBroadcastStoreRecordFailure[K, N, M]:
		mtm.handleStoreRecordResult(ev.NodeID, ev.Target)

		targetMapKey := key.HexString(ev.Target)
		mtm.keyReports[targetMapKey].failures += 1

	case *EventBroadcastPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	for node, nstate := range mtm.inflightWithCapacity {
		var popped K
		popped, nstate.todo = nstate.todo[0], nstate.todo[1:]

		nstate.inflight[key.HexString(popped)] = popped

		if len(nstate.todo) == 0 || len(nstate.inflight) == mtm.cfg.StreamConcurrency {
			delete(mtm.inflightWithCapacity, node)
			mtm.inflightAtCapacity[nstate.node.String()] = nstate
		}

		return &StateBroadcastStoreRecord[K, N, M]{
			QueryID: mtm.queryID,
			NodeID:  nstate.node,
			Target:  popped,
			Message: mtm.msgFunc(popped),
		}
	}

	// check if we are currently talking to the maximum number of nodes
	// concurrently.
	inflightNodes := len(mtm.inflightWithCapacity) + len(mtm.inflightAtCapacity)
	if inflightNodes == mtm.cfg.NodeConcurrency || (inflightNodes > 0 && len(mtm.unprocessedNodes) == 0) {
		return &StateBroadcastWaiting{
			QueryID: mtm.queryID,
		}
	}

	// we still have the capacity to contact more nodes
	for nodeStr, nstate := range mtm.unprocessedNodes {
		delete(mtm.unprocessedNodes, nodeStr)

		var popped K
		popped, nstate.todo = nstate.todo[0], nstate.todo[1:]
		nstate.inflight[key.HexString(popped)] = popped

		if len(nstate.todo) == 0 {
			mtm.inflightAtCapacity[nodeStr] = nstate
		} else {
			mtm.inflightWithCapacity[nodeStr] = nstate
		}

		return &StateBroadcastStoreRecord[K, N, M]{
			QueryID: mtm.queryID,
			NodeID:  nstate.node,
			Target:  popped,
			Message: mtm.msgFunc(popped),
		}
	}

	contacted := make([]N, 0, len(mtm.processedNodes))
	for _, ns := range mtm.processedNodes {
		contacted = append(contacted, ns.node)
	}

	return &StateBroadcastFinished[K, N]{
		QueryID:   mtm.queryID,
		Contacted: contacted,
		Errors: map[string]struct {
			Node N
			Err  error
		}{},
	}
}

func (mtm *ManyToMany[K, N, M]) handleStoreRecordResult(node N, target K) {
	nodeMapKey := node.String()
	targetMapKey := key.HexString(target)
	if nstate, found := mtm.inflightAtCapacity[nodeMapKey]; found {
		delete(mtm.inflightAtCapacity, nodeMapKey)
		delete(nstate.inflight, targetMapKey)
		nstate.done = append(nstate.done, target)

		if len(nstate.todo) == 0 {
			if len(nstate.inflight) == 0 {
				mtm.processedNodes[nodeMapKey] = nstate
			} else {
				mtm.inflightAtCapacity[nodeMapKey] = nstate
			}
		} else if len(nstate.inflight) != 0 {
			mtm.inflightWithCapacity[nodeMapKey] = nstate
		}
	} else if nstate, found := mtm.inflightWithCapacity[nodeMapKey]; found {
		delete(mtm.inflightWithCapacity, nodeMapKey)
		delete(nstate.inflight, targetMapKey)
		nstate.done = append(nstate.done, target)

		if len(nstate.todo) != 0 {
			mtm.inflightWithCapacity[nodeMapKey] = nstate
		}
	}
}
