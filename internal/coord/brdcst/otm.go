package brdcst

import (
	"context"
	"fmt"

	"github.com/plprobelab/go-libdht/kad"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/tele"
)

// ConfigOneToMany specifies the configuration for the [OneToMany] state
// machine.
type ConfigOneToMany[K kad.Key[K]] struct {
	Target K
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *ConfigOneToMany[K]) Validate() error {
	return nil
}

// DefaultConfigOneToMany returns the default configuration options for the
// [OneToMany] state machine.
func DefaultConfigOneToMany[K kad.Key[K]](target K) *ConfigOneToMany[K] {
	return &ConfigOneToMany[K]{
		Target: target,
	}
}

// OneToMany is a [Broadcast] state machine and encapsulates the logic around
// doing a ONE put operation to MANY preconfigured nodes. That static set of
// nodes is given by the list of seed nodes in the [EventBroadcastStart] event.
type OneToMany[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	// the unique ID for this broadcast operation
	queryID coordt.QueryID

	// a struct holding configuration options
	cfg *ConfigOneToMany[K]

	// the message generator that takes a target key and will return the message
	// that we will send to the closest nodes in the follow-up phase
	msgFunc func(K) M

	// nodes we still need to store records with. This map will be filled with
	// all the closest nodes after the query has finished.
	todo map[string]N

	// nodes we have contacted to store the record but haven't heard a response yet
	waiting map[string]N

	// nodes that successfully hold the record for us
	success map[string]N

	// nodes that failed to hold the record for us
	failed map[string]struct {
		Node N
		Err  error
	}
}

// NewOneToMany initializes a new [OneToMany] struct.
func NewOneToMany[K kad.Key[K], N kad.NodeID[K], M coordt.Message](qid coordt.QueryID, msgFunc func(K) M, seed []N, cfg *ConfigOneToMany[K]) *OneToMany[K, N, M] {
	otm := &OneToMany[K, N, M]{
		queryID: qid,
		cfg:     cfg,
		msgFunc: msgFunc,
		todo:    map[string]N{},
		waiting: map[string]N{},
		success: map[string]N{},
		failed: map[string]struct {
			Node N
			Err  error
		}{},
	}

	for _, s := range seed {
		otm.todo[s.String()] = s
	}

	return otm
}

// Advance advances the state of the [OneToMany] [Broadcast] state machine.
func (otm *OneToMany[K, N, M]) Advance(ctx context.Context, ev BroadcastEvent) (out BroadcastState) {
	_, span := tele.StartSpan(ctx, "OneToMany.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(
			tele.AttrOutEvent(out),
			attribute.Int("todo", len(otm.todo)),
			attribute.Int("waiting", len(otm.waiting)),
			attribute.Int("success", len(otm.success)),
			attribute.Int("failed", len(otm.failed)),
		)
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventBroadcastStop:
		for _, n := range otm.todo {
			delete(otm.todo, n.String())
			otm.failed[n.String()] = struct {
				Node N
				Err  error
			}{Node: n, Err: fmt.Errorf("cancelled")}
		}

		for _, n := range otm.waiting {
			delete(otm.waiting, n.String())
			otm.failed[n.String()] = struct {
				Node N
				Err  error
			}{Node: n, Err: fmt.Errorf("cancelled")}
		}
	case *EventBroadcastStoreRecordSuccess[K, N, M]:
		delete(otm.waiting, ev.NodeID.String())
		otm.success[ev.NodeID.String()] = ev.NodeID
	case *EventBroadcastStoreRecordFailure[K, N, M]:
		delete(otm.waiting, ev.NodeID.String())
		otm.failed[ev.NodeID.String()] = struct {
			Node N
			Err  error
		}{Node: ev.NodeID, Err: ev.Error}
	case *EventBroadcastPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	for k, n := range otm.todo {
		delete(otm.todo, k)
		otm.waiting[k] = n
		return &StateBroadcastStoreRecord[K, N, M]{
			QueryID: otm.queryID,
			NodeID:  n,
			Target:  otm.cfg.Target,
			Message: otm.msgFunc(otm.cfg.Target),
		}
	}

	if len(otm.waiting) > 0 {
		return &StateBroadcastWaiting{}
	}

	if len(otm.todo) == 0 {
		contacted := make([]N, 0, len(otm.success)+len(otm.failed))
		for _, n := range otm.success {
			contacted = append(contacted, n)
		}
		for _, n := range otm.failed {
			contacted = append(contacted, n.Node)
		}

		return &StateBroadcastFinished[K, N]{
			QueryID:   otm.queryID,
			Contacted: contacted,
			Errors:    otm.failed,
		}
	}

	return &StateBroadcastIdle{}
}
