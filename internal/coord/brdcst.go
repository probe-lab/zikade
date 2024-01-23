package coord

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/probe-lab/zikade/internal/coord/brdcst"
	"github.com/probe-lab/zikade/internal/coord/coordt"
	"github.com/probe-lab/zikade/kadt"
	"github.com/probe-lab/zikade/pb"
	"github.com/probe-lab/zikade/tele"
)

type PooledBroadcastBehaviour struct {
	logger *slog.Logger
	tracer trace.Tracer

	// performMu is held while Perform is executing to ensure sequential execution of work.
	performMu sync.Mutex

	// pool is the broadcast pool state machine used for managing individual broadcasts.
	// it must only be accessed while performMu is held
	pool coordt.StateMachine[brdcst.PoolEvent, brdcst.PoolState]

	// pendingOutbound is a queue of outbound events.
	// it must only be accessed while performMu is held
	pendingOutbound []BehaviourEvent

	// notifiers is a map that keeps track of event notifications for each running broadcast.
	// it must only be accessed while performMu is held
	notifiers map[coordt.QueryID]*queryNotifier[*EventBroadcastFinished]

	// pendingInboundMu guards access to pendingInbound
	pendingInboundMu sync.Mutex

	// pendingInbound is a queue of inbound events that are awaiting processing
	pendingInbound []CtxEvent[BehaviourEvent]

	ready chan struct{}
}

var _ Behaviour[BehaviourEvent, BehaviourEvent] = (*PooledBroadcastBehaviour)(nil)

func NewPooledBroadcastBehaviour(brdcstPool *brdcst.Pool[kadt.Key, kadt.PeerID, *pb.Message], logger *slog.Logger, tracer trace.Tracer) *PooledBroadcastBehaviour {
	b := &PooledBroadcastBehaviour{
		pool:      brdcstPool,
		notifiers: make(map[coordt.QueryID]*queryNotifier[*EventBroadcastFinished]),
		ready:     make(chan struct{}, 1),
		logger:    logger.With("behaviour", "pooledBroadcast"),
		tracer:    tracer,
	}
	return b
}

func (b *PooledBroadcastBehaviour) Ready() <-chan struct{} {
	return b.ready
}

func (b *PooledBroadcastBehaviour) Notify(ctx context.Context, ev BehaviourEvent) {
	b.pendingInboundMu.Lock()
	defer b.pendingInboundMu.Unlock()

	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.Notify")
	defer span.End()

	b.pendingInbound = append(b.pendingInbound, CtxEvent[BehaviourEvent]{Ctx: ctx, Event: ev})

	select {
	case b.ready <- struct{}{}:
	default:
	}
}

func (b *PooledBroadcastBehaviour) Perform(ctx context.Context) (BehaviourEvent, bool) {
	b.performMu.Lock()
	defer b.performMu.Unlock()

	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.Perform")
	defer span.End()

	defer b.updateReadyStatus()

	// first send any pending query notifications
	for _, w := range b.notifiers {
		w.DrainPending()
	}

	// drain queued outbound events before starting new work.
	ev, ok := b.nextPendingOutbound()
	if ok {
		return ev, true
	}

	// perform one piece of pending inbound work.
	ev, ok = b.perfomNextInbound(ctx)
	if ok {
		return ev, true
	}

	// poll the broadcast pool to trigger any timeouts and other scheduled work
	ev, ok = b.advancePool(ctx, &brdcst.EventPoolPoll{})
	if ok {
		return ev, true
	}

	// return any queued outbound work that may have been generated
	return b.nextPendingOutbound()
}

func (b *PooledBroadcastBehaviour) nextPendingOutbound() (BehaviourEvent, bool) {
	if len(b.pendingOutbound) == 0 {
		return nil, false
	}
	var ev BehaviourEvent
	ev, b.pendingOutbound = b.pendingOutbound[0], b.pendingOutbound[1:]
	return ev, true
}

func (b *PooledBroadcastBehaviour) nextPendingInbound() (CtxEvent[BehaviourEvent], bool) {
	b.pendingInboundMu.Lock()
	defer b.pendingInboundMu.Unlock()
	if len(b.pendingInbound) == 0 {
		return CtxEvent[BehaviourEvent]{}, false
	}
	var pev CtxEvent[BehaviourEvent]
	pev, b.pendingInbound = b.pendingInbound[0], b.pendingInbound[1:]
	return pev, true
}

func (b *PooledBroadcastBehaviour) updateReadyStatus() {
	if len(b.pendingOutbound) != 0 {
		select {
		case b.ready <- struct{}{}:
		default:
		}
		return
	}

	b.pendingInboundMu.Lock()
	hasPendingInbound := len(b.pendingInbound) != 0
	b.pendingInboundMu.Unlock()

	if hasPendingInbound {
		select {
		case b.ready <- struct{}{}:
		default:
		}
		return
	}
}

func (b *PooledBroadcastBehaviour) perfomNextInbound(ctx context.Context) (BehaviourEvent, bool) {
	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.perfomNextInbound")
	defer span.End()
	pev, ok := b.nextPendingInbound()
	if !ok {
		return nil, false
	}

	var cmd brdcst.PoolEvent
	switch ev := pev.Event.(type) {
	case *EventStartBroadcast:
		cmd = &brdcst.EventPoolStartBroadcast[kadt.Key, kadt.PeerID, *pb.Message]{
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Message: ev.Message,
			Seed:    ev.Seed,
			Config:  ev.Config,
		}
		if ev.Notify != nil {
			b.notifiers[ev.QueryID] = &queryNotifier[*EventBroadcastFinished]{monitor: ev.Notify}
		}

	case *EventGetCloserNodesSuccess:
		for _, info := range ev.CloserNodes {
			b.pendingOutbound = append(b.pendingOutbound, &EventAddNode{
				NodeID: info,
			})
		}

		waiter, ok := b.notifiers[ev.QueryID]
		if ok {
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed{
				NodeID:  ev.To,
				QueryID: ev.QueryID,
			})
		}

		cmd = &brdcst.EventPoolGetCloserNodesSuccess[kadt.Key, kadt.PeerID]{
			NodeID:      ev.To,
			QueryID:     ev.QueryID,
			Target:      ev.Target,
			CloserNodes: ev.CloserNodes,
		}

	case *EventGetCloserNodesFailure:
		// queue an event that will notify the routing behaviour of a failed node
		b.pendingOutbound = append(b.pendingOutbound, &EventNotifyNonConnectivity{
			ev.To,
		})

		cmd = &brdcst.EventPoolGetCloserNodesFailure[kadt.Key, kadt.PeerID]{
			NodeID:  ev.To,
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Error:   ev.Err,
		}

	case *EventSendMessageSuccess:
		for _, info := range ev.CloserNodes {
			b.pendingOutbound = append(b.pendingOutbound, &EventAddNode{
				NodeID: info,
			})
		}
		waiter, ok := b.notifiers[ev.QueryID]
		if ok {
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed{
				NodeID:   ev.To,
				QueryID:  ev.QueryID,
				Response: ev.Response,
			})
		}
		// TODO: How do we know it's a StoreRecord response?
		cmd = &brdcst.EventPoolStoreRecordSuccess[kadt.Key, kadt.PeerID, *pb.Message]{
			QueryID:  ev.QueryID,
			NodeID:   ev.To,
			Request:  ev.Request,
			Response: ev.Response,
		}

	case *EventSendMessageFailure:
		// queue an event that will notify the routing behaviour of a failed node
		b.pendingOutbound = append(b.pendingOutbound, &EventNotifyNonConnectivity{
			ev.To,
		})

		// TODO: How do we know it's a StoreRecord response?
		cmd = &brdcst.EventPoolStoreRecordFailure[kadt.Key, kadt.PeerID, *pb.Message]{
			NodeID:  ev.To,
			QueryID: ev.QueryID,
			Request: ev.Request,
			Error:   ev.Err,
		}

	case *EventStopQuery:
		cmd = &brdcst.EventPoolStopBroadcast{
			QueryID: ev.QueryID,
		}
	}

	// attempt to advance the broadcast pool
	return b.advancePool(ctx, cmd)
}

func (b *PooledBroadcastBehaviour) advancePool(ctx context.Context, ev brdcst.PoolEvent) (out BehaviourEvent, term bool) {
	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.advancePool", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	pstate := b.pool.Advance(ctx, ev)
	switch st := pstate.(type) {
	case *brdcst.StatePoolIdle:
		// nothing to do
	case *brdcst.StatePoolFindCloser[kadt.Key, kadt.PeerID]:
		return &EventOutboundGetCloserNodes{
			QueryID: st.QueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  b,
		}, true
	case *brdcst.StatePoolStoreRecord[kadt.Key, kadt.PeerID, *pb.Message]:
		return &EventOutboundSendMessage{
			QueryID: st.QueryID,
			To:      st.NodeID,
			Message: st.Message,
			Notify:  b,
		}, true
	case *brdcst.StatePoolBroadcastFinished[kadt.Key, kadt.PeerID]:
		waiter, ok := b.notifiers[st.QueryID]
		if ok {
			waiter.NotifyFinished(ctx, &EventBroadcastFinished{
				QueryID:   st.QueryID,
				Contacted: st.Contacted,
				Errors:    st.Errors,
			})
			delete(b.notifiers, st.QueryID)
		}
	}

	return nil, false
}

// A BroadcastWaiter implements [QueryMonitor] for broadcasts
type BroadcastWaiter struct {
	progressed chan CtxEvent[*EventQueryProgressed]
	finished   chan CtxEvent[*EventBroadcastFinished]
}

var _ QueryMonitor[*EventBroadcastFinished] = (*BroadcastWaiter)(nil)

func NewBroadcastWaiter(n int) *BroadcastWaiter {
	w := &BroadcastWaiter{
		progressed: make(chan CtxEvent[*EventQueryProgressed], n),
		finished:   make(chan CtxEvent[*EventBroadcastFinished], 1),
	}
	return w
}

func (w *BroadcastWaiter) Progressed() <-chan CtxEvent[*EventQueryProgressed] {
	return w.progressed
}

func (w *BroadcastWaiter) Finished() <-chan CtxEvent[*EventBroadcastFinished] {
	return w.finished
}

func (w *BroadcastWaiter) NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed] {
	return w.progressed
}

func (w *BroadcastWaiter) NotifyFinished() chan<- CtxEvent[*EventBroadcastFinished] {
	return w.finished
}
