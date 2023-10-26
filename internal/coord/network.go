package coord

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/plprobelab/zikade/errs"
	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/kadt"
	"github.com/plprobelab/zikade/pb"
	"github.com/plprobelab/zikade/tele"
)

type NetworkConfig struct {
	// Clock is a clock that may replaced by a mock when testing
	Clock clock.Clock

	// Logger is a structured logger that will be used when logging.
	Logger *slog.Logger

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *NetworkConfig) Validate() error {
	if cfg.Clock == nil {
		return &errs.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Logger == nil {
		return &errs.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.Tracer == nil {
		return &errs.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &errs.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	return nil
}

func DefaultNetworkConfig() *NetworkConfig {
	return &NetworkConfig{
		Clock:  clock.New(),
		Logger: tele.DefaultLogger("coord"),
		Tracer: tele.NoopTracer(),
		Meter:  tele.NoopMeter(),
	}
}

type NetworkBehaviour struct {
	// cfg is a copy of the optional configuration supplied to the behaviour
	cfg NetworkConfig

	// performMu is held while Perform is executing to ensure sequential execution of work.
	performMu sync.Mutex

	// rtr is the message router used to send messages
	// it must only be accessed while performMu is held
	rtr coordt.Router[kadt.Key, kadt.PeerID, *pb.Message]

	// pendingInboundMu guards access to pendingInbound
	pendingInboundMu sync.Mutex

	// pendingInbound is a queue of inbound events that are awaiting processing
	pendingInbound []CtxEvent[BehaviourEvent]

	// nodeHandlers is a map of NodeHandler by peer id
	// it must only be accessed while performMu is held
	nodeHandlers map[kadt.PeerID]*nodeHandlerEntry

	// lastActive is a list of node handlers ordered by the time the node handler was last active.
	// it must only be accessed while performMu is held
	lastActive *lastActiveNodeHandlerList

	// gaugeNodeHandlerCount is a gauge that tracks the number of node handlers that are currently active.
	gaugeNodeHandlerCount metric.Int64UpDownCounter

	ready chan struct{}
}

func NewNetworkBehaviour(rtr coordt.Router[kadt.Key, kadt.PeerID, *pb.Message], cfg *NetworkConfig) (*NetworkBehaviour, error) {
	if cfg == nil {
		cfg = DefaultNetworkConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b := &NetworkBehaviour{
		cfg:          *cfg,
		rtr:          rtr,
		nodeHandlers: make(map[kadt.PeerID]*nodeHandlerEntry),
		lastActive:   new(lastActiveNodeHandlerList),
		ready:        make(chan struct{}, 1),
	}

	// initialise metrics
	var err error
	b.gaugeNodeHandlerCount, err = cfg.Meter.Int64UpDownCounter(
		"node_handler_count",
		metric.WithDescription("Total number of node handlers currently active"),
	)
	if err != nil {
		return nil, fmt.Errorf("create node_handler_count counter: %w", err)
	}

	return b, nil
}

func (b *NetworkBehaviour) Notify(ctx context.Context, ev BehaviourEvent) {
	b.pendingInboundMu.Lock()
	defer b.pendingInboundMu.Unlock()

	ctx, span := b.cfg.Tracer.Start(ctx, "NetworkBehaviour.Notify")
	defer span.End()

	b.pendingInbound = append(b.pendingInbound, CtxEvent[BehaviourEvent]{Ctx: ctx, Event: ev})

	select {
	case b.ready <- struct{}{}:
	default:
	}
}

func (b *NetworkBehaviour) Ready() <-chan struct{} {
	return b.ready
}

func (b *NetworkBehaviour) Perform(ctx context.Context) (BehaviourEvent, bool) {
	b.performMu.Lock()
	defer b.performMu.Unlock()

	_, span := b.cfg.Tracer.Start(ctx, "NetworkBehaviour.Perform")
	defer span.End()

	defer b.updateReadyStatus()

	// perform one piece of pending inbound work.
	ev, ok := b.perfomNextInbound(ctx)
	if ok {
		return ev, true
	}

	// perform some garbage collection on idle node handlers
	b.garbageCollect(ctx)

	return nil, false
}

func (b *NetworkBehaviour) nextPendingInbound() (CtxEvent[BehaviourEvent], bool) {
	b.pendingInboundMu.Lock()
	defer b.pendingInboundMu.Unlock()
	if len(b.pendingInbound) == 0 {
		return CtxEvent[BehaviourEvent]{}, false
	}
	var pev CtxEvent[BehaviourEvent]
	pev, b.pendingInbound = b.pendingInbound[0], b.pendingInbound[1:]
	return pev, true
}

func (b *NetworkBehaviour) updateReadyStatus() {
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

func (b *NetworkBehaviour) perfomNextInbound(ctx context.Context) (BehaviourEvent, bool) {
	ctx, span := b.cfg.Tracer.Start(ctx, "NetworkBehaviour.perfomNextInbound")
	defer span.End()
	pev, ok := b.nextPendingInbound()
	if !ok {
		return nil, false
	}

	switch ev := pev.Event.(type) {
	case *EventOutboundGetCloserNodes:
		b.notifyNodeHandler(ctx, ev.To, ev)
	case *EventOutboundSendMessage:
		b.notifyNodeHandler(ctx, ev.To, ev)
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return nil, false
}

func (b *NetworkBehaviour) notifyNodeHandler(ctx context.Context, id kadt.PeerID, ev NodeHandlerRequest) {
	ctx, span := b.cfg.Tracer.Start(ctx, "NetworkBehaviour.notifyNodeHandler")
	defer span.End()
	nhe, ok := b.nodeHandlers[id]
	if !ok {
		nhe = &nodeHandlerEntry{
			nh:    NewNodeHandler(id, b.rtr, b.cfg.Logger, b.cfg.Tracer),
			index: -1,
		}
		b.nodeHandlers[id] = nhe
		b.gaugeNodeHandlerCount.Add(ctx, 1)
	}
	if nhe.index == -1 {
		heap.Push(b.lastActive, nhe)
	}
	nhe.lastActive = b.cfg.Clock.Now()
	heap.Fix(b.lastActive, nhe.index)
	nhe.nh.Notify(ctx, ev)
}

func (b *NetworkBehaviour) garbageCollect(ctx context.Context) {
	ctx, span := b.cfg.Tracer.Start(ctx, "NetworkBehaviour.garbageCollect")
	defer span.End()

	if len(*b.lastActive) == 0 {
		return
	}

	// attempt to garbage collect the node that that has been inactive the longest

	// peek at the node handler with the oldest last notified time
	nhe := (*b.lastActive)[0]

	// is the node handler still active?
	if nhe.nh.IsActive() {
		nhe.lastActive = b.cfg.Clock.Now()
		heap.Fix(b.lastActive, nhe.index)
		return
	}

	b.cfg.Logger.Debug("garbage collecting node handler", "peer_id", nhe.nh.self, "last_active", nhe.lastActive)
	nhe.nh.Close()
	delete(b.nodeHandlers, nhe.nh.self)
	b.gaugeNodeHandlerCount.Add(ctx, -1)
	if nhe.index >= 0 {
		heap.Remove(b.lastActive, nhe.index)
	}
}

type NodeHandler struct {
	self    kadt.PeerID
	rtr     coordt.Router[kadt.Key, kadt.PeerID, *pb.Message]
	queue   *WorkQueue[NodeHandlerRequest]
	logger  *slog.Logger
	tracer  trace.Tracer
	sending atomic.Bool
}

func NewNodeHandler(self kadt.PeerID, rtr coordt.Router[kadt.Key, kadt.PeerID, *pb.Message], logger *slog.Logger, tracer trace.Tracer) *NodeHandler {
	h := &NodeHandler{
		self:   self,
		rtr:    rtr,
		logger: logger,
		tracer: tracer,
	}

	h.queue = NewWorkQueue(h.send)

	return h
}

func (h *NodeHandler) Notify(ctx context.Context, ev NodeHandlerRequest) {
	ctx, span := h.tracer.Start(ctx, "NodeHandler.Notify")
	defer span.End()
	h.queue.Enqueue(ctx, ev)
}

func (h *NodeHandler) IsActive() bool {
	if len(h.queue.pending) > 0 {
		return true
	}
	return h.sending.Load()
}

func (h *NodeHandler) Close() {
	h.queue.Close()
}

func (h *NodeHandler) send(ctx context.Context, ev NodeHandlerRequest) bool {
	h.sending.Store(true)
	defer h.sending.Store(false)

	switch cmd := ev.(type) {
	case *EventOutboundGetCloserNodes:
		if cmd.Notify == nil {
			break
		}
		nodes, err := h.rtr.GetClosestNodes(ctx, h.self, cmd.Target)
		if err != nil {
			cmd.Notify.Notify(ctx, &EventGetCloserNodesFailure{
				QueryID: cmd.QueryID,
				To:      h.self,
				Target:  cmd.Target,
				Err:     fmt.Errorf("NodeHandler: %w", err),
			})
			return false
		}

		cmd.Notify.Notify(ctx, &EventGetCloserNodesSuccess{
			QueryID:     cmd.QueryID,
			To:          h.self,
			Target:      cmd.Target,
			CloserNodes: nodes,
		})
	case *EventOutboundSendMessage:
		if cmd.Notify == nil {
			break
		}
		resp, err := h.rtr.SendMessage(ctx, h.self, cmd.Message)
		if err != nil {
			cmd.Notify.Notify(ctx, &EventSendMessageFailure{
				QueryID: cmd.QueryID,
				To:      h.self,
				Request: cmd.Message,
				Err:     fmt.Errorf("NodeHandler: %w", err),
			})
			return false
		}

		cmd.Notify.Notify(ctx, &EventSendMessageSuccess{
			QueryID:     cmd.QueryID,
			To:          h.self,
			Request:     cmd.Message,
			Response:    resp,
			CloserNodes: resp.CloserNodes(),
		})
	default:
		panic(fmt.Sprintf("unexpected command type: %T", cmd))
	}

	return false
}

func (h *NodeHandler) ID() kadt.PeerID {
	return h.self
}

type nodeHandlerEntry struct {
	nh         *NodeHandler
	lastActive time.Time
	index      int
}

// lastActiveNodeHandlerList is a min-heap of NodeHandlers ordered by time the node handler was last notified.
// The root node is the NodeHandler that has the earliest last notified time and has thus been
// inactive the longest.
type lastActiveNodeHandlerList []*nodeHandlerEntry

func (o lastActiveNodeHandlerList) Len() int { return len(o) }
func (o lastActiveNodeHandlerList) Less(i, j int) bool {
	return o[i].lastActive.Before(o[j].lastActive)
}

func (o lastActiveNodeHandlerList) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
	o[i].index = i
	o[j].index = j
}

func (o *lastActiveNodeHandlerList) Push(x any) {
	n := len(*o)
	v := x.(*nodeHandlerEntry)
	v.index = n
	*o = append(*o, v)
}

func (o *lastActiveNodeHandlerList) Pop() any {
	if len(*o) == 0 {
		return nil
	}
	old := *o
	n := len(old)
	v := old[n-1]
	old[n-1] = nil
	v.index = -1
	*o = old[0 : n-1]
	return v
}
