package coord

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/probe-lab/zikade/internal/coord/coordt"
	"github.com/probe-lab/zikade/kadt"
	"github.com/probe-lab/zikade/pb"
)

type NetworkBehaviour struct {
	// rtr is the message router used to send messages
	rtr coordt.Router[kadt.Key, kadt.PeerID, *pb.Message]

	nodeHandlersMu sync.Mutex
	nodeHandlers   map[kadt.PeerID]*NodeHandler // TODO: garbage collect node handlers

	pendingMu sync.Mutex
	pending   []BehaviourEvent
	ready     chan struct{}

	logger *slog.Logger
	tracer trace.Tracer
}

func NewNetworkBehaviour(rtr coordt.Router[kadt.Key, kadt.PeerID, *pb.Message], logger *slog.Logger, tracer trace.Tracer) *NetworkBehaviour {
	b := &NetworkBehaviour{
		rtr:          rtr,
		nodeHandlers: make(map[kadt.PeerID]*NodeHandler),
		ready:        make(chan struct{}, 1),
		logger:       logger.With("behaviour", "network"),
		tracer:       tracer,
	}

	return b
}

func (b *NetworkBehaviour) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := b.tracer.Start(ctx, "NetworkBehaviour.Notify")
	defer span.End()

	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	switch ev := ev.(type) {
	case *EventOutboundGetCloserNodes:
		b.nodeHandlersMu.Lock()
		nh, ok := b.nodeHandlers[ev.To]
		if !ok {
			nh = NewNodeHandler(ev.To, b.rtr, b.logger, b.tracer)
			b.nodeHandlers[ev.To] = nh
		}
		b.nodeHandlersMu.Unlock()
		nh.Notify(ctx, ev)
	case *EventOutboundSendMessage:
		b.nodeHandlersMu.Lock()
		nh, ok := b.nodeHandlers[ev.To]
		if !ok {
			nh = NewNodeHandler(ev.To, b.rtr, b.logger, b.tracer)
			b.nodeHandlers[ev.To] = nh
		}
		b.nodeHandlersMu.Unlock()
		nh.Notify(ctx, ev)
	default:
		panic(fmt.Sprintf("unexpected dht event: %T", ev))
	}

	if len(b.pending) > 0 {
		select {
		case b.ready <- struct{}{}:
		default:
		}
	}
}

func (b *NetworkBehaviour) Ready() <-chan struct{} {
	return b.ready
}

func (b *NetworkBehaviour) Perform(ctx context.Context) (BehaviourEvent, bool) {
	_, span := b.tracer.Start(ctx, "NetworkBehaviour.Perform")
	defer span.End()
	// No inbound work can be done until Perform is complete
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	// drain queued events.
	if len(b.pending) > 0 {
		var ev BehaviourEvent
		ev, b.pending = b.pending[0], b.pending[1:]

		if len(b.pending) > 0 {
			select {
			case b.ready <- struct{}{}:
			default:
			}
		}
		return ev, true
	}

	return nil, false
}

type NodeHandler struct {
	self   kadt.PeerID
	rtr    coordt.Router[kadt.Key, kadt.PeerID, *pb.Message]
	queue  *WorkQueue[NodeHandlerRequest]
	logger *slog.Logger
	tracer trace.Tracer
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

func (h *NodeHandler) send(ctx context.Context, ev NodeHandlerRequest) bool {
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
