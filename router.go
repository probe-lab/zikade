package zikade

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/libp2p/go-msgio/pbio"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/probe-lab/zikade/internal/coord/coordt"
	"github.com/probe-lab/zikade/kadt"
	"github.com/probe-lab/zikade/pb"
	"github.com/probe-lab/zikade/tele"
)

type router struct {
	// the libp2p host to use for sending messages
	host host.Host

	// protocolID represents the DHT [protocol] we can query with and respond to.
	//
	// [protocol]: https://docs.libp2p.io/concepts/fundamentals/protocols/
	protocolID protocol.ID

	// tele holds a reference to a telemetry struct
	tele *Telemetry

	clk clock.Clock
}

var _ coordt.Router[kadt.Key, kadt.PeerID, *pb.Message] = (*router)(nil)

func (r *router) SendMessage(ctx context.Context, to kadt.PeerID, req *pb.Message) (resp *pb.Message, err error) {
	spanOpts := []trace.SpanStartOption{
		trace.WithAttributes(tele.AttrMessageType(req.GetType().String())),
		trace.WithAttributes(tele.AttrPeerID(to.String())),
		trace.WithAttributes(tele.AttrKey(base64.RawStdEncoding.EncodeToString(req.GetKey()))),
	}
	ctx, span := r.tele.Tracer.Start(ctx, "router.SendMessage", spanOpts...)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if len(r.host.Peerstore().Addrs(peer.ID(to))) == 0 {
		return nil, fmt.Errorf("no address for peer %s", to)
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var s network.Stream
	s, err = r.host.NewStream(ctx, peer.ID(to), r.protocolID)
	if err != nil {
		return nil, fmt.Errorf("stream creation: %w", err)
	}
	defer s.Close()

	w := pbio.NewDelimitedWriter(s)
	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	if !req.ExpectResponse() {
		err = w.WriteMsg(req)
		r.tele.SentMessages.Add(ctx, 1)
		if err != nil {
			r.tele.SentMessageErrors.Add(ctx, 1)
			return nil, fmt.Errorf("write message: %w", err)
		}
		r.tele.SentBytes.Record(ctx, int64(req.Size()))
		return nil, nil
	}

	start := r.clk.Now()

	err = w.WriteMsg(req)
	r.tele.SentRequests.Add(ctx, 1)
	if err != nil {
		r.tele.SentRequestErrors.Add(ctx, 1)
		return nil, fmt.Errorf("write message: %w", err)
	}
	r.tele.SentBytes.Record(ctx, int64(req.Size()))

	span.End()
	ctx, span = r.tele.Tracer.Start(ctx, "router.ReadMessage", spanOpts...)

	data, err := reader.ReadMsg()
	if err != nil {
		r.tele.SentRequestErrors.Add(ctx, 1)
		return nil, fmt.Errorf("read message: %w", err)
	}

	protoResp := pb.Message{}
	if err = proto.Unmarshal(data, &protoResp); err != nil {
		r.tele.SentRequestErrors.Add(ctx, 1)
		return nil, err
	}
	r.tele.OutboundRequestLatency.Record(ctx, float64(r.clk.Since(start))/float64(time.Millisecond))

	for _, info := range protoResp.CloserPeersAddrInfos() {
		_ = r.addToPeerStore(ctx, info, time.Hour) // TODO: replace hard coded time.Hour with config
	}

	return &protoResp, err
}

func (r *router) GetClosestNodes(ctx context.Context, to kadt.PeerID, target kadt.Key) ([]kadt.PeerID, error) {
	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  target.MsgKey(),
	}

	resp, err := r.SendMessage(ctx, to, req)
	if err != nil {
		return nil, err
	}

	return resp.CloserNodes(), nil
}

func (r *router) addToPeerStore(ctx context.Context, ai peer.AddrInfo, ttl time.Duration) error {
	// Don't add addresses for self or our connected peers. We have better ones.
	if ai.ID == r.host.ID() || r.host.Network().Connectedness(ai.ID) == network.Connected {
		return nil
	}

	r.host.Peerstore().AddAddrs(ai.ID, ai.Addrs, ttl)
	return nil
}
