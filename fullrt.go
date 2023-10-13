package zikade

import (
	"context"
	"fmt"
	"time"

	"github.com/multiformats/go-multiaddr"

	"github.com/plprobelab/zikade/internal/coord"
	"github.com/plprobelab/zikade/internal/coord/query"

	"github.com/ipfs/go-cid"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"

	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/kadt"
	"github.com/plprobelab/zikade/pb"
)

type FullRT struct {
	*DHT

	cfg *FullRTConfig
}

type FullRTConfig struct {
	*Config
	CrawlInterval time.Duration
	QuorumFrac    float64
}

func DefaultFullRTConfig() *FullRTConfig {
	return &FullRTConfig{
		Config:        DefaultConfig(),
		CrawlInterval: time.Hour, // MAGIC
		QuorumFrac:    0.25,      // MAGIC
	}
}

func NewFullRT(h host.Host, cfg *FullRTConfig) (*FullRT, error) {
	d, err := New(h, cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("new DHT: %w", err)
	}

	frt := &FullRT{
		DHT: d,
		cfg: cfg,
	}

	return frt, nil
}

var _ routing.Routing = (*FullRT)(nil)

func (f *FullRT) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	ctx, span := f.tele.Tracer.Start(ctx, "DHT.FindPeer")
	defer span.End()

	// First check locally. If we are or were recently connected to the peer,
	// return the addresses from our peerstore unless the information doesn't
	// contain any.
	switch f.host.Network().Connectedness(pid) {
	case network.Connected, network.CanConnect:
		addrInfo := f.host.Peerstore().PeerInfo(pid)
		if addrInfo.ID != "" && len(addrInfo.Addrs) > 0 {
			return addrInfo, nil
		}
	default:
		// we're not connected or were recently connected
	}

	maddrsMap := make(map[multiaddr.Multiaddr]struct{})
	quorum := int(float64(20) * f.cfg.QuorumFrac) // TODO: does not need to be 20 (can be less if routing table isn't full) - generally parameterize
	fn := func(ctx context.Context, visited kadt.PeerID, msg *pb.Message, stats coordt.QueryStats) error {
		for _, addrInfo := range msg.CloserPeersAddrInfos() {
			if addrInfo.ID != pid {
				continue
			}

			for _, maddr := range addrInfo.Addrs {
				maddrsMap[maddr] = struct{}{}
			}
		}

		quorum -= 1
		if quorum == 0 {
			return coordt.ErrSkipRemaining
		}

		return nil
	}

	_, _, err := f.kad.QueryClosest(ctx, kadt.PeerID(pid).Key(), fn, f.queryConfig())
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to run query: %w", err)
	}

	if len(maddrsMap) == 0 {
		return peer.AddrInfo{}, routing.ErrNotFound
	}

	maddrs := make([]multiaddr.Multiaddr, 0, len(maddrsMap))
	for maddr := range maddrsMap {
		maddrs = append(maddrs, maddr)
	}

	connCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // TODO: put timeout in config
	defer cancel()
	_ = f.host.Connect(connCtx, peer.AddrInfo{
		ID:    pid,
		Addrs: maddrs,
	})

	switch f.host.Network().Connectedness(pid) {
	case network.Connected, network.CanConnect:
		addrInfo := f.host.Peerstore().PeerInfo(pid)
		if addrInfo.ID != "" && len(addrInfo.Addrs) > 0 {
			return addrInfo, nil
		}
	default:
		// we're not connected or were recently connected
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

func (f *FullRT) Provide(ctx context.Context, c cid.Cid, brdcst bool) error {
	ctx, span := f.tele.Tracer.Start(ctx, "DHT.Provide", otel.WithAttributes(attribute.String("cid", c.String())))
	defer span.End()

	// verify if this DHT supports provider records by checking if a "providers"
	// backend is registered.
	b, found := f.backends[namespaceProviders]
	if !found {
		return routing.ErrNotSupported
	}

	// verify that it's "defined" CID (not empty)
	if !c.Defined() {
		return fmt.Errorf("invalid cid: undefined")
	}

	// store ourselves as one provider for that CID
	_, err := b.Store(ctx, string(c.Hash()), peer.AddrInfo{ID: f.host.ID()})
	if err != nil {
		return fmt.Errorf("storing own provider record: %w", err)
	}

	// if broadcast is "false" we won't query the DHT
	if !brdcst {
		return nil
	}

	// construct message
	addrInfo := peer.AddrInfo{
		ID:    f.host.ID(),
		Addrs: f.host.Addrs(),
	}

	msg := &pb.Message{
		Type: pb.Message_ADD_PROVIDER,
		Key:  c.Hash(),
		ProviderPeers: []*pb.Message_Peer{
			pb.FromAddrInfo(addrInfo),
		},
	}

	seed, err := f.kad.GetClosestNodes(ctx, msg.Target(), f.cfg.BucketSize)
	if err != nil {
		return fmt.Errorf("get closest nodes from routing table: %w", err)
	}

	// finally, find the closest peers to the target key.
	return f.kad.BroadcastStatic(ctx, msg, seed)
}

// PutValue satisfies the [routing.Routing] interface and will add the given
// value to the k-closest nodes to keyStr. The parameter keyStr should have the
// format `/$namespace/$binary_id`. Namespace examples are `pk` or `ipns`. To
// identify the closest peers to keyStr, that complete string will be SHA256
// hashed.
func (f *FullRT) PutValue(ctx context.Context, keyStr string, value []byte, opts ...routing.Option) error {
	ctx, span := f.tele.Tracer.Start(ctx, "DHT.PutValue")
	defer span.End()

	// first parse the routing options
	rOpt := routing.Options{} // routing config
	if err := rOpt.Apply(opts...); err != nil {
		return fmt.Errorf("apply routing options: %w", err)
	}

	// then always store the given value locally
	if err := f.putValueLocal(ctx, keyStr, value); err != nil {
		return fmt.Errorf("put value locally: %w", err)
	}

	// if the routing system should operate in offline mode, stop here
	if rOpt.Offline {
		return nil
	}

	// construct Kademlia-key. Yes, we hash the complete key string which
	// includes the namespace prefix.
	msg := &pb.Message{
		Type:   pb.Message_PUT_VALUE,
		Key:    []byte(keyStr),
		Record: record.MakePutRecord(keyStr, value),
	}

	// finally, find the closest peers to the target key.
	err := f.kad.BroadcastRecord(ctx, msg)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	return nil
}

func (f *FullRT) Bootstrap(ctx context.Context) error {
	ctx, span := f.tele.Tracer.Start(ctx, "DHT.Bootstrap")
	defer span.End()

	f.log.Info("Starting crawl bootstrap")
	seed := make([]kadt.PeerID, len(f.cfg.BootstrapPeers))
	for i, addrInfo := range f.cfg.BootstrapPeers {
		seed[i] = kadt.PeerID(addrInfo.ID)
		// TODO: how to handle TTL if BootstrapPeers become dynamic and don't
		// point to stable peers or consist of ephemeral peers that we have
		// observed during a previous run.
		f.host.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, peerstore.PermanentAddrTTL)
	}

	return f.kad.Crawl(ctx, seed)
}

func (f *FullRT) queryConfig() *coord.QueryConfig {
	cfg := coord.DefaultQueryConfig()
	cfg.NumResults = f.cfg.BucketSize
	cfg.Strategy = &query.QueryStrategyStatic{}
	return cfg
}
