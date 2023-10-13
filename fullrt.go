package zikade

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/plprobelab/zikade/internal/coord"
	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/internal/coord/query"
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

	if cfg.Query.DefaultQuorum == 0 {
		cfg.Query.DefaultQuorum = int(float64(cfg.BucketSize) * cfg.QuorumFrac)
	}

	frt := &FullRT{
		DHT: d,
		cfg: cfg,
	}

	return frt, nil
}

var _ routing.Routing = (*FullRT)(nil)

func (f *FullRT) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	ctx, span := f.tele.Tracer.Start(ctx, "FullRT.FindPeer")
	defer span.End()

	// First check locally. If we are or were recently connected to the peer,
	// return the addresses from our peerstore unless the information doesn't
	// contain any.
	addrInfo, err := f.getRecentAddrInfo(pid)
	if err == nil {
		return addrInfo, nil
	}

	maddrsMap := make(map[multiaddr.Multiaddr]struct{})
	quorum := f.cfg.Query.DefaultQuorum
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

	// start the query with a static set of peers (see queryConfig)
	_, _, err = f.kad.QueryClosest(ctx, kadt.PeerID(pid).Key(), fn, f.queryConfig())
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to run query: %w", err)
	}

	// if we haven't found any addresses, return a not found error
	if len(maddrsMap) == 0 {
		return peer.AddrInfo{}, routing.ErrNotFound
	}

	// transform map into slice
	maddrs := make([]multiaddr.Multiaddr, 0, len(maddrsMap))
	for maddr := range maddrsMap {
		maddrs = append(maddrs, maddr)
	}

	// connect to peer (this also happens in the non-fullrt case)
	connCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // TODO: put timeout in config
	defer cancel()
	_ = f.host.Connect(connCtx, peer.AddrInfo{
		ID:    pid,
		Addrs: maddrs,
	})

	// return addresses
	return f.getRecentAddrInfo(pid)
}

func (f *FullRT) Provide(ctx context.Context, c cid.Cid, brdcst bool) error {
	ctx, span := f.tele.Tracer.Start(ctx, "FullRT.Provide", otel.WithAttributes(attribute.String("cid", c.String())))
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

	// finally, store the record with the currently known closest peers
	return f.kad.BroadcastStatic(ctx, msg, seed)
}

func (f *FullRT) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	peerOut := make(chan peer.AddrInfo)
	go f.findProvidersAsyncRoutine(ctx, c, count, peerOut)
	return peerOut
}

func (f *FullRT) findProvidersAsyncRoutine(ctx context.Context, c cid.Cid, count int, out chan<- peer.AddrInfo) {
	_, span := f.tele.Tracer.Start(ctx, "DHT.findProvidersAsyncRoutine", otel.WithAttributes(attribute.String("cid", c.String()), attribute.Int("count", count)))
	defer span.End()

	defer close(out)

	// verify if this DHT supports provider records by checking
	// if a "providers" backend is registered.
	b, found := f.backends[namespaceProviders]
	if !found || !c.Defined() {
		span.RecordError(fmt.Errorf("no providers backend registered or CID undefined"))
		return
	}

	// send all providers onto the out channel until the desired count
	// was reached. If no count was specified, continue with network lookup.
	providers := map[peer.ID]struct{}{}

	// first fetch the record locally
	stored, err := b.Fetch(ctx, string(c.Hash()))
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			span.RecordError(err)
			f.log.Warn("Fetching value from provider store", slog.String("cid", c.String()), slog.String("err", err.Error()))
			return
		}

		stored = &providerSet{}
	}

	ps, ok := stored.(*providerSet)
	if !ok {
		span.RecordError(err)
		f.log.Warn("Stored value is not a provider set", slog.String("cid", c.String()), slog.String("type", fmt.Sprintf("%T", stored)))
		return
	}

	for _, provider := range ps.providers {
		providers[provider.ID] = struct{}{}

		select {
		case <-ctx.Done():
			return
		case out <- provider:
		}

		if count != 0 && len(providers) == count {
			return
		}
	}

	// Craft message to send to other peers
	msg := &pb.Message{
		Type: pb.Message_GET_PROVIDERS,
		Key:  c.Hash(),
	}

	// handle node response
	fn := func(ctx context.Context, id kadt.PeerID, resp *pb.Message, stats coordt.QueryStats) error {
		// loop through all providers that the remote peer returned
		for _, provider := range resp.ProviderAddrInfos() {

			// if we had already sent that peer on the channel -> do nothing
			if _, found := providers[provider.ID]; found {
				continue
			}

			// keep track that we will have sent this peer on the channel
			providers[provider.ID] = struct{}{}

			// actually send the provider information to the user
			select {
			case <-ctx.Done():
				return coordt.ErrSkipRemaining
			case out <- provider:
			}

			// if count is 0, we will wait until the query has exhausted the keyspace
			// if count isn't 0, we will stop if the number of providers we have sent
			// equals the number that the user has requested.
			if count != 0 && len(providers) == count {
				return coordt.ErrSkipRemaining
			}
		}

		return nil
	}

	_, _, err = f.kad.QueryMessage(ctx, msg, fn, f.queryConfig())
	if err != nil {
		span.RecordError(err)
		f.log.Warn("Failed querying", slog.String("cid", c.String()), slog.String("err", err.Error()))
		return
	}
}

// PutValue satisfies the [routing.Routing] interface and will add the given
// value to the k-closest nodes to keyStr. The parameter keyStr should have the
// format `/$namespace/$binary_id`. Namespace examples are `pk` or `ipns`. To
// identify the closest peers to keyStr, that complete string will be SHA256
// hashed.
func (f *FullRT) PutValue(ctx context.Context, keyStr string, value []byte, opts ...routing.Option) error {
	ctx, span := f.tele.Tracer.Start(ctx, "FullRT.PutValue")
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

	seed, err := f.kad.GetClosestNodes(ctx, msg.Target(), f.cfg.BucketSize)
	if err != nil {
		return fmt.Errorf("get closest nodes from routing table: %w", err)
	}

	// finally, store the record with the currently known closest peers
	return f.kad.BroadcastStatic(ctx, msg, seed)
}

func (f *FullRT) Bootstrap(ctx context.Context) error {
	ctx, span := f.tele.Tracer.Start(ctx, "FullRT.Bootstrap")
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

func (f *FullRT) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	ctx, span := f.tele.Tracer.Start(ctx, "FullRT.GetValue")
	defer span.End()

	// start searching for value
	valueChan, err := f.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	// valueChan will always emit "better" values than previous ones
	// therefore, store the latest best value until the channel was closed
	var best []byte
	for val := range valueChan {
		best = val
	}

	// if the channel was closed because the context was cancelled, return
	// the best known value and the context error.
	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	// if the query terminated without having found a value,
	// return a not found error
	if best == nil {
		return nil, routing.ErrNotFound
	}

	return best, nil
}

// SearchValue will search in the DHT for keyStr. keyStr must have the form
// `/$namespace/$binary_id`
func (f *FullRT) SearchValue(ctx context.Context, keyStr string, options ...routing.Option) (<-chan []byte, error) {
	_, span := f.tele.Tracer.Start(ctx, "FullRT.SearchValue")
	defer span.End()

	// first parse the routing options
	rOpt := &routing.Options{} // routing config
	if err := rOpt.Apply(options...); err != nil {
		return nil, fmt.Errorf("apply routing options: %w", err)
	}

	ns, path, err := record.SplitKey(keyStr)
	if err != nil {
		return nil, fmt.Errorf("splitting key: %w", err)
	}

	b, found := f.backends[ns]
	if !found {
		return nil, routing.ErrNotSupported
	}

	val, err := b.Fetch(ctx, path)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return nil, fmt.Errorf("fetch from backend: %w", err)
		}

		if rOpt.Offline {
			return nil, routing.ErrNotFound
		}

		out := make(chan []byte)
		go f.searchValueRoutine(ctx, b, ns, path, rOpt, out)
		return out, nil
	}

	rec, ok := val.(*recpb.Record)
	if !ok {
		return nil, fmt.Errorf("expected *recpb.Record from backend, got: %T", val)
	}

	if rOpt.Offline {
		out := make(chan []byte, 1)
		defer close(out)
		out <- rec.GetValue()
		return out, nil
	}

	out := make(chan []byte)
	go func() {
		out <- rec.GetValue()
		f.searchValueRoutine(ctx, b, ns, path, rOpt, out)
	}()

	return out, nil
}

func (f *FullRT) searchValueRoutine(ctx context.Context, backend Backend, ns string, path string, ropt *routing.Options, out chan<- []byte) {
	_, span := f.tele.Tracer.Start(ctx, "DHT.searchValueRoutine")
	defer span.End()
	defer close(out)

	routingKey := []byte(newRoutingKey(ns, path))

	req := &pb.Message{
		Type: pb.Message_GET_VALUE,
		Key:  routingKey,
	}

	// The currently known best value for /$ns/$path
	var best []byte

	// Peers that we identified to hold stale records
	var fixupPeers []kadt.PeerID

	// The peers that returned the best value
	quorumPeers := map[kadt.PeerID]struct{}{}

	// The quorum that we require for terminating the query. This number tells
	// us how many peers must have responded with the "best" value before we
	// cancel the query.
	quorum := f.getQuorum(ropt)

	fn := func(ctx context.Context, id kadt.PeerID, resp *pb.Message, stats coordt.QueryStats) error {
		rec := resp.GetRecord()
		if rec == nil {
			return nil
		}

		if !bytes.Equal(routingKey, rec.GetKey()) {
			return nil
		}

		idx, _ := backend.Validate(ctx, path, best, rec.GetValue())
		switch idx {
		case 0: // "best" is still the best value
			if bytes.Equal(best, rec.GetValue()) {
				quorumPeers[id] = struct{}{}
			}

		case 1: // rec.GetValue() is better than our current "best"

			// We have identified a better record. All peers that were currently
			// in our set of quorum peers need to be updated wit this new record
			for p := range quorumPeers {
				fixupPeers = append(fixupPeers, p)
			}

			// re-initialize the quorum peers set for this new record
			quorumPeers = map[kadt.PeerID]struct{}{}
			quorumPeers[id] = struct{}{}

			// submit the new value to the user
			best = rec.GetValue()
			out <- best
		case -1: // "best" and rec.GetValue() are both invalid
			return nil

		default:
			f.log.Warn("unexpected validate index", slog.Int("idx", idx))
		}

		// Check if we have reached the quorum
		if len(quorumPeers) == quorum {
			return coordt.ErrSkipRemaining
		}

		return nil
	}

	_, _, err := f.kad.QueryMessage(ctx, req, fn, f.queryConfig())
	if err != nil {
		f.warnErr(err, "Search value query failed")
		return
	}

	// check if we have peers that we found to hold stale records. If so,
	// update them asynchronously.
	if len(fixupPeers) == 0 {
		return
	}

	go func() {
		msg := &pb.Message{
			Type:   pb.Message_PUT_VALUE,
			Key:    routingKey,
			Record: record.MakePutRecord(string(routingKey), best),
		}

		if err := f.kad.BroadcastStatic(ctx, msg, fixupPeers); err != nil {
			f.log.Warn("Failed updating peer")
		}
	}()
}
