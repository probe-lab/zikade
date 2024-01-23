package zikade

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/probe-lab/zikade/kadt"
	"github.com/probe-lab/zikade/pb"
)

var rng = rand.New(rand.NewSource(1337))

// newTestHost returns a libp2p host with the given options. It also applies
// options that are common to all test hosts.
func newTestHost(t testing.TB, opts ...libp2p.Option) host.Host {
	// If two peers simultaneously connect, they could end up in a state where
	// one peer is waiting on the connection for the other one, although there
	// already exists a valid connection. The libp2p dial loop doesn't recognize
	// the new connection immediately, but only after the local dial has timed
	// out. By default, the timeout is set to 5s which results in failing tests
	// as the tests time out. By setting the timeout to a much lower value, we
	// work around the timeout issue. Try to remove the following swarm options
	// after https://github.com/libp2p/go-libp2p/issues/2589 was resolved.
	// Also, the below should be changed to [swarm.WithDialTimeoutLocal]. Change
	// that after https://github.com/libp2p/go-libp2p/pull/2595 is resolved.
	dialTimeout := 500 * time.Millisecond
	swarmOpts := libp2p.SwarmOpts(swarm.WithDialTimeout(dialTimeout))

	// The QUIC transport leaks go-routines, so we're only enabling the TCP
	// transport for our tests. Remove after:
	// https://github.com/libp2p/go-libp2p/issues/2514 was fixed
	tcpTransport := libp2p.Transport(tcp.NewTCPTransport)

	h, err := libp2p.New(append(opts, swarmOpts, tcpTransport)...)
	require.NoError(t, err)

	return h
}

func newTestDHT(t testing.TB) *DHT {
	cfg := DefaultConfig()
	cfg.Logger = devnull

	return newTestDHTWithConfig(t, cfg)
}

func newTestDHTWithConfig(t testing.TB, cfg *Config) *DHT {
	t.Helper()

	h := newTestHost(t, libp2p.NoListenAddrs)

	d, err := New(h, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("closing dht: %s", err)
		}

		if err = h.Close(); err != nil {
			t.Logf("closing host: %s", err)
		}
	})

	return d
}

func newPeerID(t testing.TB) peer.ID {
	id, _ := newIdentity(t)
	return id
}

func newIdentity(t testing.TB) (peer.ID, crypto.PrivKey) {
	t.Helper()

	priv, pub, err := crypto.GenerateEd25519Key(rng)
	require.NoError(t, err)

	id, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	return id, priv
}

// fillRoutingTable populates d's routing table and peerstore with n random peers and addresses
func fillRoutingTable(t testing.TB, d *DHT, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		// generate peer ID
		pid := newPeerID(t)

		// add peer to routing table
		d.rt.AddNode(kadt.PeerID(pid))

		// craft random network address for peer
		// use IP suffix of 1.1 to not collide with actual test hosts that
		// choose a random IP address via 127.0.0.1:0.
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.1.1/tcp/%d", 2000+i))
		require.NoError(t, err)

		// add peer information to peer store
		d.host.Peerstore().AddAddr(pid, a, time.Hour)
	}
}

func newAddrInfo(t testing.TB) peer.AddrInfo {
	return peer.AddrInfo{
		ID: newPeerID(t),
		Addrs: []ma.Multiaddr{
			ma.StringCast("/ip4/99.99.99.99/tcp/2000"), // must be a public address
		},
	}
}

func newAddProviderRequest(key []byte, addrInfos ...peer.AddrInfo) *pb.Message {
	providerPeers := make([]*pb.Message_Peer, len(addrInfos))
	for i, addrInfo := range addrInfos {
		providerPeers[i] = pb.FromAddrInfo(addrInfo)
	}

	return &pb.Message{
		Type:          pb.Message_ADD_PROVIDER,
		Key:           key,
		ProviderPeers: providerPeers,
	}
}
