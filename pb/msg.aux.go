package pb

import (
	"bytes"
	"fmt"
	math_bits "math/bits"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/probe-lab/zikade/kadt"
	"golang.org/x/exp/slog"
)

// this file contains auxiliary methods to augment the protobuf generated types.
// It is used to let these types conform to interfaces or add convenience methods.

func (m *Message) Target() kadt.Key {
	return kadt.NewKey(m.Key)
}

// ExpectResponse returns true if we expect a response from the remote peer if
// we sent a message with the given type to them. For example, when a peer sends
// a PUT_VALUE message to another peer, that other peer won't respond with
// anything.
func (m *Message) ExpectResponse() bool {
	switch m.Type {
	case Message_PUT_VALUE:
		return false
	case Message_GET_VALUE:
		return true
	case Message_ADD_PROVIDER:
		return false
	case Message_GET_PROVIDERS:
		return true
	case Message_FIND_NODE:
		return true
	case Message_PING:
		return true
	default:
		panic(fmt.Sprintf("unexpected message type %d", m.Type))
	}
}

// FromAddrInfo constructs a [Message_Peer] from the given [peer.AddrInfo].
func FromAddrInfo(p peer.AddrInfo) *Message_Peer {
	mp := &Message_Peer{
		Id:    []byte(p.ID),
		Addrs: make([][]byte, len(p.Addrs)),
	}

	for i, maddr := range p.Addrs {
		mp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}

	return mp
}

// ContainsCloserPeer returns true if the provided peer ID is among the
// list of closer peers contained in this message.
func (m *Message) ContainsCloserPeer(pid peer.ID) bool {
	for _, cp := range m.CloserPeers {
		if bytes.Equal(cp.Id, []byte(pid)) {
			return true
		}
	}
	return false
}

// ProviderAddrInfos returns the peer.AddrInfo's of the provider peers in this
// message.
func (m *Message) ProviderAddrInfos() []peer.AddrInfo {
	if m == nil {
		return nil
	}

	addrInfos := make([]peer.AddrInfo, 0, len(m.ProviderPeers))
	for _, p := range m.ProviderPeers {
		addrInfos = append(addrInfos, peer.AddrInfo{
			ID:    peer.ID(p.Id),
			Addrs: p.Addresses(),
		})
	}

	return addrInfos
}

// CloserPeersAddrInfos returns the peer.AddrInfo's of the closer peers in this
// message.
func (m *Message) CloserPeersAddrInfos() []peer.AddrInfo {
	if m == nil {
		return nil
	}

	addrInfos := make([]peer.AddrInfo, 0, len(m.CloserPeers))
	for _, p := range m.CloserPeers {
		addrInfos = append(addrInfos, peer.AddrInfo{
			ID:    peer.ID(p.Id),
			Addrs: p.Addresses(),
		})
	}

	return addrInfos
}

func (m *Message) CloserNodes() []kadt.PeerID {
	if m == nil {
		return nil
	}

	ids := make([]kadt.PeerID, 0, len(m.CloserPeers))
	for _, p := range m.CloserPeers {
		ids = append(ids, kadt.PeerID(peer.ID(p.Id)))
	}

	return ids
}

func (m *Message) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Type != 0 {
		n += 1 + sovDht(uint64(m.Type))
	}
	l = len(m.Key)
	if l > 0 {
		n += 1 + l + sovDht(uint64(l))
	}
	if m.Record != nil {
		l = m.Record.Size()
		n += 1 + l + sovDht(uint64(l))
	}
	if len(m.CloserPeers) > 0 {
		for _, e := range m.CloserPeers {
			l = e.Size()
			n += 1 + l + sovDht(uint64(l))
		}
	}
	if len(m.ProviderPeers) > 0 {
		for _, e := range m.ProviderPeers {
			l = e.Size()
			n += 1 + l + sovDht(uint64(l))
		}
	}
	if m.ClusterLevelRaw != 0 {
		n += 1 + sovDht(uint64(m.ClusterLevelRaw))
	}
	return n
}

// Addresses returns the Multiaddresses associated with the Message_Peer entry
func (m *Message_Peer) Addresses() []ma.Multiaddr {
	if m == nil {
		return nil
	}

	maddrs := make([]ma.Multiaddr, 0, len(m.Addrs))
	for _, addr := range m.Addrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			slog.Debug("error decoding multiaddr for peer", "peer", peer.ID(m.Id), "err", err)
			continue
		}

		maddrs = append(maddrs, maddr)
	}

	return maddrs
}

func (m *Message_Peer) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Id)
	n += 1 + l + sovDht(uint64(l))
	if len(m.Addrs) > 0 {
		for _, b := range m.Addrs {
			l = len(b)
			n += 1 + l + sovDht(uint64(l))
		}
	}
	if m.Connection != 0 {
		n += 1 + sovDht(uint64(m.Connection))
	}
	return n
}

func sovDht(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
