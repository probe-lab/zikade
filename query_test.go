package zikade

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/probe-lab/zikade/internal/kadtest"
	"github.com/probe-lab/zikade/kadt"
)

func TestRTAdditionOnSuccessfulQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	top := NewTopology(t)
	d1 := top.AddServer(nil)
	d2 := top.AddServer(nil)
	d3 := top.AddServer(nil)

	top.ConnectChain(ctx, d1, d2, d3)

	// d3 does not know about d1
	require.False(t, d3.kad.IsRoutable(ctx, kadt.PeerID(d1.host.ID())))

	// d1 does not know about d3
	require.False(t, d1.kad.IsRoutable(ctx, kadt.PeerID(d3.host.ID())))

	// // but when d3 queries d2, d1 and d3 discover each other
	_, _ = d3.FindPeer(ctx, "something")
	// ignore the error

	// d3 should update its routing table to include d1 during the query
	_, err := top.ExpectRoutingUpdated(ctx, d3, d1.host.ID())
	require.NoError(t, err)

	// d3 now has d1 in its routing table
	require.True(t, d3.kad.IsRoutable(ctx, kadt.PeerID(d1.host.ID())))

	// d1 should update its routing table to include d3 during the query
	_, err = top.ExpectRoutingUpdated(ctx, d1, d3.host.ID())
	require.NoError(t, err)

	// d1 now has d3 in its routing table
	require.True(t, d1.kad.IsRoutable(ctx, kadt.PeerID(d3.host.ID())))
}

func TestRTEvictionOnFailedQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	top := NewTopology(t)
	d1 := top.AddServer(nil)
	d2 := top.AddServer(nil)

	top.Connect(ctx, d1, d2)

	// close both hosts so query fails
	require.NoError(t, d1.host.Close())
	require.NoError(t, d2.host.Close())

	// peers will still be in the RT because time is paused and
	// no scheduled probes will have taken place

	// d1 still has d2 in the routing table
	require.True(t, d1.kad.IsRoutable(ctx, kadt.PeerID(d2.host.ID())))

	// d2 still has d1 in the routing table
	require.True(t, d2.kad.IsRoutable(ctx, kadt.PeerID(d1.host.ID())))

	// failed queries should remove the queried peers from the routing table
	_, _ = d1.FindPeer(ctx, "test")

	// d1 should update its routing table to remove d2 because of the failure
	_, err := top.ExpectRoutingRemoved(ctx, d1, d2.host.ID())
	require.NoError(t, err)
}
