package coord

import (
	"context"
	"sync"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/probe-lab/zikade/internal/coord/coordt"
	"github.com/probe-lab/zikade/internal/kadtest"
	"github.com/probe-lab/zikade/internal/nettest"
	"github.com/probe-lab/zikade/kadt"
	"github.com/probe-lab/zikade/pb"
)

func TestQueryConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultQueryConfig()
		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer not nil", func(t *testing.T) {
		cfg := DefaultQueryConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("query concurrency positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("query timeout positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}

func TestQueryBehaviourBase(t *testing.T) {
	suite.Run(t, new(QueryBehaviourBaseTestSuite))
}

type QueryBehaviourBaseTestSuite struct {
	suite.Suite

	cfg   *QueryConfig
	top   *nettest.Topology
	nodes []*nettest.Peer
}

func (ts *QueryBehaviourBaseTestSuite) SetupTest() {
	clk := clock.NewMock()
	top, nodes, err := nettest.LinearTopology(4, clk)
	ts.Require().NoError(err)

	ts.top = top
	ts.nodes = nodes

	ts.cfg = DefaultQueryConfig()
	ts.cfg.Clock = clk
}

func (ts *QueryBehaviourBaseTestSuite) TestNotifiesNoProgress() {
	t := ts.T()
	ctx := kadtest.CtxShort(t)

	target := ts.nodes[3].NodeID.Key()
	rt := ts.nodes[0].RoutingTable
	seeds := rt.NearestNodes(target, 5)

	b, err := NewQueryBehaviour(ts.nodes[0].NodeID, ts.cfg)
	ts.Require().NoError(err)

	waiter := NewQueryWaiter(5)
	cmd := &EventStartFindCloserQuery{
		QueryID:           "test",
		Target:            target,
		KnownClosestNodes: seeds,
		Notify:            waiter,
		NumResults:        10,
	}

	// queue the start of the query
	b.Notify(ctx, cmd)

	// behaviour should emit EventOutboundGetCloserNodes to start the query
	bev, ok := b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes{}, bev)

	egc := bev.(*EventOutboundGetCloserNodes)
	ts.Require().True(egc.To.Equal(ts.nodes[1].NodeID))

	// notify failure
	b.Notify(ctx, &EventGetCloserNodesFailure{
		QueryID: "test",
		To:      egc.To,
		Target:  target,
	})

	// query will process the response and notify that node 1 is non connective
	bev, ok = b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventNotifyNonConnectivity{}, bev)

	// ensure that the waiter received query finished event
	kadtest.ReadItem[CtxEvent[*EventQueryFinished]](t, ctx, waiter.Finished())
}

func (ts *QueryBehaviourBaseTestSuite) TestNotifiesQueryProgressed() {
	t := ts.T()
	ctx := kadtest.CtxShort(t)

	target := ts.nodes[3].NodeID.Key()
	rt := ts.nodes[0].RoutingTable
	seeds := rt.NearestNodes(target, 5)

	b, err := NewQueryBehaviour(ts.nodes[0].NodeID, ts.cfg)
	ts.Require().NoError(err)

	waiter := NewQueryWaiter(5)
	cmd := &EventStartFindCloserQuery{
		QueryID:           "test",
		Target:            target,
		KnownClosestNodes: seeds,
		Notify:            waiter,
		NumResults:        10,
	}

	// queue the start of the query
	b.Notify(ctx, cmd)

	// behaviour should emit EventOutboundGetCloserNodes to start the query
	bev, ok := b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes{}, bev)

	egc := bev.(*EventOutboundGetCloserNodes)
	ts.Require().True(egc.To.Equal(ts.nodes[1].NodeID))

	// notify success
	b.Notify(ctx, &EventGetCloserNodesSuccess{
		QueryID:     "test",
		To:          egc.To,
		Target:      target,
		CloserNodes: ts.nodes[1].RoutingTable.NearestNodes(target, 5),
	})

	// query will process the response and ask node 1 for closer nodes
	bev, ok = b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes{}, bev)

	// ensure that the waiter received query progressed event
	kadtest.ReadItem[CtxEvent[*EventQueryProgressed]](t, ctx, waiter.Progressed())
}

func (ts *QueryBehaviourBaseTestSuite) TestNotifiesQueryFinished() {
	t := ts.T()
	ctx := kadtest.CtxShort(t)

	target := ts.nodes[3].NodeID.Key()
	rt := ts.nodes[0].RoutingTable
	seeds := rt.NearestNodes(target, 5)

	b, err := NewQueryBehaviour(ts.nodes[0].NodeID, ts.cfg)
	ts.Require().NoError(err)

	waiter := NewQueryWaiter(5)
	cmd := &EventStartFindCloserQuery{
		QueryID:           "test",
		Target:            target,
		KnownClosestNodes: seeds,
		Notify:            waiter,
		NumResults:        10,
	}

	// queue the start of the query
	b.Notify(ctx, cmd)

	// behaviour should emit EventOutboundGetCloserNodes to start the query
	bev, ok := b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes{}, bev)

	egc := bev.(*EventOutboundGetCloserNodes)
	ts.Require().True(egc.To.Equal(ts.nodes[1].NodeID))

	// notify success
	b.Notify(ctx, &EventGetCloserNodesSuccess{
		QueryID:     "test",
		To:          egc.To,
		Target:      target,
		CloserNodes: ts.nodes[1].RoutingTable.NearestNodes(target, 5),
	})

	// skip events until next EventOutboundGetCloserNodes is reached
	for {
		bev, ok = b.Perform(ctx)
		ts.Require().True(ok)

		egc, ok = bev.(*EventOutboundGetCloserNodes)
		if ok {
			break
		}
	}

	// ensure that the waiter received query progressed event
	wev := kadtest.ReadItem[CtxEvent[*EventQueryProgressed]](t, ctx, waiter.Progressed())
	ts.Require().True(wev.Event.NodeID.Equal(ts.nodes[1].NodeID))

	// notify success for last seen EventOutboundGetCloserNodes but supply no further nodes
	b.Notify(ctx, &EventGetCloserNodesSuccess{
		QueryID: "test",
		To:      egc.To,
		Target:  target,
	})

	// skip events until behaviour runs out of work
	for {
		_, ok = b.Perform(ctx)
		if !ok {
			break
		}
	}

	// ensure that the waiter received query progressed event
	kadtest.ReadItem[CtxEvent[*EventQueryProgressed]](t, ctx, waiter.Progressed())

	// ensure that the waiter received query  event
	kadtest.ReadItem[CtxEvent[*EventQueryFinished]](t, ctx, waiter.Finished())
}

func TestQuery_deadlock_regression(t *testing.T) {
	t.Skip()
	ctx := kadtest.CtxShort(t)
	msg := &pb.Message{}
	queryID := coordt.QueryID("test")

	_, nodes, err := nettest.LinearTopology(3, clock.New())
	require.NoError(t, err)

	// it would be better to just work with the queryBehaviour in this test.
	// However, we want to test as many parts as possible and waitForQuery
	// is defined on the coordinator. Therfore, we instantiate a coordinator
	// and close it immediately to manually control state machine progression.
	c, err := NewCoordinator(nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, nil)
	require.NoError(t, err)
	require.NoError(t, c.Close()) // close immediately so that we control the state machine progression

	// define a function that produces success messages
	successMsg := func(to kadt.PeerID, closer ...kadt.PeerID) *EventSendMessageSuccess {
		return &EventSendMessageSuccess{
			QueryID:     queryID,
			Request:     msg,
			To:          to,
			Response:    nil,
			CloserNodes: closer,
		}
	}

	// start query
	waiter := NewQueryWaiter(5)
	wrappedWaiter := NewQueryMonitorHook[*EventQueryFinished](waiter)

	waiterDone := make(chan struct{})
	waiterMsg := make(chan struct{})
	go func() {
		defer close(waiterDone)
		defer close(waiterMsg)
		_, _, err = c.waitForQuery(ctx, queryID, waiter, func(ctx context.Context, id kadt.PeerID, resp *pb.Message, stats coordt.QueryStats) error {
			waiterMsg <- struct{}{}
			return coordt.ErrSkipRemaining
		})
	}()

	// start the message query
	c.queryBehaviour.Notify(ctx, &EventStartMessageQuery{
		QueryID:           queryID,
		Target:            msg.Target(),
		Message:           msg,
		KnownClosestNodes: []kadt.PeerID{nodes[1].NodeID},
		Notify:            wrappedWaiter,
		NumResults:        0,
	})

	// advance state machines and assert that the state machine
	// wants to send an outbound message to another peer
	ev, _ := c.queryBehaviour.Perform(ctx)
	require.IsType(t, &EventOutboundSendMessage{}, ev)

	// simulate a successful response from another node that returns one new node
	// This should result in a message for the waiter
	c.queryBehaviour.Notify(ctx, successMsg(nodes[1].NodeID, nodes[2].NodeID))

	// Because we're blocking on the waiterMsg channel in the waitForQuery
	// method above, we simulate a slow receiving waiter.

	// Advance the query pool state machine. Because we returned a new node
	// above, the query pool state machine wants to send another outbound query
	ev, _ = c.queryBehaviour.Perform(ctx)
	require.IsType(t, &EventAddNode{}, ev) // event to notify the routing table
	ev, _ = c.queryBehaviour.Perform(ctx)
	require.IsType(t, &EventOutboundSendMessage{}, ev)

	hasLock := make(chan struct{})
	var once sync.Once
	wrappedWaiter.BeforeProgressed = func() {
		once.Do(func() {
			close(hasLock)
		})
	}

	// Simulate a successful response from the new node. This node didn't return
	// any new nodes to contact. This means the query pool behaviour will notify
	// the waiter about a query progression and afterward about a finished
	// query. Because (at the time of writing) the waiter has a channel buffer
	// of 1, the channel cannot hold both events. At the same time, the waiter
	// doesn't consume the messages because it's busy processing the previous
	// query event (because we haven't released the blocking waiterMsg call above).
	go c.queryBehaviour.Notify(ctx, successMsg(nodes[2].NodeID))

	// wait until the above Notify call was handled by waiting until the hasLock
	// channel was closed in the above BeforeNotify hook. If that hook is called
	// we can be sure that the above Notify call has acquired the polled query
	// behaviour's pendingMu lock.
	kadtest.AssertClosed(t, ctx, hasLock)

	// Since we know that the pooled query behaviour holds the lock we can
	// release the slow waiter by reading an item from the waiterMsg channel.
	kadtest.ReadItem(t, ctx, waiterMsg)

	// At this point, the waitForQuery QueryFunc callback returned a
	// coordt.ErrSkipRemaining. This instructs the waitForQuery method to notify
	// the query behaviour with an EventStopQuery event. However, because the
	// query behaviour is busy sending a message to the waiter it is holding the
	// lock on the pending events to process. Therefore, this notify call will
	// also block. At the same time, the waiter cannot read the new messages
	// from the query behaviour because it tries to notify it.
	kadtest.AssertClosed(t, ctx, waiterDone)
}
