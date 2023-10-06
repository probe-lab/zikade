package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/internal/tiny"
)

var _ coordt.StateMachine[CrawlEvent, CrawlState] = (*Crawl[tiny.Key, tiny.Node])(nil)

func TestCrawlConfig_Validate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("tracer is not nil", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("max cpl positive", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 0
		require.Error(t, cfg.Validate())
		cfg.MaxCPL = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("concurrency positive", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})
}

func TestNewCrawl_Start(t *testing.T) {
	self := tiny.NewNode(0)
	a := tiny.NewNode(0b10000100)
	b := tiny.NewNode(0b11000000)

	t.Run("does not fail with default config", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 4
		qry, err := NewCrawl[tiny.Key, tiny.Node](self, tiny.NodeWithCpl, cfg)
		require.NoError(t, err)
		require.Nil(t, qry.info)
	})

	t.Run("removes self from seed", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 4
		qry, err := NewCrawl[tiny.Key, tiny.Node](self, tiny.NodeWithCpl, cfg)
		require.NoError(t, err)

		qry.Advance(context.Background(), &EventCrawlStart[tiny.Key, tiny.Node]{
			QueryID: coordt.QueryID("test"),
			Seed:    []tiny.Node{self, a, b},
		})
		require.NotNil(t, qry.info)
		require.Len(t, qry.info.todo, cfg.MaxCPL*2-1) // self is not included
		require.Len(t, qry.info.waiting, 1)
		require.Len(t, qry.info.success, 0)
		require.Len(t, qry.info.failed, 0)
		require.Len(t, qry.info.errors, 0)
	})

	t.Run("removes self from seed (no left", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 4
		qry, err := NewCrawl[tiny.Key, tiny.Node](self, tiny.NodeWithCpl, cfg)
		require.NoError(t, err)

		state := qry.Advance(context.Background(), &EventCrawlStart[tiny.Key, tiny.Node]{
			QueryID: coordt.QueryID("test"),
			Seed:    []tiny.Node{self},
		})
		require.Nil(t, qry.info)
		require.IsType(t, &StateCrawlFinished{}, state)
	})

	t.Run("handles duplicate starts (does not panic)", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 4
		qry, err := NewCrawl[tiny.Key, tiny.Node](self, tiny.NodeWithCpl, cfg)
		require.NoError(t, err)

		seed := []tiny.Node{a, b}
		qry.Advance(context.Background(), &EventCrawlStart[tiny.Key, tiny.Node]{
			QueryID: coordt.QueryID("test"),
			Seed:    seed,
		})

		qry.Advance(context.Background(), &EventCrawlStart[tiny.Key, tiny.Node]{
			QueryID: coordt.QueryID("test"),
			Seed:    seed,
		})

		qry.Advance(context.Background(), &EventCrawlStart[tiny.Key, tiny.Node]{
			QueryID: coordt.QueryID("another"),
			Seed:    seed,
		})
	})

	t.Run("handles events if no crawl started", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 4
		qry, err := NewCrawl[tiny.Key, tiny.Node](self, tiny.NodeWithCpl, cfg)
		require.NoError(t, err)

		state := qry.Advance(context.Background(), &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
			QueryID: coordt.QueryID("test"),
		})
		require.IsType(t, &StateCrawlIdle{}, state)

		state = qry.Advance(context.Background(), &EventCrawlNodeFailure[tiny.Key, tiny.Node]{
			QueryID: coordt.QueryID("test"),
		})
		require.IsType(t, &StateCrawlIdle{}, state)

		state = qry.Advance(context.Background(), &EventCrawlPoll{})
		require.IsType(t, &StateCrawlIdle{}, state)
	})
}

func TestCrawl_Advance(t *testing.T) {
	ctx := context.Background()

	// Let the state machine emit all FIND_NODE RPCs. Track them as pending responses.
	pending := []*StateCrawlFindCloser[tiny.Key, tiny.Node]{} // tracks pending requests

	self := tiny.NewNode(0)
	a := tiny.NewNode(0b10000100)
	b := tiny.NewNode(0b11000000)
	c := tiny.NewNode(0b10100000)
	seed := []tiny.Node{self, a, b}

	cfg := DefaultCrawlConfig()
	cfg.MaxCPL = 4
	cfg.Concurrency = 2

	queryID := coordt.QueryID("test")

	qry, err := NewCrawl[tiny.Key, tiny.Node](self, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := qry.Advance(context.Background(), &EventCrawlStart[tiny.Key, tiny.Node]{
		QueryID: queryID,
		Seed:    seed,
	})
	assert.Len(t, qry.info.todo, 2*cfg.MaxCPL-1)
	assert.Len(t, qry.info.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.info.waiting, 1)
	assert.Len(t, qry.info.success, 0)
	assert.Len(t, qry.info.failed, 0)
	assert.Len(t, qry.info.errors, 0)

	tstate, ok := state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "type is %T", state)
	pending = append(pending, tstate)

	for {
		state = qry.Advance(ctx, &EventCrawlPoll{})
		tstate, ok := state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
		if !ok {
			// Even if we're at capacity, we don't have any more FIND_NODE RRCs to do.
			require.IsType(t, &StateCrawlWaitingAtCapacity{}, state)
			break
		}
		pending = append(pending, tstate)
	}

	assert.Len(t, qry.info.todo, 0)
	assert.Len(t, qry.info.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.info.waiting, 2*cfg.MaxCPL)
	assert.Len(t, qry.info.success, 0)
	assert.Len(t, qry.info.failed, 0)
	assert.Len(t, qry.info.errors, 0)
	assert.Equal(t, len(qry.info.waiting), len(pending))

	// Poll again to verify that we're still AtCapacity
	state = qry.Advance(ctx, &EventCrawlPoll{})
	require.IsType(t, &StateCrawlWaitingAtCapacity{}, state)

	// simulate first successful response
	pop, pending := pending[0], pending[1:]
	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		NodeID:      pop.NodeID,
		Target:      pop.Target,
		CloserNodes: []tiny.Node{},
	})

	// we didn't have anything to do, so now we're waiting WITH capacity
	require.IsType(t, &StateCrawlWaitingWithCapacity{}, state)

	assert.Len(t, qry.info.todo, 0)
	assert.Len(t, qry.info.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.info.waiting, 2*cfg.MaxCPL-1) // one less
	assert.Len(t, qry.info.success, 1)              // one successful response
	assert.Len(t, qry.info.failed, 0)
	assert.Len(t, qry.info.errors, 0)

	// pop next successful response. This time it contains a new node!
	pop, pending = pending[0], pending[1:]
	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		NodeID:      pop.NodeID,
		Target:      pop.Target,
		CloserNodes: []tiny.Node{c},
	})

	// because the response contained a new node, we have new things to do and
	// therefore expect a FIND_NODE RPC state.
	tstate, ok = state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "type is %T", state)
	assert.Equal(t, tstate.NodeID, c)
	pending = append(pending, tstate)

	assert.Len(t, qry.info.todo, 3)
	assert.Len(t, qry.info.cpls, 3*cfg.MaxCPL)
	assert.Len(t, qry.info.waiting, 2*cfg.MaxCPL-1) // still -1 because the new FIND_NODE for c "replaced" the old one
	assert.Len(t, qry.info.success, 2)
	assert.Len(t, qry.info.failed, 0)
	assert.Len(t, qry.info.errors, 0)

	// simulate error
	pop, pending = pending[0], pending[1:]
	state = qry.Advance(ctx, &EventCrawlNodeFailure[tiny.Key, tiny.Node]{
		QueryID: queryID,
		NodeID:  pop.NodeID,
		Target:  pop.Target,
		Error:   fmt.Errorf("some error"),
	})
	tstate, ok = state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)
	pending = append(pending, tstate)

	assert.Len(t, qry.info.failed, 1)
	assert.Len(t, qry.info.errors, 1)
	assert.ErrorContains(t, qry.info.errors[qry.mapKey(pop.NodeID, pop.Target)], "some error")

	// simulate response from random node

	for {
		pop, pending = pending[0], pending[1:]
		state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
			QueryID:     queryID,
			NodeID:      pop.NodeID,
			Target:      pop.Target,
			CloserNodes: []tiny.Node{},
		})
		tstate, ok = state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
		if ok {
			pending = append(pending, tstate)
			continue
		}

		if _, ok = state.(*StateCrawlWaitingWithCapacity); ok {
			// continue simulating responses
			continue
		}

		if _, ok = state.(*StateCrawlFinished); ok {
			require.Nil(t, qry.info)
			break
		}
	}
}

func TestCrawl_Advance_unrelated_response(t *testing.T) {
	ctx := context.Background()

	self := tiny.NewNode(0)
	a := tiny.NewNode(0b10000100)
	seed := []tiny.Node{self, a}

	cfg := DefaultCrawlConfig()
	cfg.MaxCPL = 1
	cfg.Concurrency = 2

	queryID := coordt.QueryID("test")

	qry, err := NewCrawl[tiny.Key, tiny.Node](self, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := qry.Advance(context.Background(), &EventCrawlStart[tiny.Key, tiny.Node]{
		QueryID: queryID,
		Seed:    seed,
	})
	tstate, ok := state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "type is %T", state)

	state = qry.Advance(ctx, &EventCrawlPoll{})
	require.IsType(t, &StateCrawlWaitingWithCapacity{}, state)

	// send it an unrelated response
	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		QueryID:     coordt.QueryID("another"),
		NodeID:      tstate.NodeID,
		Target:      tstate.Target,
		CloserNodes: []tiny.Node{},
	})
	require.IsType(t, &StateCrawlWaitingWithCapacity{}, state) // still waiting with capacity because the response was ignored

	// send it an unrelated response
	state = qry.Advance(ctx, &EventCrawlNodeFailure[tiny.Key, tiny.Node]{
		QueryID: coordt.QueryID("another"),
		NodeID:  tstate.NodeID,
		Target:  tstate.Target,
		Error:   fmt.Errorf("some error"),
	})
	require.IsType(t, &StateCrawlWaitingWithCapacity{}, state) // still waiting with capacity because the response was ignored

	// send correct response
	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		NodeID:      tstate.NodeID,
		Target:      tstate.Target,
		CloserNodes: []tiny.Node{},
	})
	require.IsType(t, &StateCrawlFinished{}, state)
	require.Nil(t, qry.info)
}
