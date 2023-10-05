package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/internal/coord/internal/tiny"
)

var _ coordt.StateMachine[CrawlEvent, CrawlState] = (*Crawl[tiny.Key, tiny.Node, tiny.Message])(nil)

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

func TestNewCrawl(t *testing.T) {
	self := tiny.NewNode(0)
	a := tiny.NewNode(0b10000100)
	b := tiny.NewNode(0b11000000)

	t.Run("initializes maps", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 4
		seed := []tiny.Node{a}
		qry, err := NewCrawl[tiny.Key, tiny.Node, tiny.Message](self, coordt.QueryID("test"), tiny.NodeWithCpl, seed, cfg)
		require.NoError(t, err)
		require.NotNil(t, qry)
		require.Len(t, qry.todo, cfg.MaxCPL)
		require.NotNil(t, qry.waiting)
		require.NotNil(t, qry.success)
		require.NotNil(t, qry.failed)
		require.NotNil(t, qry.errors)
	})

	t.Run("removes self from seed", func(t *testing.T) {
		cfg := DefaultCrawlConfig()
		cfg.MaxCPL = 4
		seed := []tiny.Node{self, a, b}
		qry, err := NewCrawl[tiny.Key, tiny.Node, tiny.Message](self, coordt.QueryID("test"), tiny.NodeWithCpl, seed, cfg)
		require.NoError(t, err)
		require.NotNil(t, qry)
		require.Len(t, qry.todo, cfg.MaxCPL*2) // self is not included
		require.NotNil(t, qry.waiting)
		require.NotNil(t, qry.success)
		require.NotNil(t, qry.failed)
		require.NotNil(t, qry.errors)
	})
}

func TestCrawl_Advance(t *testing.T) {
	ctx := context.Background()

	self := tiny.NewNode(0)
	a := tiny.NewNode(0b10000100)
	b := tiny.NewNode(0b11000000)
	c := tiny.NewNode(0b10100000)
	seed := []tiny.Node{self, a, b}

	cfg := DefaultCrawlConfig()
	cfg.MaxCPL = 4
	cfg.Concurrency = 2

	queryID := coordt.QueryID("test")

	qry, err := NewCrawl[tiny.Key, tiny.Node, tiny.Message](self, queryID, tiny.NodeWithCpl, seed, cfg)
	require.NoError(t, err)

	assert.Len(t, qry.todo, 2*cfg.MaxCPL)
	assert.Len(t, qry.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 0)
	assert.Len(t, qry.success, 0)
	assert.Len(t, qry.failed, 0)
	assert.Len(t, qry.errors, 0)

	// Let the state machine emit all FIND_NODE RPCs. Track them as pending responses.
	pending := []*StateCrawlFindCloser[tiny.Key, tiny.Node]{} // tracks pending requests
	for {
		state := qry.Advance(ctx, &EventCrawlPoll{})
		tstate, ok := state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
		if !ok {
			// Even if we're at capacity, we don't have any more FIND_NODE RRCs to do.
			require.IsType(t, &StateCrawlWaitingAtCapacity{}, state)
			break
		}
		pending = append(pending, tstate)
	}

	assert.Len(t, qry.todo, 0)
	assert.Len(t, qry.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 2*cfg.MaxCPL)
	assert.Len(t, qry.success, 0)
	assert.Len(t, qry.failed, 0)
	assert.Len(t, qry.errors, 0)
	assert.Equal(t, len(qry.waiting), len(pending))

	// Poll again to verify that we're still AtCapacity
	state := qry.Advance(ctx, &EventCrawlPoll{})
	require.IsType(t, &StateCrawlWaitingAtCapacity{}, state)

	// simulate first successful response
	pop, pending := pending[0], pending[1:]
	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      pop.NodeID,
		Target:      pop.Target,
		CloserNodes: []tiny.Node{},
	})

	// we didn't have anything to do, so now we're waiting WITH capacity
	require.IsType(t, &StateCrawlWaitingWithCapacity{}, state)

	assert.Len(t, qry.todo, 0)
	assert.Len(t, qry.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 2*cfg.MaxCPL-1) // one less
	assert.Len(t, qry.success, 1)              // one successful response
	assert.Len(t, qry.failed, 0)
	assert.Len(t, qry.errors, 0)

	// pop next successful response. This time it contains a new node!
	pop, pending = pending[0], pending[1:]
	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      pop.NodeID,
		Target:      pop.Target,
		CloserNodes: []tiny.Node{c},
	})

	// because the response contained a new node, we have new things to do and
	// therefore expect a FIND_NODE RPC state.
	tstate, ok := state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "type is %T", state)
	assert.Equal(t, tstate.NodeID, c)
	pending = append(pending, tstate)

	assert.Len(t, qry.todo, 3)
	assert.Len(t, qry.cpls, 3*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 2*cfg.MaxCPL-1) // still -1 because the new FIND_NODE for c "replaced" the old one
	assert.Len(t, qry.success, 2)
	assert.Len(t, qry.failed, 0)
	assert.Len(t, qry.errors, 0)

	// simulate error
	pop, pending = pending[0], pending[1:]
	state = qry.Advance(ctx, &EventCrawlNodeFailure[tiny.Key, tiny.Node]{
		NodeID: pop.NodeID,
		Target: pop.Target,
		Error:  fmt.Errorf("some error"),
	})
	tstate, ok = state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)
	pending = append(pending, tstate)

	assert.Len(t, qry.failed, 1)
	assert.Len(t, qry.errors, 1)
	assert.ErrorContains(t, qry.errors[qry.mapKey(pop.NodeID, pop.Target)], "some error")

	// simulate response from random node

	for {
		pop, pending = pending[0], pending[1:]
		state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
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
			break
		}
	}
}
