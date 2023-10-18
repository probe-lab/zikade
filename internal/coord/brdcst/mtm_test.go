package brdcst

import (
	"context"
	"math/big"
	"testing"

	"github.com/plprobelab/go-libdht/kad/key"
	"github.com/stretchr/testify/require"

	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/internal/tiny"
)

func TestNewManyToMany(t *testing.T) {
	ctx := context.Background()

	msgFunc := func(k tiny.Key) tiny.Message {
		return tiny.Message{
			Content: k.String(),
		}
	}

	count := 64
	targets := make([]tiny.Key, 0, count)
	seed := make([]tiny.Node, 0, count)
	for i := 0; i < count; i++ {
		seed = append(seed, tiny.NewNode(tiny.Key(i+1)))
		targets = append(targets, tiny.Key(count+i+1))
	}

	cfg := DefaultConfigManyToMany(targets)
	cfg.NodeConcurrency = 2
	cfg.StreamConcurrency = 5

	qid := coordt.QueryID("test")

	t.Run("no seed", func(t *testing.T) {
		sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, []tiny.Node{}, cfg)
		require.Equal(t, qid, sm.queryID)
		require.Equal(t, cfg, sm.cfg)
		require.NotNil(t, sm.unprocessedNodes)
		require.Len(t, sm.unprocessedNodes, 0)
		require.NotNil(t, sm.inflightWithCapacity)
		require.Len(t, sm.inflightWithCapacity, 0)
		require.NotNil(t, sm.inflightAtCapacity)
		require.Len(t, sm.inflightAtCapacity, 0)
		require.NotNil(t, sm.processedNodes)
		require.Len(t, sm.processedNodes, 0)
		require.Len(t, sm.keyReports, 0)
		require.NotNil(t, sm.keyReports)
		require.NotNil(t, sm.msgFunc)

		state := sm.Advance(ctx, &EventBroadcastPoll{})
		tstate, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
		require.True(t, ok, "type is %T", state)

		require.Equal(t, qid, tstate.QueryID)
		require.NotNil(t, tstate.Contacted)
		require.Len(t, tstate.Contacted, 0)
		require.NotNil(t, tstate.Errors)
		require.Len(t, tstate.Errors, 0)
	})

	t.Run("no targets", func(t *testing.T) {
		cfg := DefaultConfigManyToMany([]tiny.Key{})
		sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, seed, cfg)
		require.Equal(t, qid, sm.queryID)
		require.Equal(t, cfg, sm.cfg)
		require.Len(t, sm.unprocessedNodes, 0)
		require.Len(t, sm.inflightWithCapacity, 0)
		require.Len(t, sm.inflightAtCapacity, 0)
		require.Len(t, sm.processedNodes, 0)
		require.Len(t, sm.keyReports, 0)
		require.NotNil(t, sm.msgFunc)

		state := sm.Advance(ctx, &EventBroadcastPoll{})
		tstate, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
		require.True(t, ok, "type is %T", state)

		require.Equal(t, qid, tstate.QueryID)
		require.NotNil(t, tstate.Contacted)
		require.Len(t, tstate.Contacted, 0)
		require.NotNil(t, tstate.Errors)
		require.Len(t, tstate.Errors, 0)
	})

	t.Run("bucket sized seed and targets", func(t *testing.T) {
		// TODO: make "20" based on some configuration
		cfg := DefaultConfigManyToMany(targets[:20])
		sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, seed[:20], cfg)
		require.Equal(t, qid, sm.queryID)
		require.Equal(t, cfg, sm.cfg)
		require.Len(t, sm.unprocessedNodes, 20)
		for node, nodeStatus := range sm.unprocessedNodes {
			require.Equal(t, node, nodeStatus.node.String())
			require.Len(t, nodeStatus.todo, 20)
			require.Len(t, nodeStatus.done, 0)
			require.Len(t, nodeStatus.inflight, 0)
		}
		require.Len(t, sm.inflightWithCapacity, 0)
		require.Len(t, sm.inflightAtCapacity, 0)
		require.Len(t, sm.processedNodes, 0)
		require.Len(t, sm.keyReports, 20)
		require.NotNil(t, sm.msgFunc)
	})

	t.Run("more seeds than targets", func(t *testing.T) {
		// because [seed] has incrementing IDs starting with 1, a key of 0
		// will have seeds 1-20 as their closest nodes. This is asserted in this
		// test.
		cfg := DefaultConfigManyToMany([]tiny.Key{tiny.Key(0)})
		sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, seed[:40], cfg)
		require.Len(t, sm.unprocessedNodes, 20)

		for _, s := range seed[:20] {
			nodeStatus := sm.unprocessedNodes[s.String()]
			require.NotNil(t, nodeStatus)
			require.Equal(t, s.String(), nodeStatus.node.String())
			require.Len(t, nodeStatus.todo, 1)
			require.Len(t, nodeStatus.done, 0)
			require.Len(t, nodeStatus.inflight, 0)
		}

		for _, s := range seed[20:40] {
			require.Nil(t, sm.unprocessedNodes[s.String()])
		}
	})
}

func TestManyToMany_Advance_single_target_single_seed(t *testing.T) {
	ctx := context.Background()
	msgFunc := func(k tiny.Key) tiny.Message {
		return tiny.Message{
			Content: k.String(),
		}
	}

	targets := []tiny.Key{tiny.Key(0)}
	seed := []tiny.Node{tiny.NewNode(tiny.Key(1))}

	qid := coordt.QueryID("test")

	// create new state machine
	sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, seed, DefaultConfigManyToMany(targets))
	require.Len(t, sm.unprocessedNodes, 1)

	state := sm.Advance(ctx, &EventBroadcastPoll{})
	tstate, ok := state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "type is %T", state)

	require.Equal(t, tstate.QueryID, qid)
	require.NotNil(t, tstate.NodeID)
	require.NotNil(t, tstate.Message)

	state = sm.Advance(ctx, &EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID:   tstate.NodeID,
		Target:   targets[0],
		Request:  tstate.Message,
		Response: tstate.Message,
	})
	fstate, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "type is %T", state)
	require.Equal(t, fstate.QueryID, qid)
	require.Equal(t, seed[0], fstate.Contacted[0])

	require.Equal(t, 1, sm.keyReports[key.HexString(targets[0])].successes)
	require.Equal(t, 0, sm.keyReports[key.HexString(targets[0])].failures)
	require.False(t, sm.keyReports[key.HexString(targets[0])].lastSuccess.IsZero())
}

func TestManyToMany_Advance_multi_target_single_seed(t *testing.T) {
	ctx := context.Background()
	msgFunc := func(k tiny.Key) tiny.Message {
		return tiny.Message{
			Content: k.String(),
		}
	}

	targets := []tiny.Key{tiny.Key(0), tiny.Key(2)}
	seed := []tiny.Node{tiny.NewNode(tiny.Key(1))}

	qid := coordt.QueryID("test")

	// create new state machine
	cfg := DefaultConfigManyToMany(targets)
	cfg.StreamConcurrency = 1

	sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, seed, cfg)
	require.Len(t, sm.unprocessedNodes, 1)

	state := sm.Advance(ctx, &EventBroadcastPoll{})
	tstate, ok := state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "type is %T", state)

	require.Equal(t, tstate.QueryID, qid)
	require.NotNil(t, tstate.NodeID)
	require.NotNil(t, tstate.Message)

	n := new(big.Int)
	n.SetString(tstate.Message.Content, 16)
	state = sm.Advance(ctx, &EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID:   tstate.NodeID,
		Target:   tiny.Key(n.Int64()),
		Request:  tstate.Message,
		Response: tstate.Message,
	})
	tstate, ok = state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "type is %T", state)

	require.Equal(t, tstate.QueryID, qid)
	require.NotNil(t, tstate.NodeID)
	require.NotNil(t, tstate.Message)

	state = sm.Advance(ctx, &EventBroadcastPoll{})
	require.IsType(t, &StateBroadcastWaiting{}, state)

	n.SetString(tstate.Message.Content, 16)
	state = sm.Advance(ctx, &EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID:   tstate.NodeID,
		Target:   tiny.Key(n.Int64()),
		Request:  tstate.Message,
		Response: tstate.Message,
	})

	fstate, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "type is %T", state)
	require.Equal(t, fstate.QueryID, qid)
	require.Equal(t, seed[0], fstate.Contacted[0])
}

func TestManyToMany_Advance_multi_target_multi_seed(t *testing.T) {
	ctx := context.Background()

	msgFunc := func(k tiny.Key) tiny.Message {
		return tiny.Message{
			Content: k.String(),
		}
	}

	count := 64
	targets := make([]tiny.Key, 0, count)
	seed := make([]tiny.Node, 0, count)
	for i := 0; i < count; i += 2 {
		seed = append(seed, tiny.NewNode(tiny.Key(i+1)))
		targets = append(targets, tiny.Key(i+2))
	}

	cfg := DefaultConfigManyToMany(targets)
	cfg.NodeConcurrency = 2
	cfg.StreamConcurrency = 5

	qid := coordt.QueryID("test")

	// create new state machine
	sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, seed, cfg)
	require.Len(t, sm.unprocessedNodes, 32)

	storeRecordRequestCount := 0
	// poll as often until we are at capacity
	pending := map[tiny.Node][]tiny.Message{}
	for i := 0; i < cfg.NodeConcurrency*cfg.StreamConcurrency; i++ {
		state := sm.Advance(ctx, &EventBroadcastPoll{})
		tstate, ok := state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
		require.True(t, ok, "type is %T", state)
		storeRecordRequestCount += 1

		require.Equal(t, tstate.QueryID, qid)
		require.NotNil(t, tstate.NodeID)
		require.NotNil(t, tstate.Message)
		if _, found := pending[tstate.NodeID]; !found {
			pending[tstate.NodeID] = []tiny.Message{}
		}
		pending[tstate.NodeID] = append(pending[tstate.NodeID], tstate.Message)
	}

	// assert that we're at capacity
	require.Len(t, pending, cfg.NodeConcurrency)
	require.Len(t, sm.inflightAtCapacity, cfg.NodeConcurrency)
	for _, messages := range pending {
		require.Len(t, messages, cfg.StreamConcurrency)
	}
	for _, inflight := range sm.inflightAtCapacity {
		require.Len(t, inflight.inflight, cfg.StreamConcurrency)
	}

	// because we're at capactiy another poll will return waiting.
	state := sm.Advance(ctx, &EventBroadcastPoll{})
	wstate, ok := state.(*StateBroadcastWaiting)
	require.True(t, ok, "type is %T", state)
	require.Equal(t, qid, wstate.QueryID)

	for {
		if len(pending) == 0 {
			break
		}

		var (
			node     tiny.Node
			messages []tiny.Message
		)

		for node, messages = range pending {
			break
		}

		var popped tiny.Message
		popped, pending[node] = messages[0], messages[1:]

		if len(pending[node]) == 0 {
			delete(pending, node)
		}

		n := new(big.Int)
		n.SetString(popped.Content, 16)
		state = sm.Advance(ctx, &EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
			NodeID:   node,
			Target:   tiny.Key(n.Int64()),
			Request:  popped,
			Response: popped,
		})

		tstate, ok := state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
		if !ok {
			continue
		}
		storeRecordRequestCount += 1
		if _, found := pending[tstate.NodeID]; !found {
			pending[tstate.NodeID] = []tiny.Message{}
		}
		pending[tstate.NodeID] = append(pending[tstate.NodeID], tstate.Message)
	}

	require.Equal(t, len(targets)*20, storeRecordRequestCount)
}
