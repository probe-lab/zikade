package brdcst

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/internal/tiny"
)

func TestNewManyToMany(t *testing.T) {
	ctx := context.Background()

	targets := make([]tiny.Key, 0, 100)
	seed := make([]tiny.Node, 0, 100)
	for i := 0; i < 100; i++ {
		seed = append(seed, tiny.NewNode(tiny.Key(i+1)))
		targets = append(targets, tiny.Key(i+1))
	}

	msgFunc := func(k tiny.Key) tiny.Message {
		return tiny.Message{
			Content: k.String(),
		}
	}
	cfg := DefaultConfigManyToMany(targets)
	cfg.NodeConcurrency = 2
	cfg.StreamConcurrency = 5

	qid := coordt.QueryID("test")
	sm := NewManyToMany[tiny.Key, tiny.Node, tiny.Message](qid, msgFunc, seed, cfg)
	require.Len(t, sm.unprocessedNodes, 100)

	pending := map[tiny.Node][]tiny.Message{}
	for i := 0; i < cfg.NodeConcurrency*cfg.StreamConcurrency; i++ {
		state := sm.Advance(ctx, &EventBroadcastPoll{})
		tstate, ok := state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
		require.True(t, ok, "type is %T", state)
		require.Equal(t, tstate.QueryID, qid)
		require.NotNil(t, tstate.NodeID)
		require.NotNil(t, tstate.Message)
		if _, found := pending[tstate.NodeID]; !found {
			pending[tstate.NodeID] = []tiny.Message{}
		}
		pending[tstate.NodeID] = append(pending[tstate.NodeID], tstate.Message)
	}

	require.Len(t, pending, cfg.NodeConcurrency)
	require.Len(t, sm.inflightAtCapacity, cfg.NodeConcurrency)
	for _, messages := range pending {
		require.Len(t, messages, cfg.StreamConcurrency)
	}
	for _, inflight := range sm.inflightAtCapacity {
		require.Len(t, inflight.inflight, cfg.StreamConcurrency)
	}

	state := sm.Advance(ctx, &EventBroadcastPoll{})
	wstate, ok := state.(*StateBroadcastWaiting)
	require.True(t, ok, "type is %T", state)
	require.Equal(t, qid, wstate.QueryID)

	for node, messages := range pending {
		var popped tiny.Message
		popped, messages = messages[0], messages[1:]

		n := new(big.Int)
		n.SetString(popped.Content, 16)
		state = sm.Advance(ctx, &EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
			NodeID:   node,
			Target:   tiny.Key(n.Int64()),
			Request:  popped,
			Response: popped,
		})

		tstate, ok := state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
		require.True(t, ok, "type is %T", state)
		_ = tstate

		state := sm.Advance(ctx, &EventBroadcastPoll{})
		wstate, ok := state.(*StateBroadcastWaiting)
		require.True(t, ok, "type is %T", state)
		require.Equal(t, qid, wstate.QueryID)
	}
}
