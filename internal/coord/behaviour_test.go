package coord

import (
	"context"
	"testing"
)

type RecordingSM[E any, S any] struct {
	State    S
	Received []E
}

func NewRecordingSM[E any, S any](response S) *RecordingSM[E, S] {
	return &RecordingSM[E, S]{
		State: response,
	}
}

func (r *RecordingSM[E, S]) Advance(ctx context.Context, e E) S {
	r.Received = append(r.Received, e)
	return r.State
}

func (r *RecordingSM[E, S]) first() E {
	if len(r.Received) == 0 {
		var zero E
		return zero
	}
	return r.Received[0]
}

func DrainBehaviour[I BehaviourEvent, O BehaviourEvent](t *testing.T, ctx context.Context, b Behaviour[I, O]) {
	for {
		select {
		case <-b.Ready():
			b.Perform(ctx)
		case <-ctx.Done():
			t.Fatal("context cancelled while draining behaviour")
		default:
			return
		}
	}
}
