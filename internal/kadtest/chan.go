package kadtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func ReadItem[T any](t testing.TB, ctx context.Context, c <-chan T) T {
	t.Helper()

	select {
	case val, more := <-c:
		require.True(t, more, "channel closed unexpectedly")
		return val
	case <-ctx.Done():
		t.Fatal("timeout reading item")
		return *new(T)
	}
}

// AssertClosed triggers a test failure if the given channel was not closed but
// carried more values or a timeout occurs (given by the context).
func AssertClosed[T any](t testing.TB, ctx context.Context, c <-chan T) {
	t.Helper()

	select {
	case _, more := <-c:
		assert.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout closing channel")
	}
}
