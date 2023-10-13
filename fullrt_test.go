package zikade

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewFullRT(t *testing.T) {
	cfg := &FullRTConfig{
		Config:        DefaultConfig(),
		CrawlInterval: 60 * time.Minute,
	}
	h := newTestHost(t)
	fullRT, err := NewFullRT(h, cfg)
	require.NoError(t, err)

	fullRT.Bootstrap(context.Background())

	time.Sleep(time.Hour)
}
