package coord

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNetworkConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultNetworkConfig()

		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultNetworkConfig()

		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultNetworkConfig()
		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer not nil", func(t *testing.T) {
		cfg := DefaultNetworkConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter is not nil", func(t *testing.T) {
		cfg := DefaultNetworkConfig()
		cfg.Meter = nil
		require.Error(t, cfg.Validate())
	})
}
