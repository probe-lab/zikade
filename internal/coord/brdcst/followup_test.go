package brdcst

import (
	"testing"

	"github.com/plprobelab/zikade/internal/tiny"
	"github.com/stretchr/testify/require"
)

func TestConfigFollowUp_Validate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultConfigFollowUp[tiny.Key](tiny.Key(0))
		require.NoError(t, cfg.Validate())
	})
}
