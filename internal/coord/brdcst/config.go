package brdcst

import (
	"fmt"

	"github.com/plprobelab/go-libdht/kad"

	"github.com/plprobelab/zikade/internal/coord/query"
)

// ConfigPool specifies the configuration for a broadcast [Pool].
type ConfigPool struct {
	pCfg *query.PoolConfig
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (cfg *ConfigPool) Validate() error {
	if cfg.pCfg == nil {
		return fmt.Errorf("query pool config must not be nil")
	}

	return nil
}

// DefaultConfigPool returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultConfigPool() *ConfigPool {
	return &ConfigPool{
		pCfg: query.DefaultPoolConfig(),
	}
}

// Config is an interface that all broadcast configurations must implement.
// Because we have multiple ways of broadcasting records to the network, like
// [FollowUp] or [OneToMany], the [EventPoolStartBroadcast] has a configuration
// field that depending on the concrete type of [Config] initializes the
// respective state machine. Then the broadcast operation will be performed
// based on the encoded rules in that state machine.
type Config interface {
	broadcastConfig()
}

func (c *ConfigFollowUp[K]) broadcastConfig()   {}
func (c *ConfigOneToMany[K]) broadcastConfig()  {}
func (c *ConfigManyToMany[K]) broadcastConfig() {}

// ConfigFollowUp specifies the configuration for the [FollowUp] state machine.
type ConfigFollowUp[K kad.Key[K]] struct {
	Target K
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *ConfigFollowUp[K]) Validate() error {
	return nil
}

// DefaultConfigFollowUp returns the default configuration options for the
// [FollowUp] state machine.
func DefaultConfigFollowUp[K kad.Key[K]](target K) *ConfigFollowUp[K] {
	return &ConfigFollowUp[K]{
		Target: target,
	}
}

// ConfigOneToMany specifies the configuration for the [OneToMany] state
// machine.
type ConfigOneToMany[K kad.Key[K]] struct {
	Target K
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *ConfigOneToMany[K]) Validate() error {
	return nil
}

// DefaultConfigOneToMany returns the default configuration options for the
// [OneToMany] state machine.
func DefaultConfigOneToMany[K kad.Key[K]](target K) *ConfigOneToMany[K] {
	return &ConfigOneToMany[K]{
		Target: target,
	}
}

// ConfigManyToMany specifies the configuration for the [ManyToMany] state
// machine.
type ConfigManyToMany[K kad.Key[K]] struct {
	NodeConcurrency   int
	StreamConcurrency int
	Targets           []K
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *ConfigManyToMany[K]) Validate() error {
	if len(c.Targets) == 0 {
		return fmt.Errorf("targets must not be empty")
	}
	return nil
}

// DefaultConfigManyToMany returns the default configuration options for the
// [ManyToMany] state machine.
func DefaultConfigManyToMany[K kad.Key[K]](targets []K) *ConfigManyToMany[K] {
	return &ConfigManyToMany[K]{
		NodeConcurrency:   100, // MAGIC
		StreamConcurrency: 10,  // MAGIC
		Targets:           targets,
	}
}
