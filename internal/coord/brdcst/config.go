package brdcst

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
