package query

// Strategy is an interface that all query strategies need to implement.
// This ensures that only valid and supported strategies can be passed into
// the query behaviour/state machines.
type Strategy interface {
	queryStrategy()
}

func (q *StrategyConverge) queryStrategy() {}
func (q *StrategyStatic) queryStrategy()   {}

// StrategyConverge is used by default. In this case we are searching for ever
// closer nodes to a certain key and hence converging in the key space.
type StrategyConverge struct{}

// StrategyStatic is the alternative query strategy in which we just contact
// a static list of preconfigured nodes.
type StrategyStatic struct{}
