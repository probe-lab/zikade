package query

type QueryStrategy interface {
	queryStrategy()
}

type QueryStrategyConverge struct{}

func (q *QueryStrategyConverge) queryStrategy() {}

type QueryStrategyStatic struct{}

func (q *QueryStrategyStatic) queryStrategy() {}
