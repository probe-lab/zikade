package routing

import (
	"context"
	"fmt"

	"github.com/plprobelab/go-libdht/kad"
	"github.com/plprobelab/go-libdht/kad/key"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/plprobelab/zikade/errs"
	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/tele"
)

// CrawlConfig specifies optional configuration for a Crawl
type CrawlConfig struct {
	MaxCPL      int          // the maximum CPL until we should crawl the peer
	Concurrency int          // the maximum number of concurrent peers that we may query
	Tracer      trace.Tracer // Tracer is the tracer that should be used to trace execution.
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *CrawlConfig) Validate() error {
	if cfg.MaxCPL < 1 {
		return &errs.ConfigurationError{
			Component: "CrawlConfig",
			Err:       fmt.Errorf("max cpl must be greater than zero"),
		}
	}

	if cfg.Concurrency < 1 {
		return &errs.ConfigurationError{
			Component: "CrawlConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}

	if cfg.Tracer == nil {
		return &errs.ConfigurationError{
			Component: "CrawlConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	return nil
}

// DefaultCrawlConfig returns the default configuration options for a Crawl.
// Options may be overridden before passing to NewCrawl
func DefaultCrawlConfig() *CrawlConfig {
	return &CrawlConfig{
		MaxCPL:      16,
		Concurrency: 1,
		Tracer:      tele.NoopTracer(),
	}
}

type Crawl[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	self N
	id   coordt.QueryID

	// cfg is a copy of the optional configuration supplied to the query
	cfg   CrawlConfig
	cplFn coordt.NodeIDForCplFunc[K, N]

	todo    []crawlJob[K, N]
	cpls    map[string]int
	waiting map[string]N
	success map[string]N
	failed  map[string]N
	errors  map[string]error
}

func NewCrawl[K kad.Key[K], N kad.NodeID[K], M coordt.Message](self N, id coordt.QueryID, cplFn coordt.NodeIDForCplFunc[K, N], seed []N, cfg *CrawlConfig) (*Crawl[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultCrawlConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	c := &Crawl[K, N, M]{
		self:    self,
		id:      id,
		cfg:     *cfg,
		cplFn:   cplFn,
		todo:    make([]crawlJob[K, N], 0, len(seed)*cfg.MaxCPL),
		cpls:    map[string]int{},
		waiting: map[string]N{},
		success: map[string]N{},
		failed:  map[string]N{},
		errors:  map[string]error{},
	}

	for _, node := range seed {
		// exclude self from closest nodes
		if key.Equal(node.Key(), self.Key()) {
			continue
		}

		for i := 0; i < c.cfg.MaxCPL; i++ {
			target, err := cplFn(node.Key(), i)
			if err != nil {
				return nil, fmt.Errorf("generate cpl: %w", err)
			}

			job := crawlJob[K, N]{
				node:   node,
				target: target.Key(),
			}

			c.cpls[job.mapKey()] = i
			c.todo = append(c.todo, job)
		}
	}

	if len(seed) == 0 {
		return nil, fmt.Errorf("empty seed")
	}

	return c, nil
}

func (c *Crawl[K, N, M]) Advance(ctx context.Context, ev CrawlEvent) (out CrawlState) {
	_, span := c.cfg.Tracer.Start(ctx, "Crawl.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	c.setMapSizes(span, "before")
	defer func() {
		c.setMapSizes(span, "after")
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	switch tev := ev.(type) {
	case *EventCrawlCancel:
		// TODO: ...
	case *EventCrawlNodeResponse[K, N]:
		span.SetAttributes(attribute.Int("closer_nodes", len(tev.CloserNodes)))

		job := crawlJob[K, N]{
			node:   tev.NodeID,
			target: tev.Target,
		}

		mapKey := job.mapKey()
		if _, found := c.waiting[mapKey]; !found {
			break
		}

		delete(c.waiting, mapKey)
		c.success[mapKey] = tev.NodeID

		for _, node := range tev.CloserNodes {
			for i := 0; i < c.cfg.MaxCPL; i++ {
				target, err := c.cplFn(node.Key(), i)
				if err != nil {
					// TODO: log
					continue
				}

				newJob := crawlJob[K, N]{
					node:   node,
					target: target.Key(),
				}

				newMapKey := newJob.mapKey()
				if _, found := c.cpls[newMapKey]; found {
					continue
				}

				c.cpls[newMapKey] = i
				c.todo = append(c.todo, newJob)
			}
		}

	case *EventCrawlNodeFailure[K, N]:
		span.RecordError(tev.Error)
		job := crawlJob[K, N]{
			node:   tev.NodeID,
			target: tev.Target,
		}

		mapKey := job.mapKey()
		if _, found := c.waiting[mapKey]; !found {
			break
		}

		delete(c.waiting, mapKey)
		c.failed[mapKey] = tev.NodeID
		c.errors[mapKey] = tev.Error

	case *EventCrawlPoll:
		// no event to process
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(c.waiting) >= c.cfg.MaxCPL*c.cfg.Concurrency {
		return &StateCrawlWaitingAtCapacity{
			QueryID: c.id,
		}
	}

	if len(c.todo) > 0 {

		// pop next crawl job from queue
		var job crawlJob[K, N]
		job, c.todo = c.todo[0], c.todo[1:]

		// mark the job as waiting
		mapKey := job.mapKey()
		c.waiting[mapKey] = job.node

		return &StateCrawlFindCloser[K, N]{
			QueryID: c.id,
			Target:  job.target,
			NodeID:  job.node,
		}
	}

	if len(c.waiting) > 0 {
		return &StateCrawlWaitingWithCapacity{
			QueryID: c.id,
		}
	}

	return &StateCrawlFinished{}
}

func (c *Crawl[K, N, M]) setMapSizes(span trace.Span, prefix string) {
	span.SetAttributes(
		attribute.Int(prefix+"_todo", len(c.todo)),
		attribute.Int(prefix+"_cpls", len(c.cpls)),
		attribute.Int(prefix+"_waiting", len(c.waiting)),
		attribute.Int(prefix+"_success", len(c.success)),
		attribute.Int(prefix+"_failed", len(c.failed)),
		attribute.Int(prefix+"_errors", len(c.errors)),
	)
}

func (c *Crawl[K, N, M]) mapKey(node N, target K) string {
	job := crawlJob[K, N]{node: node, target: target}
	return job.mapKey()
}

type crawlJob[K kad.Key[K], N kad.NodeID[K]] struct {
	node   N
	target K
}

func (c *crawlJob[K, N]) mapKey() string {
	return c.node.String() + key.HexString(c.target)
}

type CrawlState interface {
	crawlState()
}

type StateCrawlIdle struct{}

type StateCrawlFinished struct{}

type StateCrawlWaitingAtCapacity struct {
	QueryID coordt.QueryID
}
type StateCrawlWaitingWithCapacity struct {
	QueryID coordt.QueryID
}

type StateCrawlFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
}

// crawlState() ensures that only [Crawl] states can be assigned to a CrawlState.
func (*StateCrawlFinished) crawlState()            {}
func (*StateCrawlFindCloser[K, N]) crawlState()    {}
func (*StateCrawlWaitingAtCapacity) crawlState()   {}
func (*StateCrawlWaitingWithCapacity) crawlState() {}
func (*StateCrawlIdle) crawlState()                {}

type CrawlEvent interface {
	crawlEvent()
}

// EventCrawlPoll is an event that signals a [Crawl] that it can perform housekeeping work.
type EventCrawlPoll struct{}

type EventCrawlCancel struct{}

type EventCrawlNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	Target      K   // the key that the node was asked for
	CloserNodes []N // the closer nodes sent by the node
}

type EventCrawlNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Target K     // the key that the node was asked for
	Error  error // the error that caused the failure, if any
}

// crawlEvent() ensures that only events accepted by [Crawl] can be assigned to a [CrawlEvent].
func (*EventCrawlPoll) crawlEvent()               {}
func (*EventCrawlCancel) crawlEvent()             {}
func (*EventCrawlNodeResponse[K, N]) crawlEvent() {}
func (*EventCrawlNodeFailure[K, N]) crawlEvent()  {}
