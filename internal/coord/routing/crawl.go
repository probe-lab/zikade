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

// CrawlQueryID is the id for the query operated by the crawl state machine
const CrawlQueryID = coordt.QueryID("crawl")

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

type Crawl[K kad.Key[K], N kad.NodeID[K]] struct {
	self  N
	cfg   CrawlConfig
	cplFn coordt.NodeIDForCplFunc[K, N]
	info  *crawlInformation[K, N] // only set of crawl is in progress
}

type crawlInformation[K kad.Key[K], N kad.NodeID[K]] struct {
	todo    []crawlJob[K, N]
	cpls    map[string]int
	waiting map[string]N
	success map[string]N
	failed  map[string]N
	errors  map[string]error
}

func NewCrawl[K kad.Key[K], N kad.NodeID[K]](self N, cplFn coordt.NodeIDForCplFunc[K, N], cfg *CrawlConfig) (*Crawl[K, N], error) {
	if cfg == nil {
		cfg = DefaultCrawlConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	c := &Crawl[K, N]{
		self:  self,
		cfg:   *cfg,
		cplFn: cplFn,
		info:  nil,
	}

	return c, nil
}

func (c *Crawl[K, N]) Advance(ctx context.Context, ev CrawlEvent) (out CrawlState) {
	_, span := c.cfg.Tracer.Start(ctx, "Crawl.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	c.setMapSizes(span, "before")
	defer func() {
		c.setMapSizes(span, "after")
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	switch tev := ev.(type) {
	case *EventCrawlStart[K, N]:
		if c.info != nil {
			break // query in progress, pretend it was a poll
		}

		span.SetAttributes(attribute.Int("seed", len(tev.Seed)))

		ci := &crawlInformation[K, N]{
			todo:    []crawlJob[K, N]{},
			cpls:    map[string]int{},
			waiting: map[string]N{},
			success: map[string]N{},
			failed:  map[string]N{},
			errors:  map[string]error{},
		}

		for _, node := range tev.Seed {
			// exclude self from closest nodes
			if key.Equal(node.Key(), c.self.Key()) {
				continue
			}

			for j := 0; j < c.cfg.MaxCPL; j++ {
				target, err := c.cplFn(node.Key(), j)
				if err != nil {
					// return nil, fmt.Errorf("generate cpl: %w", err)
					// TODO: log
					continue
				}

				job := crawlJob[K, N]{
					node:   node,
					target: target.Key(),
				}

				ci.cpls[job.mapKey()] = j
				ci.todo = append(ci.todo, job)
			}
		}

		c.info = ci

	case *EventCrawlNodeResponse[K, N]:
		span.SetAttributes(attribute.Int("closer_nodes", len(tev.CloserNodes)))

		if c.info == nil {
			return &StateCrawlIdle{}
		}

		job := crawlJob[K, N]{
			node:   tev.NodeID,
			target: tev.Target,
		}

		mapKey := job.mapKey()
		if _, found := c.info.waiting[mapKey]; !found {
			break
		}

		delete(c.info.waiting, mapKey)
		c.info.success[mapKey] = tev.NodeID

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
				if _, found := c.info.cpls[newMapKey]; found {
					continue
				}

				c.info.cpls[newMapKey] = i
				c.info.todo = append(c.info.todo, newJob)
			}
		}

	case *EventCrawlNodeFailure[K, N]:
		if c.info == nil {
			return &StateCrawlIdle{}
		}

		span.RecordError(tev.Error)
		job := crawlJob[K, N]{
			node:   tev.NodeID,
			target: tev.Target,
		}

		mapKey := job.mapKey()
		if _, found := c.info.waiting[mapKey]; !found {
			break
		}

		delete(c.info.waiting, mapKey)
		c.info.failed[mapKey] = tev.NodeID
		c.info.errors[mapKey] = tev.Error

	case *EventCrawlPoll:
		// no event to process
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if c.info == nil {
		return &StateCrawlIdle{}
	}

	if len(c.info.waiting) >= c.cfg.MaxCPL*c.cfg.Concurrency {
		return &StateCrawlWaitingAtCapacity{}
	}

	if len(c.info.todo) > 0 {

		// pop next crawl job from queue
		var job crawlJob[K, N]
		job, c.info.todo = c.info.todo[0], c.info.todo[1:]

		// mark the job as waiting
		mapKey := job.mapKey()
		c.info.waiting[mapKey] = job.node

		return &StateCrawlFindCloser[K, N]{
			Target: job.target,
			NodeID: job.node,
		}
	}

	if len(c.info.waiting) > 0 {
		return &StateCrawlWaitingWithCapacity{}
	}

	c.info = nil
	return &StateCrawlFinished{}
}

func (c *Crawl[K, N]) setMapSizes(span trace.Span, prefix string) {
	if c.info == nil {
		return
	}

	span.SetAttributes(
		attribute.Int(prefix+"_todo", len(c.info.todo)),
		attribute.Int(prefix+"_cpls", len(c.info.cpls)),
		attribute.Int(prefix+"_waiting", len(c.info.waiting)),
		attribute.Int(prefix+"_success", len(c.info.success)),
		attribute.Int(prefix+"_failed", len(c.info.failed)),
		attribute.Int(prefix+"_errors", len(c.info.errors)),
	)
}

func (c *Crawl[K, N]) mapKey(node N, target K) string {
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

type StateCrawlWaitingAtCapacity struct{}
type StateCrawlWaitingWithCapacity struct{}

type StateCrawlFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	Target K // the key that the query wants to find closer nodes for
	NodeID N // the node to send the message to
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

// type EventCrawlCancel struct{} // TODO: implement

type EventCrawlStart[K kad.Key[K], N kad.NodeID[K]] struct {
	Seed []N
}

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
func (*EventCrawlPoll) crawlEvent() {}

// func (*EventCrawlCancel) crawlEvent()             {}
func (*EventCrawlStart[K, N]) crawlEvent()        {}
func (*EventCrawlNodeResponse[K, N]) crawlEvent() {}
func (*EventCrawlNodeFailure[K, N]) crawlEvent()  {}
