package coord

import (
	"context"
	"sync"
	"sync/atomic"
)

// Notify is the interface that a components to implement to be notified of
// [BehaviourEvent]'s.
type Notify[E BehaviourEvent] interface {
	Notify(ctx context.Context, ev E)
}

type NotifyCloser[E BehaviourEvent] interface {
	Notify[E]
	Close()
}

type NotifyFunc[E BehaviourEvent] func(ctx context.Context, ev E)

func (f NotifyFunc[E]) Notify(ctx context.Context, ev E) {
	f(ctx, ev)
}

type Behaviour[I BehaviourEvent, O BehaviourEvent] interface {
	// Ready returns a channel that signals when the behaviour is ready to perform work.
	Ready() <-chan struct{}

	// Notify informs the behaviour of an event. The behaviour may perform the event
	// immediately and queue the result, causing the behaviour to become ready.
	// It is safe to call Notify from the Perform method.
	Notify(ctx context.Context, ev I)

	// Perform gives the behaviour the opportunity to perform work or to return a queued
	// result as an event.
	Perform(ctx context.Context) (O, bool)
}

type WorkQueueFunc[E BehaviourEvent] func(context.Context, E) bool

// WorkQueue is buffered queue of work to be performed.
// The queue automatically drains the queue sequentially by calling a
// WorkQueueFunc for each work item, passing the original context
// and event.
type WorkQueue[E BehaviourEvent] struct {
	pending chan CtxEvent[E]
	fn      WorkQueueFunc[E]
	done    atomic.Bool
	once    sync.Once
}

func NewWorkQueue[E BehaviourEvent](fn WorkQueueFunc[E]) *WorkQueue[E] {
	w := &WorkQueue[E]{
		pending: make(chan CtxEvent[E], 1),
		fn:      fn,
	}
	return w
}

// CtxEvent holds and event with an associated context which may carry deadlines or
// tracing information pertinent to the event.
type CtxEvent[E any] struct {
	Ctx   context.Context
	Event E
}

// Enqueue queues work to be perfomed. It will block if the
// queue has reached its maximum capacity for pending work. While
// blocking it will return a context cancellation error if the work
// item's context is cancelled.
func (w *WorkQueue[E]) Enqueue(ctx context.Context, cmd E) error {
	if w.done.Load() {
		return nil
	}
	w.once.Do(func() {
		go func() {
			defer w.done.Store(true)
			for cc := range w.pending {
				if cc.Ctx.Err() != nil {
					return
				}
				if done := w.fn(cc.Ctx, cc.Event); done {
					w.done.Store(true)
					return
				}
			}
		}()
	})

	select {
	case <-ctx.Done(): // this is the context for the work item
		return ctx.Err()
	case w.pending <- CtxEvent[E]{
		Ctx:   ctx,
		Event: cmd,
	}:
		return nil

	}
}

// A Waiter is a Notifiee whose Notify method forwards the
// notified event to a channel which a client can wait on.
type Waiter[E BehaviourEvent] struct {
	pending chan WaiterEvent[E]
	done    atomic.Bool
}

var _ Notify[BehaviourEvent] = (*Waiter[BehaviourEvent])(nil)

func NewWaiter[E BehaviourEvent]() *Waiter[E] {
	w := &Waiter[E]{
		pending: make(chan WaiterEvent[E], 1),
	}
	return w
}

type WaiterEvent[E BehaviourEvent] struct {
	Ctx   context.Context
	Event E
}

func (w *Waiter[E]) Notify(ctx context.Context, ev E) {
	if w.done.Load() {
		return
	}
	select {
	case <-ctx.Done(): // this is the context for the work item
		return
	case w.pending <- WaiterEvent[E]{
		Ctx:   ctx,
		Event: ev,
	}:
		return

	}
}

// Close signals that the waiter should not forward and further calls to Notify.
// It closes the waiter channel so a client selecting on it will receive the close
// operation.
func (w *Waiter[E]) Close() {
	w.done.Store(true)
	close(w.pending)
}

func (w *Waiter[E]) Chan() <-chan WaiterEvent[E] {
	return w.pending
}

// A QueryMonitor receives event notifications on the progress of a query
type QueryMonitor[E TerminalQueryEvent] interface {
	// NotifyProgressed returns a channel that can be used to send notification that a
	// query has made progress. If the notification cannot be sent then it will be
	// queued and retried at a later time. If the query completes before the progress
	// notification can be sent the notification will be discarded.
	NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed]

	// NotifyFinished returns a channel that can be used to send the notification that a
	// query has completed. It is up to the implemention to ensure that the channel has enough
	// capacity to receive the single notification.
	// The sender must close all other QueryNotifier channels before sending on the NotifyFinished channel.
	// The sender may attempt to drain any pending notifications before closing the other channels.
	// The NotifyFinished channel will be closed once the sender has attempted to send the Finished notification.
	NotifyFinished() chan<- CtxEvent[E]
}

// QueryMonitorHook wraps a [QueryMonitor] interface and provides hooks
// that are invoked before calls to the QueryMonitor methods are forwarded.
type QueryMonitorHook[E TerminalQueryEvent] struct {
	qm               QueryMonitor[E]
	BeforeProgressed func()
	BeforeFinished   func()
}

var _ QueryMonitor[*EventQueryFinished] = (*QueryMonitorHook[*EventQueryFinished])(nil)

func NewQueryMonitorHook[E TerminalQueryEvent](qm QueryMonitor[E]) *QueryMonitorHook[E] {
	return &QueryMonitorHook[E]{
		qm:               qm,
		BeforeProgressed: func() {},
		BeforeFinished:   func() {},
	}
}

func (n *QueryMonitorHook[E]) NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed] {
	n.BeforeProgressed()
	return n.qm.NotifyProgressed()
}

func (n *QueryMonitorHook[E]) NotifyFinished() chan<- CtxEvent[E] {
	n.BeforeFinished()
	return n.qm.NotifyFinished()
}
