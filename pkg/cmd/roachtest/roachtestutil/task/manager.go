// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package task

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type (
	// Manager is responsible for managing a group of tasks initiated during
	// tests. The interface is designed for the test framework to control tasks.
	// Typically, tests will only interact, and be provided with the smaller Group
	// and Tasker interfaces to start tasks or wait on groups of tasks.
	Manager interface {
		Tasker
		GroupProvider
		Terminate(*logger.Logger)
		CompletedEvents() <-chan Event
	}

	// Event represents the result of a task execution.
	Event struct {
		Name            string
		Err             error
		TriggeredByTest bool
	}

	manager struct {
		ctx    context.Context
		logger *logger.Logger
		events chan Event
		id     atomic.Uint32
		group  *group
	}

	group struct {
		manager  *manager
		options  []Option
		ctxGroup ctxgroup.Group
		groupMu  struct {
			syncutil.Mutex
			groups []*group
		}
		cancelMu struct {
			syncutil.Mutex
			cancelFns []context.CancelFunc
		}
	}
)

// NewManager creates a new Manager. The context passed to the manager is used
// to control the lifetime of all tasks started by the manager. The logger is
// the default logger used by all tasks started by the manager.
func NewManager(ctx context.Context, l *logger.Logger) Manager {
	m := &manager{
		ctx:    ctx,
		logger: l,
		events: make(chan Event),
	}
	m.group = &group{
		manager:  m,
		options:  m.defaultOptions(),
		ctxGroup: ctxgroup.WithContext(ctx),
	}
	return m
}

func (m *manager) defaultOptions() []Option {
	// The default panic handler simply returns the panic as an error.
	defaultPanicHandlerFn := func(_ context.Context, name string, l *logger.Logger, r interface{}) error {
		return fmt.Errorf("panic: %v", r)
	}
	// The default error handler simply returns the error as is.
	defaultErrorHandlerFn := func(_ context.Context, name string, l *logger.Logger, err error) error {
		return err
	}
	return []Option{
		Logger(m.logger),
		PanicHandler(defaultPanicHandlerFn),
		ErrorHandler(defaultErrorHandlerFn),
	}
}

// Terminate will call the stop functions for every task started during the
// test. Returns when all task functions have returned, or after a 5-minute
// timeout, whichever comes first. If the timeout is reached, the function logs
// a warning message and returns.
func (m *manager) Terminate(l *logger.Logger) {
	m.group.cancelAll()

	doneCh := make(chan error)
	go func() {
		defer close(doneCh)
		m.group.Wait()
	}()

	WaitForChannel(doneCh, "tasks", l)
}

// CompletedEvents returns a channel that will receive events for all tasks
// started by the manager.
func (m *manager) CompletedEvents() <-chan Event {
	return m.events
}

// NewGroup creates a new group of tasks as a subgroup under the manager's
// default group.
func (m *manager) NewGroup(opts ...Option) Group {
	return m.group.NewGroup(opts...)
}

// NewErrorGroup creates a new error group of tasks as a subgroup under the
// manager's default group.
func (m *manager) NewErrorGroup(opts ...Option) ErrorGroup {
	return m.group.NewErrorGroup(opts...)
}

// GoWithCancel runs GoWithCancel on the manager's default group.
func (m *manager) GoWithCancel(fn Func, opts ...Option) context.CancelFunc {
	return m.group.GoWithCancel(fn, opts...)
}

// Go runs Go on the manager's default group.
func (m *manager) Go(fn Func, opts ...Option) {
	_ = m.group.GoWithCancel(fn, opts...)
}

// NewGroup implements the GroupProvider interface.
func (t *group) NewGroup(opts ...Option) Group {
	return t.newGroupInternal(OptionList(opts...))
}

// NewErrorGroup implements the GroupProvider interface.
func (t *group) NewErrorGroup(opts ...Option) ErrorGroup {
	return t.newGroupInternal(OptionList(opts...), DisableReporting())
}

// newGroupInternal creates a new group with the provided options. The options
// passed to this function are combined with this group and passed to the new
// group.
func (t *group) newGroupInternal(opts ...Option) *group {
	newOpts := append(t.options, opts...)
	opt := CombineOptions(newOpts...)
	ctx := opt.Context
	if ctx == nil {
		ctx = t.manager.ctx
	}
	subgroup := &group{
		manager:  t.manager,
		options:  newOpts,
		ctxGroup: ctxgroup.WithContext(ctx),
	}
	t.groupMu.Lock()
	defer t.groupMu.Unlock()
	t.groupMu.groups = append(t.groupMu.groups, subgroup)
	return subgroup
}

// GoWithCancel implements the Tasker interface.
func (t *group) GoWithCancel(fn Func, opts ...Option) context.CancelFunc {
	// Combine options in order of precedence: default options, task options, and
	// options passed to GoWithCancel. Always supply a default unique name for the
	// task, in case the user did not provide one.
	opt := CombineOptions(
		Name(fmt.Sprintf("task-%d", t.manager.id.Add(1))),
		OptionList(t.options...),
		OptionList(opts...),
	)
	parentCtx := opt.Context
	if parentCtx == nil {
		parentCtx = t.manager.ctx
	}
	groupCtx, cancel := context.WithCancel(parentCtx)
	var expectedContextCancellation atomic.Bool

	// internalFunc is a wrapper around the user-provided function that
	// handles panics and errors.
	internalFunc := func(l *logger.Logger) (retErr error) {
		defer func() {
			if r := recover(); r != nil {
				retErr = opt.PanicHandler(groupCtx, opt.Name, l, r)
			}
			retErr = opt.ErrorHandler(groupCtx, opt.Name, l, retErr)
		}()
		retErr = fn(groupCtx, l)
		return retErr
	}

	t.ctxGroup.Go(func() error {
		l, err := opt.L(opt.Name)
		if err != nil {
			return err
		}
		err = internalFunc(l)
		event := Event{
			Name: opt.Name,
			Err:  err,
			// TriggeredByTest is set to true if the task was canceled intentionally,
			// by the test, and we encounter an error. The assumption is that we
			// expect the error to have been caused by the cancelation, hence the
			// error above was not caused by a failure. This ensures we don't register
			// a test failure if the task was meant to be canceled. It's possible that
			// `expectedContextCancellation` could be set before the context is
			// canceled, thus we also ensure that the context is canceled.
			TriggeredByTest: err != nil && IsContextCanceled(groupCtx) && expectedContextCancellation.Load(),
		}

		// Do not send the event if the parent context is canceled. The test is
		// already aware of the cancelation and sending an event would be redundant.
		// For instance, a call to test.Fatal would already have captured the error
		// and canceled the context.
		if IsContextCanceled(t.manager.ctx) {
			return nil
		}
		if !opt.DisableReporting {
			t.manager.events <- event
		}
		return err
	})

	taskCancelFn := func() {
		expectedContextCancellation.Store(true)
		cancel()
	}
	// Collect all taskCancelFn(s) so that we can explicitly stop all tasks when
	// the tasker is terminated.
	t.cancelMu.Lock()
	defer t.cancelMu.Unlock()
	t.cancelMu.cancelFns = append(t.cancelMu.cancelFns, taskCancelFn)
	return taskCancelFn
}

// Go implements the Tasker interface.
func (t *group) Go(fn Func, opts ...Option) {
	_ = t.GoWithCancel(fn, opts...)
}

// cancelAll cancels all tasks in the group.
func (t *group) cancelAll() {
	func() {
		t.cancelMu.Lock()
		defer t.cancelMu.Unlock()
		for _, cancel := range t.cancelMu.cancelFns {
			cancel()
		}
	}()
	t.groupMu.Lock()
	defer t.groupMu.Unlock()
	for _, g := range t.groupMu.groups {
		g.cancelAll()
	}
}

// Wait implements the Group interface.
func (t *group) Wait() {
	_ = t.WaitE()
}

// WaitE implements the ErrorGroup interface.
func (t *group) WaitE() error {
	var err error
	t.groupMu.Lock()
	defer t.groupMu.Unlock()
	err = t.ctxGroup.Wait()
	for _, g := range t.groupMu.groups {
		err = errors.CombineErrors(g.WaitE(), err)
	}
	return err
}
