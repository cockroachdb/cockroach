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
)

type (
	// Manager is responsible for managing a group of tasks initiated during
	// tests. The interface is designed for the test framework to control tasks.
	// Typically, tests will only interact, and be provided with the smaller
	// Tasker interface to start tasks.
	Manager interface {
		Tasker
		Terminate(*logger.Logger)
		CompletedEvents() <-chan Event
	}

	// Event represents the result of a task execution.
	Event struct {
		Name           string
		Err            error
		ExpectedCancel bool
	}

	manager struct {
		group     ctxgroup.Group
		ctx       context.Context
		logger    *logger.Logger
		events    chan Event
		id        atomic.Uint32
		cancelFns []context.CancelFunc
	}
)

func NewManager(ctx context.Context, l *logger.Logger) Manager {
	g := ctxgroup.WithContext(ctx)
	return &manager{
		group:  g,
		ctx:    ctx,
		logger: l,
		events: make(chan Event),
	}
}

func (m *manager) defaultOptions() []Option {
	// The default panic handler simply returns the panic as an error.
	defaultPanicHandlerFn := func(_ context.Context, name string, l *logger.Logger, r interface{}) error {
		return r.(error)
	}
	// The default error handler simply returns the error as is.
	defaultErrorHandlerFn := func(_ context.Context, name string, l *logger.Logger, err error) error {
		return err
	}
	return []Option{
		Name(fmt.Sprintf("task-%d", m.id.Add(1))),
		Logger(m.logger),
		PanicHandler(defaultPanicHandlerFn),
		ErrorHandler(defaultErrorHandlerFn),
	}
}

func (m *manager) GoWithCancel(fn Func, opts ...Option) context.CancelFunc {
	opt := CombineOptions(OptionList(m.defaultOptions()...), OptionList(opts...))
	groupCtx, cancel := context.WithCancel(m.ctx)
	var expectedContextCancellation bool

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

	m.group.Go(func() error {
		l, err := opt.L(opt.Name)
		if err != nil {
			return err
		}
		err = internalFunc(l)
		event := Event{
			Name:           opt.Name,
			Err:            err,
			ExpectedCancel: err != nil && IsContextCanceled(groupCtx) && expectedContextCancellation,
		}
		select {
		case m.events <- event:
			// exit goroutine
		case <-m.ctx.Done():
			// Parent context already finished, exit goroutine.
			return nil
		}
		return err
	})

	taskCancelFn := func() {
		expectedContextCancellation = true
		cancel()
	}
	// Collect all taskCancelFn(s) so that we can explicitly stop all tasks when
	// the tasker is terminated.
	m.cancelFns = append(m.cancelFns, taskCancelFn)
	return taskCancelFn
}

func (m *manager) Go(fn Func, opts ...Option) {
	_ = m.GoWithCancel(fn, opts...)
}

// Terminate will call the stop functions for every task started during the
// test. Returns when all task functions have returned.
func (m *manager) Terminate(l *logger.Logger) {
	for _, cancel := range m.cancelFns {
		cancel()
	}

	doneCh := make(chan error)
	go func() {
		defer close(doneCh)
		_ = m.group.Wait()
	}()

	WaitForChannel(doneCh, "tasks", l)
}

func (m *manager) CompletedEvents() <-chan Event {
	return m.events
}
