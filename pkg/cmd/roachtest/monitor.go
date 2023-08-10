// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

type monitorImpl struct {
	t interface {
		Fatal(...interface{})
		Failed() bool
		WorkerStatus(...interface{})
	}
	l            *logger.Logger
	nodes        string
	ctx          context.Context
	cancel       func()
	userGroup    *errgroup.Group
	monitorGroup *errgroup.Group
	monitorOnce  sync.Once

	numTasks  int32 // atomically
	expDeaths int32 // atomically
}

func newMonitor(
	ctx context.Context,
	t interface {
		Fatal(...interface{})
		Failed() bool
		WorkerStatus(...interface{})
		L() *logger.Logger
	},
	c cluster.Cluster,
	opts ...option.Option,
) *monitorImpl {
	m := &monitorImpl{
		t:     t,
		l:     t.L(),
		nodes: c.MakeNodes(opts...),
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.userGroup, m.ctx = errgroup.WithContext(m.ctx)
	m.monitorGroup, _ = errgroup.WithContext(m.ctx)
	err := m.startNodeMonitor()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// ExpectDeath lets the monitor know that a node is about to be killed, and that
// this should be ignored.
func (m *monitorImpl) ExpectDeath() {
	m.ExpectDeaths(1)
}

// ExpectDeaths lets the monitor know that a specific number of nodes are about
// to be killed, and that they should be ignored.
func (m *monitorImpl) ExpectDeaths(count int32) {
	atomic.AddInt32(&m.expDeaths, count)
}

func (m *monitorImpl) ResetDeaths() {
	atomic.StoreInt32(&m.expDeaths, 0)
}

var errTestFatal = errors.New("t.Fatal() was called")

func (m *monitorImpl) Go(fn func(context.Context) error) {
	atomic.AddInt32(&m.numTasks, 1)

	m.userGroup.Go(func() (err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			rErr, ok := r.(error)
			if !ok {
				rErr = errors.Errorf("recovered panic: %v", r)
			}
			// t.{Skip,Fatal} perform a panic(errTestFatal). If we've caught the
			// errTestFatal sentinel we transform the panic into an error return so
			// that the wrapped errgroup cancels itself. The "panic" will then be
			// returned by `m.WaitE()`.
			//
			// Note that `t.Fatal` calls `panic(err)`, so this mechanism primarily
			// enables that use case. But it also offers protection against accidental
			// panics (NPEs and such) which should not bubble up to the runtime.
			err = errors.WithStack(rErr)
		}()
		// Automatically clear the worker status message when the goroutine exits.
		defer m.t.WorkerStatus()
		return fn(m.ctx)
	})
}

// GoWithCancel is like Go, but returns a function that can be used to cancel
// the goroutine.
func (m *monitorImpl) GoWithCancel(fn func(context.Context) error) func() {
	ctx, cancel := context.WithCancel(m.ctx)
	m.Go(func(_ context.Context) error {
		return fn(ctx)
	})
	return cancel
}

func (m *monitorImpl) WaitE() error {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return errors.New("already failed")
	}

	return errors.Wrap(m.wait(), "monitor failure")
}

func (m *monitorImpl) Wait() {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := m.WaitE(); err != nil {
		// Note that we used to avoid fataling again if we had already fatal'ed.
		// However, this error here might be the one to actually report, see:
		// https://github.com/cockroachdb/cockroach/issues/44436
		m.t.Fatal(err)
	}
}

// startNodeMonitor will start a background function that monitors
// unexpected node deaths. To read errors coming from these events,
// callers are expected to call `Wait` or `WaitForNodeDeath`.
func (m *monitorImpl) startNodeMonitor() error {
	var retErr error

	m.monitorOnce.Do(func() {
		eventsCh, err := roachprod.Monitor(m.ctx, m.l, m.nodes, install.MonitorOpts{})
		if err != nil {
			m.cancel()
			retErr = errors.Wrap(err, "monitor command failure")
			return
		}

		m.monitorGroup.Go(func() error {
			defer m.cancel()

			for info := range eventsCh {
				_, isDeath := info.Event.(install.MonitorNodeDead)
				isExpectedDeath := isDeath && atomic.AddInt32(&m.expDeaths, -1) >= 0
				var expectedDeathStr string
				if isExpectedDeath {
					expectedDeathStr = ": expected"
				}
				m.l.Printf("Monitor event: %s%s", info, expectedDeathStr)

				if isDeath && !isExpectedDeath {
					return fmt.Errorf("unexpected node event: %s", info)
				}
			}

			return nil
		})
	})

	return retErr
}

// WaitForNodeDeath blocks while the monitor is active. Any errors due
// to unexpected node deaths are returned.
func (m *monitorImpl) WaitForNodeDeath() error {
	if err := m.startNodeMonitor(); err != nil {
		return err
	}
	return m.monitorGroup.Wait()
}

func (m *monitorImpl) wait() error {
	if err := m.startNodeMonitor(); err != nil {
		return err
	}

	userErr := m.userGroup.Wait()
	m.cancel()
	return errors.CombineErrors(userErr, m.WaitForNodeDeath())
}
