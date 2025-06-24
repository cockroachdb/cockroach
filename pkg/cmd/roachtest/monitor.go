// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// monitorProcess represents a single process that the monitor monitors.
type monitorProcess struct {
	node               install.Node
	virtualClusterName string
	sqlInstance        int
}

// MonitorExpectedProcessHealth represents the expected health of a process.
type MonitorExpectedProcessHealth string

const (
	ExpectedAlive = MonitorExpectedProcessHealth("process alive")
	ExpectedDead  = MonitorExpectedProcessHealth("process dead")
)

// expectedProcessHealth is a concurrent map that stores the expected health of
// each registered monitorProcess. It is a thin wrapper over syncutil.Map that ensures
// consistent naming of the system interface.
type expectedProcessHealth struct {
	syncutil.Map[monitorProcess, MonitorExpectedProcessHealth]
}

func newProcess(node install.Node, virtualClusterName string, sqlInstance int) monitorProcess {
	if virtualClusterName == "" {
		virtualClusterName = install.SystemInterfaceName
	}
	return monitorProcess{
		node:               node,
		virtualClusterName: virtualClusterName,
		sqlInstance:        sqlInstance,
	}
}

func (m *expectedProcessHealth) get(
	node install.Node, virtualClusterName string, sqlInstance int,
) MonitorExpectedProcessHealth {
	val, ok := m.Load(newProcess(node, virtualClusterName, sqlInstance))
	if !ok {
		// If the process has no expected state, assume it should be healthy.
		return ExpectedAlive
	}
	return *val
}

func (m *expectedProcessHealth) set(
	nodes install.Nodes,
	virtualClusterName string,
	sqlInstance int,
	health MonitorExpectedProcessHealth,
) {
	for _, node := range nodes {
		m.Store(newProcess(node, virtualClusterName, sqlInstance), &health)
	}
}

// monitorImpl implements the Monitor interface. A monitor both
// manages "user tasks" -- goroutines provided by tests -- as well as
// checks that every node in the cluster is still running. A failure
// in a user task or an unexpected node death should cause all
// goroutines managed by the monitor (user tasks or internal
// monitoring) to finish, providing semantics similar to an
// `errgroup`. The actual implementation uses two different errgroups
// (one for node events, and another one for user tasks). This is to
// support the use-case where callers wish to monitor exclusively for
// node events even if they do not have a goroutine (user task) to be
// run.
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
	userGroup    *errgroup.Group // user-provided functions
	monitorGroup *errgroup.Group // monitor goroutine
	monitorOnce  sync.Once       // guarantees monitor goroutine is only started once

	// expExactProcessDeath if true indicates that the monitor should expect that a
	// specified process, as denoted by the triple in expProcessHealth.get, is dead.
	// Otherwise, the monitor will expect that only a certain number of process deaths.
	// The former is a stronger assertion used in the new global roachtest monitor,
	// while the latter should be removed when the deprecated cluster monitor is removed.
	expExactProcessDeath bool
	// Deprecated: This field is used by the deprecated cluster monitor to track the number
	// of expected process deaths, and should be removed when the cluster monitor is removed.
	expDeaths        int32 // atomically
	expProcessHealth expectedProcessHealth
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
	expectExactProcessDeath bool,
	opts ...option.Option,
) *monitorImpl {
	m := &monitorImpl{
		t:                    t,
		l:                    t.L(),
		nodes:                c.MakeNodes(opts...),
		expExactProcessDeath: expectExactProcessDeath,
		expProcessHealth:     expectedProcessHealth{},
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.userGroup, _ = errgroup.WithContext(m.ctx)
	m.monitorGroup, _ = errgroup.WithContext(m.ctx)
	return m
}

func (m *monitorImpl) ExpectProcessHealth(
	nodes install.Nodes, health MonitorExpectedProcessHealth, opts ...option.OptionFunc,
) {
	var virtualClusterOptions option.VirtualClusterOptions
	if err := option.Apply(&virtualClusterOptions, opts...); err != nil {
		m.t.Fatal(err)
	}
	m.expProcessHealth.set(nodes, virtualClusterOptions.VirtualClusterName, virtualClusterOptions.SQLInstance, health)
}

// ExpectProcessDeath lets the monitor know that a set of processes are about
// to be killed, and that their deaths should be ignored. Virtual cluster
// options can be passed to denote a separate process.
func (m *monitorImpl) ExpectProcessDead(nodes option.NodeListOption, opts ...option.OptionFunc) {
	m.ExpectProcessHealth(nodes.InstallNodes(), ExpectedDead, opts...)
}

// ExpectProcessAlive lets the monitor know that a set of processes are
// expected to be healthy. Virtual cluster options can be passed to denote
// a separate process.
func (m *monitorImpl) ExpectProcessAlive(nodes option.NodeListOption, opts ...option.OptionFunc) {
	m.ExpectProcessHealth(nodes.InstallNodes(), ExpectedAlive, opts...)
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
			err = errors.Wrap(errors.WithStack(rErr), "monitor user task failed")
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

// WaitE will wait for errors coming from user-tasks or from the node
// monitoring goroutine.
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
func (m *monitorImpl) startNodeMonitor() {
	m.monitorOnce.Do(func() {
		m.monitorGroup.Go(func() error {
			defer m.cancel() // stop user-tasks

			eventsCh, err := roachprod.Monitor(m.ctx, m.l, m.nodes, install.MonitorOpts{})
			if err != nil {
				return errors.Wrap(err, "monitor node command failure")
			}

			for info := range eventsCh {
				var expectedDeathStr string
				var retErr error

				switch e := info.Event.(type) {
				case install.MonitorError:
					if !errors.Is(e.Err, install.MonitorNoCockroachProcessesError) {
						// Monitor errors should only occur when something went
						// wrong in the monitor logic itself or test infrastructure
						// (SSH flakes, VM preemption, etc). These should be sent
						// directly to TestEng.
						//
						// NOTE: we ignore `MonitorNoCockroachProcessesError` as a
						// lot of monitors uses in current tests would fail with
						// this error if we returned it and current uses are
						// harmless.  In the future, the monitor should be able to
						// detect when new cockroach processes start running on a
						// node instead of assuming that the processes are running
						// when the monitor starts.
						retErr = registry.ErrorWithOwner(
							registry.OwnerTestEng,
							e.Err,
							registry.WithTitleOverride("monitor_failure"),
							registry.InfraFlake,
						)
					}
				case install.MonitorProcessDead:
					var isExpectedDeath bool
					if m.expExactProcessDeath {
						isExpectedDeath = m.expProcessHealth.get(info.Node, e.VirtualClusterName, e.SQLInstance) == ExpectedDead
					} else {
						isExpectedDeath = atomic.AddInt32(&m.expDeaths, -1) >= 0
					}

					if isExpectedDeath {
						expectedDeathStr = ": expected"
					} else {
						retErr = fmt.Errorf("unexpected node event: %s", info)
					}
				}

				m.l.Printf("Monitor event: %s%s", info, expectedDeathStr)
				if retErr != nil {
					return retErr
				}
			}

			return nil
		})
	})
}

// WaitForNodeDeath blocks while the monitor is active. Any errors due
// to unexpected node deaths are returned.
func (m *monitorImpl) WaitForNodeDeath() error {
	m.startNodeMonitor()
	return m.monitorGroup.Wait()
}

func (m *monitorImpl) wait() error {
	m.startNodeMonitor()
	userErr := m.userGroup.Wait()
	m.cancel() // stop monitoring goroutine
	// By canceling the monitor context above, the goroutines created by
	// `roachprod.Monitor` should terminate the session and close the
	// event channel that the monitoring goroutine is reading from. We
	// wait for that goroutine to return to ensure that we don't leak
	// goroutines after wait() returns.
	monitorErr := m.WaitForNodeDeath()

	return errors.Join(userErr, monitorErr)
}
