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
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

type monitorImpl struct {
	t interface {
		Fatal(...interface{})
		Failed() bool
		WorkerStatus(...interface{})
	}
	l         *logger.Logger
	nodes     string
	ctx       context.Context
	cancel    func()
	g         *errgroup.Group
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
	m.g, m.ctx = errgroup.WithContext(m.ctx)
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
	m.g.Go(func() (err error) {
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
			err = rErr
		}()
		// Automatically clear the worker status message when the goroutine exits.
		defer m.t.WorkerStatus()
		return fn(m.ctx)
	})
}

func (m *monitorImpl) WaitE() error {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return errors.New("already failed")
	}

	return errors.Wrap(m.wait(roachprod, "monitor", m.nodes), "monitor failure")
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

func (m *monitorImpl) wait(args ...string) error {
	// It is surprisingly difficult to get the cancellation semantics exactly
	// right. We need to watch for the "workers" group (m.g) to finish, or for
	// the monitor command to emit an unexpected node failure, or for the monitor
	// command itself to exit. We want to capture whichever error happens first
	// and then cancel the other goroutines. This ordering prevents the usage of
	// an errgroup.Group for the goroutines below. Consider:
	//
	//   g, _ := errgroup.WithContext(m.ctx)
	//   g.Go(func(context.Context) error {
	//     defer m.cancel()
	//     return m.g.Wait()
	//   })
	//
	// Now consider what happens when an error is returned. Before the error
	// reaches the errgroup, we invoke the cancellation closure which can cause
	// the other goroutines to wake up and perhaps race and set the errgroup
	// error first.
	//
	// The solution is to implement our own errgroup mechanism here which allows
	// us to set the error before performing the cancellation.

	var errOnce sync.Once
	var err error
	setErr := func(e error) {
		if e != nil {
			errOnce.Do(func() {
				err = e
			})
		}
	}

	// 1. The first goroutine waits for the worker errgroup to exit.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			m.cancel()
			wg.Done()
		}()
		setErr(errors.Wrap(m.g.Wait(), "monitor task failed"))
	}()

	setMonitorCmdErr := func(err error) {
		setErr(errors.Wrap(err, "monitor command failure"))
	}

	// 2. The second goroutine forks/execs the monitoring command.
	pipeR, pipeW := io.Pipe()
	wg.Add(1)
	go func() {
		defer func() {
			_ = pipeW.Close()
			wg.Done()
			// NB: we explicitly do not want to call m.cancel() here as we want the
			// goroutine that is reading the monitoring events to be able to decide
			// on the error if the monitoring command exits peacefully.
		}()

		monL, err := m.l.ChildLogger(`MONITOR`)
		if err != nil {
			setMonitorCmdErr(err)
			return
		}
		defer monL.Close()

		cmd := exec.CommandContext(m.ctx, args[0], args[1:]...)
		cmd.Stdout = io.MultiWriter(pipeW, monL.Stdout)
		cmd.Stderr = monL.Stderr
		if err := cmd.Run(); err != nil {
			if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "killed") {
				// The expected reason for an error is that the monitor was killed due
				// to the context being canceled. Any other error is an actual error.
				setMonitorCmdErr(err)
				return
			}
		}
		// Returning will cause the pipe to be closed which will cause the reader
		// goroutine to exit and close the monitoring channel.
	}()

	// 3. The third goroutine reads from the monitoring pipe, watching for any
	// unexpected death events.
	wg.Add(1)
	go func() {
		defer func() {
			_ = pipeR.Close()
			m.cancel()
			wg.Done()
		}()

		scanner := bufio.NewScanner(pipeR)
		for scanner.Scan() {
			msg := scanner.Text()
			var id int
			var s string
			if n, _ := fmt.Sscanf(msg, "%d: %s", &id, &s); n == 2 {
				if strings.Contains(s, "dead") && atomic.AddInt32(&m.expDeaths, -1) < 0 {
					setErr(fmt.Errorf("unexpected node event: %s", msg))
					return
				}
			}
		}
	}()

	wg.Wait()
	return err
}
