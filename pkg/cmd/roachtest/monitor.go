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
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
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

func (m *monitorImpl) wait() error {
	messagesChannel, err := roachprod.Monitor(m.ctx, m.nodes, install.MonitorOpts{})
	if err != nil {
		return err
	}
	var monitorErr error
	for msg := range messagesChannel {
		if msg.Err != nil {
			msg.Msg += "error: " + msg.Err.Error()
		}
		thisError := errors.Newf("%d: %s", msg.Node, msg.Msg)
		if msg.Err != nil || strings.Contains(msg.Msg, "dead") {
			monitorErr = errors.CombineErrors(monitorErr, thisError)
		}
		var id int
		var s string
		newMsg := thisError.Error()
		if n, _ := fmt.Sscanf(newMsg, "%d: %s", &id, &s); n == 2 {
			if strings.Contains(s, "dead") && atomic.AddInt32(&m.expDeaths, -1) < 0 {
				return fmt.Errorf("unexpected node event: %s", newMsg)
			}
		}
	}
	return nil
}
