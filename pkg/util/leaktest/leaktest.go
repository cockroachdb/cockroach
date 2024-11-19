// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package leaktest provides tools to detect leaked goroutines in tests.
// To use it, call "defer leaktest.AfterTest(t)()" at the beginning of each
// test that may use goroutines.
package leaktest

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
)

// interestingGoroutines returns all goroutines we care about for the purpose
// of leak checking. It excludes testing or runtime ones.
func interestingGoroutines() map[int64]string {
	buf := allstacks.Get()
	gs := make(map[int64]string)
	for _, g := range strings.Split(string(buf), "\n\n") {
		sl := strings.SplitN(g, "\n", 2)
		if len(sl) != 2 {
			continue
		}
		stack := strings.TrimSpace(sl[1])
		if strings.HasPrefix(stack, "testing.RunTests") {
			continue
		}

		if stack == "" ||
			// Ignore HTTP keep alives
			strings.Contains(stack, ").readLoop(") ||
			strings.Contains(stack, ").writeLoop(") ||
			// Ignore the Sentry client, which is created lazily on first use.
			strings.Contains(stack, "sentry-go.(*HTTPTransport).worker") ||
			// Ignore the opensensus worker, which is created by the event exporter.
			strings.Contains(stack, "go.opencensus.io/stats/view.(*worker).start") ||
			// Ignore pgconn which creates a goroutine to do an async cleanup.
			strings.Contains(stack, "github.com/jackc/pgconn.(*PgConn).asyncClose.func1") ||
			// Ignore pgconn which creates a goroutine to watch context cancellation.
			strings.Contains(stack, "github.com/jackc/pgconn/internal/ctxwatch.(*ContextWatcher).Watch.func1") ||
			// Ignore pq goroutine that watches for context cancellation.
			strings.Contains(stack, "github.com/lib/pq.(*conn).watchCancel") ||
			// Ignore TLS handshake related goroutine.
			// TODO(pritesh-lahoti): Revisit this once Go is updated to 1.23, as this seems to have been
			// fixed: https://github.com/golang/go/pull/62227.
			strings.Contains(stack, "net/http.(*persistConn).addTLS") ||
			// Seems to be gccgo specific.
			(runtime.Compiler == "gccgo" && strings.Contains(stack, "testing.T.Parallel")) ||
			// Ignore intentionally long-running logging goroutines that live for the
			// duration of the process.
			strings.Contains(stack, "log.flushDaemon") ||
			strings.Contains(stack, "log.signalFlusher") ||
			// Below are the stacks ignored by the upstream leaktest code.
			strings.Contains(stack, "testing.Main(") ||
			strings.Contains(stack, "testing.tRunner(") ||
			strings.Contains(stack, "runtime.goexit") ||
			strings.Contains(stack, "created by runtime.gc") ||
			strings.Contains(stack, "interestingGoroutines") ||
			strings.Contains(stack, "runtime.MHeap_Scavenger") ||
			strings.Contains(stack, "signal.signal_recv") ||
			strings.Contains(stack, "sigterm.handler") ||
			strings.Contains(stack, "runtime_mcall") ||
			strings.Contains(stack, "goroutine in C code") ||
			strings.Contains(stack, "runtime.CPUProfile") {
			continue
		}
		gs[goid.ExtractGID([]byte(g))] = g
	}
	return gs
}

// Set once a test leaks goroutines so that further tests don't attempt to
// detect leaks any more. Once a tests leaks, it has soiled the process beyond
// repair: even though other tests would take a snapshot of goroutines at the
// beginning that would include the previously-leaked goroutines, those leaked
// goroutines can spin up other goroutines at random times and these would be
// mis-attributed as leaked by the currently-running test.
var leakDetectorDisabled uint32

// PrintLeakedStoppers is injected from `pkg/util/stop` to avoid a dependency
// cycle.
var PrintLeakedStoppers = func(t testing.TB) {}

// T allows failing tests.
type T interface {
	Errorf(fmt string, args ...interface{})
}

// AfterTest snapshots the currently-running goroutines and returns a
// function to be run at the end of tests to see whether any
// goroutines leaked.
func AfterTest(t T) func() {
	if atomic.LoadUint32(&leakDetectorDisabled) != 0 {
		return func() {}
	}

	orig := interestingGoroutines()
	return func() {
		if h, ok := t.(interface {
			Helper()
		}); ok {
			h.Helper()
		}
		// If there was a panic, "leaked" goroutines are expected.
		if r := recover(); r != nil {
			// Inhibit the leak detector for future tests, in case someone (insanely?)
			// recovers our re-panic below and continues running other tests. We're
			// likely leaving goroutines around, which may spawn more goroutines in
			// the middle of another test's execution and trip the leak detector for
			// that innocent test.
			atomic.StoreUint32(&leakDetectorDisabled, 1)
			t.Errorf("panic: %s", r)
			panic(r)
		}

		// If the test already failed, we don't pile on any more errors but we check
		// to see if the leak detector should be disabled for future tests.
		if f, ok := t.(interface {
			Failed() bool
			Skipped() bool
		}); ok {
			switch {
			case f.Failed():
				if err := diffGoroutines(orig); err != nil {
					atomic.StoreUint32(&leakDetectorDisabled, 1)
				}
				fallthrough
			case f.Skipped():
				return
			}
		}

		if tb, ok := t.(testing.TB); ok {
			PrintLeakedStoppers(tb)
		}

		// Loop, waiting for goroutines to shut down.
		// Wait up to 5 seconds, but finish as quickly as possible.
		deadline := timeutil.Now().Add(5 * time.Second)
		for {
			if err := diffGoroutines(orig); err != nil {
				if timeutil.Now().Before(deadline) {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				atomic.StoreUint32(&leakDetectorDisabled, 1)
				err = errors.Wrapf(err, "\nall stacks: \n\n%s\n", allstacks.Get())
				t.Errorf("%v", err)
			}
			break
		}
	}
}

// diffGoroutines compares the current goroutines with the base snapshort and
// returns an error if they differ.
func diffGoroutines(base map[int64]string) error {
	var leaked []string
	for id, stack := range interestingGoroutines() {
		if _, ok := base[id]; !ok {
			leaked = append(leaked, stack)
		}
	}
	if len(leaked) == 0 {
		return nil
	}

	sort.Strings(leaked)
	var b strings.Builder
	for _, g := range leaked {
		b.WriteString(fmt.Sprintf("Leaked goroutine: %v\n\n", g))
	}
	return errors.Newf("%s", b.String())
}
