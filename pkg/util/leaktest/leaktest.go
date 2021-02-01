// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package leaktest provides tools to detect leaked goroutines in tests.
// To use it, call "defer leaktest.AfterTest(t)()" at the beginning of each
// test that may use goroutines.
package leaktest

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
)

// interestingGoroutines returns all goroutines we care about for the purpose
// of leak checking. It excludes testing or runtime ones.
func interestingGoroutines() map[int64]string {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
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
			// Seems to be gccgo specific.
			(runtime.Compiler == "gccgo" && strings.Contains(stack, "testing.T.Parallel")) ||
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

// AfterTest snapshots the currently-running goroutines and returns a
// function to be run at the end of tests to see whether any
// goroutines leaked.
func AfterTest(t testing.TB) func() {
	// Try a best effort GC to help the race tests move along.
	runtime.GC()
	orig := interestingGoroutines()
	return func() {
		t.Helper()
		// If there was a panic, "leaked" goroutines are expected.
		if r := recover(); r != nil {
			atomic.StoreUint32(&leakDetectorDisabled, 1)
			// NB: we don't want to re-panic here, for the Go test
			// harness will not recover that for us. Instead, it will
			// stop running the tests, but we are deciding here to fail
			// only that one test but keep going otherwise.
			// We need to explicitly print the stack trace though.
			t.Fatalf("%v\n%s", r, debug.Stack())
			return
		}

		if atomic.LoadUint32(&leakDetectorDisabled) != 0 {
			return
		}

		// If the test already failed, we don't pile on any more errors but we check
		// to see if the leak detector should be disabled for future tests.
		if t.Failed() {
			if err := diffGoroutines(orig); err != nil {
				atomic.StoreUint32(&leakDetectorDisabled, 1)
			}
			return
		}

		PrintLeakedStoppers(t)

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
				t.Error(err)
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
