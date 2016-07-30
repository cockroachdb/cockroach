// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package leaktest provides tools to detect leaked goroutines in tests.
// To use it, call "defer leaktest.AfterTest(t)()" at the beginning of each
// test that may use goroutines.
package leaktest

import (
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

var ignorestdlibleaks = envutil.EnvOrDefaultBool("ignorestdlibleaks", true)

// interestingGoroutines returns all goroutines we care about for the purpose
// of leak checking. It excludes testing or runtime ones.
func interestingGoroutines() (gs []string) {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
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
			strings.Contains(stack, "github.com/cockroachdb/cockroach/util/log.init") ||
			// It appears that the standard library TLS handshake code
			// contains a bug that can cause handshaking to deadlock. See
			// https://github.com/cockroachdb/cockroach/issues/8136.
			ignorestdlibleaks && strings.Contains(stack, "crypto/tls.(*Conn).clientHandshake") ||
			ignorestdlibleaks && strings.Contains(stack, "crypto/tls.(*Conn).serverHandshake") ||
			// GRPC's credentials do not properly cancel, so during shutdown
			// this goroutine leaks due to the standard library bug described
			// above. See https://github.com/grpc/grpc-go/pull/796 for a
			// possible solution to the cancellation problem in GRPC.
			ignorestdlibleaks && strings.Contains(stack, "google.golang.org/grpc/credentials.(*tlsCreds).ClientHandshake") ||
			// Go1.7 added a goroutine to network dialing that doesn't shut down
			// quickly.
			strings.Contains(stack, "created by net.(*netFD).connect") ||
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
			strings.Contains(stack, "goroutine in C code") {
			continue
		}
		gs = append(gs, g)
	}
	sort.Strings(gs)
	return
}

// AfterTest snapshots the currently-running goroutines and returns a
// function to be run at the end of tests to see whether any
// goroutines leaked.
func AfterTest(t testing.TB) func() {
	orig := map[string]bool{}
	for _, g := range interestingGoroutines() {
		orig[g] = true
	}
	return func() {
		if t.Failed() {
			return
		}
		if r := recover(); r != nil {
			panic(r)
		}
		// Loop, waiting for goroutines to shut down.
		// Wait up to 5 seconds, but finish as quickly as possible.
		deadline := timeutil.Now().Add(5 * time.Second)
		for {
			var leaked []string
			for _, g := range interestingGoroutines() {
				if !orig[g] {
					leaked = append(leaked, g)
				}
			}
			if len(leaked) == 0 {
				return
			}
			if timeutil.Now().Before(deadline) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			for _, g := range leaked {
				t.Errorf("Leaked goroutine: %v", g)
			}
			return
		}
	}
}
