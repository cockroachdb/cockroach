// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package leaktest provides tools to detect leaked goroutines in tests.
// To use it, call "defer leaktest.AfterTest(t)" at the beginning of each
// test that may use goroutines, and add a TestMain function for the package
// which calls leaktest.TestMainWithLeakCheck.
package leaktest

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var hasFailed int32 // updated atomially (though not necessary in normal usage)

// TestMainWithLeakCheck is an implementation of TestMain which verifies that
// there are no leaked goroutines at the end of the run (except those created
// by the system which are on a whitelist). Usage:
//
//
// // Adjust the relative path as needed.
// //go:generate ../util/leaktest/add-leaktest.sh *_test.go
//
// func TestMain(m *testing.M) {
//   leaktest.TestMainWithLeakCheck(m)
// }
func TestMainWithLeakCheck(m *testing.M) {
	v := m.Run()
	if v == 0 && goroutineLeaked() {
		os.Exit(1)
	}
	os.Exit(v)
}

func interestingGoroutines() (gs []string) {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		sl := strings.SplitN(g, "\n", 2)
		if len(sl) != 2 {
			continue
		}
		stack := strings.TrimSpace(sl[1])
		if stack == "" ||
			strings.Contains(stack, "created by google.golang.org/grpc.(*Server).Serve") ||
			strings.Contains(stack, "created by google.golang.org/grpc.NewConn") ||
			strings.Contains(stack, "created by google.golang.org/grpc/transport.newHTTP2Server") ||
			strings.Contains(stack, "created by net.startServer") ||
			strings.Contains(stack, "created by testing.RunTests") ||
			strings.Contains(stack, "closeWriteAndWait") ||
			strings.Contains(stack, "testing.Main(") ||
			// These only show up with GOTRACEBACK=2; Issue 5005 (comment 28)
			strings.Contains(stack, "runtime.goexit") ||
			strings.Contains(stack, "created by runtime.gc") ||
			strings.Contains(stack, "github.com/cockroachdb/cockroach/util/leaktest.interestingGoroutines") ||
			strings.Contains(stack, "runtime.MHeap_Scavenger") ||
			strings.Contains(stack, "github.com/cockroachdb/cockroach/util/log.init") ||
			strings.Contains(stack, "os/signal.loop") {
			continue
		}
		gs = append(gs, stack)
	}
	sort.Strings(gs)
	return
}

// Verify the other tests didn't leave any goroutines running.
func goroutineLeaked() bool {
	if testing.Short() {
		// not counting goroutines for leakage in -short mode
		return false
	}
	var stackCount map[string]int
	for i := 0; i < 8; i++ {
		gs := interestingGoroutines()

		n := 0
		stackCount = make(map[string]int)
		for _, g := range gs {
			stackCount[g]++
			n++
		}

		if n == 0 {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "Too many goroutines running after tests.\n")
	for stack, count := range stackCount {
		fmt.Fprintf(os.Stderr, "%d instances of:\n%s\n", count, stack)
	}
	return true
}

// AfterTest should be called (generally with "defer leaktest.AfterTest(t)")
// from each test which uses goroutines. This waits for all goroutines
// on a blacklist to terminate and provides more precise error reporting
// than TestMainWithLeakCheck alone.
// If a previous test's check has already failed, this is a noop (to avoid
// failing unrelated tests).
func AfterTest(t testing.TB) {
	if atomic.LoadInt32(&hasFailed) > 0 {
		t.Log("prior leak detected, leaktest disabled")
		return
	}
	if r := recover(); r != nil {
		// Don't bother with leaktest if we're recovering from a panic.
		panic(r)
	}
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	if testing.Short() || t.Failed() {
		// If a test has failed, chances are it hasn't shut down cleanly, so
		// stop checking for leaks in this run altogether.
		atomic.StoreInt32(&hasFailed, 1)
		return
	}
	if r := recover(); r != nil {
		panic(r)
	}
	var bad string
	badSubstring := map[string]string{
		").readLoop(":                                  "a Transport",
		").writeLoop(":                                 "a Transport",
		"created by net/http/httptest.(*Server).Start": "an httptest.Server",
		"timeoutHandler":                               "a TimeoutHandler",
		"net.(*netFD).connect(":                        "a timing out dial",
		").noteClientGone(":                            "a closenotifier sender",
		"created by net/rpc.NewClientWithCodec":        "an rpc client",
		"created by net/rpc.(*Server.ServeCodec)":      "an rpc server connection",
		"(*Store).Start":                               "a store",
		"(*Range).Send":                                "a range command",
	}
	var stacks string
	for i := 0; i < 8; i++ {
		bad = ""
		stacks = strings.Join(interestingGoroutines(), "\n\n")
		for substr, what := range badSubstring {
			if strings.Contains(stacks, substr) {
				bad = what
			}
		}
		if bad == "" {
			return
		}
		// Bad stuff found, but goroutines might just still be
		// shutting down, so give it some time.
		time.Sleep(10 * time.Millisecond)
	}
	atomic.StoreInt32(&hasFailed, 1)
	t.Errorf("Test appears to have leaked %s:\n%s", bad, stacks)
}
