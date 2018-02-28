// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

	"github.com/petermattis/goid"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
			strings.Contains(stack, "github.com/cockroachdb/cockroach/pkg/util/log.init") ||
			strings.Contains(stack, "github.com/cockroachdb/cockroach/pkg/util/log.NewSecondaryLogger") ||
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

// AfterTest snapshots the currently-running goroutines and returns a
// function to be run at the end of tests to see whether any
// goroutines leaked.
func AfterTest(t testing.TB) func() {
	orig := interestingGoroutines()
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
			for id, stack := range interestingGoroutines() {
				if _, ok := orig[id]; !ok {
					leaked = append(leaked, stack)
				}
			}
			if len(leaked) == 0 {
				return
			}
			if timeutil.Now().Before(deadline) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			sort.Strings(leaked)
			for _, g := range leaked {
				t.Errorf("Leaked goroutine: %v", g)
			}
			return
		}
	}
}
