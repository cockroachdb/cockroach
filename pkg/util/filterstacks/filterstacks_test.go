// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package filterstacks

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

// spawnTestGoroutines may be used to spawn goroutines with specific stacks at
// runtime. The caller provides slices of testFuncs (see a, b, c, d and e
// defined below). For each slice, spawnTestGoroutines will launch one goroutine
// that contains each of the provided functions in its call stack.
func spawnTestGoroutines(testStacks ...[]testFunc) func() {
	ch := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(testStacks))
	for _, stack := range testStacks {
		if len(stack) == 0 {
			continue
		}
		go func(stack []testFunc) {
			stack[0](&wg, ch, stack[1:]...)
		}(stack)
	}
	// Wait for all the spawned goroutines to get to their leaf stack frames.
	wg.Wait()
	return func() { close(ch) }
}

type testFunc func(*sync.WaitGroup, chan struct{}, ...testFunc)

func testFuncImpl(wg *sync.WaitGroup, ch chan struct{}, testFuncs ...testFunc) {
	if len(testFuncs) == 0 {
		// This is a leaf stack frame. Block until the channel is closed.
		wg.Done()
		<-ch
		return
	}
	// Call the next testFunc.
	testFuncs[0](wg, ch, testFuncs[1:]...)
}

// Named test funcs: These names are used within the datadriven tests to allow
// the datadriven tests to construct goroutines with these functions within
// their stacks, and then filter based on their names.

func a(wg *sync.WaitGroup, ch chan struct{}, tfs ...testFunc) { testFuncImpl(wg, ch, tfs...) }
func b(wg *sync.WaitGroup, ch chan struct{}, tfs ...testFunc) { testFuncImpl(wg, ch, tfs...) }
func c(wg *sync.WaitGroup, ch chan struct{}, tfs ...testFunc) { testFuncImpl(wg, ch, tfs...) }
func d(wg *sync.WaitGroup, ch chan struct{}, tfs ...testFunc) { testFuncImpl(wg, ch, tfs...) }
func e(wg *sync.WaitGroup, ch chan struct{}, tfs ...testFunc) { testFuncImpl(wg, ch, tfs...) }

func TestDump(t *testing.T) {
	var buf bytes.Buffer
	cleanup := func() {}
	defer cleanup()
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "dump"), func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "spawn":
			cleanup()
			cleanup = func() {}
			var stacks [][]testFunc
			for _, stackDescription := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(stackDescription)
				funcs := make([]testFunc, len(fields))
				for i, funcName := range fields {
					switch funcName {
					case "a":
						funcs[i] = a
					case "b":
						funcs[i] = b
					case "c":
						funcs[i] = c
					case "d":
						funcs[i] = d
					case "e":
						funcs[i] = e
					default:
						t.Fatalf("unrecognized function %q", funcName)
					}
				}
				stacks = append(stacks, funcs)
			}
			cleanup = spawnTestGoroutines(stacks...)
			return ""
		case "dump-all":
			buf.Reset()
			Dump(&buf, func(runtime.Frame) bool { return true })
			return ensureStacksDeterministic(buf.String())
		case "dump":
			buf.Reset()
			search := strings.Split(td.Input, "\n")
			Dump(&buf, func(f runtime.Frame) bool {
				for i := range search {
					if strings.Contains(f.Function, search[i]) {
						return true
					}
				}
				return false
			})
			return ensureStacksDeterministic(buf.String())
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// ensureStacksDeterministic cleans dumped stacks to remove sources of
// cross-platform divergences.
func ensureStacksDeterministic(s string) string {
	var buf bytes.Buffer
	for _, line := range strings.Split(s, "\n") {
		switch {
		// Skip frames in the runtime or containing GOROOT paths. The
		// runtime frames will vary depending on the Go version. So will the
		// line numbers.
		case strings.HasPrefix(strings.TrimSpace(line), "runtime."), strings.Contains(line, "GOROOT/"):
			continue
		case strings.Contains(line, "/pkg/util/"):
			fmt.Fprintln(&buf, line[strings.Index(line, "/pkg/util/"):])
		default:
			fmt.Fprintln(&buf, line)
		}
	}
	return strings.TrimSpace(buf.String())
}
