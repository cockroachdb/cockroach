// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	tests       = registry(make(map[string]*test))
	parallelism = 10
	dryrun      = false
)

func allTests() []string {
	t := tests.List(nil)
	r := make([]string, len(t))
	for i := range t {
		r[i] = t[i].name
	}
	return r
}

type registry map[string]*test

func (r registry) Add(name string, fn func(*test)) {
	if _, ok := r[name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", name)
		os.Exit(1)
	}
	r[name] = &test{name: name, fn: fn}
}

func (r registry) List(filter []string) []*test {
	if len(filter) == 0 {
		filter = []string{"."}
	}
	re := make([]*regexp.Regexp, len(filter))
	for i := range filter {
		re[i] = regexp.MustCompile(filter[i])
	}

	var results []*test
	for _, t := range r {
		var matched bool
		for _, r := range re {
			if r.MatchString(t.name) {
				matched = true
				break
			}
		}
		if matched {
			results = append(results, t)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})
	return results
}

func (r registry) Run(filter []string) int {
	initBinaries()

	tests := r.List(filter)
	wg := &sync.WaitGroup{}
	wg.Add(len(tests))

	// We can't run local tests in parallel as there is only 1 "local" cluster.
	if local {
		parallelism = 1
	}
	// Limit the parallelism to the number of tests. The primary effect this has
	// is that we'll log to stdout/stderr if only one test is being run.
	if parallelism > len(tests) {
		parallelism = len(tests)
	}

	var pass, fail int32
	go func() {
		sem := make(chan struct{}, parallelism)
		for _, t := range tests {
			sem <- struct{}{}
			fmt.Printf("=== RUN   %s\n", t.name)
			t.run(func(failed bool) {
				if failed {
					atomic.AddInt32(&fail, 1)
				} else {
					atomic.AddInt32(&pass, 1)
				}
				wg.Done()
				<-sem
			})
		}
	}()

	// TODO(peter): While tests are running, if we're not logging to
	// stdout/stderr (i.e. parallelism > 1), then periodically display test
	// progress.
	wg.Wait()

	if fail > 0 {
		fmt.Println("FAIL")
		return 1
	}
	fmt.Println("PASS")
	return 0
}

type test struct {
	name   string
	fn     func(*test)
	runner string
	start  time.Time
	mu     struct {
		syncutil.RWMutex
		failed bool
		output []byte
	}
}

func (t *test) Name() string {
	return t.name
}

func (t *test) Fatal(args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.failed = true
	t.mu.output = append(t.mu.output, t.decorate(fmt.Sprint(args...))...)
	runtime.Goexit()
}

func (t *test) Fatalf(format string, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.failed = true
	t.mu.output = append(t.mu.output, t.decorate(fmt.Sprintf(format, args...))...)
	runtime.Goexit()
}

func (t *test) decorate(s string) string {
	// Skip two extra frames to account for this function
	// and runtime.Callers itself.
	var pc [50]uintptr
	n := runtime.Callers(3, pc[:])
	if n == 0 {
		panic("zero callers found")
	}

	buf := new(bytes.Buffer)
	frames := runtime.CallersFrames(pc[:n])
	sep := "\t"
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		if frame.Function == t.runner {
			break
		}
		file := frame.File
		if index := strings.LastIndex(file, "/"); index >= 0 {
			file = file[index+1:]
		}
		fmt.Fprintf(buf, "%s%s:%d", sep, file, frame.Line)
		sep = ","
	}
	buf.WriteString(": ")

	lines := strings.Split(s, "\n")
	if l := len(lines); l > 1 && lines[l-1] == "" {
		lines = lines[:l-1]
	}
	for i, line := range lines {
		if i > 0 {
			buf.WriteString("\n\t\t")
		}
		buf.WriteString(line)
	}
	buf.WriteByte('\n')
	return buf.String()
}

func (t *test) Failed() bool {
	t.mu.RLock()
	failed := t.mu.failed
	t.mu.RUnlock()
	return failed
}

func (t *test) run(done func(failed bool)) {
	callerName := func() string {
		// Make room for the skip PC.
		var pc [2]uintptr
		n := runtime.Callers(2, pc[:]) // skip + runtime.Callers + callerName
		if n == 0 {
			panic("zero callers found")
		}
		frames := runtime.CallersFrames(pc[:n])
		frame, _ := frames.Next()
		return frame.Function
	}

	t = &test{
		name: t.name,
		fn:   t.fn,
	}

	go func() {
		t.runner = callerName()

		defer func() {
			duration := timeutil.Now().Sub(t.start)

			if err := recover(); err != nil {
				t.Fatal(err)
			}

			if !dryrun {
				dstr := fmt.Sprintf("%.2fs", duration.Seconds())
				if t.Failed() {
					fmt.Printf("--- FAIL: %s (%s)\n%s", t.name, dstr, t.mu.output)
				} else {
					fmt.Printf("--- PASS: %s (%s)\n", t.name, dstr)
				}
			}

			done(t.Failed())
		}()

		t.start = timeutil.Now()
		if !dryrun {
			t.fn(t)
		}
	}()
}
