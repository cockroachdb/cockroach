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
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/petermattis/goid"
)

var (
	tests       = &registry{m: make(map[string]*test), out: os.Stdout}
	parallelism = 10
	debug       = false
	dryrun      = false
)

func allTests() []string {
	t := tests.List(nil)
	r := make([]string, len(t))
	for i := range t {
		r[i] = t[i].Name()
	}
	return r
}

type testSpec struct {
	Name string
	// TODO(peter): Help string
	Nodes []nodeSpec
	Run   func(ctx context.Context, t *test, c *cluster)
}

type registry struct {
	m              map[string]*test
	out            io.Writer
	statusInterval time.Duration
}

func (r *registry) Add(spec testSpec) {
	if _, ok := r.m[spec.Name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", spec.Name)
		os.Exit(1)
	}
	r.m[spec.Name] = &test{spec: &spec}
}

func (r *registry) List(filter []string) []*test {
	if len(filter) == 0 {
		filter = []string{"."}
	}
	re := make([]*regexp.Regexp, len(filter))
	for i := range filter {
		re[i] = regexp.MustCompile(filter[i])
	}

	var results []*test
	for _, t := range r.m {
		var matched bool
		for _, r := range re {
			if r.MatchString(t.Name()) {
				matched = true
				break
			}
		}
		if matched {
			results = append(results, t)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name() < results[j].Name()
	})
	return results
}

func (r *registry) Run(filter []string) int {
	tests := r.List(filter)
	wg := &sync.WaitGroup{}
	wg.Add(len(tests))

	// If we're running against an existing "local" cluster, force the local flag
	// to true in order to get the "local" test configurations.
	if clusterName == "local" {
		local = true
	}
	// We can't run tests in parallel on local clusters or on an existing
	// cluster.
	if local || clusterName != "" {
		parallelism = 1
	}
	// Limit the parallelism to the number of tests. The primary effect this has
	// is that we'll log to stdout/stderr if only one test is being run.
	if parallelism > len(tests) {
		parallelism = len(tests)
	}

	var status struct {
		syncutil.Mutex
		running map[*test]struct{}
		pass    map[*test]struct{}
		fail    map[*test]struct{}
	}
	status.running = make(map[*test]struct{})
	status.pass = make(map[*test]struct{})
	status.fail = make(map[*test]struct{})

	go func() {
		sem := make(chan struct{}, parallelism)
		for i := range tests {
			t := tests[i]
			sem <- struct{}{}
			fmt.Fprintf(r.out, "=== RUN   %s\n", t.Name())
			status.Lock()
			status.running[t] = struct{}{}
			status.Unlock()
			t.run(r.out, func(failed bool) {
				status.Lock()
				delete(status.running, t)
				if failed {
					status.fail[t] = struct{}{}
				} else {
					status.pass[t] = struct{}{}
				}
				status.Unlock()
				wg.Done()
				<-sem
			})
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Periodically output test status to give an indication of progress.
	if r.statusInterval == 0 {
		r.statusInterval = time.Minute
	}
	ticker := time.NewTicker(r.statusInterval)
	defer ticker.Stop()

	// Shut down test clusters when interrupted (for example CTRL+C).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	for i := 1; ; i++ {
		select {
		case <-done:
			status.Lock()
			defer status.Unlock()
			postSlackReport(status.pass, status.fail, len(r.m)-len(tests))
			if len(status.fail) > 0 {
				fmt.Fprintln(r.out, "FAIL")
				return 1
			}
			fmt.Fprintln(r.out, "PASS")
			return 0

		case <-ticker.C:
			status.Lock()
			runningTests := make([]*test, 0, len(status.running))
			for t := range status.running {
				runningTests = append(runningTests, t)
			}
			sort.Slice(runningTests, func(i, j int) bool {
				return runningTests[i].Name() < runningTests[j].Name()
			})
			var buf bytes.Buffer
			for _, t := range runningTests {
				t.mu.Lock()
				done := t.mu.done
				var status map[int64]testStatus
				if !done {
					status = make(map[int64]testStatus, len(t.mu.status))
					for k, v := range t.mu.status {
						status[k] = v
					}
					if len(status) == 0 {
						// If we have no other status messages display this unknown state.
						status[0] = testStatus{
							msg:  "???",
							time: timeutil.Now(),
						}
					}
				}
				t.mu.Unlock()
				if !done {
					ids := make([]int64, 0, len(status))
					for id := range status {
						ids = append(ids, id)
					}
					sort.Slice(ids, func(i, j int) bool {
						// Force the goroutine ID for the main test goroutine to sort to
						// the front. NB: goroutine IDs are not monotonically increasing
						// because each thread has a small cache of IDs for allocation.
						if ids[j] == t.runnerID {
							return false
						}
						if ids[i] == t.runnerID {
							return true
						}
						return ids[i] < ids[j]
					})

					fmt.Fprintf(&buf, "[%4d] %s: ", i, t.Name())

					for j := range ids {
						s := status[ids[j]]
						duration := timeutil.Now().Sub(s.time)
						progStr := ""
						if s.progress > 0 {
							progStr = fmt.Sprintf("%.1f%%|", 100*s.progress)
						}
						if j > 0 {
							buf.WriteString(", ")
						}
						fmt.Fprintf(&buf, "%s (%s%s)", s.msg, progStr,
							time.Duration(duration.Seconds()+0.5)*time.Second)
					}

					fmt.Fprintf(&buf, "\n")
				}
			}
			fmt.Fprint(r.out, buf.String())
			status.Unlock()

		case <-sig:
			destroyAllClusters()
		}
	}
}

type testStatus struct {
	msg      string
	time     time.Time
	progress float64
}

type test struct {
	spec     *testSpec
	runner   string
	runnerID int64
	start    time.Time
	end      time.Time
	mu       struct {
		syncutil.RWMutex
		done   bool
		failed bool
		status map[int64]testStatus
		output []byte
	}
}

func (t *test) Name() string {
	return t.spec.Name
}

func (t *test) status(id int64, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.status == nil {
		t.mu.status = make(map[int64]testStatus)
	}
	if len(args) == 0 {
		delete(t.mu.status, id)
		return
	}
	t.mu.status[id] = testStatus{
		msg:  fmt.Sprint(args...),
		time: timeutil.Now(),
	}
}

// Status sets the main status message for the test. When called from the main
// test goroutine (i.e. the goroutine on which testSpec.Run is invoked), this
// is equivalent to calling WorkerStatus. If no arguments are specified, the
// status message is erased.
func (t *test) Status(args ...interface{}) {
	t.status(t.runnerID, args...)
}

// WorkerStatus sets the status message for a worker goroutine associated with
// the test. The status message should be cleared before the goroutine exits by
// calling WorkerStatus with no arguments.
func (t *test) WorkerStatus(args ...interface{}) {
	t.status(goid.Get(), args...)
}

func (t *test) progress(id int64, frac float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.status == nil {
		t.mu.status = make(map[int64]testStatus)
	}
	status := t.mu.status[id]
	status.progress = frac
	t.mu.status[id] = status
}

// Progress sets the progress (a fraction in the range [0,1]) associated with
// the main test status messasge. When called from the main test goroutine
// (i.e. the goroutine on which testSpec.Run is invoked), this is equivalent to
// calling WorkerProgress.
func (t *test) Progress(frac float64) {
	t.progress(t.runnerID, frac)
}

// WorkerProgress sets the progress (a fraction in the range [0,1]) associated
// with the a worker status messasge.
func (t *test) WorkerProgress(frac float64) {
	t.progress(goid.Get(), frac)
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

func (t *test) duration() time.Duration {
	return t.end.Sub(t.start)
}

func (t *test) Failed() bool {
	t.mu.RLock()
	failed := t.mu.failed
	t.mu.RUnlock()
	return failed
}

func (t *test) run(out io.Writer, done func(failed bool)) {
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

	go func() {
		t.runner = callerName()
		t.runnerID = goid.Get()

		defer func() {
			t.end = timeutil.Now()

			if err := recover(); err != nil {
				t.mu.Lock()
				t.mu.failed = true
				t.mu.output = append(t.mu.output, t.decorate(fmt.Sprint(err))...)
				t.mu.Unlock()
			}

			t.mu.Lock()
			t.mu.done = true
			t.mu.Unlock()

			if !dryrun {
				dstr := fmt.Sprintf("%.2fs", t.duration().Seconds())
				if t.Failed() {
					fmt.Fprintf(out, "--- FAIL: %s (%s)\n%s", t.Name(), dstr, t.mu.output)
				} else {
					fmt.Fprintf(out, "--- PASS: %s (%s)\n", t.Name(), dstr)
				}
			}

			done(t.Failed())
		}()

		t.start = timeutil.Now()
		if !dryrun {
			ctx := context.Background()
			c := newCluster(ctx, t, t.spec.Nodes)
			defer func() {
				if !debug || !t.Failed() {
					c.Destroy(ctx)
				} else {
					c.l.printf("not destroying cluster to allow debugging\n")
				}
			}()
			t.spec.Run(ctx, t, c)
		}
	}()
}
