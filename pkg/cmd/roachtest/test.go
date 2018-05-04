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
	"path/filepath"
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
	parallelism   = 10
	count         = 1
	debug         = false
	dryrun        = false
	clusterNameRE = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
)

type testSpec struct {
	SkippedBecause string // if nonzero, test will be skipped
	Name           string
	// Stable indicates whether failure of the test will result in failure of the
	// test run. Tests should be added initially as unstable, and only converted
	// to stable once they have passed successfully on multiple nightly (not
	// local) runs.
	Stable bool
	Nodes  []nodeSpec
	Run    func(ctx context.Context, t *test, c *cluster)
}

type registry struct {
	m              map[string]*testSpec
	clusters       map[string]string
	out            io.Writer
	statusInterval time.Duration
}

func newRegistry() *registry {
	return &registry{
		m:        make(map[string]*testSpec),
		clusters: make(map[string]string),
		out:      os.Stdout,
	}
}

func (r *registry) verifyClusterName(testName string) error {
	// TeamCity build IDs are currently 6 digits, but we use 7 here for a bit of
	// breathing room.
	name := makeGCEClusterName(testName, "xxxxxxx", "teamcity")
	if !clusterNameRE.MatchString(name) {
		return fmt.Errorf("cluster name '%s' must match regex '%s'",
			name, clusterNameRE)
	}
	if t, ok := r.clusters[name]; ok {
		return fmt.Errorf("test %s and test %s have equivalent nightly cluster names: %s",
			testName, t, name)
	}
	r.clusters[name] = testName
	return nil
}

func (r *registry) Add(spec testSpec) {
	if _, ok := r.m[spec.Name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", spec.Name)
		os.Exit(1)
	}
	r.m[spec.Name] = &spec
	if err := r.verifyClusterName(spec.Name); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func (r *registry) List(filter []string) []*testSpec {
	if len(filter) == 0 {
		filter = []string{"."}
	}
	re := make([]*regexp.Regexp, len(filter))
	for i := range filter {
		re[i] = regexp.MustCompile(filter[i])
	}

	var results []*testSpec
	for _, t := range r.m {
		var matched bool
		for _, r := range re {
			if r.MatchString(t.Name) {
				matched = true
				break
			}
		}
		if matched {
			results = append(results, t)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	return results
}

func (r *registry) Run(filter []string) int {
	tests := r.List(filter)
	wg := &sync.WaitGroup{}
	wg.Add(count * len(tests))

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
		for j := 0; j < count; j++ {
			for i := range tests {
				t := &test{spec: tests[i]}
				sem <- struct{}{}
				if teamCity {
					fmt.Printf("##teamcity[testStarted name='%s' flowId='%s']\n", t.Name(), t.Name())
				} else {
					stability := ""
					if !t.spec.Stable {
						stability = " [unstable]"
					}
					fmt.Fprintf(r.out, "=== RUN   %s%s\n", t.Name(), stability)
				}
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

			stableFails := 0
			for t := range status.fail {
				if t.spec.Stable {
					stableFails++
				}
			}
			if stableFails > 0 {
				fmt.Fprintln(r.out, "FAIL")
				return 1
			}
			unstableFails := ""
			if n := len(status.fail) - stableFails; n > 0 {
				unstableFails = fmt.Sprintf(" (%d unstable FAIL)", n)
			}
			fmt.Fprintf(r.out, "PASS%s\n", unstableFails)
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
		if index := strings.LastIndexByte(file, '/'); index >= 0 {
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
				stability := ""
				if !t.spec.Stable {
					stability = "[unstable] "
				}

				if t.Failed() {
					if teamCity {
						fmt.Fprintf(
							out, "##teamcity[testFailed name='%s' details='%s' flowId='%s']\n",
							t.Name(), teamCityEscape(string(t.mu.output)), t.Name(),
						)
					}
					fmt.Fprintf(out, "--- FAIL: %s %s(%s)\n%s", t.Name(), stability, dstr, t.mu.output)
				} else if t.spec.SkippedBecause == "" {
					fmt.Fprintf(out, "--- PASS: %s %s(%s)\n", t.Name(), stability, dstr)
					// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
					// TeamCity regards the test as successful.
				} else {
					if teamCity {
						fmt.Fprintf(out, "##teamcity[testIgnored name='%s' message='%s']\n",
							t.Name(), t.spec.SkippedBecause)
					}
					fmt.Fprintf(out, "--- SKIP: %s (%s)\n\t%s\n", t.Name(), dstr, t.spec.SkippedBecause)
				}

				if teamCity {
					fmt.Fprintf(out, "##teamcity[testFinished name='%s' flowId='%s']\n", t.Name(), t.Name())

					escapedTestName := teamCityNameEscape(t.Name())
					artifactsGlobPath := filepath.Join(artifacts, escapedTestName, "**")
					artifactsSpec := fmt.Sprintf("%s => %s", artifactsGlobPath, escapedTestName)
					fmt.Fprintf(out, "##teamcity[publishArtifacts '%s']\n", artifactsSpec)
				}
			}

			done(t.Failed())
		}()

		t.start = timeutil.Now()

		if t.spec.SkippedBecause != "" {
			return
		}

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

// teamCityEscape escapes a string for use as <value> in a key='<value>' attribute
// in TeamCity build output marker.
// Documentation here: https://confluence.jetbrains.com/display/TCD10/Build+Script+Interaction+with+TeamCity#BuildScriptInteractionwithTeamCity-Escapedvalues
func teamCityEscape(s string) string {
	r := strings.NewReplacer(
		"\n", "|n",
		"'", "|'",
		"|", "||",
		"[", "|[",
		"]", "|]",
	)
	return r.Replace(s)
}

func teamCityNameEscape(name string) string {
	return strings.Replace(name, ",", "_", -1)
}
