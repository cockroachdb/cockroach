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
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/kr/pretty"
)

func TestRegistryRun(t *testing.T) {
	r := newRegistry()
	r.out = ioutil.Discard
	r.Add(testSpec{
		Name:   "pass",
		Stable: true,
		Run: func(ctx context.Context, t *test, c *cluster) {
		},
	})
	r.Add(testSpec{
		Name:   "fail",
		Stable: true,
		Run: func(ctx context.Context, t *test, c *cluster) {
			t.Fatal("failed")
		},
	})
	r.Add(testSpec{
		Name:   "fail-unstable",
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			t.Fatal("failed")
		},
	})

	testCases := []struct {
		filters  []string
		expected int
	}{
		{nil, 1},
		{[]string{"pass"}, 0},
		{[]string{"fail"}, 1},
		{[]string{"fail-unstable"}, 0},
		{[]string{"pass|fail"}, 1},
		{[]string{"pass", "fail"}, 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			code := r.Run(c.filters)
			if c.expected != code {
				t.Fatalf("expected %d, but found %d", c.expected, code)
			}
		})
	}
}

type syncedBuffer struct {
	mu  syncutil.Mutex
	buf bytes.Buffer
}

func (b *syncedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRegistryStatus(t *testing.T) {
	var buf syncedBuffer
	waitingRE := regexp.MustCompile(`(?m)^.*status: waiting.*worker?.*worker?.*$`)
	cleaningUpRE := regexp.MustCompile(`(?m)^.*status: cleaning up \(.*\)$`)

	r := newRegistry()
	r.out = &buf
	r.statusInterval = 20 * time.Millisecond
	r.Add(testSpec{
		Name:   `status`,
		Stable: true,
		Run: func(ctx context.Context, t *test, c *cluster) {
			t.Status("waiting")
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					t.WorkerStatus("worker", i)
					defer t.WorkerStatus()
					for i := 0.0; i < 1.0; i += 0.01 {
						t.WorkerProgress(i)
						time.Sleep(r.statusInterval)
						if waitingRE.MatchString(buf.String()) {
							break
						}
					}
				}(i)
			}
			wg.Wait()
			t.Status("cleaning up")
			for i := 0.0; i < 1.0; i += 0.01 {
				t.Progress(i)
				time.Sleep(r.statusInterval)
				if cleaningUpRE.MatchString(buf.String()) {
					break
				}
			}
		},
	})
	r.Run([]string{"status"})

	status := buf.String()
	if !waitingRE.MatchString(status) {
		t.Fatalf("unable to find \"waiting\" status:\n%s", status)
	}
	if !cleaningUpRE.MatchString(status) {
		t.Fatalf("unable to find \"cleaning up\" status:\n%s", status)
	}
	if testing.Verbose() {
		fmt.Println(status)
	}
}

func TestRegistryStatusUnknown(t *testing.T) {
	var buf syncedBuffer
	unknownRE := regexp.MustCompile(`(?m)^.*status: \?\?\? \(.*\)$`)

	r := newRegistry()
	r.out = &buf
	r.statusInterval = 20 * time.Millisecond

	r.Add(testSpec{
		Name:   `status`,
		Stable: true,
		Run: func(ctx context.Context, t *test, c *cluster) {
			for i := 0; i < 100; i++ {
				time.Sleep(r.statusInterval)
				if unknownRE.MatchString(buf.String()) {
					break
				}
			}
		},
	})
	r.Run([]string{"status"})

	status := buf.String()
	if !unknownRE.MatchString(status) {
		t.Fatalf("unable to find \"waiting\" status:\n%s", status)
	}
	if testing.Verbose() {
		fmt.Println(status)
	}
}

func TestRegistryRunTimeout(t *testing.T) {
	var buf syncedBuffer
	timeoutRE := regexp.MustCompile(`(?m)^.*test timed out \(.*\)$`)

	r := newRegistry()
	r.out = &buf

	r.Add(testSpec{
		Name:    `timeout`,
		Timeout: 10 * time.Millisecond,
		Stable:  true,
		Run: func(ctx context.Context, t *test, c *cluster) {
			<-ctx.Done()
		},
	})
	r.Run([]string{"timeout"})

	out := buf.String()
	if !timeoutRE.MatchString(out) {
		t.Fatalf("unable to find \"timed out\" message:\n%s", out)
	}
}

func TestRegistryRunSubTestFailed(t *testing.T) {
	var buf syncedBuffer
	failedRE := regexp.MustCompile(`(?m)^.*--- FAIL: parent \(.*$`)

	r := newRegistry()
	r.out = &buf
	r.Add(testSpec{
		Name:   "parent",
		Stable: true,
		SubTests: []testSpec{{
			Name:   "child",
			Stable: true,
			Run: func(ctx context.Context, t *test, c *cluster) {
				t.Fatal("failed")
			},
		}},
	})

	r.Run([]string{"."})
	out := buf.String()
	if !failedRE.MatchString(out) {
		t.Fatalf("unable to find \"FAIL: parent\" message:\n%s", out)
	}
}

func TestRegistryRunClusterExpired(t *testing.T) {
	defer func(l, v bool, n string) {
		local, testingSkipValidation, clusterName = l, v, n
	}(local, testingSkipValidation, clusterName)
	local, testingSkipValidation, clusterName = true, true, "local"

	var buf syncedBuffer
	expiredRE := regexp.MustCompile(`(?m)^.*cluster expired \(.*\)$`)

	r := newRegistry()
	r.out = &buf

	r.Add(testSpec{
		Name:   `expired`,
		Stable: true,
		Nodes:  nodes(1, nodeLifetimeOption(time.Second)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			panic("not reached")
		},
	})
	r.Run([]string{"expired"})

	out := buf.String()
	if !expiredRE.MatchString(out) {
		t.Fatalf("unable to find \"cluster expired\" message:\n%s", out)
	}
}

func TestRegistryVerifyClusterName(t *testing.T) {
	testCases := []struct {
		testNames   []string
		expectedErr string
	}{
		{[]string{"hello"}, ""},
		{[]string{"HELLO", "hello"}, "have equivalent nightly cluster names"},
		{[]string{"hel+lo", "hel++lo"}, "have equivalent nightly cluster names"},
		{[]string{"hello+"}, "must match regex"},
		{[]string{strings.Repeat("y", 46)}, ""},
		{[]string{strings.Repeat("y", 47)}, "must match regex"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			r := newRegistry()
			var err error
			for _, n := range c.testNames {
				err = r.verifyClusterName(n)
			}
			if !testutils.IsError(err, c.expectedErr) {
				t.Fatalf("expected %s, but found %v", c.expectedErr, err)
			}
		})
	}
}

func TestRegistryPrepareSpec(t *testing.T) {
	dummyRun := func(context.Context, *test, *cluster) {}

	var listTests func(t *testSpec) []string
	listTests = func(t *testSpec) []string {
		r := []string{t.Name}
		for i := range t.SubTests {
			r = append(r, listTests(&t.SubTests[i])...)
		}
		return r
	}

	testCases := []struct {
		spec          testSpec
		expectedErr   string
		expectedTests []string
	}{
		{
			testSpec{
				Name: "a",
				Run:  dummyRun,
			},
			"",
			[]string{"a"},
		},
		{
			testSpec{
				Name: "a",
				SubTests: []testSpec{{
					Name: "b",
					Run:  dummyRun,
				}},
			},
			"",
			[]string{"a", "a/b"},
		},
		{
			testSpec{
				Name: "a",
				Run:  dummyRun,
				SubTests: []testSpec{{
					Name: "b",
					Run:  dummyRun,
				}},
			},
			"a: must specify only one of Run or SubTests",
			nil,
		},
		{
			testSpec{
				Name: "a",
				SubTests: []testSpec{{
					Name: "b",
				}},
			},
			"a/b: must specify only one of Run or SubTests",
			nil,
		},
		{
			testSpec{
				Name: "a",
				SubTests: []testSpec{{
					Name: "b",
					Run:  dummyRun,
					SubTests: []testSpec{{
						Name: "c",
						Run:  dummyRun,
					}},
				}},
			},
			"b: must specify only one of Run or SubTests",
			nil,
		},
		{
			testSpec{
				Name: "a",
				SubTests: []testSpec{{
					Name:  "b",
					Nodes: nodes(1),
					Run:   dummyRun,
				}},
			},
			"a/b: subtest may not provide cluster specification",
			nil,
		},
		{
			testSpec{
				Name:       "a",
				MinVersion: "v2.1.0",
				Run:        dummyRun,
			},
			"",
			[]string{"a"},
		},
		{
			testSpec{
				Name:       "a",
				MinVersion: "foo",
				Run:        dummyRun,
			},
			"a: unable to parse min-version: foo",
			nil,
		},
		{
			testSpec{
				Name:    "a",
				Timeout: time.Second,
				SubTests: []testSpec{{
					Name: "b",
					Run:  dummyRun,
				}},
			},
			"a: timeouts only apply to tests specifying Run",
			nil,
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			r := newRegistry()
			err := r.prepareSpec(&c.spec, 0)
			if !testutils.IsError(err, c.expectedErr) {
				t.Fatalf("expected %s, but found %v", c.expectedErr, err)
			}
			if c.expectedErr == "" {
				tests := listTests(&c.spec)
				sort.Strings(tests)
				if diff := pretty.Diff(c.expectedTests, tests); len(diff) != 0 {
					t.Fatalf("unexpected tests:\n%s", strings.Join(diff, "\n"))
				}
			}
		})
	}
}

func TestRegistryMinVersion(t *testing.T) {
	testCases := []struct {
		buildVersion string
		expectedA    bool
		expectedB    bool
	}{
		{"1.1.0", false, false},
		{"2.0.0", true, false},
		{"2.1.0", true, true},
	}
	for _, c := range testCases {
		t.Run(c.buildVersion, func(t *testing.T) {
			var buf syncedBuffer
			var runA, runB bool
			r := newRegistry()
			r.out = &buf
			r.Add(testSpec{
				Name:       "a",
				MinVersion: "2.0.0",
				Run: func(ctx context.Context, t *test, c *cluster) {
					runA = true
				},
			})
			r.Add(testSpec{
				Name:       "b",
				MinVersion: "2.1.0",
				Run: func(ctx context.Context, t *test, c *cluster) {
					runB = true
				},
			})
			if err := r.setBuildVersion(c.buildVersion); err != nil {
				t.Fatal(err)
			}
			r.Run(nil)
			if c.expectedA != runA || c.expectedB != runB {
				t.Fatalf("expected %t,%t, but got %t,%t\n%s",
					c.expectedA, c.expectedB, runA, runB, buf.String())
			}
		})
	}
}
