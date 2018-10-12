// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

const defaultParallelism = 10

func TestMatchOrSkip(t *testing.T) {
	testCases := []struct {
		filter       []string
		name         string
		tags         []string
		expected     bool
		expectedSkip string
	}{
		{nil, "foo", nil, true, ""},
		{nil, "foo", []string{"bar"}, true, "[tag:default] does not match [bar]"},
		{[]string{"tag:b"}, "foo", []string{"bar"}, true, ""},
		{[]string{"tag:b"}, "foo", nil, true, "[tag:b] does not match [default]"},
		{[]string{"tag:default"}, "foo", nil, true, ""},
		{[]string{"tag:f"}, "foo", []string{"bar"}, true, "[tag:f] does not match [bar]"},
		{[]string{"f"}, "foo", []string{"bar"}, true, "[tag:default] does not match [bar]"},
		{[]string{"f"}, "bar", []string{"bar"}, false, ""},
		{[]string{"f", "tag:b"}, "foo", []string{"bar"}, true, ""},
		{[]string{"f", "tag:f"}, "foo", []string{"bar"}, true, "[tag:f] does not match [bar]"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			f := newFilter(c.filter)
			spec := &testSpec{Name: c.name, Tags: c.tags}
			if value := spec.matchOrSkip(f); c.expected != value {
				t.Fatalf("expected %t, but found %t", c.expected, value)
			} else if value && c.expectedSkip != spec.Skip {
				t.Fatalf("expected %s, but found %s", c.expectedSkip, spec.Skip)
			}
		})
	}
}

func TestRegistryRun(t *testing.T) {
	r := newRegistry()
	r.out = ioutil.Discard
	r.Add(testSpec{
		Name:    "pass",
		Run:     func(ctx context.Context, t *test, c *cluster) {},
		Cluster: makeClusterSpec(0),
	})
	r.Add(testSpec{
		Name: "fail",
		Run: func(ctx context.Context, t *test, c *cluster) {
			t.Fatal("failed")
		},
		Cluster: makeClusterSpec(0),
	})

	testCases := []struct {
		filters  []string
		expected int
	}{
		{nil, 1},
		{[]string{"pass"}, 0},
		{[]string{"fail"}, 1},
		{[]string{"pass|fail"}, 1},
		{[]string{"pass", "fail"}, 1},
		{[]string{"notests"}, 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			code := r.Run(c.filters, defaultParallelism, "" /* artifactsDir */, "myuser")
			if c.expected != code {
				t.Fatalf("expected code %d, but found code %d. Filters: %s", c.expected, code, c.filters)
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
		Name:    `status`,
		Cluster: makeClusterSpec(0),
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
	r.Run([]string{"status"}, defaultParallelism, "" /* artifactsDir */, "myuser")

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
		Name:    `status`,
		Cluster: makeClusterSpec(0),
		Run: func(ctx context.Context, t *test, c *cluster) {
			for i := 0; i < 100; i++ {
				time.Sleep(r.statusInterval)
				if unknownRE.MatchString(buf.String()) {
					break
				}
			}
		},
	})
	r.Run([]string{"status"}, defaultParallelism, "" /* artifactsDir */, "myuser")

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
		Cluster: makeClusterSpec(0),
		Run: func(ctx context.Context, t *test, c *cluster) {
			<-ctx.Done()
		},
	})
	r.Run([]string{"timeout"}, defaultParallelism, "" /* artifactsDir */, "myuser")

	out := buf.String()
	if !timeoutRE.MatchString(out) {
		t.Fatalf("unable to find \"timed out\" message:\n%s", out)
	}
}

func TestRegistryRunClusterExpired(t *testing.T) {
	defer func(l bool, n string) {
		local, clusterName = l, n
	}(local, clusterName)
	local, clusterName = true, "local"

	var buf syncedBuffer
	expiredRE := regexp.MustCompile(`(?m)^.*cluster expired \(.*\)$`)

	r := newRegistry()
	r.config.skipClusterValidationOnAttach = true
	r.config.skipClusterStopOnAttach = true
	r.out = &buf

	r.Add(testSpec{
		Name:    `expired`,
		Cluster: makeClusterSpec(1, nodeLifetimeOption(time.Second)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			panic("not reached")
		},
	})
	r.Run([]string{"expired"}, defaultParallelism, "" /* artifactsDir */, "myuser")

	out := buf.String()
	if !expiredRE.MatchString(out) {
		t.Fatalf("unable to find \"cluster expired\" message:\n%s", out)
	}
}

func TestRegistryVerifyValidClusterName(t *testing.T) {
	testCases := []struct {
		testNames   []string
		expectedErr string
	}{
		{[]string{"hello"}, ""},
		{[]string{"HELLO", "hello"}, "have equivalent nightly cluster names"},
		{[]string{"hel+lo", "hel++lo"}, "have equivalent nightly cluster names"},
		{[]string{"hello+"}, "must match regex"},
		{[]string{strings.Repeat("y", 41)}, ""},
		{[]string{strings.Repeat("y", 42)}, "must match regex"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			r := newRegistry()
			var err error
			for _, n := range c.testNames {
				err = r.verifyValidClusterName(n)
			}
			if !testutils.IsError(err, c.expectedErr) {
				t.Fatalf("expected %s, but found %v", c.expectedErr, err)
			}
		})
	}
}

func TestRegistryPrepareSpec(t *testing.T) {
	dummyRun := func(context.Context, *test, *cluster) {}

	var listTests = func(t *testSpec) []string {
		return []string{t.Name}
	}

	testCases := []struct {
		spec          testSpec
		expectedErr   string
		expectedTests []string
	}{
		{
			testSpec{
				Name:    "a",
				Run:     dummyRun,
				Cluster: makeClusterSpec(0),
			},
			"",
			[]string{"a"},
		},
		{
			testSpec{
				Name:       "a",
				MinVersion: "v2.1.0",
				Run:        dummyRun,
				Cluster:    makeClusterSpec(0),
			},
			"",
			[]string{"a"},
		},
		{
			testSpec{
				Name:       "a",
				MinVersion: "foo",
				Run:        dummyRun,
				Cluster:    makeClusterSpec(0),
			},
			"a: unable to parse min-version: invalid version string 'foo'",
			nil,
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			r := newRegistry()
			err := r.prepareSpec(&c.spec)
			if !testutils.IsError(err, c.expectedErr) {
				t.Fatalf("expected %q, but found %q", c.expectedErr, err.Error())
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
		{"v1.1.0", false, false},
		{"v2.0.0", true, false},
		{"v2.1.0", true, true},
	}
	for _, c := range testCases {
		t.Run(c.buildVersion, func(t *testing.T) {
			var buf syncedBuffer
			var runA, runB bool
			r := newRegistry()
			r.out = &buf
			r.Add(testSpec{
				Name:       "a",
				MinVersion: "v2.0.0",
				Cluster:    makeClusterSpec(0),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runA = true
				},
			})
			r.Add(testSpec{
				Name:       "b",
				MinVersion: "v2.1.0",
				Cluster:    makeClusterSpec(0),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runB = true
				},
			})
			if err := r.setBuildVersion(c.buildVersion); err != nil {
				t.Fatal(err)
			}
			r.Run(nil /* filter */, defaultParallelism, "" /* artifactsDir */, "myuser")
			if c.expectedA != runA || c.expectedB != runB {
				t.Fatalf("expected %t,%t, but got %t,%t\n%s",
					c.expectedA, c.expectedB, runA, runB, buf.String())
			}
		})
	}
}
