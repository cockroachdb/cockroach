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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func TestRegistryRun(t *testing.T) {
	r := &registry{m: make(map[string]*test), out: ioutil.Discard}
	r.Add(testSpec{
		Name: "pass",
		Run: func(ctx context.Context, t *test, c *cluster) {
		},
	})
	r.Add(testSpec{
		Name: "fail",
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

	r := &registry{
		m:              make(map[string]*test),
		out:            &buf,
		statusInterval: 20 * time.Millisecond,
	}
	r.Add(testSpec{
		Name: `status`,
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

	r := &registry{
		m:              make(map[string]*test),
		out:            &buf,
		statusInterval: 20 * time.Millisecond,
	}
	r.Add(testSpec{
		Name: `status`,
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
