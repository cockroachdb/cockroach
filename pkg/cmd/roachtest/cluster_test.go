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
	"context"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/pkg/errors"
)

func TestClusterNodes(t *testing.T) {
	c := &cluster{nodes: 10}
	opts := func(opts ...option) []option {
		return opts
	}
	testCases := []struct {
		opts     []option
		expected string
	}{
		{opts(), ""},
		{opts(c.All()), ":1-10"},
		{opts(c.Range(1, 2)), ":1-2"},
		{opts(c.Range(2, 5)), ":2-5"},
		{opts(c.All(), c.Range(2, 5)), ":1-10"},
		{opts(c.Range(2, 5), c.Range(7, 9)), ":2-5,7-9"},
		{opts(c.Range(2, 5), c.Range(6, 8)), ":2-8"},
		{opts(c.Node(2), c.Node(4), c.Node(6)), ":2,4,6"},
		{opts(c.Node(2), c.Node(3), c.Node(4)), ":2-4"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			nodes := c.makeNodes(tc.opts...)
			if tc.expected != nodes {
				t.Fatalf("expected %s, but found %s", tc.expected, nodes)
			}
		})
	}
}

type testWrapper struct {
	*testing.T
}

func (t testWrapper) ArtifactsDir() string {
	return ""
}

func (t testWrapper) logger() *logger {
	return nil
}

func TestClusterMonitor(t *testing.T) {
	cfg := &loggerConfig{stdout: os.Stdout, stderr: os.Stderr}
	logger, err := cfg.newLogger("" /* path */)
	if err != nil {
		t.Fatal(err)
	}
	t.Run(`success`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(context.Context) error { return nil })
		if err := m.wait(`echo`, `1`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run(`dead`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			fmt.Printf("worker done\n")
			return ctx.Err()
		})

		err := m.wait(`echo`, "1: 100\n1: dead")
		expectedErr := `dead`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`worker-fail`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(context.Context) error {
			return errors.New(`worker-fail`)
		})
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		err := m.wait(`sleep`, `100`)
		expectedErr := `worker-fail`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`wait-fail`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		// Returned error should be that from the wait command.
		err := m.wait(`false`)
		expectedErr := `exit status`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`wait-ok`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		// If wait terminates, context gets canceled.
		err := m.wait(`true`)
		if err != context.Canceled {
			t.Errorf(`expected context canceled, got: %+v`, err)
		}
	})

	// NB: the forker sleeps in these tests actually get leaked, so it's important to let
	// them finish pretty soon (think stress testing). As a matter of fact, `make test` waits
	// for these child goroutines to finish (so these tests take seconds).
	t.Run(`worker-fd-error`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			defer func() {
				fmt.Println("sleep returns")
			}()
			return execCmd(ctx, logger, "/bin/bash", "-c", "sleep 3& wait")
		})
		m.Go(func(ctx context.Context) error {
			defer func() {
				fmt.Println("failure returns")
			}()
			time.Sleep(30 * time.Millisecond)
			return execCmd(ctx, logger, "/bin/bash", "-c", "echo hi && notthere")
		})
		expectedErr := regexp.QuoteMeta(`/bin/bash -c echo hi && notthere returned:
stderr:
/bin/bash: notthere: command not found

stdout:
hi
: exit status 127`)
		if err := m.wait("sleep", "100"); !testutils.IsError(err, expectedErr) {
			t.Error(err)
		}
	})
	t.Run(`worker-fd-fatal`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			err := execCmd(ctx, logger, "/bin/bash", "-c", "echo foo && sleep 3& wait")
			return err
		})
		m.Go(func(ctx context.Context) error {
			time.Sleep(30 * time.Millisecond)
			// Simulate c.t.Fatal for which there isn't enough mocking here.
			// In reality t.Fatal adds text that is returned when the test fails,
			// so the failing goroutine will be referenced (not like in the expected
			// error below, where all you see is the other one being canceled).
			runtime.Goexit()
			return errors.New("unreachable")
		})
		expectedErr := regexp.QuoteMeta(`Goexit() was called`)
		if err := m.wait("sleep", "100"); !testutils.IsError(err, expectedErr) {
			t.Error(err)
		}
	})
}
