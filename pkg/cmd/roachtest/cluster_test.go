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
	"testing"

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

func TestClusterMonitor(t *testing.T) {
	logger := &logger{stdout: os.Stdout, stderr: os.Stderr}
	t.Run(`success`, func(t *testing.T) {
		c := &cluster{t: t, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(context.Context) error { return nil })
		if err := m.wait(`sleep`, `100`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run(`dead`, func(t *testing.T) {
		c := &cluster{t: t, l: logger}
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
		c := &cluster{t: t, l: logger}
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
}
