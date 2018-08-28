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

	"github.com/pkg/errors"
)

func registerTestHarness(r *registry) {
	r.Add(testSpec{
		Skip:   "only for manual testing of the test harness; test is supposed to fail",
		Name:   "testharness",
		Nodes:  nodes(1),
		Stable: true, // DO NOT COPY to new tests
		Run:    testHarness,
	})
}

func testHarness(ctx context.Context, t *test, c *cluster) {
	m := newMonitor(ctx, c, c.Range(1, c.nodes))
	m.ExpectDeaths(int32(c.nodes))
	m.Go(func(ctx context.Context) error {
		// Make sure the context cancellation works (not true prior to the PR
		// adding this test).
		return c.RunE(ctx, c.Node(1), "sleep", "2000")
	})
	m.Go(func(ctx context.Context) error {
		// This will call c.t.Fatal which also used to wreak havoc on the test
		// harness. Now it exits just fine (and all it took were some mean hacks).
		// Note how it will exit with stderr and stdout in the failure message,
		// which is extremely helpful.
		c.Run(ctx, c.Node(1), "echo foo && echo bar && notfound")
		return errors.New("impossible")
	})
	m.Wait()
}
