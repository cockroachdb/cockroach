// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file contains dummy tests, to be used for testing the test runner by
// hand. These tests do not have the "default" tag, so they are not generally
// run by roachtest invocations. You have to ask for them explicitly with
// `roachtest run tag:dummy`.

package main

import "context"

func registerExample(r *testRegistry) {
	clusterSpec := makeClusterSpec(1, cpu(4))
	r.Add(testSpec{
		Name:    "dummy/pass",
		Owner:   OwnerKV,
		Tags:    []string{`dummy`},
		Cluster: clusterSpec,
		Run: func(ctx context.Context, t *test, c *cluster) {
		},
	})
	r.Add(testSpec{
		Name:    "dummy/fail",
		Owner:   OwnerKV,
		Tags:    []string{`dummy`},
		Cluster: clusterSpec,
		Run: func(ctx context.Context, t *test, c *cluster) {
			t.Fatal("example failure")
		},
	})
}
