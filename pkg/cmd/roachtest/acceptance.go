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

import "context"

func registerAcceptance(r *registry) {
	spec := testSpec{
		Name:  "acceptance",
		Nodes: nodes(3),
	}

	testCases := []struct {
		name string
		fn   func(ctx context.Context, t *test, c *cluster)
	}{
		{"build-info", runBuildInfo},
		{"cli/node-status", runCLINodeStatus},
		{"event-log", runEventLog},
	}
	for _, tc := range testCases {
		spec.SubTests = append(spec.SubTests, testSpec{
			Name:   tc.name,
			Stable: true, // DO NOT COPY to new tests
			Run:    tc.fn,
		})
	}

	r.Add(spec)
}
