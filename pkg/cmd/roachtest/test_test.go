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
	"io/ioutil"
	"testing"
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
