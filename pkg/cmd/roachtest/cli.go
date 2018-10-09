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
	"reflect"
	"strings"
	"time"
)

func runCLINodeStatus(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t, c.Range(1, 3), startArgs("--sequential"))

	db := c.Conn(ctx, 1)
	defer db.Close()

	waitForFullReplication(t, db)

	lastWords := func(s string) []string {
		var result []string
		for _, line := range strings.Split(s, "\n") {
			words := strings.Fields(line)
			if n := len(words); n > 0 {
				result = append(result, words[n-2]+" "+words[n-1])
			}
		}
		return result
	}

	nodeStatus := func() []string {
		out, err := c.RunWithBuffer(ctx, c.l, c.Node(1),
			"./cockroach node status --insecure -p {pgport:1}")
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		return lastWords(string(out))
	}

	{
		expected := []string{
			"is_available is_live",
			"true true",
			"true true",
			"true true",
		}
		actual := nodeStatus()
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s:\n", expected, actual)
		}
	}

	waitUntil := func(expected []string) {
		var actual []string
		// Node liveness takes ~9s to time out. Give the test double that time.
		for i := 0; i < 20; i++ {
			actual = nodeStatus()
			if reflect.DeepEqual(expected, actual) {
				break
			}
			c.l.Printf("not done: %s vs %s\n", expected, actual)
			time.Sleep(time.Second)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s:\n", expected, actual)
		}
	}

	// Kill node 2 and wait for it to be marked as !is_available and !is_live.
	c.Stop(ctx, c.Node(2))
	waitUntil([]string{
		"is_available is_live",
		"true true",
		"false false",
		"true true",
	})

	// Kill node 3 and wait for all of the nodes to be marked as
	// !is_available. Node 1 is not available because the liveness check can no
	// longer write to the liveness range due to lack of quorum. This test is
	// verifying that "node status" still returns info in this situation since
	// it only accesses gossip info.
	c.Stop(ctx, c.Node(3))
	waitUntil([]string{
		"is_available is_live",
		"false true",
		"false false",
		"false false",
	})
}
