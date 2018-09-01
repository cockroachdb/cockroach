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
	c.Start(ctx)

	db := c.Conn(ctx, 1)
	defer db.Close()

	waitForFullReplication(t, db)

	lastWords := func(s string) []string {
		var result []string
		for _, line := range strings.Split(s, "\n") {
			words := strings.Fields(line)
			if n := len(words); n > 0 {
				result = append(result, words[n-1])
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
		expected := []string{"is_live", "true", "true", "true"}
		actual := nodeStatus()
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s:\n", expected, actual)
		}
	}

	{
		// Kill nodes 2 and 3 and wait for all of the nodes to be marked as
		// !is_live. Node 1 is not live because the liveness check can no longer
		// write to the liveness range due to lack of quorum. This test is
		// verifying that "node status" still returns info in this situation sa it
		// only accesses gossip info.
		c.Stop(ctx, c.Range(2, 3))
		expected := []string{"is_live", "false", "false", "false"}
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
}
