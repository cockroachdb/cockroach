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
)

func init() {
	runRoachmart := func(t *test, partition bool) {
		ctx := context.Background()
		c := newCluster(ctx, t, 9, "--geo")
		defer c.Destroy(ctx)

		c.Put(ctx, cockroach, "<cockroach>")
		c.Put(ctx, workload, "<workload>")
		c.Start(ctx)

		// TODO(benesch): avoid hardcoding this list.
		nodes := []struct {
			i    int
			zone string
		}{
			{1, "us-east1-b"},
			{4, "us-west1-b"},
			{7, "europe-west2-b"},
		}

		roachmartRun := func(ctx context.Context, i int, args ...string) {
			args = append(args,
				"--local-zone="+nodes[i].zone,
				"--local-percent=90",
				"--users=10",
				"--orders=100",
				fmt.Sprintf("--partition=%v", partition))

			l, err := c.l.childLogger(fmt.Sprint(nodes[i].i))
			if err != nil {
				t.Fatal(err)
			}
			defer l.close()
			c.RunL(ctx, l, nodes[i].i, args...)
		}
		roachmartRun(ctx, 0, "<workload>", "init", "roachmart")

		duration := " --duration=" + ifLocal("10s", "10m")

		m := newMonitor(ctx, c)
		for i := range nodes {
			i := i
			m.Go(func(ctx context.Context) error {
				roachmartRun(ctx, i, "<workload>", "run", "roachmart", duration)
				return nil
			})
		}

		m.Wait()
	}

	for _, v := range []bool{true, false} {
		v := v
		tests.Add(fmt.Sprintf("roachmart/partition=%v", v),
			func(t *test) {
				runRoachmart(t, v)
			})
	}
}
