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

const (
	gcsTestBucket = `cockroach-tmp`
)

func init() {
	runImportTPCC := func(ctx context.Context, t *test, c *cluster, warehouses int) {
		c.Put(ctx, cockroach, "./cockroach")
		c.Put(ctx, workload, "./workload")
		t.Status("starting csv servers")
		c.Start(ctx)
		c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

		t.Status("running workload")
		m := newMonitor(ctx, c)
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				`./workload fixtures make tpcc --warehouses=%d --csv-server='http://localhost:8081' `+
					`--gcs-bucket-override=%s --gcs-prefix-override=%s`,
				warehouses, gcsTestBucket, c.name)
			c.Run(ctx, c.Node(1), cmd)
			return nil
		})
		m.Wait()
	}

	const warehouses = 1000
	const numNodes = 4
	tests.Add(testSpec{
		Name:  fmt.Sprintf("import/tpcc/warehouses=%d/nodes=%d", warehouses, numNodes),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runImportTPCC(ctx, t, c, warehouses)
		},
	})
}

func init() {
	for _, n := range []int{4, 8} {
		tests.Add(testSpec{
			Name:  fmt.Sprintf(`import/tpch/nodes=%d`, n),
			Nodes: nodes(n),
			Run: func(ctx context.Context, t *test, c *cluster) {
				c.Put(ctx, cockroach, "./cockroach")
				c.Start(ctx)
				conn := c.Conn(ctx, 1)
				if _, err := conn.Exec(`create database csv`); err != nil {
					t.Fatal(err)
				}
				t.Status(`running import`)
				if _, err := conn.Exec(`
				IMPORT TABLE csv.lineitem
				CREATE USING 'gs://cockroach-fixtures/tpch-csv/schema/lineitem.sql'
				CSV DATA (
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.1',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.2',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.3',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.4',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.5',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.6',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.7',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.8'
				) WITH  delimiter='|'
			`); err != nil {
					t.Fatal(err)
				}
			},
		})
	}
}
