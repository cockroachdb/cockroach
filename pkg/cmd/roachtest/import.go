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
	runImportTPCC := func(t *test, warehouses, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Range(1, nodes))
		t.Status("starting csv servers")
		c.Start(ctx, c.Range(1, nodes))
		for node := 1; node <= nodes; node++ {
			c.Run(ctx, node, `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)
		}

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				`./workload fixtures store tpcc --warehouses=%d --csv-server='http://localhost:8081' `+
					`--gcs-bucket-override=cockroachdb-backup-testing --gcs-prefix-override=%s`,
				warehouses, c.name)
			c.Run(ctx, 1, cmd)
			return nil
		})
		m.Wait()
	}

	const warehouses, nodes = 1000, 4
	tests.Add(fmt.Sprintf("import/tpcc/warehouses=%d/nodes=%d", warehouses, nodes), func(t *test) {
		runImportTPCC(t, warehouses, nodes)
	})
}

func init() {
	for _, nodes := range []int{4, 8} {
		nodes := nodes
		tests.Add(fmt.Sprintf(`import/tpch/nodes=%d`, nodes), func(t *test) {
			ctx := context.Background()
			c := newCluster(ctx, t, nodes)
			defer c.Destroy(ctx)

			c.Put(ctx, cockroach, "./cockroach")
			c.Start(ctx)
			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				c.status(`running import`)
				c.Run(ctx, 1, `./cockroach sql --insecure -e "create database csv"`)
				c.Run(ctx, 1, `./cockroach sql --insecure -e "
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
				) WITH  delimiter='|'`)
				return nil
			})
			m.Wait()
		})
	}
}
