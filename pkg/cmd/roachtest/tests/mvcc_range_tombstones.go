// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

func registerMVCCRangeTombstones(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    `mvcc-range-tombstones`,
		Owner:   registry.OwnerStorage,
		Timeout: 2 * time.Hour,
		Cluster: r.MakeClusterSpec(5, spec.CPU(16)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMVCCRangeTombstones(ctx, t, c)
		},
	})
}
func runMVCCRangeTombstones(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	t.Status("starting csv servers")
	c.Run(ctx, c.All(), `./cockroach workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

	conn := c.Conn(ctx, t.L(), 1)

	t.Status("creating SQL tables")
	if _, err := conn.Exec(`CREATE DATABASE csv;`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(`USE csv;`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(
		`SET CLUSTER SETTING kv.bulk_ingest.max_index_buffer_size = '2gb'`,
	); err != nil && !strings.Contains(err.Error(), "unknown cluster setting") {
		t.Fatal(err)
	}
	rng, seed := randutil.NewPseudoRand()

	tables := []string{"part", "supplier", "partsupp", "customer", "orders", "lineitem"}
	for _, tbl := range tables {
		fixtureURL := fmt.Sprintf("gs://cockroach-fixtures/tpch-csv/schema/%s.sql?AUTH=implicit", tbl)
		createStmt, err := readCreateTableFromFixture(fixtureURL, conn)
		if err != nil {
			t.Fatal(err)
		}
		// Create table to import into.
		if _, err := conn.ExecContext(ctx, createStmt); err != nil {
			t.Fatal(err)
		}
		// Set a random GC TTL that is relatively short. We want to exercise GC
		// of MVCC range tombstones, and ensure there's a mix of live MVCC Range
		// Tombstones and the Pebble RangeKeyUnset tombstones that clear them.
		ttl := randutil.RandIntInRange(rng, 60*5 /* 5m */, 60*30 /* 30m */)
		stmt := fmt.Sprintf(`ALTER TABLE csv.%s CONFIGURE ZONE USING gc.ttlseconds = $1`, tbl)
		_, err = conn.ExecContext(ctx, stmt, ttl)
		if err != nil {
			t.Fatal(err)
		}
	}

	test := mvccRangeTombstoneTest{
		Test:    t,
		c:       c,
		rootRng: rng,
		seed:    seed,
	}
	m := c.NewMonitor(ctx)
	t.Status("running canceled imports with seed", seed)
	for _, tableName := range tables {
		tableName := tableName
		rng := rand.New(rand.NewSource(test.rootRng.Int63()))
		m.Go(func(ctx context.Context) error {
			t.WorkerStatus(`launching worker for`, tableName)
			defer t.WorkerStatus()

			test.runImportSequence(ctx, rng, tableName)
			return nil
		})
	}
	m.Wait()
}

type mvccRangeTombstoneTest struct {
	test.Test
	c       cluster.Cluster
	rootRng *rand.Rand
	seed    int64
}

func (t *mvccRangeTombstoneTest) makeFilename(tableName string, number int) string {
	return fmt.Sprintf(`'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.%[2]d?AUTH=implicit'`, tableName, number)
}

func (t *mvccRangeTombstoneTest) runImportSequence(
	ctx context.Context, rng *rand.Rand, tableName string,
) {
	n := t.c.All()[rng.Intn(len(t.c.All()))]
	conn := t.c.Conn(ctx, t.L(), n)
	if _, err := conn.Exec(`USE csv;`); err != nil {
		t.Fatal(err)
	}

	attempts := randutil.RandIntInRange(rng, 1, 5)
	for i := 0; i < attempts; i++ {
		t.WorkerStatus(fmt.Sprintf(`attempt %d/%d for table %q`, i+1, attempts, tableName))
		func(i int) {
			ctx := ctx
			// If not the last attempt, run with a timeout.
			if i != attempts-1 {
				var cancel func()
				// Use a random timeout in [10s,3m).
				timeout := time.Duration(randutil.RandIntInRange(rng, 10, 60*3)) * time.Second
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			const fileCount = 8
			// If not the last attempt, import a subset of the files available.
			// This will create MVCC range tombstones across separate regions of
			// the table's keyspace.
			var files []string
			if i != attempts-1 {
				perm := rng.Perm(fileCount)
				for _, f := range perm[:randutil.RandIntInRange(rng, 1, fileCount)] {
					files = append(files, t.makeFilename(tableName, f+1))
				}
			} else {
				for j := 1; j <= fileCount; j++ {
					files = append(files, t.makeFilename(tableName, j))
				}
			}

			importStmt := fmt.Sprintf(`
				IMPORT INTO %[1]s
				CSV DATA (
				%[2]s
				) WITH  delimiter='|'
			`, tableName, strings.Join(files, ", "))
			_, err := conn.ExecContext(ctx, importStmt)
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		}(i)
	}
}
