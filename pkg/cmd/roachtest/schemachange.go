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
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func registerSchemaChangeKV(r *registry) {
	r.Add(testSpec{
		Name:   `schemachange/kv`,
		Nodes:  nodes(5),
		Stable: true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			const fixturePath = `gs://cockroach-fixtures/workload/tpch/scalefactor=10/backup`

			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, workload, "./workload")

			c.Start(ctx, t, c.All())
			db := c.Conn(ctx, 1)
			defer db.Close()

			m := newMonitor(ctx, c, c.All())
			m.Go(func(ctx context.Context) error {
				t.Status("loading fixture")
				if _, err := db.Exec(`RESTORE DATABASE tpch FROM $1`, fixturePath); err != nil {
					t.Fatal(err)
				}
				return nil
			})
			m.Wait()

			c.Run(ctx, c.Node(1), `./workload init kv --drop --db=test`)
			for node := 1; node <= c.nodes; node++ {
				node := node
				// TODO(dan): Ideally, the test would fail if this queryload failed,
				// but we can't put it in monitor as-is because the test deadlocks.
				go func() {
					const cmd = `./workload run kv --tolerate-errors --min-block-bytes=8 --max-block-bytes=128 --db=test`
					l, err := t.l.ChildLogger(fmt.Sprintf(`kv-%d`, node))
					if err != nil {
						t.Fatal(err)
					}
					defer l.close()
					_ = execCmd(ctx, t.l, roachprod, "ssh", c.makeNodes(c.Node(node)), "--", cmd)
				}()
			}

			m = newMonitor(ctx, c, c.All())
			m.Go(func(ctx context.Context) error {
				t.Status("running schema change tests")
				return waitForSchemaChanges(ctx, t.l, db)
			})
			m.Wait()
		},
	})
}

func waitForSchemaChanges(ctx context.Context, l *logger, db *gosql.DB) error {
	start := timeutil.Now()

	// These schema changes are over a table that is not actively
	// being updated.
	l.Printf("running schema changes over tpch.customer\n")
	schemaChanges := []string{
		"ALTER TABLE tpch.customer ADD COLUMN newcol INT DEFAULT 23456",
		"CREATE INDEX foo ON tpch.customer (c_name)",
	}
	if err := runSchemaChanges(ctx, l, db, schemaChanges); err != nil {
		return err
	}

	// TODO(vivek): Fix #21544.
	// if err := sqlutils.RunScrub(db, `test`, `kv`); err != nil {
	//   return err
	// }

	// All these return the same result.
	validationQueries := []string{
		"SELECT count(*) FROM tpch.customer AS OF SYSTEM TIME %s",
		"SELECT count(newcol) FROM tpch.customer AS OF SYSTEM TIME %s",
		"SELECT count(c_name) FROM tpch.customer@foo AS OF SYSTEM TIME %s",
	}
	if err := runValidationQueries(ctx, l, db, start, validationQueries, nil); err != nil {
		return err
	}

	// These schema changes are run later because the above schema
	// changes run for a decent amount of time giving kv.kv
	// an opportunity to get populate through the load generator. These
	// schema changes are acting upon a decent sized table that is also
	// being updated.
	l.Printf("running schema changes over test.kv\n")
	schemaChanges = []string{
		"ALTER TABLE test.kv ADD COLUMN created_at TIMESTAMP DEFAULT now()",
		"CREATE INDEX foo ON test.kv (v)",
	}
	if err := runSchemaChanges(ctx, l, db, schemaChanges); err != nil {
		return err
	}

	// TODO(vivek): Fix #21544.
	// if err := sqlutils.RunScrub(db, `test`, `kv`); err != nil {
	//	return err
	// }

	// All these return the same result.
	validationQueries = []string{
		"SELECT count(*) FROM test.kv AS OF SYSTEM TIME %s",
		"SELECT count(v) FROM test.kv AS OF SYSTEM TIME %s",
		"SELECT count(v) FROM test.kv@foo AS OF SYSTEM TIME %s",
	}
	// Queries to hone in on index validation problems.
	indexValidationQueries := []string{
		"SELECT count(k) FROM test.kv@primary AS OF SYSTEM TIME %s WHERE created_at > $1 AND created_at <= $2",
		"SELECT count(v) FROM test.kv@foo AS OF SYSTEM TIME %s WHERE created_at > $1 AND created_at <= $2",
	}
	return runValidationQueries(ctx, l, db, start, validationQueries, indexValidationQueries)
}

func runSchemaChanges(ctx context.Context, l *logger, db *gosql.DB, schemaChanges []string) error {
	for _, cmd := range schemaChanges {
		start := timeutil.Now()
		l.Printf("starting schema change: %s\n", cmd)
		if _, err := db.Exec(cmd); err != nil {
			l.Errorf("hit schema change error: %s, for %s, in %s\n", err, cmd, timeutil.Since(start))
			return err
		}
		l.Printf("completed schema change: %s, in %s\n", cmd, timeutil.Since(start))
		// TODO(vivek): Monitor progress of schema changes and log progress.
	}

	return nil
}

// The validationQueries all return the same result.
func runValidationQueries(
	ctx context.Context,
	l *logger,
	db *gosql.DB,
	start time.Time,
	validationQueries []string,
	indexValidationQueries []string,
) error {
	// Sleep for a bit before validating the schema changes to
	// accommodate for time differences between nodes. Some of the
	// schema change backfill transactions might use a timestamp a bit
	// into the future. This is not a problem normally because a read
	// of schema data written into the impending future gets pushed,
	// but the reads being done here are at a specific timestamp through
	// AS OF SYSTEM TIME.
	time.Sleep(5 * time.Second)

	var nowString string
	if err := db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&nowString); err != nil {
		return err
	}
	var nowInNanos int64
	if _, err := fmt.Sscanf(nowString, "%d", &nowInNanos); err != nil {
		return err
	}
	now := timeutil.Unix(0, nowInNanos)

	// Validate the different schema changes
	var eCount int64
	for i := range validationQueries {
		var count int64
		q := fmt.Sprintf(validationQueries[i], nowString)
		if err := db.QueryRow(q).Scan(&count); err != nil {
			return err
		}
		l.Printf("query: %s, found %d rows\n", q, count)
		if count == 0 {
			return errors.Errorf("%s: %d rows found", q, count)
		}
		if eCount == 0 {
			eCount = count
			// Investigate index creation problems. Always run this so we know
			// it works.
			if indexValidationQueries != nil {
				sp := timeSpan{start: start, end: now}
				if err := findIndexProblem(
					ctx, l, db, sp, nowString, indexValidationQueries,
				); err != nil {
					return err
				}
			}
		} else if count != eCount {
			return errors.Errorf("%s: %d rows found, expected %d rows", q, count, eCount)
		}
	}
	return nil
}

type timeSpan struct {
	start, end time.Time
}

// Check index inconsistencies over the timeSpan and return true when
// problems are seen.
func checkIndexOverTimeSpan(
	ctx context.Context,
	l *logger,
	db *gosql.DB,
	s timeSpan,
	nowString string,
	indexValidationQueries []string,
) (bool, error) {
	var eCount int64
	q := fmt.Sprintf(indexValidationQueries[0], nowString)
	if err := db.QueryRow(q, s.start, s.end).Scan(&eCount); err != nil {
		return false, err
	}
	var count int64
	q = fmt.Sprintf(indexValidationQueries[1], nowString)
	if err := db.QueryRow(q, s.start, s.end).Scan(&count); err != nil {
		return false, err
	}
	l.Printf("counts seen %d, %d, over [%s, %s]\n", count, eCount, s.start, s.end)
	return count != eCount, nil
}

// Keep splitting the span of time passed and log where index
// inconsistencies are seen.
func findIndexProblem(
	ctx context.Context,
	l *logger,
	db *gosql.DB,
	s timeSpan,
	nowString string,
	indexValidationQueries []string,
) error {
	spans := []timeSpan{s}
	// process all the outstanding time spans.
	for len(spans) > 0 {
		s := spans[0]
		spans = spans[1:]
		// split span into two time ranges.
		leftSpan, rightSpan := s, s
		d := s.end.Sub(s.start) / 2
		if d < 50*time.Millisecond {
			l.Printf("problem seen over [%s, %s]\n", s.start, s.end)
			continue
		}
		m := s.start.Add(d)
		leftSpan.end = m
		rightSpan.start = m

		leftState, err := checkIndexOverTimeSpan(
			ctx, l, db, leftSpan, nowString, indexValidationQueries)
		if err != nil {
			return err
		}
		rightState, err := checkIndexOverTimeSpan(
			ctx, l, db, rightSpan, nowString, indexValidationQueries)
		if err != nil {
			return err
		}
		if leftState {
			spans = append(spans, leftSpan)
		}
		if rightState {
			spans = append(spans, rightSpan)
		}
		if !(leftState || rightState) {
			l.Printf("no problem seen over [%s, %s]\n", s.start, s.end)
		}
	}
	return nil
}

func registerSchemaChangeTPCC(r *registry) {
	warehouses := 1000
	numNodes := 5
	r.Add(testSpec{
		Name:    fmt.Sprintf("schemachange/tpcc/warehouses=%d/nodes=%d", warehouses, numNodes),
		Nodes:   nodes(numNodes),
		Timeout: 4 * time.Hour,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Extra:      "--wait=false --tolerate-errors",
				During: func(ctx context.Context) error {
					conn := c.Conn(ctx, 1)
					start := timeutil.Now()
					if _, err := conn.Exec(`
					CREATE UNIQUE INDEX foo ON tpcc.order (o_entry_d, o_w_id, o_d_id, o_carrier_id, o_id);
				`); err != nil {
						t.Fatal(err)
					}
					c.l.Printf("CREATE INDEX took %s", timeutil.Since(start))
					return nil
				},
				Duration: 2 * time.Hour,
			})
		},
	})
}
