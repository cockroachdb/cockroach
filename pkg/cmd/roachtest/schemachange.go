// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func registerSchemaChangeKV(r *testRegistry) {
	r.Add(testSpec{
		Name:    `schemachange/mixed/kv`,
		Cluster: makeClusterSpec(5),
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
			for node := 1; node <= c.spec.NodeCount; node++ {
				node := node
				// TODO(dan): Ideally, the test would fail if this queryload failed,
				// but we can't put it in monitor as-is because the test deadlocks.
				go func() {
					const cmd = `./workload run kv --tolerate-errors --min-block-bytes=8 --max-block-bytes=127 --db=test`
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

func registerSchemaChangeIndexTPCC1000(r *testRegistry) {
	r.Add(makeIndexAddTpccTest(makeClusterSpec(5, cpu(16)), 1000, time.Hour*2))
}

func registerSchemaChangeIndexTPCC100(r *testRegistry) {
	r.Add(makeIndexAddTpccTest(makeClusterSpec(5), 100, time.Minute*15))
}

func makeIndexAddTpccTest(spec clusterSpec, warehouses int, length time.Duration) testSpec {
	return testSpec{
		Name:    fmt.Sprintf("schemachange/index/tpcc/w=%d", warehouses),
		Cluster: spec,
		Timeout: length * 3,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Extra:      "--wait=false --tolerate-errors",
				During: func(ctx context.Context) error {
					return runAndLogStmts(ctx, t, c, "addindex", []string{
						`CREATE UNIQUE INDEX ON tpcc.order (o_entry_d, o_w_id, o_d_id, o_carrier_id, o_id);`,
						`CREATE INDEX ON tpcc.order (o_carrier_id);`,
						`CREATE INDEX ON tpcc.customer (c_last, c_first);`,
					})
				},
				Duration: length,
			})
		},
		MinVersion: "v19.1.0",
	}
}

func registerSchemaChangeBulkIngest(r *testRegistry) {
	r.Add(makeSchemaChangeBulkIngestTest(5, 100000000, time.Minute*20))
}

func makeSchemaChangeBulkIngestTest(numNodes, numRows int, length time.Duration) testSpec {
	return testSpec{
		Name:    "schemachange/bulkingest",
		Cluster: makeClusterSpec(numNodes),
		Timeout: length * 2,
		// `fixtures import` (with the workload paths) is not supported in 2.1
		MinVersion: "v19.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			// Configure column a to have sequential ascending values, and columns b and c to be constant.
			// The payload column will be randomized and thus uncorrelated with the primary key (a, b, c).
			aNum := numRows
			if c.isLocal() {
				aNum = 100000
			}
			bNum := 1
			cNum := 1
			payloadBytes := 4

			crdbNodes := c.Range(1, c.spec.NodeCount-1)
			workloadNode := c.Node(c.spec.NodeCount)

			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, workload, "./workload", workloadNode)
			// TODO (lucy): Remove flag once the faster import is enabled by default
			c.Start(ctx, t, crdbNodes, startArgs("--env=COCKROACH_IMPORT_WORKLOAD_FASTER=true"))

			// Don't add another index when importing.
			cmdWrite := fmt.Sprintf(
				// For fixtures import, use the version built into the cockroach binary
				// so the tpcc workload-versions match on release branches.
				"./cockroach workload fixtures import bulkingest {pgurl:1} --a %d --b %d --c %d --payload-bytes %d --index-b-c-a=false",
				aNum, bNum, cNum, payloadBytes,
			)

			c.Run(ctx, workloadNode, cmdWrite)

			m := newMonitor(ctx, c, crdbNodes)

			indexDuration := length
			if c.isLocal() {
				indexDuration = time.Second * 30
			}
			cmdWriteAndRead := fmt.Sprintf(
				"./workload run bulkingest --duration %s {pgurl:1-%d} --a %d --b %d --c %d --payload-bytes %d",
				indexDuration.String(), c.spec.NodeCount-1, aNum, bNum, cNum, payloadBytes,
			)
			m.Go(func(ctx context.Context) error {
				c.Run(ctx, workloadNode, cmdWriteAndRead)
				return nil
			})

			m.Go(func(ctx context.Context) error {
				db := c.Conn(ctx, 1)
				defer db.Close()

				if !c.isLocal() {
					// Wait for the load generator to run for a few minutes before creating the index.
					sleepInterval := time.Minute * 5
					maxSleep := length / 2
					if sleepInterval > maxSleep {
						sleepInterval = maxSleep
					}
					time.Sleep(sleepInterval)
				}

				c.l.Printf("Creating index")
				before := timeutil.Now()
				if _, err := db.Exec(`CREATE INDEX payload_a ON bulkingest.bulkingest (payload, a)`); err != nil {
					t.Fatal(err)
				}
				c.l.Printf("CREATE INDEX took %v\n", timeutil.Since(before))
				return nil
			})

			m.Wait()
		},
	}
}

func runAndLogStmts(ctx context.Context, t *test, c *cluster, prefix string, stmts []string) error {
	db := c.Conn(ctx, 1)
	defer db.Close()
	c.l.Printf("%s: running %d statements\n", prefix, len(stmts))
	start := timeutil.Now()
	for i, stmt := range stmts {
		// Let some traffic run before the schema change.
		time.Sleep(time.Minute)
		c.l.Printf("%s: running statement %d...\n", prefix, i+1)
		before := timeutil.Now()
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
		c.l.Printf("%s: statement %d: %q took %v\n", prefix, i+1, stmt, timeutil.Since(before))
	}
	c.l.Printf("%s: ran %d statements in %v\n", prefix, len(stmts), timeutil.Since(start))
	return nil
}
