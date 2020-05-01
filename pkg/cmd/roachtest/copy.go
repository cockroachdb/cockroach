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
	"strings"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
)

func registerCopy(r *testRegistry) {
	// This test imports a fully-populated Bank table. It then creates an empty
	// Bank schema. Finally, it performs a series of `INSERT ... SELECT ...`
	// statements to copy all data from the first table into the second table.
	runCopy := func(ctx context.Context, t *test, c *cluster, rows int, inTxn bool) {
		// payload is the size of the payload column for each row in the Bank
		// table. If this is adjusted, a new fixture may need to be generated.
		const payload = 100
		// rowOverheadEstimate is an estimate of the overhead of a single
		// row in the Bank table, not including the size of the payload
		// itself. This overhead includes the size of the other two columns
		// in the table along with the size of each row's associated KV key.
		const rowOverheadEstimate = 160
		const rowEstimate = rowOverheadEstimate + payload

		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, t, c.All())

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			db := c.Conn(ctx, 1)
			defer db.Close()

			t.Status("importing Bank fixture")
			c.Run(ctx, c.Node(1), fmt.Sprintf(
				"./workload fixtures load bank --rows=%d --payload-bytes=%d {pgurl:1}",
				rows, payload))
			if _, err := db.Exec("ALTER TABLE bank.bank RENAME TO bank.bank_orig"); err != nil {
				t.Fatalf("failed to rename table: %v", err)
			}

			t.Status("create copy of Bank schema")
			c.Run(ctx, c.Node(1), "./workload init bank --rows=0 --ranges=0 {pgurl:1}")

			rangeCount := func() int {
				var count int
				const q = "SELECT count(*) FROM [SHOW RANGES FROM TABLE bank.bank]"
				if err := db.QueryRow(q).Scan(&count); err != nil {
					// TODO(rafi): Remove experimental_ranges query once we stop testing
					// 19.1 or earlier.
					if strings.Contains(err.Error(), "syntax error at or near \"ranges\"") {
						err = db.QueryRow("SELECT count(*) FROM [SHOW EXPERIMENTAL_RANGES FROM TABLE bank.bank]").Scan(&count)
					}
					if err != nil {
						t.Fatalf("failed to get range count: %v", err)
					}
				}
				return count
			}
			if rc := rangeCount(); rc != 1 {
				return errors.Errorf("empty bank table split over multiple ranges")
			}

			// Copy batches of rows from bank_orig to bank. Each batch needs to
			// be under kv.raft.command.max_size=64MB or we'll hit a "command is
			// too large" error. We play it safe and chose batches whose rows
			// add up to well less than this limit.
			rowsPerInsert := (60 << 20 /* 60MB */) / rowEstimate
			t.Status("copying from bank_orig to bank")

			// querier is a common interface shared by sql.DB and sql.Tx. It
			// can be replaced by https://github.com/golang/go/issues/14468 if
			// that is ever resolved.
			type querier interface {
				QueryRow(query string, args ...interface{}) *gosql.Row
			}
			runCopy := func(qu querier) error {
				for lastID := -1; lastID+1 < rows; {
					if lastID > 0 {
						t.Progress(float64(lastID+1) / float64(rows))
					}
					q := fmt.Sprintf(`
						SELECT id FROM [
							INSERT INTO bank.bank
							SELECT * FROM bank.bank_orig
							WHERE id > %d
							ORDER BY id ASC
							LIMIT %d
							RETURNING ID
						]
						ORDER BY id DESC
						LIMIT 1`,
						lastID, rowsPerInsert)
					if err := qu.QueryRow(q).Scan(&lastID); err != nil {
						return err
					}
				}
				return nil
			}

			var err error
			if inTxn {
				err = crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error { return runCopy(tx) })
			} else {
				err = runCopy(db)
			}
			if err != nil {
				t.Fatalf("failed to copy rows: %s", err)
			}
			rangeMinBytes, rangeMaxBytes, err := getDefaultRangeSize(ctx, db)
			if err != nil {
				t.Fatalf("failed to get default range size: %v", err)
			}
			rc := rangeCount()
			t.l.Printf("range count after copy = %d\n", rc)
			highExp := (rows * rowEstimate) / rangeMinBytes
			lowExp := (rows * rowEstimate) / rangeMaxBytes
			if rc > highExp || rc < lowExp {
				return errors.Errorf("expected range count for table between %d and %d, found %d",
					lowExp, highExp, rc)
			}
			return nil
		})
		m.Wait()
	}

	const rows = int(1e7)
	const numNodes = 9

	for _, inTxn := range []bool{true, false} {
		inTxn := inTxn
		r.Add(testSpec{
			Name:    fmt.Sprintf("copy/bank/rows=%d,nodes=%d,txn=%t", rows, numNodes, inTxn),
			Owner:   OwnerKV,
			Cluster: makeClusterSpec(numNodes),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runCopy(ctx, t, c, rows, inTxn)
			},
		})
	}
}

func getDefaultRangeSize(
	ctx context.Context, db *gosql.DB,
) (rangeMinBytes, rangeMaxBytes int, err error) {
	err = db.QueryRow(`SELECT
    regexp_extract(regexp_extract(raw_config_sql, e'range_min_bytes = \\d+'), e'\\d+')::INT8
        AS range_min_bytes,
    regexp_extract(regexp_extract(raw_config_sql, e'range_max_bytes = \\d+'), e'\\d+')::INT8
        AS range_max_bytes
FROM
    [SHOW ZONE CONFIGURATION FOR RANGE default];`).Scan(&rangeMinBytes, &rangeMaxBytes)
	// Older cluster versions do not contain this column. Use the old default.
	if err != nil && strings.Contains(err.Error(), `column "raw_config_sql" does not exist`) {
		rangeMinBytes, rangeMaxBytes, err = 32<<20 /* 32MB */, 64<<20 /* 64MB */, nil
	}
	return rangeMinBytes, rangeMaxBytes, err
}
