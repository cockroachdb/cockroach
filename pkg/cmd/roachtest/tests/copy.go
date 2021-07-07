// Copyright 2018 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
)

func registerCopy(r registry.Registry) {
	// This test imports a fully-populated Bank table. It then creates an empty
	// Bank schema. Finally, it performs a series of `INSERT ... SELECT ...`
	// statements to copy all data from the first table into the second table.
	runCopy := func(ctx context.Context, t test.Test, c cluster.Cluster, rows int, inTxn bool) {
		// payload is the size of the payload column for each row in the Bank
		// table. If this is adjusted, a new fixture may need to be generated.
		const payload = 100
		// rowOverheadEstimate is an estimate of the overhead of a single
		// row in the Bank table, not including the size of the payload
		// itself. This overhead includes the size of the other two columns
		// in the table along with the size of each row's associated KV key.
		const rowOverheadEstimate = 160
		const rowEstimate = rowOverheadEstimate + payload

		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.All())
		c.Start(ctx, c.All())

		m := c.NewMonitor(ctx, c.All())
		m.Go(func(ctx context.Context) error {
			db := c.Conn(ctx, 1)
			defer db.Close()

			// Disable load-based splitting so that we can more accurately
			// predict an upper-bound on the number of ranges that the cluster
			// will end up with.
			if err := disableLoadBasedSplitting(ctx, db); err != nil {
				return errors.Wrap(err, "disabling load-based splitting")
			}

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
					t.Fatalf("failed to get range count: %v", err)
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
				QueryRowContext(ctx context.Context, query string, args ...interface{}) *gosql.Row
			}
			runCopy := func(ctx context.Context, qu querier) error {
				ctx, cancel := context.WithTimeout(ctx, 10*time.Minute) // avoid infinite internal retries
				defer cancel()

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
					if err := qu.QueryRowContext(ctx, q).Scan(&lastID); err != nil {
						return err
					}
				}
				return nil
			}

			var err error
			if inTxn {
				attempt := 0
				err = crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
					attempt++
					if attempt > 5 {
						return errors.Errorf("aborting after %v failed attempts", attempt-1)
					}
					t.Status(fmt.Sprintf("copying (attempt %v)", attempt))
					return runCopy(ctx, tx)
				})
			} else {
				err = runCopy(ctx, db)
			}
			if err != nil {
				t.Fatalf("failed to copy rows: %s", err)
			}
			rangeMinBytes, rangeMaxBytes, err := getDefaultRangeSize(ctx, db)
			if err != nil {
				t.Fatalf("failed to get default range size: %v", err)
			}
			rc := rangeCount()
			t.L().Printf("range count after copy = %d\n", rc)
			lowExp := (rows * rowEstimate) / rangeMaxBytes
			highExp := int(math.Ceil(float64(rows*rowEstimate) / float64(rangeMinBytes)))
			if rc > highExp || rc < lowExp {
				return errors.Errorf("expected range count for table between %d and %d, found %d",
					lowExp, highExp, rc)
			}
			return nil
		})
		m.Wait()
	}

	// We use a smaller dataset with a txn, to have a very large margin to the
	// commit deadline (5 minutes). Otherwise, if the txn takes longer than
	// usual (for whatever reason) and triggers a RETRY_COMMIT_DEADLINE_EXCEEDED
	// error, the retry is likely to 1) take longer due to e.g. intent handling,
	// and 2) have a shorter deadline due to existing table descriptor leases.
	// If the margin is too small, the retries will always exceed the deadline
	// and keep retrying forever. See:
	// https://github.com/cockroachdb/cockroach/issues/62470
	testcases := []struct {
		rows  int
		nodes int
		txn   bool
	}{
		{rows: int(1e6), nodes: 9, txn: false},
		{rows: int(1e5), nodes: 5, txn: true},
	}

	for _, tc := range testcases {
		tc := tc
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("copy/bank/rows=%d,nodes=%d,txn=%t", tc.rows, tc.nodes, tc.txn),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(tc.nodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runCopy(ctx, t, c, tc.rows, tc.txn)
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
