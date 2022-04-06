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
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerIndexBackfill(r registry.Registry) {

	setupTest := func(ctx context.Context, t test.Test, c cluster.Cluster) (option.NodeListOption, option.NodeListOption) {
		roachNodes := c.Range(1, c.Spec().NodeCount-1)
		loadNode := c.Node(c.Spec().NodeCount)
		t.Status("copying binaries")
		c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", loadNode)

		t.Status("starting cockroach nodes")
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), roachNodes)
		return roachNodes, loadNode
	}

	// runIndexBackfillTPCC creates an index while the TPCC workload runs. After
	// the index creation and the workload finishes, the test creates another
	// index on the same columns using the old index backfiller, and compares the
	// fingerprints of the two indexes.
	runIndexBackfillTPCC := func(ctx context.Context, t test.Test, c cluster.Cluster, warehouses int, duration time.Duration) {
		roachNodes, loadNode := setupTest(ctx, t, c)

		cmd := fmt.Sprintf(
			"./cockroach workload fixtures import tpcc --warehouses=%d --db=tpcc",
			warehouses,
		)
		if err := c.RunE(ctx, c.Node(roachNodes[0]), cmd); err != nil {
			t.Fatal(err)
		}

		db := c.Conn(ctx, t.L(), roachNodes[0])
		defer db.Close()

		// Pick a random type of index to create.
		createFmts := []string{
			`CREATE INDEX %s ON tpcc.order (o_carrier_id);`,
			`CREATE UNIQUE INDEX %s ON tpcc."order" (o_entry_d, o_w_id, o_d_id, o_carrier_id, o_id);`,
			`CREATE INDEX %s ON tpcc."order" (mod(o_id+o_carrier_id, 10));`,
		}

		rand, _ := randutil.NewTestRand()
		randFmt := createFmts[rand.Intn(len(createFmts))]
		t.Status("Running command: ", randFmt)

		oldIdx := "idx_old_ib"
		newIdx := "idx_new_ib"

		m := c.NewMonitor(ctx, roachNodes)
		m.Go(func(ctx context.Context) error {
			// Start running the workload.
			runCmd := fmt.Sprintf(
				"./workload run tpcc --warehouses=%d --split --scatter --duration=%s {pgurl%s}",
				warehouses,
				duration,
				roachNodes,
			)
			t.Status("beginning workload")
			c.Run(ctx, loadNode, runCmd)
			t.Status("finished running workload")
			return nil
		})
		m.Go(func(ctx context.Context) error {
			// Start a secondary index build after some delay.
			time.Sleep(duration / 10)

			createStmt := fmt.Sprintf(randFmt, newIdx)
			t.Status("creating index")
			if _, err := db.ExecContext(ctx, createStmt); err != nil {
				t.Fatal(err)
			}
			t.Status("index build finished")
			return nil
		})

		m.Wait()

		// Create index using old index backfiller after workload has finished.
		time.Sleep(duration / 10)
		createStmt := fmt.Sprintf(randFmt, oldIdx)
		alterCmd := `SET CLUSTER SETTING sql.mvcc_compliant_index_creation.enabled = false; %s;`

		t.Status("creating index using old index backfiller")
		if _, err := db.ExecContext(ctx, fmt.Sprintf(alterCmd, createStmt)); err != nil {
			t.Fatal(err)
		}

		t.Status("starting validation")
		err := validateSecondaryIndex(db, `tpcc."order"`, oldIdx, newIdx)
		if err != nil {
			t.Fatal(err)
		}
	}

	// runBackfillInvertedIndex first creates a table with a JSON column and loads
	// it with 10 minutes of JSON workload. It then creates an inverted index
	// while the JSON workload runs. After the index creation and the workload
	// finishes, the test creates another index on the same columns using the old
	// index backfiller, and compares the fingerprints of the two indexes.
	runBackfillInvertedIndex := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		roachNodes, loadNode := setupTest(ctx, t, c)

		cmd := fmt.Sprintf("./workload init json {pgurl%s}", roachNodes)
		if err := c.RunE(ctx, loadNode, cmd); err != nil {
			t.Fatal(err)
		}

		db := c.Conn(ctx, t.L(), roachNodes[0])
		defer db.Close()

		initialDataDuration := time.Minute * 10
		indexDuration := 2 * initialDataDuration

		// First generate random JSON data using the JSON workload.
		m := c.NewMonitor(ctx, roachNodes)

		cmdWrite := fmt.Sprintf(
			"./workload run json --read-percent=0 --duration %s {pgurl%s} --batch 1000 --sequential",
			initialDataDuration.String(), roachNodes,
		)
		m.Go(func(ctx context.Context) error {
			c.Run(ctx, loadNode, cmdWrite)

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			var count int
			if err := db.QueryRow(`SELECT count(*) FROM json.j`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			t.L().Printf("finished writing %d rows to table", count)

			return nil
		})

		m.Wait()

		// Run the workload (with both reads and writes), and create the index at the same time.
		m = c.NewMonitor(ctx, roachNodes)
		oldIdx := "idx_old_ib"
		newIdx := "idx_new_ib"
		createFmt := `CREATE INVERTED INDEX %s ON json.j (v)`

		cmdWriteAndRead := fmt.Sprintf(
			"./workload run json --read-percent=50 --duration %s {pgurl:1-%d} --sequential",
			indexDuration.String(), c.Spec().NodeCount-1,
		)
		m.Go(func(ctx context.Context) error {
			c.Run(ctx, loadNode, cmdWriteAndRead)
			return nil
		})

		m.Go(func(ctx context.Context) error {
			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			t.L().Printf("creating index %s", newIdx)
			start := timeutil.Now()
			if _, err := db.Exec(fmt.Sprintf(createFmt, newIdx)); err != nil {
				return err
			}
			t.L().Printf("index was created, took %v", timeutil.Since(start))

			return nil
		})

		m.Wait()

		createStmt := fmt.Sprintf(createFmt, oldIdx)
		alterCmd := `SET CLUSTER SETTING sql.mvcc_compliant_index_creation.enabled = false; %s;`

		t.Status("creating index using old index backfiller")
		if _, err := db.ExecContext(ctx, fmt.Sprintf(alterCmd, createStmt)); err != nil {
			t.Fatal(err)
		}

		t.Status("starting validation")
		err := validateSecondaryIndex(db, "json.j", oldIdx, newIdx)
		if err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:  "indexbackfill-secondary-tpcc-250",
		Owner: registry.OwnerSQLSchema,
		// Use a 4 node cluster -- 3 nodes will run cockroach, and the last will be the
		// workload driver node.
		Cluster: r.MakeClusterSpec(4, spec.CPU(32)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runIndexBackfillTPCC(ctx, t, c, 250 /* warehouses */, 10*time.Minute /* duration */)
		},
	})

	r.Add(registry.TestSpec{
		Name:  "indexbackfill-secondary-inverted",
		Owner: registry.OwnerSQLSchema,
		// Use a 4 node cluster -- 3 nodes will run cockroach, and the last will be the
		// workload driver node.
		Cluster: r.MakeClusterSpec(4, spec.CPU(32)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runBackfillInvertedIndex(ctx, t, c)
		},
	})
}

// validateSecondaryIndex validates the contents of newIdx by getting its
// experimental fingerprint and comparing it to the fingerprint of oldIdx.
func validateSecondaryIndex(db *gosql.DB, table string, oldIdx string, newIdx string) error {
	eFingerprint, err := getFingerprintForIndex(db, table, oldIdx)
	if err != nil {
		return err
	}
	fingerprint, err := getFingerprintForIndex(db, table, newIdx)
	if err != nil {
		return err
	}

	if fingerprint != eFingerprint {
		return errors.Errorf("expected fingerprint for index %s to equal %d in index %s, got %d",
			newIdx, eFingerprint, oldIdx, fingerprint)
	}

	return nil
}

func getFingerprintForIndex(db *gosql.DB, table string, index string) (int64, error) {
	query := `SELECT fingerprint FROM [ SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s ] WHERE index_name = '%s';`
	var fingerprint int64
	err := db.QueryRow(fmt.Sprintf(query, table, index)).Scan(&fingerprint)
	return fingerprint, err
}
