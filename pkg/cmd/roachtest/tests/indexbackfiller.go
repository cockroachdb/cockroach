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
		testCases := []struct {
			createFmt string
			table     string
		}{
			{`CREATE INDEX %s ON tpcc.order (o_carrier_id);`, `tpcc."order"`},
			{`CREATE UNIQUE INDEX %s ON tpcc."order" (o_entry_d, o_w_id, o_d_id, o_carrier_id, o_id);`, `tpcc."order"`},
			{`CREATE INDEX %s ON tpcc.customer (c_last, c_first)`, "tpcc.customer"},
			{`CREATE UNIQUE INDEX %s ON tpcc.customer (c_w_id, c_d_id, c_id, c_last, c_first)`, "tpcc.customer"},
		}

		rand, _ := randutil.NewTestRand()
		randTest := testCases[rand.Intn(len(testCases))]
		t.Status("Running command: ", randTest.createFmt)

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

			createStmt := fmt.Sprintf(randTest.createFmt, newIdx)
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
		createStmt := fmt.Sprintf(randTest.createFmt, oldIdx)
		t.Status("creating index using old index backfiller")
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING sql.mvcc_compliant_index_creation.enabled = false`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, createStmt); err != nil {
			t.Fatal(err)
		}

		t.Status("starting validation")
		err := validateSecondaryIndex(db, randTest.table, oldIdx, newIdx)
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
