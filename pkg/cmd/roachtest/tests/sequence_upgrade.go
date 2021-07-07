// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerSequenceUpgrade(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "version/sequence-upgrade",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(1),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSequenceUpgradeMigration(ctx, t, c)
		},
	})
}

func statementStep(stmt string, node int, args ...interface{}) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx, stmt, args...)
		if err != nil {
			t.Fatal(fmt.Sprintf("error executing statement %s with args %v: %+v", stmt, args, err))
		}
	}
}

func createSequenceStep(node int, name string) versionStep {
	stmt := fmt.Sprintf(`CREATE SEQUENCE %s`, name)
	return statementStep(stmt, node)
}

func setSerialNormalizationStep(node int) versionStep {
	stmt := `SET serial_normalization = 'sql_sequence'`
	return statementStep(stmt, node)
}

func createSeqTableStep(node int, tblName string, seqName string) versionStep {
	stmt := fmt.Sprintf(`CREATE TABLE %s (i SERIAL PRIMARY KEY, j INT NOT NULL DEFAULT nextval('%s'))`, tblName, seqName)
	return statementStep(stmt, node)
}

func insertSeqTableStep(node int, name string) versionStep {
	stmt := fmt.Sprintf(`INSERT INTO %s VALUES (default, default)`, name)
	return statementStep(stmt, node)
}

func verifySequences(node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)

		// Verify that sequences created in older versions cannot be renamed, nor can any
		// databases that they are referencing.
		_, err := db.ExecContext(ctx, `ALTER SEQUENCE test.public.s RENAME TO test.public.s_new`)
		if err == nil {
			t.Fatal("expected: cannot rename relation \"test.public.s\" because view \"t1\" depends on it")
		}
		_, err = db.ExecContext(ctx, `ALTER SEQUENCE t1_i_seq RENAME TO t1_i_seq_new`)
		if err == nil {
			t.Fatal("expected: cannot rename relation \"t1_i_seq\" because view \"test.public.t1\" depends on it")
		}
		_, err = db.ExecContext(ctx, `ALTER DATABASE test RENAME TO test_new`)
		if err == nil {
			t.Fatal("expected: cannot rename database because relation \"test.public.t1\" depends on relation \"test.public.s\"")
		}
	}
}

func runSequenceUpgradeMigration(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		v20_2 = "20.2.4"
		// An empty string means that the cockroach binary specified by flag
		// `cockroach` will be used.
		mainVersion = ""
	)

	roachNodes := c.All()
	u := newVersionUpgradeTest(c,
		// Set up descriptors in 20.2, when sequence renaming and other
		// renaming involving sequence dependencies was unsupported.
		uploadAndStart(roachNodes, v20_2),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		createDBStep(1, "test"),
		createSequenceStep(1, "test.public.s"),
		setSerialNormalizationStep(1),
		createSeqTableStep(1, "t1", "test.public.s"),

		// Upgrade cluster to latest version, where renaming
		// is supported.
		allowAutoUpgradeStep(1),
		binaryUpgradeStep(c.All(), mainVersion),
		waitForUpgradeStep(roachNodes),

		// Verify that the sequences created in older versions
		// still behave as old sequences (i.e. cannot be renamed).
		verifySequences(1),
		insertSeqTableStep(1, "t1"),
	)
	u.run(ctx, t)
}
