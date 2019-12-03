// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
)

// getShardColumnID fetches the id of the shard column associated with the given sharded
// index.
func getShardColumnID(
	t *testing.T, tableDesc *sqlbase.TableDescriptor, shardedIndexName string,
) sqlbase.ColumnID {
	idx, _, err := tableDesc.FindIndexByName(shardedIndexName)
	if err != nil {
		t.Fatal(err)
	}
	shardCol, _, err := tableDesc.FindColumnByName(tree.Name(idx.Sharded.Name))
	if err != nil {
		t.Fatal(err)
	}
	return shardCol.ID
}

// verifyTableDescriptorStates ensures that the given table descriptor fulfills the
// following conditions after the creation of a sharded index:
// 1. A hidden shard column was created.
// 2. A hidden check constraint was created on the aforementioned shard column.
// 3. The first column in the index set is the aforementioned shard column.
func verifyTableDescriptorState(
	t *testing.T, tableDesc *sqlbase.TableDescriptor, shardedIndexName string,
) {
	idx, _, err := tableDesc.FindIndexByName(shardedIndexName)
	if err != nil {
		t.Fatal(err)
	}

	if !idx.IsSharded() {
		t.Fatalf(`Expected index %s to be sharded`, shardedIndexName)
	}
	// Note that this method call will fail if the shard column doesn't exist
	shardColID := getShardColumnID(t, tableDesc, shardedIndexName)
	foundCheckConstraint := false
	for _, check := range tableDesc.AllActiveAndInactiveChecks() {
		usesShard, err := check.UsesColumn(tableDesc, shardColID)
		if err != nil {
			t.Fatal(err)
		}
		if usesShard && check.Hidden {
			foundCheckConstraint = true
			break
		}
	}
	if !foundCheckConstraint {
		t.Fatalf(`Could not find hidden check constraint for shard column`)
	}
	if idx.ColumnIDs[0] != shardColID {
		t.Fatalf(`Expected shard column to be the first column in the set of index columns`)
	}
}

func TestBasicHashShardedIndexes(t *testing.T) {
	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`USE d`); err != nil {
		t.Fatal(err)
	}

	t.Run("primary", func(t *testing.T) {
		if _, err := db.Exec(`
			CREATE TABLE kv_primary (
				k INT PRIMARY KEY USING HASH WITH BUCKET_COUNT=5,
				v BYTES
			)
		`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`CREATE INDEX foo ON kv_primary (v)`); err != nil {
			t.Fatal(err)
		}
		tableDesc := sqlbase.GetTableDescriptor(kvDB, `d`, `kv_primary`)
		verifyTableDescriptorState(t, tableDesc, "primary" /* shardedIndexName */)
		shardColID := getShardColumnID(t, tableDesc, "primary" /* shardedIndexName */)

		// Ensure that secondary indexes on table `kv` have the shard column in their
		// `ExtraColumnIDs` field so they can reconstruct the sharded primary key.
		fooDesc, _, err := tableDesc.FindIndexByName("foo")
		if err != nil {
			t.Fatal(err)
		}
		foundShardColumn := false
		for _, colID := range fooDesc.ExtraColumnIDs {
			if colID == shardColID {
				foundShardColumn = true
				break
			}
		}
		if !foundShardColumn {
			t.Fatalf(`Secondary index cannot reconstruct sharded primary key`)
		}
	})

	t.Run("secondary_in_create_table", func(t *testing.T) {
		if _, err := db.Exec(`
			CREATE TABLE kv_secondary (
				k INT,
				v BYTES,
				INDEX sharded_secondary (k) USING HASH WITH BUCKET_COUNT = 12
			)
		`); err != nil {
			t.Fatal(err)
		}

		tableDesc := sqlbase.GetTableDescriptor(kvDB, `d`, `kv_secondary`)
		verifyTableDescriptorState(t, tableDesc, "sharded_secondary" /* shardedIndexName */)
	})

	t.Run("secondary_in_separate_ddl", func(t *testing.T) {
		if _, err := db.Exec(`
			CREATE TABLE kv_secondary2 (
				k INT,
				v BYTES
			)
		`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`CREATE INDEX sharded_secondary2 ON kv_secondary2 (k) USING HASH WITH BUCKET_COUNT = 12`); err != nil {
			t.Fatal(err)
		}
		tableDesc := sqlbase.GetTableDescriptor(kvDB, `d`, `kv_secondary2`)
		verifyTableDescriptorState(t, tableDesc, "sharded_secondary2" /* shardedIndexName */)
	})
}

func TestHashShardedIndexVersionGating(t *testing.T) {
	var _ = cluster.VersionHashShardedIndexes
	var oldVersion = cluster.Version19_2
	ctx := context.Background()

	startOldVersionCluster := func() (*testcluster.TestCluster, *base.TestingKnobs) {
		knobs := base.TestingKnobs{
			Server: &server.TestingKnobs{
				BootstrapVersionOverride:       cluster.VersionByKey(oldVersion),
				DisableAutomaticVersionUpgrade: 1,
			},
		}
		return testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: knobs,
			},
		}), &knobs
	}
	ensureFailure := func(t *testing.T, tc *testcluster.TestCluster, stmt string) {
		_, err := tc.ServerConn(0).Exec(stmt)
		if !testutils.IsError(err, `can only be created on a cluster that has fully migrated to version 20.1`) {
			t.Fatalf(`Unexpected error state when creating a hash sharded index on unsupported cluster version: %v`, err)
		}
	}
	ensureSuccessAfterUpgrade := func(t *testing.T, tc *testcluster.TestCluster, knobs *base.TestingKnobs, stmt string) {
		atomic.StoreInt32(&knobs.Server.(*server.TestingKnobs).DisableAutomaticVersionUpgrade, 0)
		testutils.SucceedsSoon(t, func() error {
			if _, err := tc.ServerConn(0).Exec(stmt); err != nil {
				return err
			}
			return nil
		})
	}

	t.Run("primary", func(t *testing.T) {
		tc, knobs := startOldVersionCluster()
		defer tc.Stopper().Stop(ctx)
		stmt := `
			CREATE TABLE sharded_primary (
				k INT PRIMARY KEY USING HASH WITH BUCKET_COUNT = 12
			)
		`
		ensureFailure(t, tc, stmt)
		ensureSuccessAfterUpgrade(t, tc, knobs, stmt)
	})
	t.Run("secondary_in_create_table", func(t *testing.T) {
		tc, knobs := startOldVersionCluster()
		defer tc.Stopper().Stop(ctx)
		stmt := `
			CREATE TABLE sharded_secondary (
				k INT,
				INDEX foo (k) USING HASH WITH BUCKET_COUNT = 12
			)
		`
		ensureFailure(t, tc, stmt)
		ensureSuccessAfterUpgrade(t, tc, knobs, stmt)
	})
	t.Run("secondary_in_separate_ddl", func(t *testing.T) {
		tc, knobs := startOldVersionCluster()
		defer tc.Stopper().Stop(ctx)
		if _, err := tc.ServerConn(0).Exec(`CREATE TABLE sharded_secondary2 (k INT)`); err != nil {
			t.Fatal(err)
		}
		stmt := `CREATE INDEX foo on sharded_secondary2 (k) USING HASH WITH BUCKET_COUNT=12`
		ensureFailure(t, tc, stmt)
		ensureSuccessAfterUpgrade(t, tc, knobs, stmt)
	})
}
