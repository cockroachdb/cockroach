// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestEnsureNoDrainingNames tests that the draining names migration performs
// as expected.
func TestEnsureNoDrainingNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	renameBlocked := make(chan struct{})
	renameUnblocked := make(chan struct{})
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.AvoidDrainingNames - 1),
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
					OldNamesDrainedNotification: func() {
						renameBlocked <- struct{}{}
						<-renameUnblocked
					},
				},
			},
		},
	}

	c := keys.SystemSQLCodec
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	s := tc.Server(0)
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Create a database and a schema, rename the schema.
	tdb.Exec(t, "CREATE DATABASE t")
	tdb.Exec(t, "CREATE SCHEMA t.foo")

	// Concurrently, rename the table.
	threadDone := make(chan error)
	go func() {
		_, err := sqlDB.Exec(`ALTER SCHEMA t.foo RENAME TO bar`)
		threadDone <- err
	}()
	defer func() {
		close(renameBlocked)
		close(renameUnblocked)
		// Block until the thread doing the rename has finished, so the test can
		// clean up. It needs to wait for the transaction to release its lease.
		if err := <-threadDone; err != nil {
			t.Fatal(err)
		}
	}()
	<-renameBlocked

	// Check that the draining name persists in the descriptor and in the db's
	// schema mapping.
	{
		version := tc.Server(0).ClusterSettings().Version.ActiveVersion(ctx)
		db := desctestutils.TestingGetDatabaseDescriptorWitVersion(
			s.DB(), c, version, "t")
		_ = db.ForEachSchemaInfo(func(id descpb.ID, name string, isDropped bool) error {
			switch name {
			case "foo":
				require.True(t, isDropped)
			case "bar":
				require.False(t, isDropped)
			}
			return nil
		})
		foo := desctestutils.TestingGetSchemaDescriptorWithVersion(
			s.DB(), c, version, db.GetID(), "foo")
		require.NotEmpty(t, foo.GetDrainingNames())
	}

	// Reuse the old schema name.
	// This should fail in this pre-AvoidDrainingNames cluster version.
	tdb.ExpectErr(t, `schema "foo" already exists`, "CREATE SCHEMA t.foo")

	renameUnblocked <- struct{}{}

	// Migrate to the new cluster version.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.DrainingNamesMigration).String())

	tdb.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
		[][]string{{clusterversion.ByKey(clusterversion.DrainingNamesMigration).String()}})

	tdb.Exec(t, "CREATE SCHEMA t.foo")

	// Check that there are no draining names and that the database schema mapping
	// is correct.
	{
		version := tc.Server(0).ClusterSettings().Version.ActiveVersion(ctx)
		db := desctestutils.TestingGetDatabaseDescriptorWitVersion(s.DB(), c, version, "t")
		_ = db.ForEachSchemaInfo(func(id descpb.ID, name string, isDropped bool) error {
			require.False(t, isDropped)
			require.True(t, name == "foo" || name == "bar")
			return nil
		})
		foo := desctestutils.TestingGetSchemaDescriptorWithVersion(s.DB(), c, version, db.GetID(), "foo")
		require.Empty(t, foo.GetDrainingNames())
		bar := desctestutils.TestingGetSchemaDescriptorWithVersion(s.DB(), c, version, db.GetID(), "bar")
		require.Empty(t, bar.GetDrainingNames())
	}
}
