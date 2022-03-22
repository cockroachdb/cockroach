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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestEnsureConstraintIDs tests that constraint IDs are added as expected.
func TestEnsureConstraintIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start off with the version that did not support
	// constraint IDs.
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						tabledesc.ConstraintIDsAddedToTableDescsVersion - 1),
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
	// Create table with a primary key constraint.
	tdb.Exec(t, "CREATE TABLE dep_t (fk INT8 PRIMARY KEY);")
	tdb.Exec(t,
		`CREATE TABLE t (
	name INT8 PRIMARY KEY,
	val  INT8 REFERENCES dep_t (fk),
	val2 INT8 CHECK (val2 > 0)
);`,
	)
	tdb.Exec(t,
		`CREATE TABLE t_fk_dst (name INT8 PRIMARY KEY, val INT8 REFERENCES t (name));`)
	// Validate the comments on constraints are blocked.
	tdb.ExpectErr(t,
		"pq: cannot comment on constraint",
		"COMMENT ON CONSTRAINT \"t_pkey\" ON t IS 'primary_comment'")
	// Validate that we have a constraint ID due to post deserialization logic

	desc := desctestutils.TestingGetMutableExistingTableDescriptor(s.DB(), c, "defaultdb", "t")
	// Reset all constraint IDs
	desc.PrimaryIndex.ConstraintID = 0
	require.NoError(t, desc.ForeachOutboundFK(func(constraint *descpb.ForeignKeyConstraint) error {
		constraint.ConstraintID = 0
		return nil
	}))
	require.NoError(t, desc.ForeachInboundFK(func(constraint *descpb.ForeignKeyConstraint) error {
		constraint.ConstraintID = 0
		return nil
	}))
	for _, check := range desc.GetChecks() {
		check.ConstraintID = 0
	}
	require.NoError(t, s.DB().Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
		desc.DescriptorProto(),
	))
	// Validate that the post serialization will recompute the constraint IDs
	// if they are missing.
	desc = desctestutils.TestingGetMutableExistingTableDescriptor(s.DB(), c, "defaultdb", "t")
	require.Equal(t, desc.PrimaryIndex.ConstraintID, descpb.ConstraintID(4))
	// If we set both the constraint ID / next value to 0, then we will have
	// it assigned form scratch.
	desc.PrimaryIndex.ConstraintID = 0
	desc.NextConstraintID = 0
	require.NoError(t, s.DB().Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
		desc.DescriptorProto(),
	))
	// Validate that the descriptor is invalid, since the constraint IDs
	// are missing. Note: Constraint IDs on FKs and other objects will exist.
	tdb.CheckQueryResults(t,
		`SELECT strpos(desc_json, 'nextConstraintId') > 0,
       array_length(string_to_array(desc_json, 'constraintId'), 1) > 4
  FROM (
		SELECT jsonb_pretty(
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor,
					false
				)
		       ) AS desc_json
		  FROM system.descriptor
		 WHERE id = `+
			fmt.Sprintf("%d", desc.GetID())+
			`);`,
		[][]string{{"false", "false"}},
	)
	// Migrate to the new cluster version.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(tabledesc.ConstraintIDsAddedToTableDescsVersion).String())
	tdb.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
		[][]string{{clusterversion.ByKey(tabledesc.ConstraintIDsAddedToTableDescsVersion).String()}})
	// Validate the constraint IDs are populated.
	// Validate that the descriptor is invalid, since the constraint IDs
	// are missing.
	tdb.CheckQueryResults(t,
		`SELECT strpos(desc_json, 'nextConstraintId') > 0,
       array_length(string_to_array(desc_json, 'constraintId'), 1) > 4
  FROM (
		SELECT jsonb_pretty(
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor,
					false
				)
		       ) AS desc_json
		  FROM system.descriptor
		 WHERE id = `+
			fmt.Sprintf("%d", desc.GetID())+
			`);`,
		[][]string{{"true", "true"}},
	)
	// Validate we can comment constraints.
	tdb.Exec(t,
		"COMMENT ON CONSTRAINT \"t_pkey\" ON t IS 'primary_comment'")
}
