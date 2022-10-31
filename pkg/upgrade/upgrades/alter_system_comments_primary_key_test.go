// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterCommentsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1CommentsSystemTableUseNewPrimaryKey - 1),
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	validationSchemas := []upgrades.Schema{
		{Name: "primary", ValidationFn: upgrades.HasPrimaryKey},
	}

	upgrades.InjectLegacyTable(ctx, t, s, systemschema.CommentsTable, getDeprecatedCommentsTableDesc)

	upgrades.ValidateSchemaExists(
		ctx, t, s, sqlDB, keys.CommentsTableID, systemschema.CommentsTable, []string{}, validationSchemas, false,
	)

	upgrades.Upgrade(t, sqlDB, clusterversion.V23_1CommentsSystemTableUseNewPrimaryKey, nil, false)

	upgrades.ValidateSchemaExists(
		ctx, t, s, sqlDB, keys.CommentsTableID, systemschema.CommentsTable, []string{}, validationSchemas, true,
	)
}

// getDeprecatedCommentsTableDesc returns the system.comments table descriptor
// that was being used before the primary key is altered.
func getDeprecatedCommentsTableDesc() *descpb.TableDescriptor {
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.CommentsTableName),
		ID:                      keys.CommentsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "type", ID: 1, Type: types.Int},
			{Name: "object_id", ID: 2, Type: types.Int},
			{Name: "sub_id", ID: 3, Type: types.Int},
			{Name: "comment", ID: 4, Type: types.String},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{Name: "primary", ID: 0, ColumnNames: []string{"type", "object_id", "sub_id"}, ColumnIDs: []descpb.ColumnID{1, 2, 3}},
			{Name: "fam_4_comment", ID: 4, ColumnNames: []string{"comment"}, ColumnIDs: []descpb.ColumnID{4}, DefaultColumnID: 4},
		},
		NextFamilyID: 5,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"type", "object_id", "sub_id"},
			KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC, catpb.IndexColumn_ASC, catpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 2, 3},
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		NextConstraintID: 1,
		FormatVersion:    descpb.InterleavedFormatVersion,
	}
}
