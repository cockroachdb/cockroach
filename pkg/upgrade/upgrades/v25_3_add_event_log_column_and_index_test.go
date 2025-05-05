// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestEventLogTableMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_3)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	var (
		validationStmts = []string{
			`SELECT payload FROM system.eventlog LIMIT 0`,
			`SELECT "eventType" FROM system.eventlog@event_type_idx LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			{Name: "payload", ValidationFn: upgrades.HasColumn},
			{Name: "fam_7_payload", ValidationFn: upgrades.HasColumnFamily},
			{Name: "event_type_idx", ValidationFn: upgrades.HasIndex},
		}
	)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.EventLogTable,
		getOldEventLogDescriptor)
	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx,
			t,
			s,
			sqlDB,
			keys.EventLogTableID,
			systemschema.EventLogTable,
			validationStmts,
			validationSchemas,
			expectExists,
		)
	}
	// Validate that the eventlog table has the old
	// schema.
	validateSchemaExists(false)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V25_3_AddEventLogColumnAndIndex,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	validateSchemaExists(true)
}

// getOldEventLogDescriptor returns the
// system.eventlog table descriptor that was being used
// before adding the username column to the current version.
func getOldEventLogDescriptor() *descpb.TableDescriptor {
	uuidV4String := "uuid_v4()"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.EventLogTableName),
		ID:                      keys.EventLogTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "timestamp", ID: 1, Type: types.Timestamp},
			{Name: "eventType", ID: 2, Type: types.String},
			{Name: "targetID", ID: 3, Type: types.Int},
			{Name: "reportingID", ID: 4, Type: types.Int},
			{Name: "info", ID: 5, Type: types.String, Nullable: true},
			{Name: "uniqueID", ID: 6, Type: types.Bytes, DefaultExpr: &uuidV4String}},
		NextColumnID: 7,
		Families: []descpb.ColumnFamilyDescriptor{
			{Name: "primary", ID: 0, ColumnNames: []string{"timestamp", "uniqueID"}, ColumnIDs: []descpb.ColumnID{1, 6}},
			{Name: "fam_2_eventType", ID: 2, ColumnNames: []string{"eventType"}, ColumnIDs: []descpb.ColumnID{2}, DefaultColumnID: 2},
			{Name: "fam_3_targetID", ID: 3, ColumnNames: []string{"targetID"}, ColumnIDs: []descpb.ColumnID{3}, DefaultColumnID: 3},
			{Name: "fam_4_reportingID", ID: 4, ColumnNames: []string{"reportingID"}, ColumnIDs: []descpb.ColumnID{4}, DefaultColumnID: 4},
			{Name: "fam_5_info", ID: 5, ColumnNames: []string{"info"}, ColumnIDs: []descpb.ColumnID{5}, DefaultColumnID: 5},
		},
		NextFamilyID: 6,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                tabledesc.PrimaryKeyIndexName("eventlog"),
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"timestamp", "uniqueID"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 6},
			ConstraintID:        1,
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 3,
	}
}
