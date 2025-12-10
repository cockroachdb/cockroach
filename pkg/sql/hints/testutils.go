// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// GetOldStatementHintsDescriptor returns a descriptor for
// system.statement_hints as it existed before the hint_type, hint_name,
// enabled, and database columns were added (pre-26.1). This is used by tests
// to simulate the old schema for testing upgrades and backups.
//
// The tableID parameter specifies the ID to use for the descriptor. Pass
// descpb.InvalidID if the ID will be set later (e.g., by InjectLegacyTable).
func GetOldStatementHintsDescriptor(tableID descpb.ID) *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	nowTZString := "now():::TIMESTAMPTZ"
	statementHintsComputeExpr := "fnv64(fingerprint)"

	return &descpb.TableDescriptor{
		Name:                    string(catconstants.StatementHintsTableName),
		ID:                      tableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "row_id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDString, Nullable: false},
			{Name: "hash", ID: 2, Type: types.Int, Hidden: true, ComputeExpr: &statementHintsComputeExpr, Nullable: false},
			{Name: "fingerprint", ID: 3, Type: types.String, Nullable: false},
			{Name: "hint", ID: 4, Type: types.Bytes, Nullable: false},
			{Name: "created_at", ID: 5, Type: types.TimestampTZ, DefaultExpr: &nowTZString, Nullable: false},
			// NOTE: hint_type, hint_name, enabled, and database columns
			// (IDs 6, 7, 8, 9) are NOT included.
		},
		NextColumnID: 6,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:            "primary",
				ID:              0,
				ColumnNames:     []string{"row_id", "hash", "fingerprint", "hint", "created_at"},
				ColumnIDs:       []descpb.ColumnID{1, 2, 3, 4, 5},
				DefaultColumnID: 5,
			},
		},
		NextFamilyID: 2,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"row_id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			ConstraintID:        1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "hash_idx",
				ID:                  2,
				KeyColumnNames:      []string{"hash"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
				ConstraintID:        2,
			},
		},
		NextIndexID:      3,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 3,
	}
}
