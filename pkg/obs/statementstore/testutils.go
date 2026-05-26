// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statementstore

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

// GetOldStatementsDescriptor returns the system.statements table descriptor as
// it existed before V26_3_AlterStatementsTablePK: an `id` column as the primary
// key and a unique secondary index on `fingerprint_id`. Used by tests that need
// to simulate a v26.2 cluster (for migration tests and pre-V26_3 backup/restore
// roundtrips).
//
// Pass descpb.InvalidID for tableID if the ID will be filled in later (e.g.,
// by upgrades.InjectLegacyTable).
func GetOldStatementsDescriptor(tableID descpb.ID) *descpb.TableDescriptor {
	uniqueRowIDExpr := descpb.Expression("unique_rowid()")
	nowExpr := descpb.Expression("now():::TIMESTAMPTZ")
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.StatementsTableName),
		ID:                      tableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.SystemPublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDExpr},
			{Name: "fingerprint_id", ID: 2, Type: types.Bytes},
			{Name: "fingerprint", ID: 3, Type: types.String},
			{Name: "summary", ID: 4, Type: types.String},
			{Name: "db", ID: 5, Type: types.String},
			{Name: "metadata", ID: 6, Type: types.Jsonb},
			{Name: "created_at", ID: 7, Type: types.TimestampTZ, DefaultExpr: &nowExpr},
			{Name: "last_upserted", ID: 8, Type: types.TimestampTZ, DefaultExpr: &nowExpr},
		},
		NextColumnID: 9,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ID:          0,
				ColumnNames: []string{"id", "fingerprint_id", "fingerprint", "summary", "db", "metadata", "created_at", "last_upserted"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			ConstraintID:        1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "statements_fingerprint_id_key",
				ID:                  2,
				Unique:              true,
				KeyColumnNames:      []string{"fingerprint_id"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				StoreColumnNames:    []string{"fingerprint", "summary", "db"},
				StoreColumnIDs:      []descpb.ColumnID{3, 4, 5},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
				ConstraintID:        2,
			},
			{
				Name:                "statements_fingerprint_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"fingerprint"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{3},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID:      4,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextConstraintID: 3,
	}
}
