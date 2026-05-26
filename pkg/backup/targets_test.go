// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// db creates a database descriptor for testing.
func db(id descpb.ID, name string) catalog.DatabaseDescriptor {
	return dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
		ID:         id,
		Name:       name,
		Version:    1,
		State:      descpb.DescriptorState_PUBLIC,
		Privileges: catpb.NewBaseDatabasePrivilegeDescriptor(username.RootUserName()),
	}).BuildImmutableDatabase()
}

// dbs creates a slice of database descriptors for testing.
func dbs(databases ...catalog.DatabaseDescriptor) []catalog.DatabaseDescriptor {
	if databases == nil {
		return []catalog.DatabaseDescriptor{}
	}
	return databases
}

// table creates a table descriptor for testing.
func table(id descpb.ID, parentID descpb.ID, name string) catalog.Descriptor {
	return tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:       id,
		Name:     name,
		ParentID: parentID,
		Version:  1,
		State:    descpb.DescriptorState_PUBLIC,
	}).BuildImmutable()
}

// descriptors creates a slice of descriptors for testing.
func descriptors(ds ...catalog.Descriptor) []catalog.Descriptor {
	if ds == nil {
		return []catalog.Descriptor{}
	}
	return ds
}

func TestFilterTempSystemDBDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name          string
		descs         []catalog.Descriptor
		expectedDescs []catalog.Descriptor
		expectedDBs   []catalog.DatabaseDescriptor
	}{
		{
			name: "no temporary databases",
			descs: descriptors(
				db(100, "defaultdb"),
				table(1000, 100, "table1"),
			),
			expectedDescs: descriptors(
				db(100, "defaultdb"),
				table(1000, 100, "table1"),
			),
			expectedDBs: dbs(db(100, "defaultdb")),
		},
		{
			name: "single temp database with child descriptors",
			descs: descriptors(
				db(100, "defaultdb"),
				db(101, "crdb_temp_system_restore_123"),
				table(1000, 100, "regular_table"),
				table(1001, 101, "temp_table"),
			),
			expectedDescs: descriptors(
				db(100, "defaultdb"),
				table(1000, 100, "regular_table"),
			),
			expectedDBs: dbs(db(100, "defaultdb")),
		},
		{
			name: "multiple temp databases",
			descs: descriptors(
				db(100, "defaultdb"),
				db(101, "crdb_temp_system_123"),
				db(102, "crdb_temp_system_restore_456"),
				table(1000, 101, "temp_table"),
			),
			expectedDescs: descriptors(db(100, "defaultdb")),
			expectedDBs:   dbs(db(100, "defaultdb")),
		},
		{
			name: "temp database without children",
			descs: descriptors(
				db(100, "defaultdb"),
				db(101, "crdb_temp_system_123"),
				table(1000, 100, "regular_table"),
			),
			expectedDescs: descriptors(
				db(100, "defaultdb"),
				table(1000, 100, "regular_table"),
			),
			expectedDBs: dbs(db(100, "defaultdb")),
		},
		{
			name:          "empty inputs",
			descs:         descriptors(),
			expectedDescs: descriptors(),
			expectedDBs:   dbs(),
		},
		{
			name: "database with prefix substring match",
			descs: descriptors(
				db(100, "my_crdb_temp_system_db"),
				table(1000, 100, "table1"),
			),
			expectedDescs: descriptors(
				db(100, "my_crdb_temp_system_db"),
				table(1000, 100, "table1"),
			),
			expectedDBs: dbs(db(100, "my_crdb_temp_system_db")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Extract database descriptors from the input descriptors.
			inputDBs := []catalog.DatabaseDescriptor{}
			for _, desc := range tt.descs {
				if dbDesc, ok := desc.(catalog.DatabaseDescriptor); ok {
					inputDBs = append(inputDBs, dbDesc)
				}
			}

			resultDescs, resultDBs := filterTempSystemDBDescriptors(tt.descs, inputDBs)

			require.Len(t, resultDescs, len(tt.expectedDescs))
			require.Len(t, resultDBs, len(tt.expectedDBs))

			require.Equal(t, tt.expectedDescs, resultDescs)
			require.Equal(t, tt.expectedDBs, resultDBs)
		})
	}
}
