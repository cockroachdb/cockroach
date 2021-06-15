// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package doctor_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

var validTableDesc = &descpb.Descriptor{
	Union: &descpb.Descriptor_Table{
		Table: &descpb.TableDescriptor{
			Name: "t", ID: 51, ParentID: 52,
			Columns: []descpb.ColumnDescriptor{
				{Name: "col", ID: 1, Type: types.Int},
			},
			NextColumnID: 2,
			Families: []descpb.ColumnFamilyDescriptor{
				{ID: 0, Name: "f", ColumnNames: []string{"col"}, ColumnIDs: []descpb.ColumnID{1}, DefaultColumnID: 1},
			},
			NextFamilyID: 1,
			PrimaryIndex: descpb.IndexDescriptor{
				Name:                tabledesc.PrimaryKeyIndexName,
				ID:                  1,
				Unique:              true,
				KeyColumnNames:      []string{"col"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{1},
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				EncodingType:        descpb.PrimaryIndexEncoding,
			},
			NextIndexID: 2,
			Privileges: descpb.NewCustomSuperuserPrivilegeDescriptor(
				descpb.SystemAllowedPrivileges[keys.SqllivenessID], security.NodeUserName()),
			FormatVersion:  descpb.InterleavedFormatVersion,
			NextMutationID: 1,
		},
	},
}

func toBytes(t *testing.T, desc *descpb.Descriptor) []byte {
	table, database, typ, schema := descpb.FromDescriptor(desc)
	if table != nil {
		descpb.MaybeFixPrivileges(
			table.GetID(), table.GetParentID(),
			&table.Privileges, privilege.Table,
		)
		if table.FormatVersion == 0 {
			table.FormatVersion = descpb.InterleavedFormatVersion
		}
	} else if database != nil {
		descpb.MaybeFixPrivileges(
			database.GetID(), database.GetID(),
			&database.Privileges, privilege.Database,
		)
	} else if typ != nil {
		descpb.MaybeFixPrivileges(
			typ.GetID(), typ.GetParentID(),
			&typ.Privileges, privilege.Type,
		)
	} else if schema != nil {
		descpb.MaybeFixPrivileges(
			schema.GetID(), schema.GetParentID(),
			&schema.Privileges, privilege.Schema,
		)
	}
	res, err := protoutil.Marshal(desc)
	require.NoError(t, err)
	return res
}

func fromHex(t *testing.T, s string) []byte {
	decoded, err := hex.DecodeString(s)
	require.NoError(t, err)
	return decoded
}

func TestExamineDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	droppedValidTableDesc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
	{
		tbl, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(droppedValidTableDesc, hlc.Timestamp{WallTime: 1})
		tbl.State = descpb.DescriptorState_DROP
	}

	// Use 51 as the Schema ID, we do not want to use a reserved system ID (1-49)
	// for the Schema because there should be no schemas with 1-49. A schema with
	// an ID from 1-49 would fail privilege checks due to incompatible privileges
	// the privileges returned from the SystemAllowedPrivileges map in privilege.go.
	validTableDescWithParentSchema := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
	{
		tbl, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(validTableDescWithParentSchema, hlc.Timestamp{WallTime: 1})
		tbl.UnexposedParentSchemaID = 53
	}

	tests := []struct {
		descTable      doctor.DescriptorTable
		namespaceTable doctor.NamespaceTable
		jobsTable      doctor.JobsTable
		valid          bool
		errStr         string
		expected       string
	}{
		{ // 1
			valid:    true,
			expected: "Examining 0 descriptors and 0 namespace entries...\n",
		},
		{ // 2
			descTable: doctor.DescriptorTable{{ID: 1, DescBytes: []byte("#$@#@#$#@#")}},
			errStr:    "failed to unmarshal descriptor",
			expected:  "Examining 1 descriptors and 0 namespace entries...\n",
		},
		{ // 3
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Table{
						Table: &descpb.TableDescriptor{ID: 2},
					}}),
				},
			},
			expected: `Examining 1 descriptors and 0 namespace entries...
  ParentID   0, ParentSchemaID 29: relation "" (2): different id in descriptor table: 1
`,
		},
		{ // 4
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Table{
						Table: &descpb.TableDescriptor{Name: "foo", ID: 1, State: descpb.DescriptorState_DROP},
					}}),
				},
			},
			expected: `Examining 1 descriptors and 0 namespace entries...
  ParentID   0, ParentSchemaID 29: relation "foo" (1): invalid parent ID 0
  ParentID   0, ParentSchemaID 29: relation "foo" (1): table must contain at least 1 column
`,
		},
		{ // 5
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Table{
						Table: &descpb.TableDescriptor{Name: "foo", ID: 1},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentSchemaID: 29, Name: "foo"}, ID: 1},
			},
			expected: `Examining 1 descriptors and 1 namespace entries...
  ParentID   0, ParentSchemaID 29: relation "foo" (1): invalid parent ID 0
  ParentID   0, ParentSchemaID 29: relation "foo" (1): table must contain at least 1 column
`,
		},
		{ // 6
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 1},
					}}),
				},
			},
			expected: `Examining 1 descriptors and 0 namespace entries...
  ParentID   0, ParentSchemaID  0: database "db" (1): expected matching namespace entry, found none
`,
		},
		{ // 7
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, validTableDesc)},
				{
					ID: 52,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 52},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentSchemaID: 29, Name: "t"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 52},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID  52, ParentSchemaID 29: relation "t" (51): expected matching namespace entry, found none
  ParentID   0, ParentSchemaID 29: namespace entry "t" (51): no matching name info found in non-dropped relation "t"
`,
		},
		{ // 8
			descTable: doctor.DescriptorTable{
				{
					ID: 51,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Schema{
						Schema: &descpb.SchemaDescriptor{Name: "schema", ID: 51, ParentID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, Name: "schema"}, ID: 51},
			},
			expected: `Examining 1 descriptors and 1 namespace entries...
  ParentID   2, ParentSchemaID  0: schema "schema" (51): referenced database ID 2: descriptor not found
`,
		},
		{ // 9
			descTable: doctor.DescriptorTable{
				{
					ID: 51,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Type{
						Type: &descpb.TypeDescriptor{Name: "type", ID: 51},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "type"}, ID: 51},
			},
			expected: `Examining 1 descriptors and 1 namespace entries...
  ParentID   0, ParentSchemaID  0: type "type" (51): invalid parentID 0
  ParentID   0, ParentSchemaID  0: type "type" (51): invalid parent schema ID 0
`,
		},
		{ // 10
			descTable: doctor.DescriptorTable{
				{
					ID: 51,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Type{
						Type: &descpb.TypeDescriptor{Name: "type", ID: 51, ParentID: 3, ParentSchemaID: 2},
					}}),
				},
				{
					ID: 3,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 3},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 3, ParentSchemaID: 2, Name: "type"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 3},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID   3, ParentSchemaID  2: type "type" (51): referenced schema ID 2: descriptor not found
  ParentID   3, ParentSchemaID  2: type "type" (51): arrayTypeID 0 does not exist for "ENUM": referenced type ID 0: descriptor not found
`,
		},
		{ // 11
			descTable: doctor.DescriptorTable{
				{
					ID: 51,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 51},
					}}),
				},
				{
					ID: 52,
					DescBytes: func() []byte {
						desc := &descpb.Descriptor{Union: &descpb.Descriptor_Type{
							Type: &descpb.TypeDescriptor{Name: "type", ID: 52, ParentID: 51, ParentSchemaID: keys.PublicSchemaID},
						}}
						res, err := protoutil.Marshal(desc)
						require.NoError(t, err)
						return res
					}(),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 51},
				{NameInfo: descpb.NameInfo{ParentID: 51, ParentSchemaID: keys.PublicSchemaID, Name: "type"}, ID: 52},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID  51, ParentSchemaID 29: type "type" (52): arrayTypeID 0 does not exist for "ENUM": referenced type ID 0: descriptor not found
`,
		},
		{ // 12
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, validTableDescWithParentSchema)},
				{
					ID: 52,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 52},
					}}),
				},
				{
					ID: 53,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Schema{
						Schema: &descpb.SchemaDescriptor{Name: "schema", ID: 53, ParentID: 54},
					}}),
				},
				{
					ID: 54,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db2", ID: 54},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 52, ParentSchemaID: 53, Name: "t"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 52},
				{NameInfo: descpb.NameInfo{ParentID: 54, Name: "schema"}, ID: 53},
				{NameInfo: descpb.NameInfo{Name: "db2"}, ID: 54},
			},
			expected: `Examining 4 descriptors and 4 namespace entries...
  ParentID  52, ParentSchemaID 53: relation "t" (51): parent schema 53 is in different database 54
  ParentID  54, ParentSchemaID  0: schema "schema" (53): not present in parent database [54] schemas mapping
`,
		},
		{ // 13
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "foo"}, ID: keys.PublicSchemaID},
				{NameInfo: descpb.NameInfo{Name: "bar"}, ID: keys.PublicSchemaID},
				{NameInfo: descpb.NameInfo{Name: "pg_temp_foo", ParentID: 123}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "causes_error"}, ID: 2},
			},
			expected: `Examining 0 descriptors and 4 namespace entries...
  ParentID   0, ParentSchemaID  0: namespace entry "causes_error" (2): descriptor not found
`,
		},
		{ // 14
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "null"}, ID: int64(descpb.InvalidID)},
			},
			expected: `Examining 0 descriptors and 1 namespace entries...
  ParentID   0, ParentSchemaID  0: namespace entry "null" (0): invalid descriptor ID
`,
		},
		{ // 15
			valid: true,
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, validTableDesc)},
				{
					ID: 52,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 52},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 52, ParentSchemaID: 29, Name: "t"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 52},
			},
			expected: "Examining 2 descriptors and 2 namespace entries...\n",
		},
		{ // 16
			valid: true,
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{
							ID:            1,
							Name:          "db",
							DrainingNames: []descpb.NameInfo{{Name: "db1"}, {Name: "db2"}},
						},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db1"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db2"}, ID: 1},
			},
			expected: "Examining 1 descriptors and 3 namespace entries...\n",
		},
		{ // 17
			valid: false,
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{
							ID:            1,
							Name:          "db",
							DrainingNames: []descpb.NameInfo{{Name: "db1"}, {Name: "db2"}, {Name: "db3"}},
						},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db1"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db2"}, ID: 1},
			},
			expected: `Examining 1 descriptors and 3 namespace entries...
  ParentID   0, ParentSchemaID  0: database "db" (1): expected matching namespace entry for draining name (0, 0, db3), found none
`,
		},
		{ // 18
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, droppedValidTableDesc)},
				{
					ID: 52,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 52},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 52, ParentSchemaID: 29, Name: "t"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 52},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID  52, ParentSchemaID 29: namespace entry "t" (51): no matching name info in draining names of dropped relation
`,
		},
		{ // 19
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, func() *descpb.Descriptor {
					desc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
					tbl, _, _, _ := descpb.FromDescriptor(desc)
					tbl.PrimaryIndex.Disabled = true
					tbl.PrimaryIndex.InterleavedBy = make([]descpb.ForeignKeyReference, 1)
					tbl.PrimaryIndex.InterleavedBy[0].Name = "bad_backref"
					tbl.PrimaryIndex.InterleavedBy[0].Table = 500
					tbl.PrimaryIndex.InterleavedBy[0].Index = 1
					return desc
				}())},
				{
					ID: 52,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 52},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 52, ParentSchemaID: 29, Name: "t"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 52},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID  52, ParentSchemaID 29: relation "t" (51): invalid interleave backreference table=500 index=1: referenced table ID 500: descriptor not found
  ParentID  52, ParentSchemaID 29: relation "t" (51): unimplemented: primary key dropped without subsequent addition of new primary key in same transaction
`,
		},
		{ // 20

			// The data for these descriptors was generated by running the following
			// SQL in 19.1. Then upgrading the cluster to 20.1.8. Running a backup
			// with revision history after modifying the tables. Then restoring with
			// AS OF SYSTEM TIME so as to get the revisions which did not have their
			// fk representation upgraded. This caused a bug in that version. The
			// purpose of this test is to ensure that the doctor will catch the broken
			// foreign key references.
			//
			//  CREATE TABLE a (i INT8 PRIMARY KEY);
			//  CREATE TABLE b (i INT8 PRIMARY KEY, j INT8 REFERENCES a (i));
			//  CREATE TABLE c (i INT8 PRIMARY KEY REFERENCES b (i));
			//
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: fromHex(t, "122d0a02646210331a1d0a090a0561646d696e10020a080a04726f6f741002120464656d6f18012200280140004a00")},
				{ID: 53, DescBytes: fromHex(t, "0ae7010a01611835203328013a0042200a016910011a0c080110401800300050146000200030006800700078008001004802524c0a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a0408002000800100880100900102980100a20106080012001800a80100b20100ba010060026a1d0a090a0561646d696e10020a080a04726f6f741002120464656d6f1801800101880103980100b201120a077072696d61727910001a016920012800b80101c20100e80100f2010408001200f801008002009202009a0200b20200b80200c0021dc80200e00200")},
				{ID: 57, DescBytes: fromHex(t, "12240a0964656661756c74646210391a150a090a0561646d696e10020a080a04726f6f741002")},
				{ID: 58, DescBytes: fromHex(t, "0aed010a0161183a203928043a0a08c2a7a5b1b7b9c8b81642170a016910011a0c08011040180030005014600020003000480252560a077072696d61727910011801220169300140004a10080010001a00200028003000380040005210083510021a00200028003000380040005a007a020800800100880100900100980100a20106080012001800a8010060026a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201120a077072696d61727910001a016920012800b80101c20100e80100f2010408001200f801008002009202009a0200b20209726573746f72696e67b80200c00200")},
				{ID: 59, DescBytes: fromHex(t, "0aed020a0162183b203928053a0a08abc5d096d7b9c8b81642170a016910011a0c0801104018003000501460002000300042170a016a10021a0c08011040180030005014600020013000480352560a077072696d61727910011801220169300140004a10080010001a00200028003000380040005210083610011a00200028003000380040005a007a020800800100880100900100980100a20106080012001800a801005a600a17625f6175746f5f696e6465785f666b5f6a5f7265665f611002180022016a3002380140004a1a083410011a0a666b5f6a5f7265665f61200028013000380040005a007a020800800100880100900100980100a20106080012001800a8010060036a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f801008002009202009a0200b20209726573746f72696e67b80200c00200")},
				{ID: 60, DescBytes: fromHex(t, "0ae5010a0163183c203928043a0a08fbf39aacd7b9c8b81642170a016910011a0c080110401800300050146000200030004802524e0a077072696d61727910011801220169300140004a1a083510011a0a666b5f695f7265665f62200028013000380040005a007a020800800100880100900100980100a20106080012001800a8010060026a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201120a077072696d61727910001a016920012800b80101c20100e80100f2010408001200f801008002009202009a0200b20209726573746f72696e67b80200c00200")},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 0, ParentSchemaID: 0, Name: "db"}, ID: 51},
				{NameInfo: descpb.NameInfo{ParentID: 51, ParentSchemaID: 29, Name: "a"}, ID: 53},
				{NameInfo: descpb.NameInfo{ParentID: 0, ParentSchemaID: 0, Name: "defaultdb"}, ID: 57},
				{NameInfo: descpb.NameInfo{ParentID: 57, ParentSchemaID: 29, Name: "a"}, ID: 58},
				{NameInfo: descpb.NameInfo{ParentID: 57, ParentSchemaID: 29, Name: "b"}, ID: 59},
				{NameInfo: descpb.NameInfo{ParentID: 57, ParentSchemaID: 29, Name: "c"}, ID: 60},
			},
			expected: `Examining 6 descriptors and 6 namespace entries...
  ParentID  57, ParentSchemaID 29: relation "a" (58): failed to upgrade descriptor: index-id "2" does not exist
  ParentID  57, ParentSchemaID 29: relation "b" (59): failed to upgrade descriptor: referenced table ID 52: descriptor not found
  ParentID  57, ParentSchemaID 29: relation "a" (58): primary index "primary" has invalid version 0, expected 4
  ParentID  57, ParentSchemaID 29: relation "b" (59): primary index "primary" has invalid version 0, expected 4
  ParentID  57, ParentSchemaID 29: relation "c" (60): missing fk back reference "fk_i_ref_b" to "c" from "a"
`,
		},
		{ // 21
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, func() *descpb.Descriptor {
					desc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
					tbl, _, _, _ := descpb.FromDescriptor(desc)
					tbl.MutationJobs = []descpb.TableDescriptor_MutationJob{{MutationID: 1, JobID: 123}}
					return desc
				}())},
				{
					ID: 52,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 52},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 52, ParentSchemaID: 29, Name: "t"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 52},
			},
			jobsTable: doctor.JobsTable{
				{
					ID:      123,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeDetails{})},
					Status:  jobs.StatusCanceled,
				},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID  52, ParentSchemaID 29: relation "t" (51): mutation job 123 has terminal status (canceled)
`,
		},
	}

	for i, test := range tests {
		var buf bytes.Buffer
		valid, err := doctor.ExamineDescriptors(
			context.Background(), test.descTable, test.namespaceTable, test.jobsTable, false, &buf)
		msg := fmt.Sprintf("Test %d failed!", i+1)
		if test.errStr != "" {
			require.Containsf(t, err.Error(), test.errStr, msg)
		} else {
			require.NoErrorf(t, err, msg)
		}
		require.Equalf(t, test.valid, valid, msg)
		require.Equalf(t, test.expected, buf.String(), msg)
	}
}

func TestExamineJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		descTable doctor.DescriptorTable
		jobsTable doctor.JobsTable
		valid     bool
		errStr    string
		expected  string
	}{
		{
			jobsTable: doctor.JobsTable{
				{
					ID:      1,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.BackupDetails{})},
					Status:  jobs.StatusRunning,
				},
			},
			valid:    true,
			expected: "Examining 1 jobs...\n",
		},
		{
			descTable: doctor.DescriptorTable{
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Table{
						Table: &descpb.TableDescriptor{ID: 2},
					}}),
				},
			},
			jobsTable: doctor.JobsTable{
				{
					ID:      100,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeGCDetails{})},
					Progress: &jobspb.Progress{Details: jobspb.WrapProgressDetails(
						jobspb.SchemaChangeGCProgress{
							Tables: []jobspb.SchemaChangeGCProgress_TableProgress{
								{ID: 1, Status: jobspb.SchemaChangeGCProgress_DELETED},
								{ID: 2, Status: jobspb.SchemaChangeGCProgress_DELETING},
								{ID: 3, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_GC},
							},
						})},
					Status: jobs.StatusRunning,
				},
				{
					ID:      200,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeGCDetails{})},
					Progress: &jobspb.Progress{Details: jobspb.WrapProgressDetails(
						jobspb.SchemaChangeGCProgress{
							Tables: []jobspb.SchemaChangeGCProgress_TableProgress{
								{ID: 1, Status: jobspb.SchemaChangeGCProgress_DELETED},
								{ID: 3, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_GC},
							},
						})},
					Status: jobs.StatusPauseRequested,
				},
				{
					ID:      300,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeGCDetails{})},
					Progress: &jobspb.Progress{Details: jobspb.WrapProgressDetails(
						jobspb.SchemaChangeGCProgress{
							Tables: []jobspb.SchemaChangeGCProgress_TableProgress{
								{ID: 1, Status: jobspb.SchemaChangeGCProgress_DELETED},
								{ID: 3, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_GC},
							},
							Indexes: []jobspb.SchemaChangeGCProgress_IndexProgress{
								{IndexID: 10, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_GC},
							},
						})},
					Status: jobs.StatusPaused,
				},
			},
			expected: `Examining 3 jobs...
job 100: running schema change GC refers to missing table descriptor(s) [3]; existing descriptors that still need to be dropped [2]; job safe to delete: false.
job 200: pause-requested schema change GC refers to missing table descriptor(s) [3]; existing descriptors that still need to be dropped []; job safe to delete: true.
job 300: paused schema change GC refers to missing table descriptor(s) [3]; existing descriptors that still need to be dropped []; job safe to delete: false.
`,
		},
		{
			jobsTable: doctor.JobsTable{
				{
					ID:       100,
					Payload:  &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.TypeSchemaChangeDetails{TypeID: 500})},
					Progress: &jobspb.Progress{Details: jobspb.WrapProgressDetails(jobspb.TypeSchemaChangeProgress{})},
					Status:   jobs.StatusRunning,
				},
			},
			expected: `Examining 1 jobs...
job 100: running type schema change refers to missing type descriptor [500].
`,
		},
		{
			descTable: doctor.DescriptorTable{
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Table{
						Table: &descpb.TableDescriptor{ID: 2},
					}}),
				},
			},
			jobsTable: doctor.JobsTable{
				{
					ID:       100,
					Payload:  &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeDetails{DescID: 2, DroppedDatabaseID: 500})},
					Progress: &jobspb.Progress{Details: jobspb.WrapProgressDetails(jobspb.SchemaChangeProgress{})},
					Status:   jobs.StatusRunning,
				},
			},
			expected: `Examining 1 jobs...
job 100: running schema change refers to missing descriptor(s) [500].
`,
		},
	}

	for i, test := range tests {
		var buf bytes.Buffer
		valid, err := doctor.ExamineJobs(
			context.Background(), test.descTable, test.jobsTable, false, &buf)
		msg := fmt.Sprintf("Test %d failed!", i+1)
		if test.errStr != "" {
			require.Containsf(t, err.Error(), test.errStr, msg)
		} else {
			require.NoErrorf(t, err, msg)
		}
		if test.valid != valid {
			t.Errorf("%s valid\n\texpected: %v\n\tactual: %v", msg, test.valid, valid)
		}
		require.Equalf(t, test.expected, buf.String(), msg)
	}
}
