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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
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
			Name: "t", ID: 1, ParentID: 2,
			Columns: []descpb.ColumnDescriptor{
				{Name: "col", ID: 1, Type: types.Int},
			},
			NextColumnID: 2,
			Families: []descpb.ColumnFamilyDescriptor{
				{ID: 0, Name: "f", ColumnNames: []string{"col"}, ColumnIDs: []descpb.ColumnID{1}, DefaultColumnID: 1},
			},
			NextFamilyID: 1,
			PrimaryIndex: descpb.IndexDescriptor{
				Name:             tabledesc.PrimaryKeyIndexName,
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"col"},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				ColumnIDs:        []descpb.ColumnID{1},
				Version:          descpb.StrictIndexColumnIDGuaranteesVersion,
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
		descpb.MaybeFixPrivileges(table.GetID(), &table.Privileges)
		if table.FormatVersion == 0 {
			table.FormatVersion = descpb.InterleavedFormatVersion
		}
	} else if database != nil {
		descpb.MaybeFixPrivileges(database.GetID(), &database.Privileges)
	} else if typ != nil {
		descpb.MaybeFixPrivileges(typ.GetID(), &typ.Privileges)
	} else if schema != nil {
		descpb.MaybeFixPrivileges(schema.GetID(), &schema.Privileges)
	}
	res, err := protoutil.Marshal(desc)
	require.NoError(t, err)
	return res
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
		tbl.UnexposedParentSchemaID = 51
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
				{ID: 1, DescBytes: toBytes(t, validTableDesc)},
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentSchemaID: 29, Name: "t"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 2},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID   2, ParentSchemaID 29: relation "t" (1): expected matching namespace entry, found none
  ParentID   0, ParentSchemaID 29: namespace entry "t" (1): no matching name info found in non-dropped relation "t"
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
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Type{
						Type: &descpb.TypeDescriptor{Name: "type", ID: 1},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "type"}, ID: 1},
			},
			expected: `Examining 1 descriptors and 1 namespace entries...
  ParentID   0, ParentSchemaID  0: type "type" (1): invalid parentID 0
  ParentID   0, ParentSchemaID  0: type "type" (1): invalid parent schema ID 0
`,
		},
		{ // 10
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Type{
						Type: &descpb.TypeDescriptor{Name: "type", ID: 1, ParentID: 3, ParentSchemaID: 2},
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
				{NameInfo: descpb.NameInfo{ParentID: 3, ParentSchemaID: 2, Name: "type"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 3},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID   3, ParentSchemaID  2: type "type" (1): referenced schema ID 2: descriptor not found
  ParentID   3, ParentSchemaID  2: type "type" (1): arrayTypeID 0 does not exist for "ENUM": referenced type ID 0: descriptor not found
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
						// Skip `toBytes` to produce a descriptor with unset privileges field.
						// The purpose of this is to produce a nil dereference during validation
						// in order to test that doctor recovers from this.
						//
						// Note that it might be the case that validation aught to check that
						// this field is not nil in the first place, in which case this test case
						// will need to craft a corrupt descriptor serialization in a more
						// creative way. Ideally validation code should never cause runtime errors
						// but there's no way to guarantee that short of formally verifying it. We
						// therefore have to consider the possibility of runtime errors (sadly) and
						// doctor should absolutely make every possible effort to continue executing
						// in the face of these, considering its main use case!
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
  ParentID  51, ParentSchemaID 29: type "type" (52): validation: runtime error: invalid memory address or nil pointer dereference
`,
		},
		{ // 12
			descTable: doctor.DescriptorTable{
				{ID: 1, DescBytes: toBytes(t, validTableDescWithParentSchema)},
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 2},
					}}),
				},
				{
					ID: 51,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Schema{
						Schema: &descpb.SchemaDescriptor{Name: "schema", ID: 51, ParentID: 4},
					}}),
				},
				{
					ID: 4,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db2", ID: 4},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, ParentSchemaID: 51, Name: "t"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 2},
				{NameInfo: descpb.NameInfo{ParentID: 4, Name: "schema"}, ID: 51},
				{NameInfo: descpb.NameInfo{Name: "db2"}, ID: 4},
			},
			expected: `Examining 4 descriptors and 4 namespace entries...
  ParentID   2, ParentSchemaID 51: relation "t" (1): parent schema 51 is in different database 4
  ParentID   4, ParentSchemaID  0: schema "schema" (51): not present in parent database [4] schemas mapping
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
				{ID: 1, DescBytes: toBytes(t, validTableDesc)},
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, ParentSchemaID: 29, Name: "t"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 2},
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
				{ID: 1, DescBytes: toBytes(t, droppedValidTableDesc)},
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, ParentSchemaID: 29, Name: "t"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 2},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID   2, ParentSchemaID 29: namespace entry "t" (1): no matching name info in draining names of dropped relation
`,
		},
		{ // 19
			descTable: doctor.DescriptorTable{
				{ID: 1, DescBytes: toBytes(t, func() *descpb.Descriptor {
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
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, ParentSchemaID: 29, Name: "t"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 2},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID   2, ParentSchemaID 29: relation "t" (1): invalid interleave backreference table=500 index=1: referenced table ID 500: descriptor not found
  ParentID   2, ParentSchemaID 29: relation "t" (1): unimplemented: primary key dropped without subsequent addition of new primary key in same transaction
`,
		},
		{ // 20
			descTable: doctor.DescriptorTable{
				{ID: 1, DescBytes: toBytes(t, func() *descpb.Descriptor {
					desc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
					tbl, _, _, _ := descpb.FromDescriptor(desc)
					tbl.MutationJobs = []descpb.TableDescriptor_MutationJob{{MutationID: 1, JobID: 123}}
					return desc
				}())},
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, ParentSchemaID: 29, Name: "t"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 2},
			},
			jobsTable: doctor.JobsTable{
				{
					ID:      123,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeDetails{})},
					Status:  jobs.StatusCanceled,
				},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID   2, ParentSchemaID 29: relation "t" (1): mutation job 123 has terminal status (canceled)
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
