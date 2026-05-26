// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package doctor_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
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
			NextColumnID:     2,
			NextConstraintID: 2,
			Families: []descpb.ColumnFamilyDescriptor{
				{ID: 0, Name: "f", ColumnNames: []string{"col"}, ColumnIDs: []descpb.ColumnID{1}, DefaultColumnID: 1},
			},
			NextFamilyID: 1,
			PrimaryIndex: descpb.IndexDescriptor{
				Name:                tabledesc.PrimaryKeyIndexName("t"),
				ID:                  1,
				Unique:              true,
				KeyColumnNames:      []string{"col"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{1},
				Version:             descpb.LatestIndexDescriptorVersion,
				EncodingType:        catenumpb.PrimaryIndexEncoding,
				ConstraintID:        1,
			},
			NextIndexID: 2,
			Privileges: catpb.NewCustomSuperuserPrivilegeDescriptor(
				privilege.ReadWriteData, username.NodeUserName()),
			FormatVersion:  descpb.InterleavedFormatVersion,
			NextMutationID: 1,
		},
	},
}

func toBytes(t *testing.T, desc *descpb.Descriptor) []byte {
	table, database, typ, schema, function := descpb.GetDescriptors(desc)
	if table != nil {
		parentSchemaID := table.GetUnexposedParentSchemaID()
		if parentSchemaID == descpb.InvalidID {
			parentSchemaID = keys.PublicSchemaID
		}
		if _, err := catprivilege.MaybeFixPrivileges(
			&table.Privileges,
			table.GetParentID(),
			parentSchemaID,
			privilege.Table,
			table.GetName(),
		); err != nil {
			panic(err)
		}
		if table.FormatVersion == 0 {
			table.FormatVersion = descpb.InterleavedFormatVersion
		}
	} else if database != nil {
		if _, err := catprivilege.MaybeFixPrivileges(
			&database.Privileges,
			descpb.InvalidID,
			descpb.InvalidID,
			privilege.Database,
			database.GetName(),
		); err != nil {
			panic(err)
		}
	} else if typ != nil {
		if _, err := catprivilege.MaybeFixPrivileges(
			&typ.Privileges,
			typ.GetParentID(),
			typ.GetParentSchemaID(),
			privilege.Type,
			typ.GetName(),
		); err != nil {
			panic(err)
		}
	} else if schema != nil {
		if _, err := catprivilege.MaybeFixPrivileges(
			&schema.Privileges,
			schema.GetParentID(),
			descpb.InvalidID,
			privilege.Schema,
			schema.GetName(),
		); err != nil {
			panic(err)
		}
	} else if function != nil {
		if _, err := catprivilege.MaybeFixPrivileges(
			&function.Privileges,
			function.GetParentID(),
			descpb.InvalidID,
			privilege.Routine,
			function.GetName(),
		); err != nil {
			panic(err)
		}
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
		tbl, _, _, _, _ := descpb.GetDescriptors(droppedValidTableDesc)
		tbl.State = descpb.DescriptorState_DROP
	}

	// Use 51 as the Schema ID, we do not want to use a reserved system ID (1-49)
	// for the Schema because there should be no schemas with 1-49. A schema with
	// an ID from 1-49 would fail privilege checks due to incompatible privileges
	// the privileges returned from the SystemAllowedPrivileges map in privilege.go.
	validTableDescWithParentSchema := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
	{
		tbl, _, _, _, _ := descpb.GetDescriptors(validTableDescWithParentSchema)
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
  ParentID   0, ParentSchemaID 29: relation "" (2): empty relation name
  ParentID   0, ParentSchemaID 29: relation "" (2): invalid parent ID 0
  ParentID   0, ParentSchemaID 29: relation "" (2): table must contain at least 1 column
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
			valid:    true,
			expected: "Examining 1 descriptors and 0 namespace entries...\n",
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
  ParentID   0, ParentSchemaID 29: namespace entry "t" (51): mismatched name "t" in relation descriptor
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
  ParentID   2, ParentSchemaID  0: schema "schema" (51): referenced database ID 2: referenced descriptor not found
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
  ParentID   3, ParentSchemaID  2: type "type" (51): referenced schema ID 2: referenced descriptor not found
  ParentID   3, ParentSchemaID  2: type "type" (51): arrayTypeID 0 does not exist for "ENUM": referenced type ID 0: referenced descriptor not found
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
  ParentID  51, ParentSchemaID 29: type "type" (52): arrayTypeID 0 does not exist for "ENUM": referenced type ID 0: referenced descriptor not found
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
  ParentID   0, ParentSchemaID  0: namespace entry "causes_error" (2): referenced schema ID 2: referenced descriptor not found
`,
		},
		{ // 14
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "null"}, ID: int64(descpb.InvalidID)},
			},
			expected: `Examining 0 descriptors and 1 namespace entries...
  ParentID   0, ParentSchemaID  0: namespace entry "null" (0): invalid namespace entry
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
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 52},
			},
			valid:    true,
			expected: "Examining 2 descriptors and 1 namespace entries...\n",
		},
		{ // 17
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, func() *descpb.Descriptor {
					desc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
					tbl, _, _, _, _ := descpb.GetDescriptors(desc)
					tbl.PrimaryIndex.Disabled = true
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
  ParentID  52, ParentSchemaID 29: relation "t" (51): unimplemented: primary key dropped without subsequent addition of new primary key in same transaction
`,
		},
		{ // 18
			descTable: doctor.DescriptorTable{
				{ID: 51, DescBytes: toBytes(t, func() *descpb.Descriptor {
					desc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
					tbl, _, _, _, _ := descpb.GetDescriptors(desc)
					tbl.MutationJobs = []descpb.TableDescriptor_MutationJob{{MutationID: 1, JobID: 123}}
					tbl.Mutations = []descpb.DescriptorMutation{{MutationID: 1}}
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
					State:   jobs.StateCanceled,
				},
			},
			expected: `Examining 2 descriptors and 2 namespace entries...
  ParentID  52, ParentSchemaID 29: relation "t" (51): mutation in state UNKNOWN, direction NONE, and no column/index descriptor
  ParentID  52, ParentSchemaID 29: relation "t" (51): mutation job 123 has terminal state (canceled)
`,
		},
	}

	for i, test := range tests {
		var buf bytes.Buffer
		for j := range test.descTable {
			test.descTable[j].ModTime = hlc.MaxTimestamp
		}
		valid, err := doctor.ExamineDescriptors(
			context.Background(),
			clusterversion.TestingClusterVersion,
			test.descTable,
			test.namespaceTable,
			test.jobsTable,
			true,
			false,
			&buf)
		msg := fmt.Sprintf("Test %d failed!", i+1)
		if test.errStr != "" {
			require.Containsf(t, err.Error(), test.errStr, msg)
		} else {
			require.NoErrorf(t, err, msg)
		}
		require.Equalf(t, test.expected, buf.String(), msg)
		require.Equalf(t, test.valid, valid, msg)
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
					State:   jobs.StateRunning,
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
								{ID: 1, Status: jobspb.SchemaChangeGCProgress_CLEARED},
								{ID: 2, Status: jobspb.SchemaChangeGCProgress_CLEARING},
								{ID: 3, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR},
							},
						})},
					State: jobs.StateRunning,
				},
				{
					ID:      200,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeGCDetails{})},
					Progress: &jobspb.Progress{Details: jobspb.WrapProgressDetails(
						jobspb.SchemaChangeGCProgress{
							Tables: []jobspb.SchemaChangeGCProgress_TableProgress{
								{ID: 1, Status: jobspb.SchemaChangeGCProgress_CLEARED},
								{ID: 3, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR},
							},
						})},
					State: jobs.StatePauseRequested,
				},
				{
					ID:      300,
					Payload: &jobspb.Payload{Details: jobspb.WrapPayloadDetails(jobspb.SchemaChangeGCDetails{})},
					Progress: &jobspb.Progress{Details: jobspb.WrapProgressDetails(
						jobspb.SchemaChangeGCProgress{
							Tables: []jobspb.SchemaChangeGCProgress_TableProgress{
								{ID: 1, Status: jobspb.SchemaChangeGCProgress_CLEARED},
								{ID: 3, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR},
							},
							Indexes: []jobspb.SchemaChangeGCProgress_IndexProgress{
								{IndexID: 10, Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR},
							},
						})},
					State: jobs.StatePaused,
				},
			},
			valid: true,
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
					State:    jobs.StateRunning,
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
					State:    jobs.StateRunning,
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
