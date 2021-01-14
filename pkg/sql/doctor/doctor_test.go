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
				Version:          descpb.EmptyArraysInInvertedIndexesVersion,
			},
			NextIndexID: 2,
			Privileges: descpb.NewCustomSuperuserPrivilegeDescriptor(
				descpb.SystemAllowedPrivileges[keys.SqllivenessID], security.NodeUserName()),
			FormatVersion:  descpb.InterleavedFormatVersion,
			NextMutationID: 1,
		},
	},
}

func toBytes(t *testing.T, pb protoutil.Message) []byte {
	res, err := protoutil.Marshal(pb)
	require.NoError(t, err)
	return res
}

func TestExamineDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	droppedValidTableDesc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
	descpb.TableFromDescriptor(droppedValidTableDesc, hlc.Timestamp{WallTime: 1}).
		State = descpb.DescriptorState_DROP

	inSchemaValidTableDesc := protoutil.Clone(validTableDesc).(*descpb.Descriptor)
	descpb.TableFromDescriptor(inSchemaValidTableDesc, hlc.Timestamp{WallTime: 1}).
		UnexposedParentSchemaID = 3

	tests := []struct {
		descTable      doctor.DescriptorTable
		namespaceTable doctor.NamespaceTable
		valid          bool
		errStr         string
		expected       string
	}{
		{
			valid:    true,
			expected: "Examining 0 descriptors and 0 namespace entries...\n",
		},
		{
			descTable: doctor.DescriptorTable{{ID: 1, DescBytes: []byte("#$@#@#$#@#")}},
			errStr:    "failed to unmarshal descriptor",
			expected:  "Examining 1 descriptors and 0 namespace entries...\n",
		},
		{
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Table{
						Table: &descpb.TableDescriptor{ID: 2},
					}}),
				},
			},
			expected: `Examining 1 descriptors and 0 namespace entries...
   Table   2: ParentID   0, ParentSchemaID 29, Name '': different id in descriptor table: 1
`,
		},
		{
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Table{
						Table: &descpb.TableDescriptor{Name: "foo", ID: 1, State: descpb.DescriptorState_DROP},
					}}),
				},
			},
			expected: `Examining 1 descriptors and 0 namespace entries...
   Table   1: ParentID   0, ParentSchemaID 29, Name 'foo': invalid parent ID 0
`,
		},
		{
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
   Table   1: ParentID   0, ParentSchemaID 29, Name 'foo': invalid parent ID 0
`,
		},
		{
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 1},
					}}),
				},
			},
			expected: `Examining 1 descriptors and 0 namespace entries...
Database   1: ParentID   0, ParentSchemaID  0, Name 'db': not being dropped but no namespace entry found
`,
		},
		{
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
   Table   1: ParentID   2, ParentSchemaID 29, Name 't': namespace entry {ParentID:0 ParentSchemaID:29 Name:t} not found in draining names
   Table   1: ParentID   2, ParentSchemaID 29, Name 't': could not find name in namespace table
`,
		},
		{
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Schema{
						Schema: &descpb.SchemaDescriptor{Name: "schema", ID: 1, ParentID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, Name: "schema"}, ID: 1},
			},
			expected: `Examining 1 descriptors and 1 namespace entries...
  Schema   1: ParentID   2, ParentSchemaID  0, Name 'schema': invalid parent id 2
`,
		},
		{
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
    Type   1: ParentID   0, ParentSchemaID  0, Name 'type': invalid parentID 0
`,
		},
		{
			descTable: doctor.DescriptorTable{
				{
					ID: 1,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Type{
						Type: &descpb.TypeDescriptor{Name: "type", ID: 1, ParentSchemaID: 2},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentSchemaID: 2, Name: "type"}, ID: 1},
			},
			expected: `Examining 1 descriptors and 1 namespace entries...
    Type   1: ParentID   0, ParentSchemaID  2, Name 'type': invalid parentID 0
    Type   1: ParentID   0, ParentSchemaID  2, Name 'type': invalid parent schema id 2
`,
		},
		{
			descTable: doctor.DescriptorTable{
				{ID: 1, DescBytes: toBytes(t, inSchemaValidTableDesc)},
				{
					ID: 2,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Database{
						Database: &descpb.DatabaseDescriptor{Name: "db", ID: 2},
					}}),
				},
				{
					ID: 3,
					DescBytes: toBytes(t, &descpb.Descriptor{Union: &descpb.Descriptor_Schema{
						Schema: &descpb.SchemaDescriptor{Name: "schema", ID: 3, ParentID: 0},
					}}),
				},
			},
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{ParentID: 2, ParentSchemaID: 3, Name: "t"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "db"}, ID: 2},
				{NameInfo: descpb.NameInfo{ParentID: 0, Name: "schema"}, ID: 3},
			},
			expected: `Examining 3 descriptors and 3 namespace entries...
   Table   1: ParentID   2, ParentSchemaID  3, Name 't': invalid parent id of parent schema, expected 2, found 0
`,
		},
		{
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "foo"}, ID: keys.PublicSchemaID},
				{NameInfo: descpb.NameInfo{Name: "bar"}, ID: keys.PublicSchemaID},
				{NameInfo: descpb.NameInfo{Name: "pg_temp_foo"}, ID: 1},
				{NameInfo: descpb.NameInfo{Name: "causes_error"}, ID: 2},
			},
			expected: `Examining 0 descriptors and 4 namespace entries...
Descriptor 2: has namespace row(s) [{ParentID:0 ParentSchemaID:0 Name:causes_error}] but no descriptor
`,
		},
		{
			namespaceTable: doctor.NamespaceTable{
				{NameInfo: descpb.NameInfo{Name: "null"}, ID: int64(descpb.InvalidID)},
			},
			expected: `Examining 0 descriptors and 1 namespace entries...
Row(s) [{ParentID:0 ParentSchemaID:0 Name:null}]: NULL value found
`,
		},
		{
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
		{
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
		{
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
Database   1: ParentID   0, ParentSchemaID  0, Name 'db': extra draining names found [{ParentID:0 ParentSchemaID:0 Name:db3}]
`,
		},
		{
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
   Table   1: ParentID   2, ParentSchemaID 29, Name 't': dropped but namespace entry(s) found: [{2 29 t}]
`,
		},
	}

	for i, test := range tests {
		var buf bytes.Buffer
		valid, err := doctor.ExamineDescriptors(
			context.Background(), test.descTable, test.namespaceTable, false, &buf)
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
				},
			},
			valid:    true,
			expected: "Examining 1 running jobs...\n",
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
				},
			},
			expected: `Examining 3 running jobs...
job 100: schema change GC refers to missing table descriptor(s) [3]
	existing descriptors that still need to be dropped [2]
job 200: schema change GC refers to missing table descriptor(s) [3]
	existing descriptors that still need to be dropped []
	job 200 can be safely deleted
job 300: schema change GC refers to missing table descriptor(s) [3]
	existing descriptors that still need to be dropped []
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
