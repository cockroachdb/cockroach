// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestExtractParentDatabaseID(t *testing.T) {
	tests := []struct {
		name          string
		desc          *Descriptor
		expectedID    ID
		expectedTable bool
	}{
		{
			name: "table descriptor with parent ID",
			desc: &Descriptor{
				Union: &Descriptor_Table{
					Table: &TableDescriptor{
						ID:       100,
						Name:     "test_table",
						ParentID: 50,
					},
				},
			},
			expectedID:    50,
			expectedTable: true,
		},
		{
			name: "table descriptor with zero parent ID",
			desc: &Descriptor{
				Union: &Descriptor_Table{
					Table: &TableDescriptor{
						ID:       100,
						Name:     "test_table",
						ParentID: 0,
					},
				},
			},
			expectedID:    0,
			expectedTable: true,
		},
		{
			name: "table descriptor with large parent ID",
			desc: &Descriptor{
				Union: &Descriptor_Table{
					Table: &TableDescriptor{
						ID:       100,
						Name:     "test_table",
						ParentID: 1000000,
					},
				},
			},
			expectedID:    1000000,
			expectedTable: true,
		},
		{
			name: "database descriptor",
			desc: &Descriptor{
				Union: &Descriptor_Database{
					Database: &DatabaseDescriptor{
						ID:   50,
						Name: "test_db",
					},
				},
			},
			expectedID:    0,
			expectedTable: false,
		},
		{
			name: "schema descriptor",
			desc: &Descriptor{
				Union: &Descriptor_Schema{
					Schema: &SchemaDescriptor{
						ID:       100,
						Name:     "test_schema",
						ParentID: 50,
					},
				},
			},
			expectedID:    0,
			expectedTable: false,
		},
		{
			name: "type descriptor",
			desc: &Descriptor{
				Union: &Descriptor_Type{
					Type: &TypeDescriptor{
						ID:                       100,
						Name:                     "test_type",
						ParentID:                 50,
						ParentSchemaID:           51,
						Kind:                     TypeDescriptor_ENUM,
						ArrayTypeID:              0,
						ReferencingDescriptorIDs: nil,
					},
				},
			},
			expectedID:    0,
			expectedTable: false,
		},
		{
			name: "function descriptor",
			desc: &Descriptor{
				Union: &Descriptor_Function{
					Function: &FunctionDescriptor{
						ID:             100,
						Name:           "test_func",
						ParentID:       50,
						ParentSchemaID: 51,
					},
				},
			},
			expectedID:    0,
			expectedTable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize the descriptor to bytes.
			descBytes, err := protoutil.Marshal(tc.desc)
			require.NoError(t, err)

			// Extract parent ID from raw bytes.
			parentID, isTable, err := ExtractParentDatabaseID(descBytes)
			require.NoError(t, err)
			require.Equal(t, tc.expectedID, parentID, "parent ID mismatch")
			require.Equal(t, tc.expectedTable, isTable, "isTable mismatch")
		})
	}
}

func TestExtractParentDatabaseID_EmptyBytes(t *testing.T) {
	parentID, isTable, err := ExtractParentDatabaseID(nil)
	require.NoError(t, err)
	require.Equal(t, ID(0), parentID)
	require.False(t, isTable)

	parentID, isTable, err = ExtractParentDatabaseID([]byte{})
	require.NoError(t, err)
	require.Equal(t, ID(0), parentID)
	require.False(t, isTable)
}

func TestExtractParentDatabaseID_MalformedBytes(t *testing.T) {
	// Malformed bytes should return an error.
	tests := []struct {
		name  string
		bytes []byte
	}{
		{
			name:  "truncated tag",
			bytes: []byte{0x80}, // Incomplete varint
		},
		{
			name:  "truncated length",
			bytes: []byte{0x0A, 0x80}, // Field 1, bytes type, incomplete length
		},
		{
			name:  "truncated data",
			bytes: []byte{0x0A, 0x10, 0x01, 0x02}, // Field 1, length 16, only 2 bytes
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ExtractParentDatabaseID(tc.bytes)
			require.Error(t, err)
		})
	}
}
