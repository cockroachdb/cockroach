// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replication

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestShouldSetupForReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		id       descpb.ID
		descName string
		parentID descpb.ID
		expected bool
	}{
		{
			name:     "system database excluded",
			id:       keys.SystemDatabaseID,
			descName: catconstants.SystemDatabaseName,
			parentID: 0,
			expected: false,
		},
		{
			name:     "system public schema excluded",
			id:       keys.SystemPublicSchemaID,
			descName: catconstants.PublicSchemaName,
			parentID: descpb.InvalidID,
			expected: false,
		},
		{
			name:     "users table included",
			id:       keys.UsersTableID,
			descName: string(catconstants.UsersTableName),
			parentID: keys.SystemDatabaseID,
			expected: true,
		},
		{
			name:     "role_members table included",
			id:       keys.RoleMembersTableID,
			descName: string(catconstants.RoleMembersTableName),
			parentID: keys.SystemDatabaseID,
			expected: true,
		},
		{
			name:     "system_privileges included",
			id:       descpb.ID(4),
			descName: string(catconstants.SystemPrivilegeTableName),
			parentID: keys.SystemDatabaseID,
			expected: true,
		},
		{
			name:     "other system table excluded",
			id:       descpb.ID(5),
			descName: "some_system_table",
			parentID: keys.SystemDatabaseID,
			expected: false,
		},
		{
			name:     "user descriptor included",
			id:       descpb.ID(100),
			descName: "my_table",
			parentID: descpb.ID(52),
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := shouldSetupForReader(tc.id, tc.descName, tc.parentID)
			require.Equal(t, tc.expected, result)
		})
	}
}
