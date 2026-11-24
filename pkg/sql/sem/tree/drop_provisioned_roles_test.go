// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDropProvisionedRolesFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		node     *tree.DropProvisionedRoles
		expected string
	}{
		{
			name:     "no options",
			node:     &tree.DropProvisionedRoles{},
			expected: "DROP PROVISIONED ROLES",
		},
		{
			name: "source only",
			node: &tree.DropProvisionedRoles{
				Options: &tree.DropProvisionedRolesOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
			},
			expected: "DROP PROVISIONED ROLES WITH SOURCE = 'ldap:ldap.example.com'",
		},
		{
			name: "last login before only",
			node: &tree.DropProvisionedRoles{
				Options: &tree.DropProvisionedRolesOptions{
					LastLoginBefore: tree.NewStrVal("2025-01-01"),
				},
			},
			expected: "DROP PROVISIONED ROLES WITH LAST LOGIN BEFORE '2025-01-01'",
		},
		{
			name: "both options",
			node: &tree.DropProvisionedRoles{
				Options: &tree.DropProvisionedRolesOptions{
					Source:          tree.NewStrVal("ldap:ldap.example.com"),
					LastLoginBefore: tree.NewStrVal("2025-01-01"),
				},
			},
			expected: "DROP PROVISIONED ROLES WITH SOURCE = 'ldap:ldap.example.com', LAST LOGIN BEFORE '2025-01-01'",
		},
		{
			name: "source with limit",
			node: &tree.DropProvisionedRoles{
				Options: &tree.DropProvisionedRolesOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
				Limit: &tree.Limit{Count: tree.NewDInt(10)},
			},
			expected: "DROP PROVISIONED ROLES WITH SOURCE = 'ldap:ldap.example.com' LIMIT 10",
		},
		{
			name: "limit only",
			node: &tree.DropProvisionedRoles{
				Limit: &tree.Limit{Count: tree.NewDInt(5)},
			},
			expected: "DROP PROVISIONED ROLES LIMIT 5",
		},
		{
			name: "all options with limit",
			node: &tree.DropProvisionedRoles{
				Options: &tree.DropProvisionedRolesOptions{
					Source:          tree.NewStrVal("ldap:ldap.example.com"),
					LastLoginBefore: tree.NewStrVal("2025-01-01"),
				},
				Limit: &tree.Limit{Count: tree.NewDInt(10)},
			},
			expected: "DROP PROVISIONED ROLES WITH SOURCE = 'ldap:ldap.example.com', LAST LOGIN BEFORE '2025-01-01' LIMIT 10",
		},
		{
			name: "nil options pointer",
			node: &tree.DropProvisionedRoles{
				Options: nil,
			},
			expected: "DROP PROVISIONED ROLES",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tree.AsString(tc.node)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestDropProvisionedRolesOptionsCombineWith(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("merge disjoint options", func(t *testing.T) {
		a := &tree.DropProvisionedRolesOptions{Source: tree.NewStrVal("ldap:x")}
		b := &tree.DropProvisionedRolesOptions{
			LastLoginBefore: tree.NewStrVal("2025-01-01"),
		}
		err := a.CombineWith(b)
		require.NoError(t, err)
		require.NotNil(t, a.Source)
		require.NotNil(t, a.LastLoginBefore)
	})

	t.Run("duplicate source", func(t *testing.T) {
		a := &tree.DropProvisionedRolesOptions{Source: tree.NewStrVal("ldap:x")}
		b := &tree.DropProvisionedRolesOptions{Source: tree.NewStrVal("ldap:y")}
		err := a.CombineWith(b)
		require.ErrorContains(t, err, "SOURCE")
		require.ErrorContains(t, err, "specified multiple times")
	})

	t.Run("duplicate last login before", func(t *testing.T) {
		a := &tree.DropProvisionedRolesOptions{
			LastLoginBefore: tree.NewStrVal("2025-01-01"),
		}
		b := &tree.DropProvisionedRolesOptions{
			LastLoginBefore: tree.NewStrVal("2025-06-01"),
		}
		err := a.CombineWith(b)
		require.ErrorContains(t, err, "LAST LOGIN BEFORE")
		require.ErrorContains(t, err, "specified multiple times")
	})
}

func TestDropProvisionedRolesOptionsIsDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.True(t, tree.DropProvisionedRolesOptions{}.IsDefault())
	require.False(t,
		tree.DropProvisionedRolesOptions{Source: tree.NewStrVal("x")}.IsDefault())
	require.False(t,
		tree.DropProvisionedRolesOptions{
			LastLoginBefore: tree.NewStrVal("2025-01-01"),
		}.IsDefault())
}
