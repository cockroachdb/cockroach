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

func TestShowUsersFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		node     *tree.ShowUsers
		expected string
	}{
		{
			name:     "no options",
			node:     &tree.ShowUsers{},
			expected: "SHOW USERS",
		},
		{
			name: "source only",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
			},
			expected: "SHOW USERS WITH SOURCE = 'ldap:ldap.example.com'",
		},
		{
			name: "last login before only",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					LastLoginBefore: tree.NewStrVal("2024-01-01"),
				},
			},
			expected: "SHOW USERS WITH LAST LOGIN BEFORE '2024-01-01'",
		},
		{
			name: "both options",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source:          tree.NewStrVal("ldap:ldap.example.com"),
					LastLoginBefore: tree.NewStrVal("2024-01-01"),
				},
			},
			expected: "SHOW USERS WITH SOURCE = 'ldap:ldap.example.com', LAST LOGIN BEFORE '2024-01-01'",
		},
		{
			name: "source with limit",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
				Limit: &tree.Limit{Count: tree.NewDInt(10)},
			},
			expected: "SHOW USERS WITH SOURCE = 'ldap:ldap.example.com' LIMIT 10",
		},
		{
			name: "limit only",
			node: &tree.ShowUsers{
				Limit: &tree.Limit{Count: tree.NewDInt(5)},
			},
			expected: "SHOW USERS LIMIT 5",
		},
		{
			name: "nil options pointer",
			node: &tree.ShowUsers{
				Options: nil,
			},
			expected: "SHOW USERS",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tree.AsString(tc.node)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestShowRolesFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		node     *tree.ShowRoles
		expected string
	}{
		{
			name:     "no options",
			node:     &tree.ShowRoles{},
			expected: "SHOW ROLES",
		},
		{
			name: "source only",
			node: &tree.ShowRoles{
				Options: &tree.ShowUsersOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
			},
			expected: "SHOW ROLES WITH SOURCE = 'ldap:ldap.example.com'",
		},
		{
			name: "last login before only",
			node: &tree.ShowRoles{
				Options: &tree.ShowUsersOptions{
					LastLoginBefore: tree.NewStrVal("2024-01-01"),
				},
			},
			expected: "SHOW ROLES WITH LAST LOGIN BEFORE '2024-01-01'",
		},
		{
			name: "both options",
			node: &tree.ShowRoles{
				Options: &tree.ShowUsersOptions{
					Source:          tree.NewStrVal("ldap:ldap.example.com"),
					LastLoginBefore: tree.NewStrVal("2024-01-01"),
				},
			},
			expected: "SHOW ROLES WITH SOURCE = 'ldap:ldap.example.com', LAST LOGIN BEFORE '2024-01-01'",
		},
		{
			name: "source with limit",
			node: &tree.ShowRoles{
				Options: &tree.ShowUsersOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
				Limit: &tree.Limit{Count: tree.NewDInt(10)},
			},
			expected: "SHOW ROLES WITH SOURCE = 'ldap:ldap.example.com' LIMIT 10",
		},
		{
			name: "limit only",
			node: &tree.ShowRoles{
				Limit: &tree.Limit{Count: tree.NewDInt(5)},
			},
			expected: "SHOW ROLES LIMIT 5",
		},
		{
			name: "nil options pointer",
			node: &tree.ShowRoles{
				Options: nil,
			},
			expected: "SHOW ROLES",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tree.AsString(tc.node)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestShowUsersOptionsCombineWith(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("merge disjoint options", func(t *testing.T) {
		a := &tree.ShowUsersOptions{Source: tree.NewStrVal("ldap:x")}
		b := &tree.ShowUsersOptions{
			LastLoginBefore: tree.NewStrVal("2024-01-01"),
		}
		err := a.CombineWith(b)
		require.NoError(t, err)
		require.NotNil(t, a.Source)
		require.NotNil(t, a.LastLoginBefore)
	})

	t.Run("duplicate source", func(t *testing.T) {
		a := &tree.ShowUsersOptions{Source: tree.NewStrVal("ldap:x")}
		b := &tree.ShowUsersOptions{Source: tree.NewStrVal("ldap:y")}
		err := a.CombineWith(b)
		require.ErrorContains(t, err, "SOURCE option specified multiple times")
	})

	t.Run("duplicate last login before", func(t *testing.T) {
		a := &tree.ShowUsersOptions{
			LastLoginBefore: tree.NewStrVal("2024-01-01"),
		}
		b := &tree.ShowUsersOptions{
			LastLoginBefore: tree.NewStrVal("2025-01-01"),
		}
		err := a.CombineWith(b)
		require.ErrorContains(t, err,
			"LAST LOGIN BEFORE option specified multiple times")
	})
}

func TestShowUsersOptionsIsDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.True(t, tree.ShowUsersOptions{}.IsDefault())
	require.False(t,
		tree.ShowUsersOptions{Source: tree.NewStrVal("x")}.IsDefault())
	require.False(t,
		tree.ShowUsersOptions{
			LastLoginBefore: tree.NewStrVal("2024-01-01"),
		}.IsDefault())
}
