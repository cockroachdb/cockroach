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
			name: "last access older than only",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
				},
			},
			expected: "SHOW USERS WITH LAST ACCESS TIME OLDER THAN '2024-01-01'",
		},
		{
			name: "both options",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source:              tree.NewStrVal("ldap:ldap.example.com"),
					LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
				},
			},
			expected: "SHOW USERS WITH SOURCE = 'ldap:ldap.example.com', LAST ACCESS TIME OLDER THAN '2024-01-01'",
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
			name: "all options with limit",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source:              tree.NewStrVal("ldap:ldap.example.com"),
					LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
				},
				Limit: &tree.Limit{Count: tree.NewDInt(10)},
			},
			expected: "SHOW USERS WITH SOURCE = 'ldap:ldap.example.com', LAST ACCESS TIME OLDER THAN '2024-01-01' LIMIT 10",
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

func TestShowUsersOptionsCombineWith(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("merge disjoint options", func(t *testing.T) {
		a := &tree.ShowUsersOptions{Source: tree.NewStrVal("ldap:x")}
		b := &tree.ShowUsersOptions{
			LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
		}
		err := a.CombineWith(b)
		require.NoError(t, err)
		require.NotNil(t, a.Source)
		require.NotNil(t, a.LastAccessOlderThan)
	})

	t.Run("duplicate source", func(t *testing.T) {
		a := &tree.ShowUsersOptions{Source: tree.NewStrVal("ldap:x")}
		b := &tree.ShowUsersOptions{Source: tree.NewStrVal("ldap:y")}
		err := a.CombineWith(b)
		require.ErrorContains(t, err, "SOURCE")
		require.ErrorContains(t, err, "specified multiple times")
	})

	t.Run("duplicate last access older than", func(t *testing.T) {
		a := &tree.ShowUsersOptions{
			LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
		}
		b := &tree.ShowUsersOptions{
			LastAccessOlderThan: tree.NewStrVal("2025-01-01"),
		}
		err := a.CombineWith(b)
		require.ErrorContains(t, err, "LAST ACCESS TIME OLDER THAN")
		require.ErrorContains(t, err, "specified multiple times")
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
			LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
		}.IsDefault())
}
