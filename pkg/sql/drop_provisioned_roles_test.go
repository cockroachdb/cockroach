// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBuildFilterQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		node     DropProvisionedRolesNode
		contains []string
		excludes []string
	}{
		{
			name: "no options — all provisioned users",
			node: DropProvisionedRolesNode{},
			contains: []string{
				"SELECT u.username FROM system.users AS u",
				"PROVISIONSRC",
			},
			excludes: []string{"LIMIT", "estimated_last_login_time"},
		},
		{
			name: "source filter",
			node: DropProvisionedRolesNode{
				options: &tree.DropProvisionedRolesOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
			},
			contains: []string{
				"PROVISIONSRC",
				"ldap:ldap.example.com",
				"src.value =",
			},
			excludes: []string{"LIMIT", "estimated_last_login_time"},
		},
		{
			name: "last login before filter",
			node: DropProvisionedRolesNode{
				options: &tree.DropProvisionedRolesOptions{
					LastLoginBefore: tree.NewStrVal("2025-01-01"),
				},
			},
			contains: []string{
				"PROVISIONSRC",
				"estimated_last_login_time <",
				"2025-01-01",
				"TIMESTAMPTZ",
			},
			excludes: []string{"LIMIT", "src.value ="},
		},
		{
			name: "both filters",
			node: DropProvisionedRolesNode{
				options: &tree.DropProvisionedRolesOptions{
					Source:          tree.NewStrVal("ldap:ad.corp.com"),
					LastLoginBefore: tree.NewStrVal("2024-06-15"),
				},
			},
			contains: []string{
				"PROVISIONSRC",
				"ldap:ad.corp.com",
				"estimated_last_login_time <",
				"2024-06-15",
			},
			excludes: []string{"LIMIT"},
		},
		{
			name: "limit only",
			node: DropProvisionedRolesNode{
				limit: &tree.Limit{Count: tree.NewDInt(10)},
			},
			contains: []string{"PROVISIONSRC", "LIMIT 10"},
			excludes: []string{"estimated_last_login_time", "src.value ="},
		},
		{
			name: "source with limit",
			node: DropProvisionedRolesNode{
				options: &tree.DropProvisionedRolesOptions{
					Source: tree.NewStrVal("oidc:okta.example.com"),
				},
				limit: &tree.Limit{Count: tree.NewDInt(5)},
			},
			contains: []string{
				"PROVISIONSRC",
				"oidc:okta.example.com",
				"LIMIT 5",
			},
		},
		{
			name: "all options with limit",
			node: DropProvisionedRolesNode{
				options: &tree.DropProvisionedRolesOptions{
					Source:          tree.NewStrVal("ldap:ldap.example.com"),
					LastLoginBefore: tree.NewStrVal("2025-01-01"),
				},
				limit: &tree.Limit{Count: tree.NewDInt(100)},
			},
			contains: []string{
				"PROVISIONSRC",
				"ldap:ldap.example.com",
				"estimated_last_login_time <",
				"LIMIT 100",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, _ := tc.node.buildFilterQuery()
			for _, substr := range tc.contains {
				require.True(t, strings.Contains(query, substr),
					"expected query to contain %q, got:\n%s", substr, query)
			}
			for _, substr := range tc.excludes {
				require.False(t, strings.Contains(query, substr),
					"expected query to NOT contain %q, got:\n%s", substr, query)
			}
		})
	}
}

func TestBuildFilterQuerySQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	n := DropProvisionedRolesNode{
		options: &tree.DropProvisionedRolesOptions{
			Source: tree.NewStrVal("ldap:evil' OR 1=1 --"),
		},
	}
	query, _ := n.buildFilterQuery()
	// The injected single quote must be escaped.
	require.NotContains(t, query, "evil' OR")
}
