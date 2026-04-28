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
		name         string
		node         DropProvisionedRolesNode
		contains     []string
		excludes     []string
		expectedArgs []interface{}
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
				"src.value = $1",
			},
			excludes:     []string{"LIMIT", "estimated_last_login_time"},
			expectedArgs: []interface{}{"ldap:ldap.example.com"},
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
				"$1)::TIMESTAMPTZ",
			},
			excludes:     []string{"LIMIT", "src.value ="},
			expectedArgs: []interface{}{"2025-01-01"},
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
				"src.value = $1",
				"estimated_last_login_time <",
				"$2)::TIMESTAMPTZ",
			},
			excludes:     []string{"LIMIT"},
			expectedArgs: []interface{}{"ldap:ad.corp.com", "2024-06-15"},
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
				"src.value = $1",
				"LIMIT 5",
			},
			expectedArgs: []interface{}{"oidc:okta.example.com"},
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
				"src.value = $1",
				"estimated_last_login_time <",
				"LIMIT 100",
			},
			expectedArgs: []interface{}{"ldap:ldap.example.com", "2025-01-01"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, args := tc.node.buildFilterQuery()
			for _, substr := range tc.contains {
				require.True(t, strings.Contains(query, substr),
					"expected query to contain %q, got:\n%s", substr, query)
			}
			for _, substr := range tc.excludes {
				require.False(t, strings.Contains(query, substr),
					"expected query to NOT contain %q, got:\n%s", substr, query)
			}
			if tc.expectedArgs != nil {
				require.Equal(t, tc.expectedArgs, args,
					"unexpected query args")
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
	query, args := n.buildFilterQuery()
	// With parameterized queries, the malicious value must not appear
	// in the query string — it is safely passed as a parameter.
	require.NotContains(t, query, "evil")
	require.Contains(t, query, "$1")
	require.Len(t, args, 1)
}
