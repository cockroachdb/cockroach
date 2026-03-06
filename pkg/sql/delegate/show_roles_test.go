// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDelegateShowRolesExtended(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeCtx := func() (context.Context, *delegator) {
		ctx := context.Background()
		evalCtx := &eval.Context{
			Planner:            &faketreeeval.DummyEvalPlanner{},
			ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
		}
		d := &delegator{ctx: ctx, evalCtx: evalCtx}
		return ctx, d
	}

	testCases := []struct {
		name     string
		node     *tree.ShowUsers
		contains []string // substrings expected in the generated query
		excludes []string // substrings that must NOT appear
	}{
		{
			name: "no options falls back to delegateShowRoles",
			node: &tree.ShowUsers{},
			// The fallback query uses the same base SELECT.
			contains: []string{
				"u.username",
				"estimated_last_login_time",
				"system.users AS u",
			},
			excludes: []string{"PROVISIONSRC", "LIMIT"},
		},
		{
			name: "source filter",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source: tree.NewStrVal("ldap:ldap.example.com"),
				},
			},
			contains: []string{
				"WHERE",
				"PROVISIONSRC",
				"ldap:ldap.example.com",
				"system.role_options AS src",
			},
			excludes: []string{"LIMIT", "estimated_last_login_time <"},
		},
		{
			name: "last access older than filter",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
				},
			},
			contains: []string{
				"WHERE",
				"estimated_last_login_time <",
				"2024-01-01",
				"TIMESTAMPTZ",
			},
			excludes: []string{"LIMIT", "PROVISIONSRC"},
		},
		{
			name: "both filters",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source:              tree.NewStrVal("ldap:ldap.example.com"),
					LastAccessOlderThan: tree.NewStrVal("2024-06-15"),
				},
			},
			contains: []string{
				"WHERE",
				"PROVISIONSRC",
				"ldap:ldap.example.com",
				"estimated_last_login_time <",
				"2024-06-15",
				"AND",
			},
			excludes: []string{"LIMIT"},
		},
		{
			name: "limit only",
			node: &tree.ShowUsers{
				Limit: &tree.Limit{Count: tree.NewDInt(10)},
			},
			contains: []string{"LIMIT 10"},
			excludes: []string{"PROVISIONSRC", "estimated_last_login_time <"},
		},
		{
			name: "source with limit",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source: tree.NewStrVal("ldap:ad.corp.com"),
				},
				Limit: &tree.Limit{Count: tree.NewDInt(5)},
			},
			contains: []string{
				"WHERE",
				"PROVISIONSRC",
				"ldap:ad.corp.com",
				"LIMIT 5",
			},
		},
		{
			name: "all options with limit",
			node: &tree.ShowUsers{
				Options: &tree.ShowUsersOptions{
					Source:              tree.NewStrVal("ldap:ldap.example.com"),
					LastAccessOlderThan: tree.NewStrVal("2024-01-01"),
				},
				Limit: &tree.Limit{Count: tree.NewDInt(25)},
			},
			contains: []string{
				"WHERE",
				"PROVISIONSRC",
				"estimated_last_login_time <",
				"LIMIT 25",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, d := makeCtx()
			stmt, err := d.delegateShowRolesExtended(tc.node)
			require.NoError(t, err)
			require.NotNil(t, stmt)

			query := tree.AsString(stmt)
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

func TestDelegateShowRolesExtendedSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	evalCtx := &eval.Context{
		Planner:            &faketreeeval.DummyEvalPlanner{},
		ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
	}
	d := &delegator{ctx: ctx, evalCtx: evalCtx}

	// Verify that a source value with SQL injection attempts is properly
	// escaped via lexbase.EscapeSQLString.
	n := &tree.ShowUsers{
		Options: &tree.ShowUsersOptions{
			Source: tree.NewStrVal("ldap:evil' OR 1=1 --"),
		},
	}
	stmt, err := d.delegateShowRolesExtended(n)
	require.NoError(t, err)

	query := tree.AsString(stmt)
	// The injected single quote must be escaped so it cannot terminate
	// the string literal. The unescaped form "evil' OR" must not appear;
	// instead it should be escaped (e.g. "evil'' OR" or "evil\' OR").
	require.NotContains(t, query, "evil' OR")
}
