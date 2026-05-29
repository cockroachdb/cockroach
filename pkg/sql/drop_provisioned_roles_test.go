// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestBuildProvisionedRolesQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	strPtr := func(s string) *string { return &s }
	tsVal, err := tree.MakeDTimestampTZ(
		timeutil.Now().Add(-7*24*time.Hour), time.Microsecond,
	)
	require.NoError(t, err)
	// Use the truncated time from the datum for expected args.
	ts := tsVal.Time

	testCases := []struct {
		name         string
		source       *string
		lastLogin    *tree.DTimestampTZ
		limit        int64
		contains     []string
		excludes     []string
		expectedArgs []any
	}{
		{
			name:  "limit only — all provisioned users",
			limit: 10,
			contains: []string{
				"SELECT u.username FROM system.users AS u",
				"PROVISIONSRC",
				"LIMIT $1",
			},
			excludes:     []string{"estimated_last_login_time"},
			expectedArgs: []any{int64(10)},
		},
		{
			name:   "source filter with limit",
			source: strPtr("ldap:ldap.example.com"),
			limit:  50,
			contains: []string{
				"PROVISIONSRC",
				"src.value = $1",
				"LIMIT $2",
			},
			excludes:     []string{"estimated_last_login_time"},
			expectedArgs: []any{"ldap:ldap.example.com", int64(50)},
		},
		{
			name:      "last login before filter with limit",
			lastLogin: tsVal,
			limit:     100,
			contains: []string{
				"PROVISIONSRC",
				"estimated_last_login_time IS NULL OR",
				"estimated_last_login_time < $1",
				"LIMIT $2",
			},
			excludes:     []string{"src.value ="},
			expectedArgs: []any{ts, int64(100)},
		},
		{
			name:      "both filters with limit",
			source:    strPtr("ldap:ad.corp.com"),
			lastLogin: tsVal,
			limit:     200,
			contains: []string{
				"PROVISIONSRC",
				"src.value = $1",
				"estimated_last_login_time < $2",
				"LIMIT $3",
			},
			expectedArgs: []any{"ldap:ad.corp.com", ts, int64(200)},
		},
		{
			name:      "max limit",
			source:    strPtr("ldap:ldap.example.com"),
			lastLogin: tsVal,
			limit:     maxDropProvisionedRolesLimit,
			contains: []string{
				"PROVISIONSRC",
				"src.value = $1",
				"estimated_last_login_time < $2",
				"LIMIT $3",
			},
			expectedArgs: []any{
				"ldap:ldap.example.com", ts, int64(maxDropProvisionedRolesLimit),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, args := buildProvisionedRolesQuery(
				tc.source, tc.lastLogin, tc.limit,
			)
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

func TestBuildProvisionedRolesQuerySQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	malicious := "ldap:evil' OR 1=1 --"
	query, args := buildProvisionedRolesQuery(&malicious, nil, 10)
	// With parameterized queries, the malicious value must not appear
	// in the query string — it is safely passed as a parameter.
	require.NotContains(t, query, "evil")
	require.Contains(t, query, "$1")
	require.Len(t, args, 2) // source + limit
}
