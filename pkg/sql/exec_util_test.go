// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestHideNonVirtualTableNameFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := cluster.MakeTestingClusterSettings()
	vt, err := NewVirtualSchemaHolder(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	tableNameFunc := hideNonVirtualTableNameFunc(vt, nil)

	testData := []struct {
		stmt     string
		expected string
	}{
		{`SELECT * FROM a.b`,
			`SELECT * FROM _`},
		{`SELECT * FROM pg_type`,
			`SELECT * FROM _`},
		{`SELECT * FROM pg_catalog.pg_type`,
			`SELECT * FROM pg_catalog.pg_type`},
	}

	for _, test := range testData {
		stmt, err := parser.ParseOne(test.stmt)
		if err != nil {
			t.Fatal(err)
		}
		f := tree.NewFmtCtx(
			tree.FmtSimple,
			tree.FmtReformatTableNames(tableNameFunc),
		)
		f.FormatNode(stmt.AST)
		actual := f.CloseAndGetString()
		require.Equal(t, test.expected, actual)
	}
}

func TestMaybeHashAppName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	secret := "secret"

	for _, tc := range []struct {
		name     string
		appName  string
		expected string
	}{
		{
			"basic app name is hashed",
			"my_app",
			"a4bc27b7",
		},
		{
			"internal app name is not hashed",
			"$ internal_app",
			"$ internal_app",
		},
		{
			"delegated app name is hashed",
			"$$ my_app",
			"$$ 9a7e689f",
		},
		{
			"delegated and reportable app name is not hashed",
			"$$ $ internal_app",
			"$$ $ internal_app",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := MaybeHashAppName(tc.appName, secret)
			require.Equal(t, tc.expected, out)
		})
	}
}

// TestSessionDefaultsSafeFormat tests the redacted output of SessionDefaults.
func TestSessionDefaultsSafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	session := SessionDefaults(make(map[string]string))
	session["database"] = "test"
	session["statement_timeout"] = "250ms"
	session["disallow_full_table_scans"] = "true"
	require.Contains(t, redact.Sprint(session), "database=‹test›")
	require.Contains(t, redact.Sprint(session).Redact(), "statement_timeout=‹×›")
}

func TestTruncatePlaceholderValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		input    string
		expected func(string) bool
	}{
		{
			name:  "short string unchanged",
			input: "short",
			expected: func(result string) bool {
				return result == "short"
			},
		},
		{
			name:  "exactly max length unchanged",
			input: strings.Repeat("A", MaxPlaceholderValueBytes),
			expected: func(result string) bool {
				return result == strings.Repeat("A", MaxPlaceholderValueBytes)
			},
		},
		{
			name:  "long string truncated with ellipsis",
			input: strings.Repeat("B", MaxPlaceholderValueBytes+100),
			expected: func(result string) bool {
				return len(result) <= MaxPlaceholderValueBytes &&
					strings.HasSuffix(result, "…") &&
					strings.HasPrefix(result, "BB")
			},
		},
		{
			name:  "unicode string truncated properly",
			input: strings.Repeat("你好", MaxPlaceholderValueBytes), // Each char is 3 bytes
			expected: func(result string) bool {
				return len(result) <= MaxPlaceholderValueBytes &&
					strings.HasSuffix(result, "…") &&
					utf8.ValidString(result)
			},
		},
		{
			name:  "empty string unchanged",
			input: "",
			expected: func(result string) bool {
				return result == ""
			},
		},
		{
			name:  "string with mixed unicode",
			input: strings.Repeat("A你B好", MaxPlaceholderValueBytes/2), // Mix ASCII and unicode
			expected: func(result string) bool {
				return len(result) <= MaxPlaceholderValueBytes &&
					utf8.ValidString(result) &&
					(len(result) < len(strings.Repeat("A你B好", MaxPlaceholderValueBytes/2)) || strings.HasSuffix(result, "…"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncatePlaceholderValue(tt.input)

			require.True(t, tt.expected(result),
				"input: %q (len=%d) -> result: %q (len=%d)",
				tt.input, len(tt.input), result, len(result))

			require.True(t, utf8.ValidString(result),
				"result should be valid UTF-8: %q", result)

			require.LessOrEqual(t, len(result), MaxPlaceholderValueBytes,
				"result should not exceed MaxPlaceholderValueBytes")
		})
	}
}
