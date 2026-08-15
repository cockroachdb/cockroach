// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// These throwaway settings back TestFormatSetClusterSettingSensitive. Registering
// them at package scope adds them to the global registry for the test binary,
// which is what SetClusterSetting.Format consults.
var _ = settings.RegisterStringSetting(
	settings.ApplicationLevel, "sql.testing.format_sensitive", "desc", "",
	settings.Sensitive,
)
var _ = settings.RegisterStringSetting(
	settings.ApplicationLevel, "sql.testing.format_nonsensitive", "desc", "",
)

// TestFormatSetClusterSettingSensitive verifies that formatting a SET CLUSTER
// SETTING statement substitutes the value of a sensitive setting unless
// FmtShowPasswords is set, mirroring the treatment of passwords.
func TestFormatSetClusterSettingSensitive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const secret = "hunter2"
	const sub = "'*****'"

	testCases := []struct {
		name          string
		stmt          string
		flags         tree.FmtFlags
		wantContains  string
		wantExcludes  string
		placeholderTo string // if set, interpolate $1 to this value
	}{
		{
			name:         "sensitive redacted by default",
			stmt:         "SET CLUSTER SETTING sql.testing.format_sensitive = 'hunter2'",
			flags:        tree.FmtSimple,
			wantContains: sub,
			wantExcludes: secret,
		},
		{
			name:         "sensitive revealed with FmtShowPasswords",
			stmt:         "SET CLUSTER SETTING sql.testing.format_sensitive = 'hunter2'",
			flags:        tree.FmtShowPasswords,
			wantContains: secret,
		},
		{
			name:         "sensitive still redacted when hiding constants",
			stmt:         "SET CLUSTER SETTING sql.testing.format_sensitive = 'hunter2'",
			flags:        tree.FmtHideConstants,
			wantContains: sub,
			wantExcludes: secret,
		},
		{
			name:         "non-sensitive is not substituted",
			stmt:         "SET CLUSTER SETTING sql.testing.format_nonsensitive = 'hunter2'",
			flags:        tree.FmtShowPasswords,
			wantContains: secret,
		},
		{
			// A typo'd sensitive setting name does not resolve in the registry,
			// but its value is still the intended secret; an unrecognized name
			// is conservatively treated as sensitive.
			name:         "unknown name is redacted",
			stmt:         "SET CLUSTER SETTING sql.testing.format_sensitivee = 'hunter2'",
			flags:        tree.FmtSimple,
			wantContains: sub,
			wantExcludes: secret,
		},
		{
			name:         "unknown name revealed with FmtShowPasswords",
			stmt:         "SET CLUSTER SETTING sql.testing.format_sensitivee = 'hunter2'",
			flags:        tree.FmtShowPasswords,
			wantContains: secret,
		},
		{
			name:         "unknown name reset is left untouched",
			stmt:         "SET CLUSTER SETTING sql.testing.format_sensitivee = DEFAULT",
			flags:        tree.FmtSimple,
			wantContains: "DEFAULT",
			wantExcludes: sub,
		},
		{
			name:         "reset is left untouched",
			stmt:         "SET CLUSTER SETTING sql.testing.format_sensitive = DEFAULT",
			flags:        tree.FmtSimple,
			wantContains: "DEFAULT",
			wantExcludes: sub,
		},
		{
			name:         "tenant variant is substituted",
			stmt:         "ALTER VIRTUAL CLUSTER 'foo' SET CLUSTER SETTING sql.testing.format_sensitive = 'hunter2'",
			flags:        tree.FmtSimple,
			wantContains: sub,
			wantExcludes: secret,
		},
		{
			name:          "placeholder interpolation is preempted",
			stmt:          "SET CLUSTER SETTING sql.testing.format_sensitive = $1",
			flags:         tree.FmtSimple,
			wantContains:  sub,
			wantExcludes:  secret,
			placeholderTo: secret,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.stmt)
			require.NoError(t, err)

			var opts []tree.FmtCtxOption
			if tc.placeholderTo != "" {
				opts = append(opts, tree.FmtPlaceholderFormat(
					func(ctx *tree.FmtCtx, p *tree.Placeholder) {
						// Simulate formatActiveQuery interpolating a bound value
						// back into the statement text; the substitution must win.
						ctx.WriteString(tc.placeholderTo)
					}))
			}

			ctx := tree.NewFmtCtx(tc.flags, opts...)
			ctx.FormatNode(stmt.AST)
			out := ctx.CloseAndGetString()

			if tc.wantContains != "" {
				require.Containsf(t, out, tc.wantContains, "output: %s", out)
			}
			if tc.wantExcludes != "" {
				require.Falsef(t, strings.Contains(out, tc.wantExcludes),
					"output %q must not contain %q", out, tc.wantExcludes)
			}
		})
	}
}
