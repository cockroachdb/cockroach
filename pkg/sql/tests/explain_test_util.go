// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// GenerateAndCheckRedactedExplainsForPII generates num random statements and
// checks that the output of all variants of EXPLAIN (REDACT) on each random
// statement does not contain injected PII.
//
// The caller is expected to have set statement timeout on the connection.
func GenerateAndCheckRedactedExplainsForPII(
	t *testing.T,
	smith *sqlsmith.Smither,
	num int,
	conn *gosql.Conn,
	containsPII func(explain, output string) error,
) {
	ctx := context.Background()
	// We expect that the caller has set non-zero statement timeout - double
	// check that.
	rows, err := conn.QueryContext(ctx, "SHOW statement_timeout;")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var timeout int
		if err = rows.Scan(&timeout); err != nil {
			t.Fatal(err)
		}
		if timeout == 0 {
			t.Fatal("expected non-zero timeout")
		}
	}

	// Generate a few random statements.
	statements := make([]string, 0, num)
	t.Log("generated statements:")
	for len(statements) < num {
		stmt := smith.Generate()
		// Try vanilla EXPLAIN on this stmt to ensure that it is sound.
		rows, err := conn.QueryContext(ctx, "EXPLAIN "+stmt)
		if err != nil {
			// We shouldn't see any internal errors - ignore all others.
			if strings.Contains(err.Error(), "internal error") {
				t.Error(err)
			}
		} else {
			rows.Close()
			statements = append(statements, stmt)
			t.Log(stmt + ";")
		}
	}

	// Gather EXPLAIN variants to test.
	commands := []string{"EXPLAIN", "EXPLAIN ANALYZE"}

	modes := []string{"NO MODE"}
	for modeStr, mode := range tree.ExplainModes() {
		switch mode {
		case tree.ExplainDebug:
			// EXPLAIN ANALYZE (DEBUG, REDACT) is checked by TestExplainAnalyzeDebug/redact.
			continue
		}
		modes = append(modes, modeStr)
	}

	flags := []string{"NO FLAG"}
	for flagStr, flag := range tree.ExplainFlags() {
		switch flag {
		case tree.ExplainFlagRedact:
			// We add REDACT to each EXPLAIN below.
			continue
		}
		flags = append(flags, flagStr)
	}

	testName := func(s string) string {
		return strings.ReplaceAll(cases.Title(language.English).String(s), " ", "")
	}

	// Execute each EXPLAIN variant on each random statement, and look for PII.
	for _, cmd := range commands {
		t.Run(testName(cmd), func(t *testing.T) {
			for _, mode := range modes {
				t.Run(testName(mode), func(t *testing.T) {
					if mode == "NO MODE" {
						mode = ""
					} else {
						mode += ", "
					}
					for _, flag := range flags {
						t.Run(testName(flag), func(t *testing.T) {
							if flag == "NO FLAG" {
								flag = ""
							} else {
								flag += ", "
							}
							for _, stmt := range statements {
								explain := cmd + " (" + mode + flag + "REDACT) " + stmt
								rows, err := conn.QueryContext(ctx, explain)
								if err != nil {
									// There are many legitimate errors that could be returned
									// that don't indicate a PII leak or a test failure. For
									// example, EXPLAIN (OPT, JSON) is always a syntax error, or
									// EXPLAIN ANALYZE of a random query might timeout. To avoid
									// these false positives, we only fail on internal errors.
									msg := err.Error()
									if strings.Contains(msg, "internal error") {
										t.Error(err)
									} else if !strings.Contains(msg, "syntax error") {
										// Skip logging syntax errors, since they're expected to be
										// common and uninteresting.
										t.Logf("encountered non-internal error: %s\n%s\n\n", err, explain)
									}
									continue
								}
								var output strings.Builder
								for rows.Next() {
									var out string
									if err := rows.Scan(&out); err != nil {
										t.Fatal(err)
									}
									output.WriteString(out)
									output.WriteRune('\n')
								}
								if err := containsPII(explain, output.String()); err != nil {
									t.Error(err)
									continue
								}
								// TODO(michae2): When they are supported, also check HTML returned by
								// EXPLAIN (DISTSQL, REDACT) and EXPLAIN (OPT, ENV, REDACT).
							}
						})
					}
				})
			}
		})
	}
}
