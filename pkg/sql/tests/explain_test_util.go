// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
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
func GenerateAndCheckRedactedExplainsForPII(
	t *testing.T,
	smith *sqlsmith.Smither,
	num int,
	query func(sql string) (*gosql.Rows, error),
	containsPII func(explain, output string) error,
) {
	// Generate a few random statements.
	statements := make([]string, num)
	t.Log("generated statements:")
	for i := range statements {
		statements[i] = smith.Generate()
		t.Log(statements[i])
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
								rows, err := query(explain)
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
										t.Logf("encountered non-internal error: %s\n", err)
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
