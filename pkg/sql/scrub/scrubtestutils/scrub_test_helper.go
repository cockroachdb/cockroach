// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scrubtestutils

import (
	gosql "database/sql"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

// ExpectedScrubResult contains details about errors that are expected during
// SCRUB testing.
type ExpectedScrubResult struct {
	ErrorType    string
	Database     string
	Table        string
	PrimaryKey   string
	Repaired     bool
	DetailsRegex string
}

// CheckScrubResult compares the results from running SCRUB with the expected
// results and throws an error if they do not match.
func CheckScrubResult(t *testing.T, ress []sqlutils.ScrubResult, exps []ExpectedScrubResult) {
	t.Helper()

	for i := 0; i < len(exps); i++ {
		res := ress[i]
		exp := exps[i]
		if res.ErrorType != exp.ErrorType {
			t.Errorf("expected %q error, instead got: %s", exp.ErrorType, res.ErrorType)
		}

		if res.Database != exp.Database {
			t.Errorf("expected database %q, got %q", exp.Database, res.Database)
		}

		if res.Table != exp.Table {
			t.Errorf("expected table %q, got %q", exp.Table, res.Table)
		}

		if res.PrimaryKey != exp.PrimaryKey {
			t.Errorf("expected primary key %q, got %q", exp.PrimaryKey, res.PrimaryKey)
		}
		if res.Repaired != exp.Repaired {
			t.Fatalf("expected repaired %v, got %v", exp.Repaired, res.Repaired)
		}

		if matched, err := regexp.MatchString(exp.DetailsRegex, res.Details); err != nil {
			t.Fatal(err)
		} else if !matched {
			t.Errorf("expected error details to contain `%s`, got `%s`", exp.DetailsRegex, res.Details)
		}
	}
}

// RunScrub runs a SCRUB statement and checks that it returns exactly one scrub
// result and that it matches the expected result.
func RunScrub(t *testing.T, db *gosql.DB, scrubStmt string, exp []ExpectedScrubResult) {
	t.Helper()

	// Run SCRUB and find the violation created.
	rows, err := db.Query(scrubStmt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != len(exp) {
		t.Fatalf("expected %d results, got %d. got %#v", len(exp), len(results), results)
	}
	CheckScrubResult(t, results, exp)
}
