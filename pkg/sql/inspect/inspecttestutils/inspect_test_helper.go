// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspecttestutils

import (
	gosql "database/sql"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

// ExpectedInspectResult contains details about errors that are expected during
// INSPECT testing.
type ExpectedInspectResult struct {
	ErrorType    string
	Database     string
	Table        string
	PrimaryKey   string
	Repaired     bool
	DetailsRegex string
}

// CheckInspectResult compares the results from running INSPECT with the expected
// results and throws an error if they do not match.
func CheckInspectResult(t *testing.T, ress []sqlutils.InspectResult, exps []ExpectedInspectResult) {
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

// RunInspect runs a INSPECT statement and checks that it returns exactly one inspect
// result and that it matches the expected result.
func RunInspect(t *testing.T, db *gosql.DB, inspectStmt string, exp []ExpectedInspectResult) {
	t.Helper()

	// Run INSPECT and find the violation created.
	rows, err := db.Query(inspectStmt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetInspectResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != len(exp) {
		t.Fatalf("expected %d results, got %d. got %#v", len(exp), len(results), results)
	}
	CheckInspectResult(t, results, exp)
}
