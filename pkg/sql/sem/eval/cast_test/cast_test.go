// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cast_test

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type testCase struct {
	literal, typ string
}

// TestExplicitCastsMatchPostgres compares results of various explicit casts
// with results from Postgres. This is similar to TestEval/sql/cast but has
// looser error matching. To avoid starting a Postgres instance, expected
// results from Postgres are captured for all cases, which means we have to
// enumerate cases ahead of time instead of randomly generating them.
func TestExplicitCastsMatchPostgres(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// See explicit_casts_gen.sh for how the case file is generated.
	const caseFile = "explicit_casts.csv"
	const skipFile = "explicit_casts_skip.csv"

	toSkip := loadCasesToSkip(t, skipFile)

	csvForEach(t, caseFile, func(caseLineno int, line []string) {
		skipLineno, skip := toSkip[testCase{literal: line[0], typ: line[1]}]

		expr := fmt.Sprintf("(%s)::%s", line[0], line[1])
		sel := fmt.Sprintf("SELECT quote_nullable(%s)", expr)
		var result string
		if err := sqlDB.QueryRow(sel).Scan(&result); err != nil {
			result = "error"
			if line[2] != "error" && !skip {
				t.Errorf("%s:%d: unexpected error during %s: %v", caseFile, caseLineno, sel, err)
			}
		}
		if result != line[2] {
			if !skip {
				t.Errorf(
					"%s:%d: quote_nullable(%s) expected %s but was: %s",
					caseFile, caseLineno, expr, line[2], result,
				)
			}
		} else if skip {
			t.Errorf("%s:%d no longer need to skip %s", skipFile, skipLineno, expr)
		}
	})
}

// TestAssignmentCastsMatchPostgres compares results of various assignment casts
// with results from Postgres. To avoid starting a Postgres instance, expected
// results from Postgres are captured for all cases, which means we have to
// enumerate cases ahead of time instead of randomly generating them.
func TestAssignmentCastsMatchPostgres(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	defer func() {
		_, _ = sqlDB.Exec("DROP TABLE IF EXISTS assignment_casts")
	}()

	// See assignment_casts_gen.sh for how the case file is generated.
	const caseFile = "assignment_casts.csv"
	const skipFile = "assignment_casts_skip.csv"

	toSkip := loadCasesToSkip(t, skipFile)

	var colType string
	csvForEach(t, caseFile, func(caseLineno int, line []string) {
		skipLineno, skip := toSkip[testCase{literal: line[0], typ: line[1]}]

		if line[1] != colType {
			colType = line[1]
			drop := "DROP TABLE IF EXISTS assignment_casts"
			if _, err := sqlDB.Exec(drop); err != nil {
				t.Fatalf("%s:%d: error during %s: %v", caseFile, caseLineno, drop, err)
			}
			create := fmt.Sprintf(
				"CREATE TABLE assignment_casts ("+
					"lineno INT, "+
					"ins %s, "+
					"onc %s, "+
					"upd %s, "+
					"ups %s, "+
					"PRIMARY KEY (lineno))",
				colType, colType, colType, colType,
			)
			if _, err := sqlDB.Exec(create); err != nil {
				t.Fatalf("%s:%d: error during %s: %v", caseFile, caseLineno, create, err)
			}
		}

		ins := fmt.Sprintf(
			"INSERT INTO assignment_casts (lineno, ins) VALUES (%d, %s)",
			caseLineno, line[0],
		)
		onc := fmt.Sprintf(
			"INSERT INTO assignment_casts (lineno, onc) VALUES (%d, %s) "+
				"ON CONFLICT (lineno) DO UPDATE SET onc = excluded.onc",
			caseLineno, line[0],
		)
		upd := fmt.Sprintf(
			"UPDATE assignment_casts SET upd = %s WHERE lineno = %d",
			line[0], caseLineno,
		)
		ups := fmt.Sprintf(
			"UPSERT INTO assignment_casts (lineno, ups) VALUES (%d, %s)",
			caseLineno, line[0],
		)
		sel := fmt.Sprintf(
			"SELECT quote_nullable(ins), quote_nullable(onc), quote_nullable(upd), quote_nullable(ups) "+
				"FROM assignment_casts WHERE lineno = %d",
			caseLineno,
		)

		var errors [4]bool
		if _, err := sqlDB.Exec(ins); err != nil {
			errors[0] = true
			if line[2] != "error" && !skip {
				t.Errorf("%s:%d: unexpected error during %s: %v", caseFile, caseLineno, ins, err)
			}
			// Insert a row of NULLS so that additional modifications have a chance to
			// do something.
			ins2 := fmt.Sprintf("INSERT INTO assignment_casts (lineno) VALUES (%d)", caseLineno)
			if _, err := sqlDB.Exec(ins2); err != nil {
				t.Errorf("%s:%d: error during %s: %v", caseFile, caseLineno, ins2, err)
				return
			}
		}

		for i, stmt := range []string{onc, upd, ups} {
			if _, err := sqlDB.Exec(stmt); err != nil {
				errors[i+1] = true
				if line[2] != "error" && !skip {
					t.Errorf("%s:%d: unexpected error during %s: %v", caseFile, caseLineno, stmt, err)
				}
			}
		}

		row := sqlDB.QueryRow(sel)
		var results [4]string
		if err := row.Scan(&results[0], &results[1], &results[2], &results[3]); err != nil {
			t.Errorf("%s:%d: error during %s: %v", caseFile, caseLineno, sel, err)
			return
		}
		for i := range results {
			if errors[i] {
				results[i] = "error"
			}
		}
		expects := [4]string{line[2], line[2], line[2], line[2]}
		if !reflect.DeepEqual(results, expects) {
			if !skip {
				t.Errorf(
					"%s:%d: assignment casts of %s to %s expected %v but were: %v",
					caseFile, caseLineno, line[0], line[1], expects, results,
				)
			}
		} else if skip {
			t.Errorf("%s:%d no longer need to skip (%s)::%s", skipFile, skipLineno, line[0], line[1])
		}
	})
}

func loadCasesToSkip(t *testing.T, skipFile string) map[testCase]int {
	toSkip := make(map[testCase]int)
	csvForEach(t, skipFile, func(lineno int, line []string) {
		toSkip[testCase{literal: line[0], typ: line[1]}] = lineno
	})
	return toSkip
}

func csvForEach(t *testing.T, csvFile string, each func(lineno int, line []string)) {
	csvPath := datapathutils.TestDataPath(t, csvFile)
	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.Comment = '#'

	// Read header row
	if _, err = reader.Read(); err != nil {
		t.Fatal(err)
	}

	// Start lineno at 9 to account for the comment lines at the beginning of
	// the CSV files.
	for lineno := 9; ; lineno++ {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("%s:%d: could not read line: %v", csvFile, lineno, err)
			continue
		}
		each(lineno, line)
	}
}
