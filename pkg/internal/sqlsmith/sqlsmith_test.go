// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestRandTableInserts tests that valid INSERTS can be generated for the
// rand-tables setup.
//
// There is little visibility into the validity of statements created by
// sqlsmith. This makes it easy to unknowingly make a change to sqlsmith that
// results in many or all generated statements being invalid, reducing the
// efficacy of sqlsmith. sqlsmith is greatly hampered if all INSERTs fail,
// because queries will only run on empty tables. See #63190 for additional
// context.
//
// This test is meant to catch these types of regressions in sqlsmith or
// rand-tables. It creates 10 random tables, and performs up to 1000 inserts.
// The test fails if not a single row was inserted.
//
// If this test fails, there is likely a bug in:
//
//  1. sqlsmith that makes valid INSERTs impossible or very unlikely
//  2. Or rand-tables that makes it impossible or very unlikely to ever
//     generate a successful INSERT
//
// Note that there is a small but non-zero chance that this test produces a
// false-negative.
func TestRandTableInserts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rnd, _ := randutil.NewTestRand()
	setup := randTablesN(rnd, 10, "", false /* isMultiRegion */)
	for _, stmt := range setup {
		if _, err := sqlDB.Exec(stmt); err != nil {
			t.Log(stmt)
			t.Fatal(err)
		}
	}

	insertOnly := simpleOption("insert only", func(s *Smither) {
		s.stmtWeights = []statementWeight{
			{1, makeInsert},
		}
	})

	smither, err := NewSmither(sqlDB, rnd, insertOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	// Run up to numInserts INSERT statements.
	success := false
	numErrors := 0
	numZeroRowInserts := 0
	numInserts := 1000
	for i := 0; i < numInserts; i++ {
		stmt := smither.Generate()
		res, err := sqlDB.Exec(stmt)
		if err != nil {
			numErrors++
			continue
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			numZeroRowInserts++
			continue
		}
		success = true
		break
	}

	if !success {
		t.Errorf(
			"expected 1 success out of %v inserts, got %v errors and %v zero-row inserts",
			numInserts, numErrors, numZeroRowInserts,
		)
		t.Log(setup)
	}
}
