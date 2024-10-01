// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestAggregateIgnoresDuplicates is a random test that attempts to prove that
// all operators for which opt.AggregateIgnoresDuplicates returns true actually
// ignore duplicates.
//
// The test operates as follows:
//
// For each test case's operator and type, we choose a random datum of that type
// and aggregate the datum a varying number of times. The result of each
// aggregation is inserted into a table. After all the aggregations have been
// performed for the same datum, the table should have only one distinct value.
// If there is more than one distinct value, then the aggregate does not ignore
// duplicates and should not be marked as such.
//
// For example, the SQL generated for a test case with the max operator and the
// INT type would look like:
//
//	CREATE TABLE results (r INT);
//	-- Randomly picked the datum: 145.
//	INSERT INTO results
//	  SELECT max(d) FROM (SELECT 145::INT FROM generate_series(1, 3)) g(d);
//	INSERT INTO results
//	  SELECT max(d) FROM (SELECT 145::INT FROM generate_series(1, 5)) g(d);
//	INSERT INTO results
//	  SELECT max(d) FROM (SELECT 145::INT FROM generate_series(1, 9)) g(d);
//	-- ...
//	-- The query below should return 1.
//	SELECT count(*) FROM (SELECT r FROM results GROUP BY r);
//
// This test will error if opt.AggregateIgnoresDuplicates returns true for any
// operator that is not included in at least one test case below. This is to
// ensure that the behavior of new operators which are assumed to ignore
// duplicates are tested. The exceptions to this are opt.AnyNotNullAggOp,
// opt.ConstAggOp, opt.ConstNotNullAggOp, and opt.FirstAggOp which are
// internal-only and do not have SQL equivalents so they cannot be tested in
// this way.
//
// TODO(mgartner): Tests other aggregate property functions like
// AggregateIgnoresNulls.
func TestAggregateIgnoresDuplicates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tDB := sqlutils.MakeSQLRunner(sqlDB)

	type testCase struct {
		op  opt.Operator
		typ *types.T
	}

	testCases := []testCase{
		// bit_and
		{op: opt.BitAndAggOp, typ: types.VarBit},
		{op: opt.BitAndAggOp, typ: types.MakeBit(1)},
		{op: opt.BitAndAggOp, typ: types.MakeBit(10)},
		// bit_or
		{op: opt.BitOrAggOp, typ: types.VarBit},
		{op: opt.BitOrAggOp, typ: types.MakeBit(1)},
		{op: opt.BitOrAggOp, typ: types.MakeBit(10)},
		// bool_and
		{op: opt.BoolAndOp, typ: types.Bool},
		// bool_or
		{op: opt.BoolOrOp, typ: types.Bool},
		// max
		{op: opt.MaxOp, typ: types.Int2},
		{op: opt.MaxOp, typ: types.Int4},
		{op: opt.MaxOp, typ: types.Int},
		{op: opt.MaxOp, typ: types.Float},
		{op: opt.MaxOp, typ: types.Decimal},
		{op: opt.MaxOp, typ: types.String},
		// min
		{op: opt.MinOp, typ: types.Int2},
		{op: opt.MinOp, typ: types.Int4},
		{op: opt.MinOp, typ: types.Int},
		{op: opt.MinOp, typ: types.Float},
		{op: opt.MinOp, typ: types.Decimal},
		{op: opt.MinOp, typ: types.String},
		// st_extent
		{op: opt.STExtentOp, typ: types.Geometry},
	}
	// Re-order test cases so that all cases that use the same type are
	// contiguous (to avoid recreating the table).
	sort.Slice(testCases, func(i, j int) bool {
		return testCases[i].typ.SQLString() < testCases[j].typ.SQLString()
	})

	// Ensure that a test case exists for each operator that
	// AggregateIgnoresDuplicates returns true for.
	for op := range opt.AggregateOpReverseMap {
		if !opt.AggregateIgnoresDuplicates(op) {
			continue
		}
		switch op {
		case opt.AnyNotNullAggOp, opt.ConstAggOp, opt.ConstNotNullAggOp, opt.FirstAggOp:
			// These operators are for internal use and don't have SQL
			// equivalents, so they cannot be tested with random inputs.
			continue
		}
		foundTestCase := false
		for _, tc := range testCases {
			if tc.op == op {
				foundTestCase = true
				break
			}
		}
		if !foundTestCase {
			t.Fatalf("test case required for %s operator", op.String())
		}
	}

	const (
		// numDatums is the number of random datums to test for each test case.
		numDatums = 5
		// numIters is the number of times to test each test case's aggregate
		// function and input with a random number of duplicate inputs.
		numIters = 10
	)
	rng, _ := randutil.NewTestRand()
	var prevTyp *types.T
	for _, tc := range testCases {
		sqlOp, ok := opt.AggregateOpReverseMap[tc.op]
		if !ok {
			t.Fatalf("%s is not an aggregate function", tc.op.String())
		}

		// Create the results table if the previous one used a different type.
		if prevTyp == nil || !prevTyp.Identical(tc.typ) {
			tDB.Exec(t, "DROP TABLE IF EXISTS results")
			tDB.Exec(t, fmt.Sprintf("CREATE TABLE results (r %s)", tc.typ.SQLString()))
			prevTyp = tc.typ
		}
		for i := 0; i < numDatums; i++ {
			// Clear the table.
			tDB.Exec(t, "DELETE FROM results WHERE true")

			// Generate a random datum.
			datum := randgen.RandDatum(rng, tc.typ, false /* nullOk */)
			fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
			datum.Format(fmtCtx)
			datumStr := fmtCtx.CloseAndGetString()

			for j := 0; j < numIters; j++ {
				// Generate and execute a query that aggregates a random number
				// of duplicate inputs and inserts the result into the results
				// table.
				numInputs := rng.Intn(100) + 1
				query := fmt.Sprintf(
					"INSERT INTO results SELECT %s(d) FROM (SELECT %s::%s FROM generate_series(1, %d)) g(d)",
					sqlOp, datumStr, tc.typ.SQLString(), numInputs)
				tDB.Exec(t, query)
			}
		}

		// Ensure that there is only one distinct result in the results
		// table.
		rows := tDB.Query(t, "SELECT count(*) FROM (SELECT r FROM results GROUP BY r)")
		if ok := rows.Next(); !ok {
			t.Fatalf("expected distinct count query to return a row")
		}
		var count int
		if err := rows.Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf(
				"expected %s operator to ignore duplicates, found %d distinct results",
				tc.op.String(), count,
			)
		}
	}
}
