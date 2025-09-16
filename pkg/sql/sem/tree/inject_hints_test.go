// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestInjectHints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name          string
		originalSQL   string
		donorSQL      string
		expectedSQL   string
		expectChanged bool
		expectError   bool
	}{
		{
			name:          "inject index hint on simple table",
			originalSQL:   "SELECT * FROM t",
			donorSQL:      "SELECT * FROM t@idx",
			expectedSQL:   "SELECT * FROM t@idx",
			expectChanged: true,
		},
		{
			name:          "inject index hint on aliased table",
			originalSQL:   "SELECT * FROM t AS t1",
			donorSQL:      "SELECT * FROM t@idx AS t1",
			expectedSQL:   "SELECT * FROM t@idx AS t1",
			expectChanged: true,
		},
		{
			name:          "inject multiple index hints",
			originalSQL:   "SELECT * FROM t1, t2",
			donorSQL:      "SELECT * FROM t1@idx1, t2@idx2",
			expectedSQL:   "SELECT * FROM t1@idx1, t2@idx2",
			expectChanged: true,
		},
		{
			name:          "inject join hint",
			originalSQL:   "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
			donorSQL:      "SELECT * FROM t1 INNER HASH JOIN t2 ON t1.id = t2.id",
			expectedSQL:   "SELECT * FROM t1 INNER HASH JOIN t2 ON t1.id = t2.id",
			expectChanged: true,
		},
		{
			name:          "inject both index and join hints",
			originalSQL:   "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
			donorSQL:      "SELECT * FROM t1@idx1 INNER HASH JOIN t2@idx2 ON t1.id = t2.id",
			expectedSQL:   "SELECT * FROM t1@idx1 INNER HASH JOIN t2@idx2 ON t1.id = t2.id",
			expectChanged: true,
		},
		{
			name:          "no hints to inject",
			originalSQL:   "SELECT * FROM t",
			donorSQL:      "SELECT * FROM t",
			expectedSQL:   "SELECT * FROM t",
			expectChanged: false,
		},
		{
			name:          "hints already present on table",
			originalSQL:   "SELECT * FROM t@idx",
			donorSQL:      "SELECT * FROM t@idx",
			expectedSQL:   "SELECT * FROM t@idx",
			expectChanged: true, // we still perform a rewrite, and still return true
		},
		{
			name:          "different constants should work",
			originalSQL:   "SELECT * FROM t WHERE id = 1",
			donorSQL:      "SELECT * FROM t@idx WHERE id = 2",
			expectedSQL:   "SELECT * FROM t@idx WHERE id = 1",
			expectChanged: true,
		},
		{
			name:        "structural mismatch should fail",
			originalSQL: "SELECT * FROM t1",
			donorSQL:    "SELECT * FROM t2",
			expectError: true,
		},
		{
			name:        "different number of tables should fail",
			originalSQL: "SELECT * FROM t1",
			donorSQL:    "SELECT * FROM t1, t2",
			expectError: true,
		},
		{
			name:          "complex query with subquery",
			originalSQL:   "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)",
			donorSQL:      "SELECT * FROM t1@idx WHERE id IN (SELECT id FROM t2@idx2)",
			expectedSQL:   "SELECT * FROM t1@idx WHERE id IN (SELECT id FROM t2@idx2)",
			expectChanged: true,
		},
		{
			name:          "multiple join types",
			originalSQL:   "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id INNER JOIN t3 ON t1.id = t3.id",
			donorSQL:      "SELECT * FROM t1@idx1 LEFT HASH JOIN t2@idx2 ON t1.id = t2.id INNER MERGE JOIN t3@idx3 ON t1.id = t3.id",
			expectedSQL:   "SELECT * FROM t1@idx1 LEFT HASH JOIN t2@idx2 ON t1.id = t2.id INNER MERGE JOIN t3@idx3 ON t1.id = t3.id",
			expectChanged: true,
		},
		{
			name:          "no table expressions",
			originalSQL:   "SELECT 1",
			donorSQL:      "SELECT 1",
			expectedSQL:   "SELECT 1",
			expectChanged: false,
		},
		{
			name:          "inject hint on table in subquery",
			originalSQL:   "SELECT * FROM (SELECT * FROM t) AS sub",
			donorSQL:      "SELECT * FROM (SELECT * FROM t@idx) AS sub",
			expectedSQL:   "SELECT * FROM (SELECT * FROM t@idx) AS sub",
			expectChanged: true,
		},
		{
			name:          "inject FORCE_INDEX hint",
			originalSQL:   "SELECT * FROM t",
			donorSQL:      "SELECT * FROM t@{FORCE_INDEX=idx}",
			expectedSQL:   "SELECT * FROM t@idx",
			expectChanged: true,
		},
		{
			name:          "inject hint with multiple items",
			originalSQL:   "SELECT * FROM t",
			donorSQL:      "SELECT * FROM t@{FORCE_INDEX=idx,DESC,NO_ZIGZAG_JOIN}",
			expectedSQL:   "SELECT * FROM t@{FORCE_INDEX=idx,DESC,NO_ZIGZAG_JOIN}",
			expectChanged: true,
		},
		{
			name:          "replace existing hint",
			originalSQL:   "SELECT * FROM t@old_idx",
			donorSQL:      "SELECT * FROM t@new_idx",
			expectedSQL:   "SELECT * FROM t@new_idx",
			expectChanged: true,
		},
		{
			name:          "hint unchanged by donor",
			originalSQL:   "SELECT * FROM t@idx",
			donorSQL:      "SELECT * FROM t",
			expectedSQL:   "SELECT * FROM t@idx",
			expectChanged: false,
		},
		{
			name:          "subquery in join condition",
			originalSQL:   "SELECT * FROM t JOIN u ON (SELECT v FROM v WHERE w = _ ORDER BY x LIMIT _)",
			donorSQL:      "SELECT * FROM t JOIN u ON (SELECT v FROM v@{AVOID_FULL_SCAN} WHERE w = _ ORDER BY x LIMIT _)",
			expectedSQL:   "SELECT * FROM t JOIN u ON (SELECT v FROM v@{AVOID_FULL_SCAN} WHERE w = _ ORDER BY x LIMIT _)",
			expectChanged: true,
		},
		{
			name:          "subquery in window partition",
			originalSQL:   "SELECT string_agg(a) OVER (PARTITION BY b ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING AND (SELECT d FROM t ORDER BY e LIMIT 1) FOLLOWING) FROM u GROUP BY f",
			donorSQL:      "SELECT string_agg(a) OVER (PARTITION BY b ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING AND (SELECT d FROM t@new_idx ORDER BY e LIMIT 1) FOLLOWING) FROM u GROUP BY f",
			expectedSQL:   "SELECT string_agg(a) OVER (PARTITION BY b ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING AND (SELECT d FROM t@new_idx ORDER BY e LIMIT 1) FOLLOWING) FROM u GROUP BY f",
			expectChanged: true,
		},
		{
			name:          "hint inside function argument",
			originalSQL:   "SELECT unnest((SELECT array_agg(y) FROM xy))",
			donorSQL:      "SELECT unnest((SELECT array_agg(y) FROM xy@foo))",
			expectedSQL:   "SELECT unnest((SELECT array_agg(y) FROM xy@foo))",
			expectChanged: true,
		},
		{
			name:        "failure to change join type",
			originalSQL: "SELECT * FROM a INNER JOIN b USING (c)",
			donorSQL:    "SELECT * FROM a LEFT LOOKUP JOIN b USING (c)",
			expectError: true,
		},
		{
			name:          "index hint with placeholders",
			originalSQL:   "SELECT * FROM a WHERE b = $1 ORDER BY c LIMIT $2",
			donorSQL:      "SELECT * FROM a@a_b_idx WHERE b = $1 ORDER BY c LIMIT $2",
			expectedSQL:   "SELECT * FROM a@a_b_idx WHERE b = $1 ORDER BY c LIMIT $2",
			expectChanged: true,
		},
	}

	st := cluster.MakeTestingClusterSettings()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse original statement.
			originalStmt, err := parser.ParseOne(tc.originalSQL)
			require.NoError(t, err)

			// Parse donor statement.
			donorStmt, err := parser.ParseOne(tc.donorSQL)
			require.NoError(t, err)

			// Create hint injection donor.
			donor, err := tree.NewHintInjectionDonor(donorStmt.AST, &st.SV)
			require.NoError(t, err)

			// Validate.
			if tc.expectError {
				require.Error(t, donor.Validate(originalStmt.AST, &st.SV))
				return
			} else {
				require.NoError(t, donor.Validate(originalStmt.AST, &st.SV))
			}

			// Inject hints.
			result, changed, err := donor.InjectHints(originalStmt.AST)
			require.NoError(t, err)
			require.Equal(t, tc.expectChanged, changed, "changed flag mismatch")

			// Check the result.
			resultSQL := tree.AsString(result)
			require.Equal(t, tc.expectedSQL, resultSQL, "resulting SQL mismatch")

			// Verify the statement is valid by parsing it again
			_, err = parser.ParseOne(resultSQL)
			require.NoError(t, err, "result SQL should be parseable")
		})
	}
}

func TestRandomInjectHints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "the test is too slow")
	skip.UnderRace(t, "the test is too slow")

	const numStatements = 500

	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	// Create a server to seed SQLSmith with DDL. We don't actually run anything
	// against the server in this test.
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	setup := sqlsmith.Setups["rand-tables"](rng)
	for _, setupSQL := range setup {
		if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
			// Ignore errors.
			continue
		}
		// Only log successful statements.
		t.Log(setupSQL + ";")
	}

	st := cluster.MakeTestingClusterSettings()

	// Set up smither to generate random DML statements with hints.
	smith, err := sqlsmith.NewSmither(sqlDB, rng,
		sqlsmith.OnlySingleDMLs(),
		sqlsmith.SimpleNames(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smith.Close()

	for range numStatements {
		randSQL := smith.Generate()
		t.Log(randSQL + ";")

		// Generate the statement fingerprint for the random statement.
		randStmt, err := parser.ParseOne(randSQL)
		require.NoError(t, err)
		fingerprintFlags := tree.FmtHideConstants | tree.FmtCollapseLists | tree.FmtConstantsAsUnderscores
		randFingerprint := tree.AsStringWithFlags(randStmt.AST, fingerprintFlags)

		// Parse random fingerprint to make the donor AST.
		donorStmt, err := parser.ParseOne(randFingerprint)
		require.NoError(t, err)
		donor, err := tree.NewHintInjectionDonor(donorStmt.AST, &st.SV)
		require.NoError(t, err)
		donorSQL := tree.AsString(donorStmt.AST)

		// Format the donor AST without hints, and parse again to make the target
		// statement fingerprint.
		targetSQL := tree.AsStringWithFlags(donorStmt.AST, tree.FmtHideHints)
		targetStmt, err := parser.ParseOne(targetSQL)
		require.NoError(t, err)

		// Validate.
		require.NoError(t, donor.Validate(targetStmt.AST, &st.SV))

		// Inject hints.
		result, changed, err := donor.InjectHints(targetStmt.AST)
		require.NoError(t, err)

		// Check that the rewritten statement matches the donor statement.
		if changed {
			resultSQL := tree.AsString(result)
			require.Equal(t, donorSQL, resultSQL, "resulting SQL mismatch")
		} else {
			require.Same(t, targetStmt.AST, result, "resulting statement was changed")
			require.Equal(t, donorSQL, targetSQL)
		}
	}
}
