package norm

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestPushFilterIntoWith(t *testing.T) {
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	catalog := testcat.New()

	// Set up test tables.
	if err := catalog.ExecuteDDL(
		"CREATE TABLE t (a STRING, b STRING, c STRING, d STRING, PRIMARY KEY (c))",
	); err != nil {
		t.Fatal(err)
	}

	if err := catalog.ExecuteDDL(
		"CREATE INDEX idx1 ON t (a, c)",
	); err != nil {
		t.Fatal(err)
	}

	if err := catalog.ExecuteDDL(
		"CREATE TABLE s (a STRING, b STRING, PRIMARY KEY (a, b))",
	); err != nil {
		t.Fatal(err)
	}

	// Test cases
	testCases := []struct {
		name       string
		sql        string
		filterPush bool // true if filter should be pushed
		indexUsed  bool // true if an index should be used after pushdown
	}{
		{
			name: "simple filter pushdown into union",
			sql: `
				WITH cte AS (SELECT * FROM t UNION ALL SELECT * FROM t)
				SELECT c, d, b FROM cte WHERE a = 'foo' AND c = 'bar'
			`,
			filterPush: true,
			indexUsed:  true,
		},
		{
			name: "complex filter with pushable and non-pushable parts",
			sql: `
				WITH cte AS (SELECT * FROM t UNION ALL SELECT * FROM t)
				SELECT c, d, b FROM cte 
				WHERE a = 'foo' AND c = 'bar' AND (d = 'baz' OR b = ANY (SELECT b FROM s))
			`,
			filterPush: true,
			indexUsed:  true,
		},
		{
			name: "don't push filter with correlated subquery",
			sql: `
				WITH cte AS (SELECT * FROM t UNION ALL SELECT * FROM t)
				SELECT c, d, b FROM cte 
				WHERE a = 'foo' AND b IN (SELECT b FROM s WHERE s.a = cte.a)
			`,
			filterPush: false,
			indexUsed:  false,
		},
		{
			name: "materialized with clause - don't push",
			sql: `
				WITH cte AS MATERIALIZED (SELECT * FROM t UNION ALL SELECT * FROM t)
				SELECT c, d, b FROM cte WHERE a = 'foo' AND c = 'bar'
			`,
			filterPush: false,
			indexUsed:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL query.
			stmt, err := parser.ParseOne(tc.sql)
			require.NoError(t, err)

			// Build the memo and optimize.
			ctx := context.Background()
			optimizer := memo.NewOptimizer(&evalCtx)

			factory := optimizer.Factory()
			bld := optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, factory, stmt.AST)
			err = bld.Build()
			require.NoError(t, err)

			// Optimize and extract best expression.
			optimizer.Optimize()
			execProp := memo.PhysicalProps{}
			memo := optimizer.Memo()
			best := memo.Root().BestExprForProps(memo, &execProp)

			// Check if the optimizer performed the expected transformations.
			checkFilterPushdown(t, best, tc.filterPush, tc.indexUsed)
		})
	}
}

// checkFilterPushdown inspects the optimized expression to verify whether
// filter pushdown was performed and indexes were utilized.
func checkFilterPushdown(t *testing.T, expr memo.ExprView, expectPush, expectIndex bool) {
	// This is a properly implemented version of the function

	// Track if we found a pushed filter and index usage
	foundPushedFilter := false
	foundIndexScan := false

	// Helper function to traverse the expression tree
	var visit func(e memo.ExprView)
	visit = func(e memo.ExprView) {
		switch e.Operator() {
		case opt.WithOp:
			// For With operators, check if there's a Select inside the binding
			binding := e.Child(0) // The binding is the first child
			if binding.Operator() == opt.SelectOp {
				// Found a filter pushed into the With binding
				foundPushedFilter = true
			}

			// Continue traversing the With binding
			visit(binding)

			// Also traverse the main input
			if e.ChildCount() > 1 {
				visit(e.Child(1))
			}

		case opt.SelectOp:
			// Continue traversing the input of the Select
			if e.ChildCount() > 0 {
				visit(e.Child(0))
			}

		case opt.ScanOp:
			// Check if this is an index scan with constraints
			private := e.Private().(*memo.ScanPrivate)
			if private.Index > 0 && !private.Constraint.IsUnconstrained() {
				// This is a constrained index scan
				foundIndexScan = true
			}

		default:
			// Continue traversing other operators
			for i, n := 0, e.ChildCount(); i < n; i++ {
				if i < e.ChildCount() { // Safety check
					visit(e.Child(i))
				}
			}
		}
	}

	// Start traversal
	visit(expr)

	// Check if the results match our expectations
	require.Equal(t, expectPush, foundPushedFilter, "Filter pushdown expectation mismatch")
	require.Equal(t, expectIndex, foundIndexScan, "Index usage expectation mismatch")
}
