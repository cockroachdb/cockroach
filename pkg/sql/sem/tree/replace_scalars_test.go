// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestReplaceScalarsWithPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		sql      string
		expected string
	}{{
		name:     "order_by_ordinal_with_select_constant",
		sql:      "SELECT x, 1 FROM t ORDER BY 1",
		expected: "SELECT x, $1 FROM t ORDER BY 1",
	}, {
		name:     "group_by_ordinal",
		sql:      "SELECT x, count(y) FROM t GROUP BY 1",
		expected: "SELECT x, count(y) FROM t GROUP BY 1",
	}, {
		name:     "distinct_on_ordinal",
		sql:      "SELECT DISTINCT ON (1) x, y FROM t ORDER BY 1",
		expected: "SELECT DISTINCT ON (1) x, y FROM t ORDER BY 1",
	}, {
		name:     "order_by_non_ordinal_expr",
		sql:      "SELECT x FROM t ORDER BY x + 1",
		expected: "SELECT x FROM t ORDER BY x + $1",
	}, {
		name:     "order_by_ordinal_with_where",
		sql:      "SELECT x FROM t WHERE x > 5 ORDER BY 1",
		expected: "SELECT x FROM t WHERE x > $1 ORDER BY 1",
	}, {
		name:     "group_by_and_order_by_ordinals",
		sql:      "SELECT x, count(y) FROM t WHERE x > 5 GROUP BY 1 ORDER BY 1",
		expected: "SELECT x, count(y) FROM t WHERE x > $1 GROUP BY 1 ORDER BY 1",
	}, {
		name:     "fractional_literal_cast_decimal",
		sql:      "SELECT x FROM t WHERE x < 4.0",
		expected: "SELECT x FROM t WHERE x < CAST($1 AS DECIMAL)",
	}, {
		name:     "integer_literal_bare_placeholder",
		sql:      "SELECT x FROM t WHERE x < 4",
		expected: "SELECT x FROM t WHERE x < $1",
	}, {
		name:     "limit_bare_placeholder",
		sql:      "SELECT x FROM t LIMIT 10",
		expected: "SELECT x FROM t LIMIT $1",
	}, {
		name:     "offset_bare_placeholder",
		sql:      "SELECT x FROM t OFFSET 5",
		expected: "SELECT x FROM t OFFSET $1",
	}, {
		name:     "limit_and_offset_bare_placeholder",
		sql:      "SELECT x FROM t LIMIT 10 OFFSET 5",
		expected: "SELECT x FROM t LIMIT $2 OFFSET $1",
	}, {
		name:     "limit_with_where_int_constant",
		sql:      "SELECT x FROM t WHERE x > 5 LIMIT 10",
		expected: "SELECT x FROM t WHERE x > $1 LIMIT $2",
	}, {
		name:     "string_literal_bare_placeholder",
		sql:      "SELECT x FROM t WHERE x = 'hello'",
		expected: "SELECT x FROM t WHERE x = $1",
	}, {
		name:     "order_by_fractional_not_replaced",
		sql:      "SELECT * FROM t ORDER BY 2.5",
		expected: "SELECT * FROM t ORDER BY 2.5",
	}, {
		name:     "order_by_string_not_replaced",
		sql:      "SELECT * FROM t ORDER BY 'a'",
		expected: "SELECT * FROM t ORDER BY 'a'",
	}, {
		name:     "bool_datum_replaced_in_where",
		sql:      "SELECT x FROM t WHERE true",
		expected: "SELECT x FROM t WHERE $1",
	}, {
		name:     "order_by_bool_datum_not_replaced",
		sql:      "SELECT * FROM t ORDER BY true",
		expected: "SELECT * FROM t ORDER BY true",
	}, {
		name:     "as_of_system_time_not_replaced",
		sql:      "SELECT x FROM t AS OF SYSTEM TIME 1",
		expected: "SELECT x FROM t AS OF SYSTEM TIME 1",
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parser.Parse(tc.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			newStmt, _ := tree.TestingReplaceScalarsWithPlaceholders(stmts[0].AST)
			result := newStmt.String()
			require.Equal(t, tc.expected, result)
		})
	}
}
