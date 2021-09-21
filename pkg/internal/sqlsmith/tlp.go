// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// GenerateTLP returns two SQL queries as strings that can be used for Ternary
// Logic Partitioning (TLP). TLP is a method for logically testing DBMSs which
// is based on the logical guarantee that for a given predicate p, all rows must
// satisfy exactly one of the following three predicates: p, NOT p, p IS NULL.
// TLP can find bugs when an unpartitioned query and a query partitioned into
// three sub-queries do not yield the same results.
//
// More information on TLP: https://www.manuelrigger.at/preprints/TLP.pdf.
//
// We currently implement a limited form of TLP that can only verify that the
// number of rows returned by the unpartitioned and the partitioned queries are
// equal.
//
// This TLP implementation is also limited in the types of queries that are
// tested. We currently only test basic WHERE and JOIN query filters. It is
// possible to use TLP to test aggregations, GROUP BY, and HAVING, which have
// all been implemented in SQLancer. See:
// https://github.com/sqlancer/sqlancer/tree/1.1.0/src/sqlancer/cockroachdb/oracle/tlp.
func (s *Smither) GenerateTLP() (unpartitioned, partitioned string) {
	// Set disableImpureFns to true so that generated predicates are immutable.
	originalDisableImpureFns := s.disableImpureFns
	s.disableImpureFns = true
	defer func() {
		s.disableImpureFns = originalDisableImpureFns
	}()

	switch tlpType := rand.Intn(3); tlpType {
	case 0:
		return s.generateWhereTLP()
	case 1:
		return s.generateOuterJoinTLP()
	default:
		return s.generateInnerJoinTLP()
	}
}

// generateWhereTLP returns two SQL queries as strings that can be used by the
// GenerateTLP function. These queries make use of the WHERE clause to partition
// the original query into three.

// The first query returned is an unpartitioned query of the form:
//
//   SELECT count(*) FROM table
//
// The second query returned is a partitioned query of the form:
//
//   SELECT count(*) FROM (
//     SELECT * FROM table WHERE (p)
//     UNION ALL
//     SELECT * FROM table WHERE NOT (p)
//     UNION ALL
//     SELECT * FROM table WHERE (p) IS NULL
//   )
//
// If the resulting counts of the two queries are not equal, there is a logical
// bug.
func (s *Smither) generateWhereTLP() (unpartitioned, partitioned string) {
	f := tree.NewFmtCtx(tree.FmtParsable)

	table, _, _, cols, ok := s.getSchemaTable()
	if !ok {
		panic(errors.AssertionFailedf("failed to find random table"))
	}
	table.Format(f)
	tableName := f.CloseAndGetString()

	unpartitioned = fmt.Sprintf("SELECT count(*) FROM %s", tableName)

	pred := makeBoolExpr(s, cols)
	pred.Format(f)
	predicate := f.CloseAndGetString()

	part1 := fmt.Sprintf("SELECT * FROM %s WHERE %s", tableName, predicate)
	part2 := fmt.Sprintf("SELECT * FROM %s WHERE NOT (%s)", tableName, predicate)
	part3 := fmt.Sprintf("SELECT * FROM %s WHERE (%s) IS NULL", tableName, predicate)

	partitioned = fmt.Sprintf(
		"SELECT count(*) FROM (%s UNION ALL %s UNION ALL %s)",
		part1, part2, part3,
	)

	return unpartitioned, partitioned
}

// generateOuterJoinTLP returns two SQL queries as strings that can be used by the
// GenerateTLP function. These queries make use of LEFT JOIN to partition the
// original query in two ways. The latter query is partitioned by a predicate p,
// while the former is not.

// The first query returned is an unpartitioned query of the form:
//
//   SELECT count(*) FROM (
//     SELECT * FROM table1 LEFT JOIN table2 ON TRUE
//     UNION ALL
//     SELECT * FROM table1 LEFT JOIN table2 ON FALSE
//     UNION ALL
//     SELECT * FROM table1 LEFT JOIN table2 ON FALSE
//   )
//
// The second query returned is a partitioned query of the form:
//
//   SELECT count(*) FROM (
//     SELECT * FROM table1 LEFT JOIN table2 ON (p)
//     UNION ALL
//     SELECT * FROM table1 LEFT JOIN table2 ON NOT (p)
//     UNION ALL
//     SELECT * FROM table1 LEFT JOIN table2 ON (p) IS NULL
//   )
//
// From the first query, we have a CROSS JOIN of the two tables (JOIN ON TRUE)
// and then all rows concatenated with NULL values for the second and third
// parts (JOIN ON FALSE). Recall our TLP logical guarantee that a given
// predicate p always evaluates to either TRUE, FALSE, or NULL. It follows that
// for any row in table1, exactly one of the expressions (p), NOT (p), or (p) is
// NULL will resolve to TRUE. For a given row, when the expression resolves to
// TRUE in table1, it matches with every row in table2. Otherwise, it is
// concatenated with null values. So each row in table1 is matched with every
// row in table2 exactly once (CROSS JOIN) and also matched with NULL values
// exactly twice, as expected by the unpartitioned query.
//
// Note that this implementation is restricted in that it only uses columns from
// the left table in the predicate p.

// If the resulting counts of the two queries are not equal, there is a logical
// bug.
func (s *Smither) generateOuterJoinTLP() (unpartitioned, partitioned string) {
	f := tree.NewFmtCtx(tree.FmtParsable)

	table1, _, _, cols1, ok1 := s.getSchemaTable()
	table2, _, _, _, ok2 := s.getSchemaTable()
	if !ok1 || !ok2 {
		panic(errors.AssertionFailedf("failed to find random tables"))
	}
	table1.Format(f)
	tableName1 := f.CloseAndGetString()
	table2.Format(f)
	tableName2 := f.CloseAndGetString()

	leftJoinTrue := fmt.Sprintf(
		"SELECT * FROM %s LEFT JOIN %s ON TRUE",
		tableName1, tableName2,
	)
	leftJoinFalse := fmt.Sprintf(
		"SELECT * FROM %s LEFT JOIN %s ON FALSE",
		tableName1, tableName2,
	)

	unpartitioned = fmt.Sprintf(
		"SELECT count(*) FROM (%s UNION ALL %s UNION ALL %s)",
		leftJoinTrue, leftJoinFalse, leftJoinFalse,
	)

	pred := makeBoolExpr(s, cols1)
	pred.Format(f)
	predicate := f.CloseAndGetString()

	part1 := fmt.Sprintf(
		"SELECT * FROM %s LEFT JOIN %s ON %s",
		tableName1, tableName2, predicate,
	)
	part2 := fmt.Sprintf(
		"SELECT * FROM %s LEFT JOIN %s ON NOT (%s)",
		tableName1, tableName2, predicate,
	)
	part3 := fmt.Sprintf(
		"SELECT * FROM %s LEFT JOIN %s ON (%s) IS NULL",
		tableName1, tableName2, predicate,
	)

	partitioned = fmt.Sprintf(
		"SELECT count(*) FROM (%s UNION ALL %s UNION ALL %s)",
		part1, part2, part3,
	)

	return unpartitioned, partitioned
}

// generateInnerJoinTLP returns two SQL queries as strings that can be used by
// the GenerateTLP function. These queries make use of INNER JOIN to partition
// the original query in two ways. The latter query is partitioned by a
// predicate p, while the former is not.

// The first query returned is an unpartitioned query of the form:
//
//   SELECT count(*) FROM table1 JOIN table2 ON TRUE
//
// The second query returned is a partitioned query of the form:
//
//   SELECT count(*) FROM (
//     SELECT * FROM table1 JOIN table2 ON (p)
//     UNION ALL
//     SELECT * FROM table1 JOIN table2 ON NOT (p)
//     UNION ALL
//     SELECT * FROM table1 JOIN table2 ON (p) IS NULL
//   )
//
// From the first query, we have a CROSS JOIN of the two tables (JOIN ON TRUE).
// Recall our TLP logical guarantee that a given predicate p always evaluates to
// either TRUE, FALSE, or NULL. It follows that for any row returned by the
// first query, exactly one of the expressions (p), NOT (p), or (p) is NULL will
// resolve to TRUE. So the partitioned query accounts for each row in the
// CROSS JOIN exactly once.
//
// If the resulting counts of the two queries are not equal, there is a logical
// bug.
func (s *Smither) generateInnerJoinTLP() (unpartitioned, partitioned string) {
	f := tree.NewFmtCtx(tree.FmtParsable)

	table1, _, _, cols1, ok1 := s.getSchemaTable()
	table2, _, _, cols2, ok2 := s.getSchemaTable()
	if !ok1 || !ok2 {
		panic(errors.AssertionFailedf("failed to find random tables"))
	}
	table1.Format(f)
	tableName1 := f.CloseAndGetString()
	table2.Format(f)
	tableName2 := f.CloseAndGetString()

	unpartitioned = fmt.Sprintf(
		"SELECT count(*) FROM %s JOIN %s ON true",
		tableName1, tableName2,
	)

	cols := cols1.extend(cols2...)
	pred := makeBoolExpr(s, cols)
	pred.Format(f)
	predicate := f.CloseAndGetString()

	part1 := fmt.Sprintf(
		"SELECT * FROM %s JOIN %s ON %s",
		tableName1, tableName2, predicate,
	)
	part2 := fmt.Sprintf(
		"SELECT * FROM %s JOIN %s ON NOT (%s)",
		tableName1, tableName2, predicate,
	)
	part3 := fmt.Sprintf(
		"SELECT * FROM %s JOIN %s ON (%s) IS NULL",
		tableName1, tableName2, predicate,
	)

	partitioned = fmt.Sprintf(
		"SELECT count(*) FROM (%s UNION ALL %s UNION ALL %s)",
		part1, part2, part3,
	)

	return unpartitioned, partitioned
}
