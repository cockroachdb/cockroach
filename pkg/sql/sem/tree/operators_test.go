// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// TestOperatorVolatilityMatchesPostgres checks that our defined operators match
// Postgres' operators for Volatility.
//
// Dump command below:
// COPY (
//   SELECT o.oprname, o.oprleft, o.oprright, o.oprresult, p.provolatile, p.proleakproof
//   FROM pg_operator AS o JOIN pg_proc AS p ON (o.oprcode = p.oid)
//   ORDER BY o.oprname, o.oprleft, o.oprright, o.oprresult
// ) TO STDOUT WITH CSV DELIMITER '|' HEADER;
func TestOperatorVolatilityMatchesPostgres(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	csvPath := filepath.Join("testdata", "pg_operator_provolatile_dump.csv")
	f, err := os.Open(csvPath)
	require.NoError(t, err)

	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '|'

	// Read header row
	_, err = reader.Read()
	require.NoError(t, err)

	type pgOp struct {
		name       string
		leftType   oid.Oid
		rightType  oid.Oid
		volatility Volatility
	}
	var pgOps []pgOp
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		// Columns are:
		//     0       1       2        3         4           5
		//     oprname oprleft oprright oprresult provolatile proleakproof
		require.NoError(t, err)
		require.Len(t, line, 6)

		name := line[0]

		leftOid, err := strconv.Atoi(line[1])
		require.NoError(t, err)

		rightOid, err := strconv.Atoi(line[2])
		require.NoError(t, err)

		provolatile := line[4]
		require.Len(t, provolatile, 1)
		proleakproof := line[5]
		require.Len(t, proleakproof, 1)

		v, err := VolatilityFromPostgres(provolatile, proleakproof[0] == 't')
		require.NoError(t, err)
		pgOps = append(pgOps, pgOp{
			name:       name,
			leftType:   oid.Oid(leftOid),
			rightType:  oid.Oid(rightOid),
			volatility: v,
		})
	}

	check := func(name string, leftType, rightType *types.T, volatility Volatility) {
		t.Helper()
		if volatility == 0 {
			t.Errorf("operator %s(%v,%v) has no volatility set", name, leftType, rightType)
			return
		}

		pgName := name
		// Postgres doesn't have separate operators for IS (NOT) DISTINCT FROM; remap
		// to equality.
		switch name {
		case IsDistinctFrom.String(), IsNotDistinctFrom.String():
			pgName = EQ.String()
		}

		var leftOid oid.Oid
		if leftType != nil {
			leftOid = leftType.Oid()
		}
		rightOid := rightType.Oid()
		for _, o := range pgOps {
			if o.name == pgName && o.leftType == leftOid && o.rightType == rightOid {
				if o.volatility != volatility {
					t.Errorf(
						"operator %s(%v,%v) has volatility %s, corresponding pg operator has %s",
						name, leftType, rightType, volatility, o.volatility,
					)
				}
				return
			}
		}
		if testing.Verbose() {
			t.Logf("operator %s(%v,%v) %d %d with volatility %s has no corresponding pg operator",
				name, leftType, rightType, leftOid, rightOid, volatility,
			)
		}
	}

	// Check unary ops. We don't just go through the map so we process them in
	// an orderly fashion.
	for op := UnaryOperatorSymbol(0); op < NumUnaryOperatorSymbols; op++ {
		for _, impl := range UnaryOps[op] {
			o := impl.(*UnaryOp)
			check(op.String(), nil /* leftType */, o.Typ, o.Volatility)
		}
	}

	// Check comparison ops.
	for op := ComparisonOperatorSymbol(0); op < NumComparisonOperatorSymbols; op++ {
		for _, impl := range CmpOps[op] {
			o := impl.(*CmpOp)
			check(op.String(), o.LeftType, o.RightType, o.Volatility)
		}
	}

	// Check binary ops.
	for op := BinaryOperatorSymbol(0); op < NumBinaryOperatorSymbols; op++ {
		for _, impl := range BinOps[op] {
			o := impl.(*BinOp)
			check(op.String(), o.LeftType, o.RightType, o.Volatility)
		}
	}
}
