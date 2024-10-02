// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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
//
//	SELECT o.oprname, o.oprleft, o.oprright, o.oprresult, p.provolatile, p.proleakproof
//	FROM pg_operator AS o JOIN pg_proc AS p ON (o.oprcode = p.oid)
//	ORDER BY o.oprname, o.oprleft, o.oprright, o.oprresult
//
// ) TO STDOUT WITH CSV DELIMITER '|' HEADER;
func TestOperatorVolatilityMatchesPostgres(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	csvPath := datapathutils.TestDataPath(t, "pg_operator_provolatile_dump.csv")
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
		volatility volatility.V
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

		v, err := volatility.FromPostgres(provolatile, proleakproof[0] == 't')
		require.NoError(t, err)
		pgOps = append(pgOps, pgOp{
			name:       name,
			leftType:   oid.Oid(leftOid),
			rightType:  oid.Oid(rightOid),
			volatility: v,
		})
	}

	check := func(name string, leftType, rightType *types.T, volatility volatility.V) {
		t.Helper()
		if volatility == 0 {
			t.Errorf("operator %s(%v,%v) has no volatility set", name, leftType, rightType)
			return
		}

		pgName := name
		// Postgres doesn't have separate operators for IS (NOT) DISTINCT FROM; remap
		// to equality.
		switch name {
		case treecmp.IsDistinctFrom.String(), treecmp.IsNotDistinctFrom.String():
			pgName = treecmp.EQ.String()
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
		_ = UnaryOps[op].ForEachUnaryOp(func(o *UnaryOp) error {
			check(op.String(), nil /* leftType */, o.Typ, o.Volatility)
			return nil
		})
	}

	// Check comparison ops.
	for op := treecmp.ComparisonOperatorSymbol(0); op < treecmp.NumComparisonOperatorSymbols; op++ {
		_ = CmpOps[op].ForEachCmpOp(func(o *CmpOp) error {
			check(op.String(), o.LeftType, o.RightType, o.Volatility)
			return nil
		})
	}

	// Check binary ops.
	for op := treebin.BinaryOperatorSymbol(0); op < treebin.NumBinaryOperatorSymbols; op++ {
		_ = BinOps[op].ForEachBinOp(func(o *BinOp) error {
			check(op.String(), o.LeftType, o.RightType, o.Volatility)
			return nil
		})
	}
}
