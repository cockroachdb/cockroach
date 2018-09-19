// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func parseTableDesc(createTableStmt string) (*sqlbase.TableDescriptor, error) {
	ctx := context.Background()
	stmt, err := parser.ParseOne(createTableStmt)
	if err != nil {
		return nil, errors.Wrapf(err, `parsing %s`, createTableStmt)
	}
	createTable, ok := stmt.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("expected *tree.CreateTable got %T", stmt)
	}
	st := cluster.MakeTestingClusterSettings()
	const parentID = sqlbase.ID(keys.MaxReservedDescID + 1)
	const tableID = sqlbase.ID(keys.MaxReservedDescID + 2)
	tableDesc, err := importccl.MakeSimpleTableDescriptor(
		ctx, st, createTable, parentID, tableID, importccl.NoFKs, hlc.UnixNano())
	if err != nil {
		return nil, err
	}
	return tableDesc, tableDesc.ValidateTable(st)
}

func parseValues(tableDesc *sqlbase.TableDescriptor, values string) ([]sqlbase.EncDatumRow, error) {
	semaCtx := &tree.SemaContext{}
	evalCtx := &tree.EvalContext{}

	valuesStmt, err := parser.ParseOne(values)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := valuesStmt.(*tree.Select)
	if !ok {
		return nil, errors.Errorf("expected *tree.Select got %T", valuesStmt)
	}
	valuesClause, ok := selectStmt.Select.(*tree.ValuesClause)
	if !ok {
		return nil, errors.Errorf("expected *tree.ValuesClause got %T", selectStmt.Select)
	}

	var rows []sqlbase.EncDatumRow
	for _, rowTuple := range valuesClause.Rows {
		var row sqlbase.EncDatumRow
		for colIdx, expr := range rowTuple {
			col := tableDesc.Columns[colIdx]
			typedExpr, err := sqlbase.SanitizeVarFreeExpr(
				expr, col.Type.ToDatumType(), "avro", semaCtx, evalCtx, false /* allowImpure */)
			if err != nil {
				return nil, err
			}
			datum, err := typedExpr.Eval(evalCtx)
			if err != nil {
				return nil, errors.Wrap(err, typedExpr.String())
			}
			row = append(row, sqlbase.DatumToEncDatum(col.Type, datum))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func TestAvroSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()

	type avroTest struct {
		name   string
		schema string
		values string
	}
	tests := []avroTest{
		{
			name:   `NULLABLE`,
			schema: `(a INT PRIMARY KEY, b INT NULL)`,
			values: `(1, 2), (3, NULL)`,
		},
		{
			name:   `TUPLE`,
			schema: `(a INT PRIMARY KEY, b STRING)`,
			values: `(1, 'a')`,
		},
	}
	// Generate a test for each column type with a random datum of that type.
	for semTypeID, semTypeName := range sqlbase.ColumnType_SemanticType_name {
		typ := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_SemanticType(semTypeID)}
		colType := semTypeName
		switch typ.SemanticType {
		case sqlbase.ColumnType_DECIMAL, sqlbase.ColumnType_DATE, sqlbase.ColumnType_TIMESTAMP,
			sqlbase.ColumnType_INTERVAL, sqlbase.ColumnType_TIMESTAMPTZ,
			sqlbase.ColumnType_COLLATEDSTRING, sqlbase.ColumnType_NAME, sqlbase.ColumnType_OID,
			sqlbase.ColumnType_UUID, sqlbase.ColumnType_ARRAY, sqlbase.ColumnType_INET,
			sqlbase.ColumnType_TIME, sqlbase.ColumnType_JSONB, sqlbase.ColumnType_BIT,
			sqlbase.ColumnType_TUPLE:
			continue
			// TODO(dan): Implement these.
		}
		datum := sqlbase.RandDatum(rng, typ, false /* nullOk */)
		if datum == tree.DNull {
			// DNull is returned by RandDatum for ColumnType_NULL or if the
			// column type is unimplemented in RandDatum. In either case, the
			// correct thing to do is skip this one.
			continue
		}
		serializedDatum := tree.Serialize(datum)
		// schema is used in a fmt.Sprintf to fill in the table name, so we have
		// to escape any stray %s.
		escapedDatum := strings.Replace(serializedDatum, `%`, `%%`, -1)
		test := avroTest{
			name:   semTypeName,
			schema: fmt.Sprintf(`(a %s PRIMARY KEY)`, colType),
			values: fmt.Sprintf(`(%s)`, escapedDatum),
		}
		tests = append(tests, test)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.schema))
			require.NoError(t, err)
			tableSchema, err := tableToAvroSchema(tableDesc)
			require.NoError(t, err)

			rows, err := parseValues(tableDesc, `VALUES `+test.values)
			require.NoError(t, err)

			for _, row := range rows {
				serialized, err := tableSchema.TextualFromRow(row)
				require.NoError(t, err)
				roundtripped, err := tableSchema.RowFromTextual(serialized)
				require.NoError(t, err)
				require.Equal(t, row, roundtripped)

				serialized, err = tableSchema.BinaryFromRow(nil, row)
				require.NoError(t, err)
				roundtripped, err = tableSchema.RowFromBinary(serialized)
				require.NoError(t, err)
				require.Equal(t, row, roundtripped)
			}
		})
	}
}
