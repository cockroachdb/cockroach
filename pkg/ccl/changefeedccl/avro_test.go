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
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd"

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

func parseAvroSchema(j string) (*avroSchemaRecord, error) {
	var s avroSchemaRecord
	if err := json.Unmarshal([]byte(j), &s); err != nil {
		return nil, err
	}
	// This avroSchemaRecord doesn't have any of the derived fields we need for
	// serde. Instead of duplicating the logic, fake out a TableDescriptor, so
	// we can reuse tableToAvroSchema and get them for free.
	tableDesc := &sqlbase.TableDescriptor{
		Name: AvroNameToSQLName(s.Name),
	}
	for _, f := range s.Fields {
		// s.Fields[idx] has `Name` and `SchemaType` set but nonething else.
		// They're needed for serialization/deserialization, so fake out a
		// column descriptor so that we can reuse columnDescToAvroSchema to get
		// all the various fields of avroSchemaField populated for free.
		colDesc, err := avroSchemaToColDesc(AvroNameToSQLName(f.Name), f.SchemaType)
		if err != nil {
			return nil, err
		}
		tableDesc.Columns = append(tableDesc.Columns, *colDesc)
	}
	return tableToAvroSchema(tableDesc)
}

func avroSchemaToColDesc(
	name string, schemaType avroSchemaType,
) (*sqlbase.ColumnDescriptor, error) {
	colDesc := &sqlbase.ColumnDescriptor{Name: name}

	union, ok := schemaType.([]interface{})
	if ok {
		if len(union) == 2 && union[0] == avroSchemaNull {
			colDesc.Nullable = true
			schemaType = union[1]
		} else {
			return nil, errors.Errorf(`unsupported union: %v`, union)
		}
	}

	switch t := schemaType.(type) {
	case string:
		switch t {
		case avroSchemaLong:
			colDesc.Type = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
		case avroSchemaString:
			colDesc.Type = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}
		case avroSchemaBoolean:
			colDesc.Type = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}
		case avroSchemaBytes:
			colDesc.Type = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
		case avroSchemaDouble:
			colDesc.Type = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}
		default:
			return nil, errors.Errorf(`unknown schema type: %s`, t)
		}
	case map[string]interface{}:
		switch t[`logicalType`] {
		case `timestamp-micros`:
			colDesc.Type = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIMESTAMP}
		case `decimal`:
			colDesc.Type = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}
			if p, ok := t[`precision`]; ok {
				colDesc.Type.Precision = int32(p.(float64))
			}
			if s, ok := t[`scale`]; ok {
				colDesc.Type.Width = int32(s.(float64))
			}
		default:
			return nil, errors.Errorf(`unknown logical type: %s`, t[`logicalType`])
		}
	default:
		return nil, errors.Errorf(`unknown schema type: %T %s`, schemaType, schemaType)
	}
	return colDesc, nil
}

func TestAvroSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()

	type test struct {
		name   string
		schema string
		values string
	}
	tests := []test{
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
		switch typ.SemanticType {
		case sqlbase.ColumnType_DATE, sqlbase.ColumnType_INTERVAL, sqlbase.ColumnType_TIMESTAMPTZ,
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
		switch typ.SemanticType {
		case sqlbase.ColumnType_TIMESTAMP:
			// Truncate to millisecond instead of microsecond because of a bug
			// in the avro lib's deserialization code. The serialization seems
			// to be fine and we only use deserialization for testing, so we
			// should patch the bug but it's not currently affecting changefeed
			// correctness.
			t := datum.(*tree.DTimestamp).Time.Truncate(time.Millisecond)
			datum = tree.MakeDTimestamp(t, time.Microsecond)
		case sqlbase.ColumnType_DECIMAL:
			// TODO(dan): Make RandDatum respect Precision and Width instead.
			// TODO(dan): The precision is really meant to be in [1,10], but it
			// sure looks like there's an off by one error in the avro library
			// that makes this test flake if it picks precision of 1.
			typ.Precision = rng.Int31n(10) + 2
			typ.Width = rng.Int31n(typ.Precision + 1)
			coeff := rng.Int63n(int64(math.Pow10(int(typ.Precision))))
			datum = &tree.DDecimal{Decimal: *apd.New(coeff, -typ.Width)}
		}
		serializedDatum := tree.Serialize(datum)
		// schema is used in a fmt.Sprintf to fill in the table name, so we have
		// to escape any stray %s.
		escapedDatum := strings.Replace(serializedDatum, `%`, `%%`, -1)
		randTypeTest := test{
			name:   semTypeName,
			schema: fmt.Sprintf(`(a %s PRIMARY KEY)`, typ.SQLString()),
			values: fmt.Sprintf(`(%s)`, escapedDatum),
		}
		tests = append(tests, randTypeTest)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.schema))
			require.NoError(t, err)
			origSchema, err := tableToAvroSchema(tableDesc)
			require.NoError(t, err)
			jsonSchema := origSchema.codec.Schema()
			roundtrippedSchema, err := parseAvroSchema(jsonSchema)
			require.NoError(t, err)
			// It would require some work, but we could also check that the
			// roundtrippedSchema can be used to recreate the original `CREATE
			// TABLE`.

			rows, err := parseValues(tableDesc, `VALUES `+test.values)
			require.NoError(t, err)

			for _, row := range rows {
				serialized, err := origSchema.textualFromRow(row)
				require.NoError(t, err)
				roundtripped, err := roundtrippedSchema.rowFromTextual(serialized)
				require.NoError(t, err)
				require.Equal(t, row, roundtripped)

				serialized, err = origSchema.BinaryFromRow(nil, row)
				require.NoError(t, err)
				roundtripped, err = roundtrippedSchema.RowFromBinary(serialized)
				require.NoError(t, err)
				require.Equal(t, row, roundtripped)
			}
		})
	}

	t.Run("escaping", func(t *testing.T) {
		tableDesc, err := parseTableDesc(`CREATE TABLE "☃" (🍦 INT PRIMARY KEY)`)
		require.NoError(t, err)
		tableSchema, err := tableToAvroSchema(tableDesc)
		require.NoError(t, err)
		require.Equal(t,
			`{"type":"record","name":"_u2603_","fields":[{"type":"long","name":"_u0001f366_"}]}`,
			tableSchema.codec.Schema())
		indexSchema, err := indexToAvroSchema(tableDesc, &tableDesc.PrimaryIndex)
		require.NoError(t, err)
		require.Equal(t,
			`{"type":"record","name":"_u2603_","fields":[{"type":"long","name":"_u0001f366_"}]}`,
			indexSchema.codec.Schema())
	})
}

func (f *avroSchemaField) defaultValueNative() (interface{}, bool) {
	schemaType := f.SchemaType
	if union, ok := schemaType.([]avroSchemaType); ok {
		// "Default values for union fields correspond to the first schema in
		// the union."
		schemaType = union[0]
	}
	switch schemaType {
	case avroSchemaNull:
		return nil, true
	}
	panic(errors.Errorf(`unimplemented %T: %v`, schemaType, schemaType))
}

// rowFromBinaryEvolved decodes `buf` using writerSchema but evolves/resolves it
// to readerSchema using the rules from the avro spec:
// https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution
//
// It'd be nice if our avro library handled this for us, but neither of the
// popular golang once seem to have it implemented.
func rowFromBinaryEvolved(
	buf []byte, writerSchema, readerSchema *avroSchemaRecord,
) (sqlbase.EncDatumRow, error) {
	native, newBuf, err := writerSchema.codec.NativeFromBinary(buf)
	if err != nil {
		return nil, err
	}
	if len(newBuf) > 0 {
		return nil, errors.New(`only one row was expected`)
	}
	nativeMap, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf(`unknown avro native type: %T`, native)
	}
	adjustNative(nativeMap, writerSchema, readerSchema)
	return readerSchema.rowFromNative(nativeMap)
}

func adjustNative(native map[string]interface{}, writerSchema, readerSchema *avroSchemaRecord) {
	for _, writerField := range writerSchema.Fields {
		if _, inReader := readerSchema.fieldIdxByName[writerField.Name]; !inReader {
			// "If the writer's record contains a field with a name not present
			// in the reader's record, the writer's value for that field is
			// ignored."
			delete(native, writerField.Name)
		}
	}
	for _, readerField := range readerSchema.Fields {
		if _, inWriter := writerSchema.fieldIdxByName[readerField.Name]; !inWriter {
			// "If the reader's record schema has a field that contains a
			// default value, and writer's schema does not have a field with the
			// same name, then the reader should use the default value from its
			// field."
			if readerFieldDefault, ok := readerField.defaultValueNative(); ok {
				native[readerField.Name] = readerFieldDefault
			}
		}
	}
}

func TestAvroMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type test struct {
		name           string
		writerSchema   string
		writerValues   string
		readerSchema   string
		expectedValues string
	}
	tests := []test{
		{
			name:           `add_nullable`,
			writerSchema:   `(a INT PRIMARY KEY)`,
			writerValues:   `(1)`,
			readerSchema:   `(a INT PRIMARY KEY, b INT)`,
			expectedValues: `(1, NULL)`,
		},
		// TODO(dan): add a column with a default value
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writerDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.writerSchema))
			require.NoError(t, err)
			writerSchema, err := tableToAvroSchema(writerDesc)
			require.NoError(t, err)
			readerDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.readerSchema))
			require.NoError(t, err)
			readerSchema, err := tableToAvroSchema(readerDesc)
			require.NoError(t, err)

			writerRows, err := parseValues(writerDesc, `VALUES `+test.writerValues)
			require.NoError(t, err)
			expectedRows, err := parseValues(readerDesc, `VALUES `+test.expectedValues)
			require.NoError(t, err)

			for i := range writerRows {
				writerRow, expectedRow := writerRows[i], expectedRows[i]
				encoded, err := writerSchema.BinaryFromRow(nil, writerRow)
				require.NoError(t, err)
				row, err := rowFromBinaryEvolved(encoded, writerSchema, readerSchema)
				require.NoError(t, err)
				require.Equal(t, expectedRow, row)
			}
		})
	}
}

func TestDecimalRatRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run(`table`, func(t *testing.T) {
		tests := []struct {
			scale int32
			dec   *apd.Decimal
		}{
			{0, apd.New(0, 0)},
			{0, apd.New(1, 0)},
			{0, apd.New(-1, 0)},
			{0, apd.New(123, 0)},
			{1, apd.New(0, -1)},
			{1, apd.New(1, -1)},
			{1, apd.New(123, -1)},
			{5, apd.New(1, -5)},
		}
		for d, test := range tests {
			rat, err := decimalToRat(*test.dec, test.scale)
			require.NoError(t, err)
			roundtrip := ratToDecimal(rat, test.scale)
			if test.dec.CmpTotal(&roundtrip) != 0 {
				t.Errorf(`%d: %s != %s`, d, test.dec, &roundtrip)
			}
		}
	})
	t.Run(`error`, func(t *testing.T) {
		_, err := decimalToRat(*apd.New(1, -2), 1)
		require.EqualError(t, err, "0.01 will not roundtrip at scale 1")
		_, err = decimalToRat(*apd.New(1, -1), 2)
		require.EqualError(t, err, "0.1 will not roundtrip at scale 2")
		_, err = decimalToRat(apd.Decimal{Form: apd.Infinite}, 0)
		require.EqualError(t, err, "cannot convert Infinite form decimal")
	})
	t.Run(`rand`, func(t *testing.T) {
		rng, _ := randutil.NewPseudoRand()
		precision := rng.Int31n(10) + 1
		scale := rng.Int31n(precision + 1)
		coeff := rng.Int63n(int64(math.Pow10(int(precision))))
		dec := apd.New(coeff, -scale)
		rat, err := decimalToRat(*dec, scale)
		require.NoError(t, err)
		roundtrip := ratToDecimal(rat, scale)
		if dec.CmpTotal(&roundtrip) != 0 {
			t.Errorf(`%s != %s`, dec, &roundtrip)
		}
	})
}
