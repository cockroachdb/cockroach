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
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func parseTableDesc(createTableStmt string) (*sqlbase.TableDescriptor, error) {
	ctx := context.Background()
	stmt, err := parser.ParseOne(createTableStmt)
	if err != nil {
		return nil, errors.Wrapf(err, `parsing %s`, createTableStmt)
	}
	createTable, ok := stmt.AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("expected *tree.CreateTable got %T", stmt)
	}
	st := cluster.MakeTestingClusterSettings()
	const parentID = sqlbase.ID(keys.MaxReservedDescID + 1)
	const tableID = sqlbase.ID(keys.MaxReservedDescID + 2)
	semaCtx := tree.MakeSemaContext()
	mutDesc, err := importccl.MakeSimpleTableDescriptor(
		ctx, &semaCtx, st, createTable, parentID, tableID, importccl.NoFKs, hlc.UnixNano())
	if err != nil {
		return nil, err
	}
	return mutDesc.TableDesc(), mutDesc.TableDesc().ValidateTable()
}

func parseValues(tableDesc *sqlbase.TableDescriptor, values string) ([]sqlbase.EncDatumRow, error) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	evalCtx := &tree.EvalContext{}

	valuesStmt, err := parser.ParseOne(values)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := valuesStmt.AST.(*tree.Select)
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
			col := &tableDesc.Columns[colIdx]
			typedExpr, err := sqlbase.SanitizeVarFreeExpr(
				ctx, expr, col.Type, "avro", &semaCtx, tree.VolatilityStable)
			if err != nil {
				return nil, err
			}
			datum, err := typedExpr.Eval(evalCtx)
			if err != nil {
				return nil, errors.Wrapf(err, "evaluating %s", typedExpr)
			}
			row = append(row, sqlbase.DatumToEncDatum(col.Type, datum))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseAvroSchema(j string) (*avroDataRecord, error) {
	var s avroDataRecord
	if err := json.Unmarshal([]byte(j), &s); err != nil {
		return nil, err
	}
	// This avroDataRecord doesn't have any of the derived fields we need for
	// serde. Instead of duplicating the logic, fake out a TableDescriptor, so
	// we can reuse tableToAvroSchema and get them for free.
	tableDesc := &sqlbase.TableDescriptor{
		Name: AvroNameToSQLName(s.Name),
	}
	for _, f := range s.Fields {
		// s.Fields[idx] has `Name` and `SchemaType` set but nothing else.
		// They're needed for serialization/deserialization, so fake out a
		// column descriptor so that we can reuse columnDescToAvroSchema to get
		// all the various fields of avroSchemaField populated for free.
		colDesc, err := avroFieldMetadataToColDesc(f.Metadata)
		if err != nil {
			return nil, err
		}
		tableDesc.Columns = append(tableDesc.Columns, *colDesc)
	}
	return tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
}

func avroFieldMetadataToColDesc(metadata string) (*sqlbase.ColumnDescriptor, error) {
	parsed, err := parser.ParseOne(`ALTER TABLE FOO ADD COLUMN ` + metadata)
	if err != nil {
		return nil, err
	}
	def := parsed.AST.(*tree.AlterTable).Cmds[0].(*tree.AlterTableAddColumn).ColumnDef
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	col, _, _, err := sqlbase.MakeColumnDefDescs(ctx, def, &semaCtx, &tree.EvalContext{})
	return col, err
}

// randTime generates a random time.Time whose .UnixNano result doesn't
// overflow an int64.
func randTime(rng *rand.Rand) time.Time {
	return timeutil.Unix(0, rng.Int63())
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
		{
			name:   `MULTI_WIDTHS`,
			schema: `(a INT PRIMARY KEY, b DECIMAL (3,2), c DECIMAL (2, 1))`,
			values: `(1, 1.23, 4.5)`,
		},
	}
	// Generate a test for each column type with a random datum of that type.
	for _, typ := range types.OidToType {
		switch typ.Family() {
		case types.AnyFamily, types.OidFamily, types.TupleFamily:
			// These aren't expected to be needed for changefeeds.
			continue
		case types.IntervalFamily, types.ArrayFamily, types.BitFamily,
			types.CollatedStringFamily:
			// Implement these as customer demand dictates.
			continue
		}
		datum := sqlbase.RandDatum(rng, typ, false /* nullOk */)
		if datum == tree.DNull {
			// DNull is returned by RandDatum for types.UNKNOWN or if the
			// column type is unimplemented in RandDatum. In either case, the
			// correct thing to do is skip this one.
			continue
		}
		switch typ.Family() {
		case types.TimestampFamily:
			// Truncate to millisecond instead of microsecond because of a bug
			// in the avro lib's deserialization code. The serialization seems
			// to be fine and we only use deserialization for testing, so we
			// should patch the bug but it's not currently affecting changefeed
			// correctness.
			// TODO(mjibson): goavro mishandles timestamps
			// whose nanosecond representation overflows an
			// int64, so restrict input to fit.
			t := randTime(rng).Truncate(time.Millisecond)
			datum = tree.MustMakeDTimestamp(t, time.Microsecond)
		case types.TimestampTZFamily:
			// See comments above for TimestampFamily.
			t := randTime(rng).Truncate(time.Millisecond)
			datum = tree.MustMakeDTimestampTZ(t, time.Microsecond)
		case types.DecimalFamily:
			// TODO(dan): Make RandDatum respect Precision and Width instead.
			// TODO(dan): The precision is really meant to be in [1,10], but it
			// sure looks like there's an off by one error in the avro library
			// that makes this test flake if it picks precision of 1.
			precision := rng.Int31n(10) + 2
			scale := rng.Int31n(precision + 1)
			typ = types.MakeDecimal(precision, scale)
			coeff := rng.Int63n(int64(math.Pow10(int(precision))))
			datum = &tree.DDecimal{Decimal: *apd.New(coeff, -scale)}
		case types.DateFamily:
			// TODO(mjibson): goavro mishandles dates whose
			// nanosecond representation overflows an int64,
			// so restrict input to fit.
			var err error
			datum, err = tree.NewDDateFromTime(randTime(rng))
			if err != nil {
				panic(err)
			}
		}
		serializedDatum := tree.Serialize(datum)
		// name can be "char" (with quotes), so needs to be escaped.
		escapedName := fmt.Sprintf("%s_table", strings.Replace(typ.String(), "\"", "", -1))
		// schema is used in a fmt.Sprintf to fill in the table name, so we have
		// to escape any stray %s.
		escapedDatum := strings.Replace(serializedDatum, `%`, `%%`, -1)
		randTypeTest := test{
			name:   escapedName,
			schema: fmt.Sprintf(`(a INT PRIMARY KEY, b %s)`, typ.SQLString()),
			values: fmt.Sprintf(`(1, %s)`, escapedDatum),
		}
		tests = append(tests, randTypeTest)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.schema))
			require.NoError(t, err)
			origSchema, err := tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
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
				evalCtx := &tree.EvalContext{SessionData: &sessiondata.SessionData{}}
				serialized, err := origSchema.textualFromRow(row)
				require.NoError(t, err)
				roundtripped, err := roundtrippedSchema.rowFromTextual(serialized)
				require.NoError(t, err)
				require.Equal(t, 0, row[1].Datum.Compare(evalCtx, roundtripped[1].Datum),
					`%s != %s`, row[1].Datum, roundtripped[1].Datum)

				serialized, err = origSchema.BinaryFromRow(nil, row)
				require.NoError(t, err)
				roundtripped, err = roundtrippedSchema.RowFromBinary(serialized)
				require.NoError(t, err)
				require.Equal(t, 0, row[1].Datum.Compare(evalCtx, roundtripped[1].Datum),
					`%s != %s`, row[1].Datum, roundtripped[1].Datum)
			}
		})
	}

	t.Run("escaping", func(t *testing.T) {
		tableDesc, err := parseTableDesc(`CREATE TABLE "☃" (🍦 INT PRIMARY KEY)`)
		require.NoError(t, err)
		tableSchema, err := tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
		require.NoError(t, err)
		require.Equal(t,
			`{"type":"record","name":"_u2603_","fields":[`+
				`{"type":["null","long"],"name":"_u0001f366_","default":null,`+
				`"__crdb__":"🍦 INT8 NOT NULL"}]}`,
			tableSchema.codec.Schema())
		indexSchema, err := indexToAvroSchema(tableDesc, &tableDesc.PrimaryIndex)
		require.NoError(t, err)
		require.Equal(t,
			`{"type":"record","name":"_u2603_","fields":[`+
				`{"type":["null","long"],"name":"_u0001f366_","default":null,`+
				`"__crdb__":"🍦 INT8 NOT NULL"}]}`,
			indexSchema.codec.Schema())
	})

	// This test shows what avro schema each sql column maps to, for easy
	// reference.
	t.Run("type_goldens", func(t *testing.T) {
		goldens := map[string]string{
			`BOOL`:         `["null","boolean"]`,
			`BYTES`:        `["null","bytes"]`,
			`DATE`:         `["null",{"type":"int","logicalType":"date"}]`,
			`FLOAT8`:       `["null","double"]`,
			`GEOGRAPHY`:    `["null","bytes"]`,
			`GEOMETRY`:     `["null","bytes"]`,
			`INET`:         `["null","string"]`,
			`INT8`:         `["null","long"]`,
			`JSONB`:        `["null","string"]`,
			`STRING`:       `["null","string"]`,
			`TIME`:         `["null",{"type":"long","logicalType":"time-micros"}]`,
			`TIMETZ`:       `["null","string"]`,
			`TIMESTAMP`:    `["null",{"type":"long","logicalType":"timestamp-micros"}]`,
			`TIMESTAMPTZ`:  `["null",{"type":"long","logicalType":"timestamp-micros"}]`,
			`UUID`:         `["null","string"]`,
			`DECIMAL(3,2)`: `["null",{"type":"bytes","logicalType":"decimal","precision":3,"scale":2}]`,
		}

		for _, typ := range types.Scalar {
			switch typ.Family() {
			case types.IntervalFamily, types.OidFamily, types.BitFamily:
				continue
			case types.DecimalFamily:
				typ = types.MakeDecimal(3, 2)
			}

			colType := typ.SQLString()
			tableDesc, err := parseTableDesc(`CREATE TABLE foo (pk INT PRIMARY KEY, a ` + colType + `)`)
			require.NoError(t, err)
			field, err := columnDescToAvroSchema(&tableDesc.Columns[1])
			require.NoError(t, err)
			schema, err := json.Marshal(field.SchemaType)
			require.NoError(t, err)
			require.Equal(t, goldens[colType], string(schema), `SQL type %s`, colType)

			// Delete from goldens for the following assertion that we don't have any
			// unexpectedly unused goldens.
			delete(goldens, colType)
		}
		if len(goldens) > 0 {
			t.Fatalf("expected all goldens to be consumed: %v", goldens)
		}
	})

	// This test shows what avro value some sql datums map to, for easy reference.
	// The avro golden strings are in the textual format defined in the spec.
	t.Run("value_goldens", func(t *testing.T) {
		goldens := []struct {
			sqlType string
			sql     string
			avro    string
		}{
			{sqlType: `INT`, sql: `NULL`, avro: `null`},
			{sqlType: `INT`,
				sql:  `1`,
				avro: `{"long":1}`},

			{sqlType: `BOOL`, sql: `NULL`, avro: `null`},
			{sqlType: `BOOL`,
				sql:  `true`,
				avro: `{"boolean":true}`},

			{sqlType: `FLOAT`, sql: `NULL`, avro: `null`},
			{sqlType: `FLOAT`,
				sql:  `1.2`,
				avro: `{"double":1.2}`},

			{sqlType: `GEOGRAPHY`, sql: `NULL`, avro: `null`},
			{sqlType: `GEOGRAPHY`,
				sql:  "'POINT(1.0 1.0)'",
				avro: `{"bytes":"\u0001\u0001\u0000\u0000 \u00E6\u0010\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u00F0?\u0000\u0000\u0000\u0000\u0000\u0000\u00F0?"}`},
			{sqlType: `GEOMETRY`, sql: `NULL`, avro: `null`},
			{sqlType: `GEOMETRY`,
				sql:  "'POINT(1.0 1.0)'",
				avro: `{"bytes":"\u0001\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u00F0?\u0000\u0000\u0000\u0000\u0000\u0000\u00F0?"}`},

			{sqlType: `STRING`, sql: `NULL`, avro: `null`},
			{sqlType: `STRING`,
				sql:  `'foo'`,
				avro: `{"string":"foo"}`},

			{sqlType: `BYTES`, sql: `NULL`, avro: `null`},
			{sqlType: `BYTES`,
				sql:  `'foo'`,
				avro: `{"bytes":"foo"}`},

			{sqlType: `DATE`, sql: `NULL`, avro: `null`},
			{sqlType: `DATE`,
				sql:  `'2019-01-02'`,
				avro: `{"int.date":17898}`},

			{sqlType: `TIME`, sql: `NULL`, avro: `null`},
			{sqlType: `TIME`,
				sql:  `'03:04:05'`,
				avro: `{"long.time-micros":11045000000}`},

			{sqlType: `TIMESTAMP`, sql: `NULL`, avro: `null`},
			{sqlType: `TIMESTAMP`,
				sql:  `'2019-01-02 03:04:05'`,
				avro: `{"long.timestamp-micros":1546398245000000}`},

			{sqlType: `TIMESTAMPTZ`, sql: `NULL`, avro: `null`},
			{sqlType: `TIMESTAMPTZ`,
				sql:  `'2019-01-02 03:04:05'`,
				avro: `{"long.timestamp-micros":1546398245000000}`},

			{sqlType: `DECIMAL(4,1)`, sql: `NULL`, avro: `null`},
			{sqlType: `DECIMAL(4,1)`,
				sql:  `1.2`,
				avro: `{"bytes.decimal":"\f"}`},

			{sqlType: `UUID`, sql: `NULL`, avro: `null`},
			{sqlType: `UUID`,
				sql:  `'27f4f4c9-e35a-45dd-9b79-5ff0f9b5fbb0'`,
				avro: `{"string":"27f4f4c9-e35a-45dd-9b79-5ff0f9b5fbb0"}`},

			{sqlType: `INET`, sql: `NULL`, avro: `null`},
			{sqlType: `INET`,
				sql:  `'190.0.0.0'`,
				avro: `{"string":"190.0.0.0"}`},
			{sqlType: `INET`,
				sql:  `'190.0.0.0/24'`,
				avro: `{"string":"190.0.0.0\/24"}`},
			{sqlType: `INET`,
				sql:  `'2001:4f8:3:ba:2e0:81ff:fe22:d1f1'`,
				avro: `{"string":"2001:4f8:3:ba:2e0:81ff:fe22:d1f1"}`},
			{sqlType: `INET`,
				sql:  `'2001:4f8:3:ba:2e0:81ff:fe22:d1f1/120'`,
				avro: `{"string":"2001:4f8:3:ba:2e0:81ff:fe22:d1f1\/120"}`},
			{sqlType: `INET`,
				sql:  `'::ffff:192.168.0.1/24'`,
				avro: `{"string":"::ffff:192.168.0.1\/24"}`},

			{sqlType: `JSONB`, sql: `NULL`, avro: `null`},
			{sqlType: `JSONB`,
				sql:  `'null'`,
				avro: `{"string":"null"}`},
			{sqlType: `JSONB`,
				sql:  `'{"b": 1}'`,
				avro: `{"string":"{\"b\": 1}"}`},
		}

		for _, test := range goldens {
			tableDesc, err := parseTableDesc(
				`CREATE TABLE foo (pk INT PRIMARY KEY, a ` + test.sqlType + `)`)
			require.NoError(t, err)
			rows, err := parseValues(tableDesc, `VALUES (1, `+test.sql+`)`)
			require.NoError(t, err)

			schema, err := tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
			require.NoError(t, err)
			textual, err := schema.textualFromRow(rows[0])
			require.NoError(t, err)
			// Trim the outermost {}.
			value := string(textual[1 : len(textual)-1])
			// Strip out the pk field.
			value = strings.Replace(value, `"pk":{"long":1}`, ``, -1)
			// Trim the `,`, which could be on either side because of the avro library
			// doesn't deterministically order the fields.
			value = strings.Trim(value, `,`)
			// Strip out the field name.
			value = strings.Replace(value, `"a":`, ``, -1)
			require.Equal(t, test.avro, value)
		}
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
	buf []byte, writerSchema, readerSchema *avroDataRecord,
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

func adjustNative(native map[string]interface{}, writerSchema, readerSchema *avroDataRecord) {
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writerDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.writerSchema))
			require.NoError(t, err)
			writerSchema, err := tableToAvroSchema(writerDesc, avroSchemaNoSuffix)
			require.NoError(t, err)
			readerDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.readerSchema))
			require.NoError(t, err)
			readerSchema, err := tableToAvroSchema(readerDesc, avroSchemaNoSuffix)
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
