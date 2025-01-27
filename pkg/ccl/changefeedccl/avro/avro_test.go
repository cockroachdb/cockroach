// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/collatedstring"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var testTypes = make(map[string]*types.T)
var testTypeResolver = tree.MakeTestingMapTypeResolver(testTypes)

const primary = descpb.FamilyID(0)

func makeTestSemaCtx() tree.SemaContext {
	return tree.MakeSemaContext(testTypeResolver)
}

func parseTableDesc(createTableStmt string) (catalog.TableDescriptor, error) {
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
	parentID := descpb.ID(bootstrap.TestingUserDescID(0))
	tableID := descpb.ID(bootstrap.TestingUserDescID(1))
	semaCtx := makeTestSemaCtx()
	mutDesc, err := importer.MakeTestingSimpleTableDescriptor(
		ctx, &semaCtx, st, createTable, parentID, keys.PublicSchemaID, tableID, importer.NoFKs, timeutil.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, len(mutDesc.PublicColumns()))
	for i, col := range mutDesc.PublicColumns() {
		columnNames[i] = col.GetName()
	}
	mutDesc.Families = []descpb.ColumnFamilyDescriptor{
		{ID: primary, Name: "primary", ColumnIDs: mutDesc.PublicColumnIDs(), ColumnNames: columnNames},
	}
	return mutDesc, desctestutils.TestingValidateSelf(mutDesc)
}

func parseValues(tableDesc catalog.TableDescriptor, values string) ([]rowenc.EncDatumRow, error) {
	ctx := context.Background()
	semaCtx := makeTestSemaCtx()
	evalCtx := &eval.Context{}

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

	var rows []rowenc.EncDatumRow
	for _, rowTuple := range valuesClause.Rows {
		var row rowenc.EncDatumRow
		for colIdx, expr := range rowTuple {
			col := tableDesc.PublicColumns()[colIdx]
			typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
				ctx, expr, col.GetType(), "avro", &semaCtx, volatility.Stable, false /*allowAssignmentCast*/)
			if err != nil {
				return nil, err
			}
			datum, err := eval.Expr(ctx, evalCtx, typedExpr)
			if err != nil {
				return nil, errors.Wrapf(err, "evaluating %s", typedExpr)
			}
			row = append(row, rowenc.DatumToEncDatum(col.GetType(), datum))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseAvroSchema(t *testing.T, evalCtx *eval.Context, j string) (*DataRecord, error) {
	var s DataRecord
	if err := json.Unmarshal([]byte(j), &s); err != nil {
		return nil, err
	}
	// This avroDataRecord doesn't have any of the derived fields we need for
	// serde. Instead of duplicating the logic, fake out a TableDescriptor, so
	// we can reuse tableToAvroSchema and get them for free.
	tableDesc := descpb.TableDescriptor{
		Name: changefeedbase.AvroNameToSQLName(s.Name),
	}
	for i, f := range s.Fields {
		// s.Fields[idx] has `Name` and `SchemaType` set but nothing else.
		// They're needed for serialization/deserialization, so fake out a
		// column descriptor so that we can reuse columnToAvroSchema to get
		// all the various fields of avroSchemaField populated for free.
		colDesc, err := avroFieldMetadataToColDesc(evalCtx, f.Metadata)
		if err != nil {
			return nil, err
		}
		colDesc.ID = descpb.ColumnID(i)
		tableDesc.Columns = append(tableDesc.Columns, *colDesc)
	}
	columnNames := make([]string, len(tableDesc.Columns))
	columnIDs := make([]descpb.ColumnID, len(tableDesc.Columns))
	for i, col := range tableDesc.Columns {
		columnNames[i] = col.Name
		columnIDs[i] = col.ID
	}
	tableDesc.Families = []descpb.ColumnFamilyDescriptor{
		{ID: primary, Name: "primary", ColumnIDs: columnIDs, ColumnNames: columnNames},
	}
	return TableToAvroSchema(
		cdcevent.TestingMakeEventRow(
			tabledesc.NewBuilder(&tableDesc).BuildImmutableTable(), 0, nil, false,
		), "", "")
}

func avroFieldMetadataToColDesc(
	evalCtx *eval.Context, metadata string,
) (*descpb.ColumnDescriptor, error) {
	parsed, err := parser.ParseOne(`ALTER TABLE FOO ADD COLUMN ` + metadata)
	if err != nil {
		return nil, err
	}
	def := parsed.AST.(*tree.AlterTable).Cmds[0].(*tree.AlterTableAddColumn).ColumnDef
	ctx := context.Background()
	semaCtx := makeTestSemaCtx()
	cdd, err := tabledesc.MakeColumnDefDescs(ctx, def, &semaCtx, evalCtx, tree.ColumnDefaultExprInAddColumn)
	if err != nil {
		return nil, err
	}
	return cdd.ColumnDescriptor, err
}

// randTime generates a random time.Time whose .UnixNano result doesn't
// overflow an int64.
func randTime(rng *rand.Rand) time.Time {
	return timeutil.Unix(0, rng.Int63())
}

// Create a thin, in-memory user-defined enum type
func createEnum(enumLabels tree.EnumValueList, typeName tree.TypeName) *types.T {

	members := make([]descpb.TypeDescriptor_EnumMember, len(enumLabels))
	physReps := enum.GenerateNEvenlySpacedBytes(len(enumLabels))
	for i := range enumLabels {
		members[i] = descpb.TypeDescriptor_EnumMember{
			LogicalRepresentation:  string(enumLabels[i]),
			PhysicalRepresentation: physReps[i],
			Capability:             descpb.TypeDescriptor_EnumMember_ALL,
		}
	}

	enumKind := descpb.TypeDescriptor_ENUM

	typeDesc := typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:        typeName.Type(),
		ID:          0,
		Kind:        enumKind,
		EnumMembers: members,
		Version:     1,
	}).BuildCreatedMutableType()

	typ, _ := typedesc.HydratedTFromDesc(context.Background(), &typeName, typeDesc, nil /* res */)

	testTypes[typeName.SQLString()] = typ

	return typ

}

func TestAvroSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()

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

	// Type-specific random logic for when we can't use the randgen library
	// and/or need to modify the type to add random user-defined values
	// The returned datum will be nil if we can just use randgen
	var overrideRandGen func(typ *types.T) (tree.Datum, *types.T)
	overrideRandGen = func(typ *types.T) (tree.Datum, *types.T) {
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
			return tree.MustMakeDTimestamp(t, time.Microsecond), typ
		case types.TimestampTZFamily:
			// See comments above for TimestampFamily.
			t := randTime(rng).Truncate(time.Millisecond)
			return tree.MustMakeDTimestampTZ(t, time.Microsecond), typ
		case types.DecimalFamily:
			// TODO(dan): Make RandDatum respect Precision and Width instead.
			// TODO(dan): The precision is really meant to be in [1,10], but it
			// sure looks like there's an off by one error in the avro library
			// that makes this test flake if it picks precision of 1.
			var precision, scale int32
			if typ.Precision() < 2 {
				precision = rng.Int31n(10) + 2
				scale = rng.Int31n(precision + 1)
				typ = types.MakeDecimal(precision, scale)
			} else {
				precision = typ.Precision()
				scale = typ.Scale()
			}
			coeff := rng.Int63n(int64(math.Pow10(int(precision))))
			return &tree.DDecimal{Decimal: *apd.New(coeff, -scale)}, typ
		case types.DateFamily:
			// TODO(mjibson): goavro mishandles dates whose
			// nanosecond representation overflows an int64,
			// so restrict input to fit.
			var err error
			datum, err := tree.NewDDateFromTime(randTime(rng))
			if err != nil {
				panic(err)
			}
			return datum, typ
		case types.ArrayFamily:
			// Apply the other cases in this function
			// to the contents of the array
			contentType := typ.ArrayContents()
			el, contentType := overrideRandGen(contentType)
			if el == nil {
				return nil, typ
			}
			typ.InternalType.ArrayContents = contentType
			datum := randgen.RandDatum(rng, typ, false /* nullOk */)
			for i := range datum.(*tree.DArray).Array {
				datum.(*tree.DArray).Array[i], _ = overrideRandGen(contentType)
			}
			return datum, typ

		}
		return nil, typ
	}

	// Types we don't support that are present in types.OidToType
	var skipType func(typ *types.T) bool
	skipType = func(typ *types.T) bool {
		switch typ.Family() {
		case types.AnyFamily, types.OidFamily, types.TupleFamily:
			// These aren't expected to be needed for changefeeds.
			return true
		case types.PGVectorFamily, types.JsonpathFamily:
			// We don't support PGVector and Jsonpath in Avro yet.
			return true
		case types.ArrayFamily:
			if !randgen.IsAllowedForArray(typ.ArrayContents()) {
				return true
			}
			if skipType(typ.ArrayContents()) {
				return true
			}
		}
		return !randgen.IsLegalColumnType(typ)
	}

	typesToTest := make([]*types.T, 0, 256)

	for _, typ := range types.OidToType {
		if skipType(typ) {
			continue
		}
		typesToTest = append(typesToTest, typ)
		switch typ.Family() {
		case types.StringFamily:
			collationTags := collatedstring.Supported()
			// "C" and "POSIX" locales are not allowed for collated string
			// columns in CRDB (see collatedstring logic tests),
			// so we don't expect these types to be emitted by changefeeds.
			// TODO(#140632): Reenable "default" locale.
			randCollationTag := collationTags[rand.Intn(len(collationTags))]
			for randCollationTag == collatedstring.CCollationTag ||
				randCollationTag == collatedstring.PosixCollationTag ||
				randCollationTag == collatedstring.DefaultCollationTag {
				randCollationTag = collationTags[rand.Intn(len(collationTags))]
			}
			collatedType := types.MakeCollatedString(typ, randCollationTag)
			typesToTest = append(typesToTest, collatedType)
		}
	}

	testEnum := createEnum(
		tree.EnumValueList{tree.EnumValue(`open`), tree.EnumValue(`closed`)},
		tree.MakeUnqualifiedTypeName(`switch`),
	)

	typesToTest = append(typesToTest, testEnum)

	// Generate a test for each column type with a random datum of that type.
	for _, typ := range typesToTest {
		var datum tree.Datum
		datum, typ = overrideRandGen(typ)
		if datum == nil {
			datum = randgen.RandDatum(rng, typ, false /* nullOk */)
		}
		if datum == tree.DNull {
			// DNull is returned by RandDatum for types.UNKNOWN or if the
			// column type is unimplemented in RandDatum. In either case, the
			// correct thing to do is skip this one.
			continue
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

	ctx := context.Background()
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.schema))
			require.NoError(t, err)
			origSchema, err := TableToAvroSchema(
				cdcevent.TestingMakeEventRow(tableDesc, 0, nil, false),
				SchemaNoSuffix, "")
			require.NoError(t, err)
			jsonSchema := origSchema.codec.Schema()
			roundtrippedSchema, err := parseAvroSchema(t, evalCtx, jsonSchema)
			require.NoError(t, err)
			// It would require some work, but we could also check that the
			// roundtrippedSchema can be used to recreate the original `CREATE
			// TABLE`.

			rows, err := parseValues(tableDesc, `VALUES `+test.values)
			require.NoError(t, err)

			for _, encDatums := range rows {
				row := cdcevent.TestingMakeEventRow(tableDesc, 0, encDatums, false)
				evalCtx := &eval.Context{
					SessionDataStack: sessiondata.NewStack(&sessiondata.SessionData{}),
				}
				serialized, err := origSchema.textualFromRow(row)
				require.NoError(t, err)
				roundtripped, err := roundtrippedSchema.rowFromTextual(serialized)
				require.NoError(t, err)
				cmp, err := encDatums[1].Datum.Compare(ctx, evalCtx, roundtripped[1].Datum)
				require.NoError(t, err)
				require.Equal(t, 0, cmp, `%s != %s`, encDatums[1].Datum, roundtripped[1].Datum)

				serialized, err = origSchema.BinaryFromRow(nil, row.ForEachColumn())
				require.NoError(t, err)
				roundtripped, err = roundtrippedSchema.RowFromBinary(serialized)
				require.NoError(t, err)
				cmp, err = encDatums[1].Datum.Compare(ctx, evalCtx, roundtripped[1].Datum)
				require.NoError(t, err)
				require.Equal(t, 0, cmp, `%s != %s`, encDatums[1].Datum, roundtripped[1].Datum)
			}
		})
	}

	t.Run("escaping", func(t *testing.T) {
		tableDesc, err := parseTableDesc(`CREATE TABLE "☃" (🍦 INT PRIMARY KEY)`)
		require.NoError(t, err)
		tableSchema, err := TableToAvroSchema(
			cdcevent.TestingMakeEventRow(tableDesc, 0, nil, false), SchemaNoSuffix, "")
		require.NoError(t, err)
		require.Equal(t,
			`{"type":"record","name":"_u2603_","fields":[`+
				`{"type":["null","long"],"name":"_u0001f366_","default":null,`+
				`"__crdb__":"🍦 INT8 NOT NULL"}]}`,
			tableSchema.codec.Schema())
		indexSchema, err := PrimaryIndexToAvroSchema(
			cdcevent.TestingMakeEventRow(tableDesc, 0, nil, false), tableDesc.GetName(), "")
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
			`BOOL`:              `["null","boolean"]`,
			`BOOL[]`:            `["null",{"type":"array","items":["null","boolean"]}]`,
			`BOX2D`:             `["null","string"]`,
			`BYTES`:             `["null","bytes"]`,
			`DATE`:              `["null",{"type":"int","logicalType":"date"}]`,
			`FLOAT8`:            `["null","double"]`,
			`GEOGRAPHY`:         `["null","bytes"]`,
			`GEOMETRY`:          `["null","bytes"]`,
			`INET`:              `["null","string"]`,
			`INT8`:              `["null","long"]`,
			`INTERVAL`:          `["null","string"]`,
			`JSONB`:             `["null","string"]`,
			`PG_LSN`:            `["null","string"]`,
			`REFCURSOR`:         `["null","string"]`,
			`STRING`:            `["null","string"]`,
			`STRING COLLATE fr`: `["null","string"]`,
			`TIME`:              `["null",{"type":"long","logicalType":"time-micros"}]`,
			`TIMETZ`:            `["null","string"]`,
			`TIMESTAMP`:         `["null",{"type":"long","logicalType":"timestamp-micros"}]`,
			`TIMESTAMPTZ`:       `["null",{"type":"long","logicalType":"timestamp-micros"}]`,
			`UUID`:              `["null","string"]`,
			`VARBIT`:            `["null",{"type":"array","items":"long"}]`,

			`BIT(3)`:       `["null",{"type":"array","items":"long"}]`,
			`DECIMAL(3,2)`: `["null",{"type":"bytes","logicalType":"decimal","precision":3,"scale":2},"string"]`,
		}

		for _, typ := range append(types.Scalar, types.BoolArray, types.MakeCollatedString(types.String, `fr`), types.MakeBit(3)) {
			switch typ.Family() {
			case types.OidFamily:
				continue
			case types.DecimalFamily:
				typ = types.MakeDecimal(3, 2)
			}

			colType := typ.SQLString()

			t.Run(typ.String(), func(t *testing.T) {
				tableDesc, err := parseTableDesc(`CREATE TABLE foo (pk INT PRIMARY KEY, a ` + colType + `)`)
				require.NoError(t, err)
				field, err := columnToAvroSchema(
					cdcevent.ResultColumn{ResultColumn: colinfo.ResultColumn{Typ: tableDesc.PublicColumns()[1].GetType()}},
				)
				require.NoError(t, err)
				schema, err := json.Marshal(field.SchemaType)
				require.NoError(t, err)
				require.Equal(t, goldens[colType], string(schema), `SQL type %s`, colType)
			})

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
			sqlType     string
			sql         string
			avro        string
			numRawBytes int
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

			{sqlType: `BOX2D`, sql: `NULL`, avro: `null`},
			{sqlType: `BOX2D`, sql: `'BOX(1 2,3 4)'`, avro: `{"string":"BOX(1 2,3 4)"}`},
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

			{sqlType: `INTERVAL`, sql: `NULL`, avro: `null`},
			{sqlType: `INTERVAL`,
				sql:  `INTERVAL '1 yr 2 mons 3 d 4 hrs 5 mins 6 secs'`,
				avro: `{"string":"P1Y2M3DT4H5M6S"}`},
			{sqlType: `INTERVAL`,
				sql:  `INTERVAL '1 yr -6 ms'`,
				avro: `{"string":"P1YT-0.006S"}`},

			{sqlType: `DECIMAL(4,1)`, sql: `NULL`, avro: `null`},
			{sqlType: `DECIMAL(4,1)`,
				sql:  `1.2`,
				avro: `{"bytes.decimal":"\f"}`},
			{sqlType: `DECIMAL(4,1)`,
				sql:  `DECIMAL 'Infinity'`,
				avro: `{"string":"Infinity"}`},
			{sqlType: `DECIMAL(4,1)`,
				sql:  `DECIMAL '-Infinity'`,
				avro: `{"string":"-Infinity"}`},
			{sqlType: `DECIMAL(4,1)`,
				sql:  `DECIMAL 'NaN'`,
				avro: `{"string":"NaN"}`},

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

			{sqlType: `VARBIT`, sql: `B'010'`, avro: `{"array":[3,4611686018427387904]}`}, // Take the 3 most significant bits of 1<<62

			{sqlType: `BOOL[]`,
				sql:  `'{true, true, false, null}'`,
				avro: `{"array":[{"boolean":true},{"boolean":true},{"boolean":false},null]}`},
			{sqlType: `VARCHAR COLLATE "fr"`,
				sql:  `'Bonjour' COLLATE "fr"`,
				avro: `{"string":"Bonjour"}`},
			{sqlType: `switch`, // User-defined enum with values "open", "closed"
				sql:  `'open'`,
				avro: `{"string":"open"}`},

			// The following test cases document the way goavro encodes and decodes
			// the "bytes" type. We'll need to keep this behavior as the default to
			// avoid any breaking changes.
			{sqlType: `BYTES`,
				sql:         `b'\xff'`,
				avro:        `{"bytes":"\u00FF"}`,
				numRawBytes: 1},
			{sqlType: `BYTES`,
				sql:         `'a'`,
				avro:        `{"bytes":"a"}`,
				numRawBytes: 1},
			{sqlType: `BYTES`,
				sql:         `b'\001\002\003\004\005\006\007\010\011\012\013'`,
				avro:        `{"bytes":"\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000B"}`,
				numRawBytes: 11},
			{sqlType: `BYTES`,
				sql:         `''`,
				avro:        `{"bytes":""}`,
				numRawBytes: 0},

			{
				sqlType: `PG_LSN`,
				sql:     `'A/0'`,
				avro:    `{"string":"A\/0"}`,
			},
		}

		for _, test := range goldens {
			tableDesc, err := parseTableDesc(
				`CREATE TABLE foo (pk INT PRIMARY KEY, a ` + test.sqlType + `)`)
			require.NoError(t, err)
			encDatums, err := parseValues(tableDesc, `VALUES (1, `+test.sql+`)`)
			require.NoError(t, err)

			row := cdcevent.TestingMakeEventRow(tableDesc, 0, encDatums[0], false)
			schema, err := TableToAvroSchema(
				row, SchemaNoSuffix, "")
			require.NoError(t, err)
			if test.numRawBytes > 0 {
				overhead := 4
				binary, err := schema.BinaryFromRow(make([]byte, 0, test.numRawBytes+20), row.ForEachColumn())
				require.NoError(t, err)
				require.Equal(t, test.numRawBytes, len(binary)-overhead)
			}
			textual, err := schema.textualFromRow(row)
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

	// These are values stored with less precision than the column definition allows,
	// which is still roundtrippable
	t.Run("lossless_truncations", func(t *testing.T) {
		truncs := []struct {
			sqlType string
			sql     string
			avro    string
		}{

			{sqlType: `DECIMAL(4,2)`,
				sql:  `1.2`,
				avro: `{"bytes.decimal":"x"}`},

			{sqlType: `DECIMAL(12,2)`,
				sql:  `1e2`,
				avro: `{"bytes.decimal":"'\u0010"}`},

			{sqlType: `DECIMAL(4,2)`,
				sql:  `12e-1`,
				avro: `{"bytes.decimal":"x"}`},

			{sqlType: `DECIMAL(4,2)`,
				sql:  `12e-2`,
				avro: `{"bytes.decimal":"\f"}`},
		}

		for _, test := range truncs {
			tableDesc, err := parseTableDesc(
				`CREATE TABLE foo (pk INT PRIMARY KEY, a ` + test.sqlType + `)`)
			require.NoError(t, err)
			encDatums, err := parseValues(tableDesc, `VALUES (1, `+test.sql+`)`)
			require.NoError(t, err)

			row := cdcevent.TestingMakeEventRow(tableDesc, 0, encDatums[0], false)
			schema, err := TableToAvroSchema(row, SchemaNoSuffix, "")
			require.NoError(t, err)
			textual, err := schema.textualFromRow(row)
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

func (f *SchemaField) defaultValueNative() (interface{}, bool) {
	schType := f.SchemaType
	if union, ok := schType.([]SchemaType); ok {
		// "Default values for union fields correspond to the first schema in
		// the union."
		schType = union[0]
	}
	switch schType {
	case SchemaTypeNull:
		return nil, true
	}
	panic(errors.Errorf(`unimplemented %T: %v`, schType, schType))
}

// rowFromBinaryEvolved decodes `buf` using writerSchema but evolves/resolves it
// to readerSchema using the rules from the avro spec:
// https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution
//
// It'd be nice if our avro library handled this for us, but neither of the
// popular golang once seem to have it implemented.
func rowFromBinaryEvolved(
	buf []byte, writerSchema, readerSchema *DataRecord,
) (rowenc.EncDatumRow, error) {
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

func adjustNative(native map[string]interface{}, writerSchema, readerSchema *DataRecord) {
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
	defer log.Scope(t).Close(t)

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
			writerSchema, err := TableToAvroSchema(
				cdcevent.TestingMakeEventRow(writerDesc, 0, nil, false), SchemaNoSuffix, "")
			require.NoError(t, err)
			readerDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.readerSchema))
			require.NoError(t, err)
			readerSchema, err := TableToAvroSchema(
				cdcevent.TestingMakeEventRow(readerDesc, 0, nil, false), SchemaNoSuffix, "")
			require.NoError(t, err)

			writerRows, err := parseValues(writerDesc, `VALUES `+test.writerValues)
			require.NoError(t, err)
			expectedRows, err := parseValues(readerDesc, `VALUES `+test.expectedValues)
			require.NoError(t, err)

			for i := range writerRows {
				writerEvent := cdcevent.TestingMakeEventRow(writerDesc, 0, writerRows[i], false)
				encoded, err := writerSchema.BinaryFromRow(nil, writerEvent.ForEachColumn())
				require.NoError(t, err)
				row, err := rowFromBinaryEvolved(encoded, writerSchema, readerSchema)
				require.NoError(t, err)
				require.Equal(t, expectedRows[i], row)
			}
		})
	}
}

func TestDecimalRatRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		rng, _ := randutil.NewTestRand()
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

func benchmarkEncodeType(b *testing.B, typ *types.T, encRow rowenc.EncDatumRow) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	tableDesc, err := parseTableDesc(
		fmt.Sprintf(`CREATE TABLE bench_table (bench_field %s)`, typ.SQLString()))
	require.NoError(b, err)
	row := cdcevent.TestingMakeEventRow(tableDesc, 0, encRow, false)
	schema, err := TableToAvroSchema(row, "suffix", "namespace")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := schema.BinaryFromRow(nil, row.ForEachColumn())
		require.NoError(b, err)
	}
}

// returns random EncDatum row where the first column is of specified
// type and the second one an types.Int, corresponding to a row id.
func randEncDatumRow(typ *types.T) rowenc.EncDatumRow {
	const allowNull = true
	const notNull = false
	rnd, _ := randutil.NewTestRand()
	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(typ, randgen.RandDatum(rnd, typ, allowNull)),
		rowenc.DatumToEncDatum(types.Int, randgen.RandDatum(rnd, types.Int, notNull)),
	}
}

func BenchmarkEncodeIntArray(b *testing.B) {
	benchmarkEncodeType(b, types.IntArray, randEncDatumRow(types.IntArray))
}

func BenchmarkEncodeInt(b *testing.B) {
	benchmarkEncodeType(b, types.Int, randEncDatumRow(types.Int))
}

func BenchmarkEncodeBitSmall(b *testing.B) {
	smallBit := types.MakeBit(8)
	benchmarkEncodeType(b, smallBit, randEncDatumRow(smallBit))
}

func BenchmarkEncodeBitLarge(b *testing.B) {
	largeBit := types.MakeBit(64*3 + 1)
	benchmarkEncodeType(b, largeBit, randEncDatumRow(largeBit))
}

func BenchmarkEncodeVarbit(b *testing.B) {
	benchmarkEncodeType(b, types.VarBit, randEncDatumRow(types.VarBit))
}

func BenchmarkEncodeBool(b *testing.B) {
	benchmarkEncodeType(b, types.Bool, randEncDatumRow(types.Bool))
}

func BenchmarkEncodeEnum(b *testing.B) {
	testEnum := createEnum(
		tree.EnumValueList{tree.EnumValue(`open`), tree.EnumValue(`closed`)},
		tree.MakeUnqualifiedTypeName(`switch`),
	)
	benchmarkEncodeType(b, testEnum, randEncDatumRow(testEnum))
}

func BenchmarkEncodeFloat(b *testing.B) {
	benchmarkEncodeType(b, types.Float, randEncDatumRow(types.Float))
}

func BenchmarkEncodeBox2D(b *testing.B) {
	benchmarkEncodeType(b, types.Box2D, randEncDatumRow(types.Box2D))
}

func BenchmarkEncodeGeography(b *testing.B) {
	benchmarkEncodeType(b, types.Geography, randEncDatumRow(types.Geography))
}

func BenchmarkEncodeGeometry(b *testing.B) {
	benchmarkEncodeType(b, types.Geometry, randEncDatumRow(types.Geometry))
}

func BenchmarkEncodeBytes(b *testing.B) {
	benchmarkEncodeType(b, types.Bytes, randEncDatumRow(types.Bytes))
}

func BenchmarkEncodeString(b *testing.B) {
	benchmarkEncodeType(b, types.String, randEncDatumRow(types.String))
}

var collatedStringType = types.MakeCollatedString(types.String, `fr`)

func BenchmarkEncodeCollatedString(b *testing.B) {
	benchmarkEncodeType(b, collatedStringType, randEncDatumRow(collatedStringType))
}

func BenchmarkEncodeDate(b *testing.B) {
	// RandDatum could return "interesting" dates (infinite past, etc).  Alas, avro
	// doesn't support those yet, so override it to something we do support.
	encRow := randEncDatumRow(types.Date)
	if d, ok := encRow[0].Datum.(*tree.DDate); ok && !d.IsFinite() {
		d.Date = pgdate.LowDate
	}
	benchmarkEncodeType(b, types.Date, encRow)
}

func BenchmarkEncodeTime(b *testing.B) {
	benchmarkEncodeType(b, types.Time, randEncDatumRow(types.Time))
}

func BenchmarkEncodeTimeTZ(b *testing.B) {
	benchmarkEncodeType(b, types.TimeTZ, randEncDatumRow(types.TimeTZ))
}

func BenchmarkEncodeTimestamp(b *testing.B) {
	benchmarkEncodeType(b, types.Timestamp, randEncDatumRow(types.Timestamp))
}

func BenchmarkEncodeTimestampTZ(b *testing.B) {
	benchmarkEncodeType(b, types.TimestampTZ, randEncDatumRow(types.TimestampTZ))
}

func BenchmarkEncodeInterval(b *testing.B) {
	benchmarkEncodeType(b, types.Interval, randEncDatumRow(types.Interval))
}

func BenchmarkEncodeDecimal(b *testing.B) {
	typ := types.MakeDecimal(10, 4)
	encRow := randEncDatumRow(typ)

	// rowenc.RandDatum generates all possible datums. We just want small subset
	// to fit in our specified precision/scale.
	d := &tree.DDecimal{}
	coeff := int64(rand.Uint64()) % 10000
	d.Decimal.SetFinite(coeff, 2)
	encRow[0] = rowenc.DatumToEncDatum(typ, d)
	benchmarkEncodeType(b, typ, encRow)
}

func BenchmarkEncodeUUID(b *testing.B) {
	benchmarkEncodeType(b, types.Uuid, randEncDatumRow(types.Uuid))
}

func BenchmarkEncodeINet(b *testing.B) {
	benchmarkEncodeType(b, types.INet, randEncDatumRow(types.INet))
}

func BenchmarkEncodeJSON(b *testing.B) {
	benchmarkEncodeType(b, types.Jsonb, randEncDatumRow(types.Jsonb))
}
