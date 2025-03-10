// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kcjsonschema

import (
	"context"
	gojson "encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestSchema_AsJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	schema := Schema{
		TypeName: SchemaTypeString,
		Name:     schemaNameDate,
		Field:    "testField",
		Parameters: map[string]string{
			"param1": "value1",
		},
		Fields: []Schema{
			{
				TypeName: SchemaTypeInt8,
				Field:    "nestedField",
			},
		},
		Optional: true,
		Items: &Schema{
			TypeName: SchemaTypeArray,
		},
	}

	expectedJSON := `{
		"type": "string",
		"name": "date",
		"field": "testField",
		"parameters": {
			"param1": "value1"
		},
		"fields": [
			{
				"type": "int8",
				"field": "nestedField",
				"optional": false
			}
		],
		"optional": true,
		"items": {
			"type": "array",
			"optional": false
		}
	}`

	j, err := schema.AsJSON()
	require.NoError(t, err)
	require.JSONEq(t, expectedJSON, j.String())
}

// Much of this test is adapted from avro_test.go/TestAvroSchema.
func TestRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()

	type test struct {
		name   string
		schema string
		values string
	}

	var skipType func(typ *types.T) bool
	skipType = func(typ *types.T) bool {
		switch typ.Family() {
		case types.AnyFamily, types.OidFamily, types.TupleFamily, types.PGVectorFamily, types.JsonpathFamily:
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
	}

	var tests []test
	// Generate a test for each column type with a random datum of that type.
	for _, typ := range typesToTest {
		datum := randgen.RandDatum(rng, typ, false /* nullOk */)
		if datum == tree.DNull {
			continue
		}

		serializedDatum := tree.Serialize(datum)
		// name can be "char" with quotes which need to be stripped.
		strippedName := fmt.Sprintf("%s_table", strings.Replace(typ.String(), "\"", "", -1))
		// schema is used in a fmt.Sprintf to fill in the table name, so we have
		// to escape any stray %s.
		escapedDatum := strings.Replace(serializedDatum, `%`, `%%`, -1)
		randTypeTest := test{
			name:   strippedName,
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

			rows, err := parseValues(tableDesc, `VALUES `+test.values)
			require.NoError(t, err)

			for _, encDatums := range rows {
				row := cdcevent.TestingMakeEventRow(tableDesc, 0, encDatums, false)
				keySchema, err := NewSchemaFromIterator(row.ForEachKeyColumn(), "the_keys")
				require.NoError(t, err)
				valueSchema, err := NewSchemaFromIterator(row.ForEachColumn(), "the_values")
				require.NoError(t, err)

				validateSchemas(t, row, keySchema, valueSchema)
			}

		})
	}
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

func makeTestSemaCtx() tree.SemaContext {
	return tree.MakeSemaContext(testTypeResolver)
}

var testTypes = make(map[string]*types.T)
var testTypeResolver = tree.MakeTestingMapTypeResolver(testTypes)

const primary = descpb.FamilyID(0)

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
				ctx, expr, col.GetType(), "kctest", &semaCtx, volatility.Stable, false /*allowAssignmentCast*/)
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

func validateSchemas(t *testing.T, row cdcevent.Row, keySchema, valueSchema Schema) {
	valJB := json.NewObjectBuilder(2)
	require.NoError(t, row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		require.NoError(t, err)
		valJB.Add(col.Name, j)
		return nil
	}))
	valJ := valJB.Build()

	keyJB := json.NewObjectBuilder(2)
	require.NoError(t, row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		require.NoError(t, err)
		keyJB.Add(col.Name, j)
		return nil
	}))
	keyJ := keyJB.Build()

	validateSchema(t, valJ, valueSchema)
	validateSchema(t, keyJ, keySchema)
}

func validateSchema(t *testing.T, dataJ json.JSON, schema Schema) {
	// Turn the data json.JSON into map[string]any because its so much easier to deal with.
	var data map[string]any
	dec := gojson.NewDecoder(strings.NewReader(dataJ.String()))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&data))
	require.Equal(t, dec.InputOffset(), int64(len(dataJ.String())), "didn't consume all of the input")
	require.NoError(t, TestingMatchesJSON(schema, data))
}
