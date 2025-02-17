// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gojson "encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONEncoderJSONNullAsObject(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tableDesc, err := parseTableDesc(`CREATE TABLE foo (a JSONB PRIMARY KEY, b JSONB)`)
	require.NoError(t, err)

	objb := json.NewObjectBuilder(1)
	objb.Add("foo", json.FromString("bar"))
	obj := objb.Build()

	rowWithData := rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDJSON(json.FromInt(1))},
		rowenc.EncDatum{Datum: tree.NewDJSON(obj)},
	}
	rowWithSQLNull := rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDJSON(json.FromInt(1))},
		rowenc.EncDatum{Datum: tree.DNull},
	}
	rowWithJSONNull := rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDJSON(json.FromInt(1))},
		rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
	}
	rowWithJSONNullKey := rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
		rowenc.EncDatum{Datum: tree.NewDJSON(obj)},
	}

	ts := hlc.Timestamp{WallTime: 1, Logical: 2}
	evCtx := eventContext{updated: ts}

	targets := mkTargets(tableDesc)

	cases := []struct {
		name                       string
		envelope                   changefeedbase.EnvelopeType
		row, prevRow               rowenc.EncDatumRow
		expectedKey, expectedValue []byte
	}{
		{
			name:          "wrapped: data",
			envelope:      changefeedbase.OptEnvelopeWrapped,
			row:           rowWithData,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"after": {"a":1,"b":{"foo":"bar"}}, "before": null}`),
		},
		{
			name:          "wrapped: sql null",
			envelope:      changefeedbase.OptEnvelopeWrapped,
			row:           rowWithSQLNull,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"after": {"a":1,"b":null}, "before": null}`),
		},
		{
			name:          "wrapped: json null",
			envelope:      changefeedbase.OptEnvelopeWrapped,
			row:           rowWithJSONNull,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"after": {"a":1,"b":{"__crdb_json_null__":true}}, "before": null}`),
		},
		{
			name:          "wrapped: json null key",
			envelope:      changefeedbase.OptEnvelopeWrapped,
			row:           rowWithJSONNullKey,
			expectedKey:   []byte(`[{"__crdb_json_null__":true}]`),
			expectedValue: []byte(`{"after": {"a":{"__crdb_json_null__":true},"b":{"foo":"bar"}}, "before": null}`),
		},
		{
			name:          "wrapped: prev sql null",
			envelope:      changefeedbase.OptEnvelopeWrapped,
			row:           rowWithData,
			prevRow:       rowWithSQLNull,
			expectedValue: []byte(`{"before": {"a":1,"b":null}, "after": {"a":1,"b":{"foo":"bar"}}}`),
		},
		{
			name:          "wrapped: prev json null",
			envelope:      changefeedbase.OptEnvelopeWrapped,
			row:           rowWithData,
			prevRow:       rowWithJSONNull,
			expectedValue: []byte(`{"after": {"a":1,"b":{"foo":"bar"}}, "before": {"a":1,"b":{"__crdb_json_null__":true}}}`),
		},

		{
			name:          "bare: data",
			envelope:      changefeedbase.OptEnvelopeBare,
			row:           rowWithData,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"a":1,"b":{"foo":"bar"}}`),
		},
		{
			name:          "bare: sql null",
			envelope:      changefeedbase.OptEnvelopeBare,
			row:           rowWithSQLNull,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"a":1,"b":null}`),
		},
		{
			name:          "bare: json null",
			envelope:      changefeedbase.OptEnvelopeBare,
			row:           rowWithJSONNull,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"a":1,"b":{"__crdb_json_null__":true}}`),
		},
		{
			name:          "bare: json null key",
			envelope:      changefeedbase.OptEnvelopeBare,
			row:           rowWithJSONNullKey,
			expectedKey:   []byte(`[{"__crdb_json_null__":true}]`),
			expectedValue: []byte(`{"a":{"__crdb_json_null__":true},"b":{"foo":"bar"}}`),
		},
	}

	for _, c := range cases {
		opts := changefeedbase.EncodingOptions{
			Format: changefeedbase.OptFormatJSON, Envelope: c.envelope, Diff: true, EncodeJSONValueNullAsObject: true,
		}
		require.NoError(t, opts.Validate())

		// NOTE: This is no longer required in go 1.22+, but bazel still requires it. See https://github.com/bazelbuild/rules_go/issues/3924
		c := c
		t.Run(c.name, func(t *testing.T) {
			e, err := getEncoder(ctx, opts, targets, false, nil, nil, getTestingEnrichedSourceProvider(opts))
			require.NoError(t, err)

			row := cdcevent.TestingMakeEventRow(tableDesc, 0, c.row, false)
			prevRow := cdcevent.TestingMakeEventRow(tableDesc, 0, c.prevRow, false)

			key, err := e.EncodeKey(ctx, row)
			require.NoError(t, err)
			key = append([]byte(nil), key...)
			value, err := e.EncodeValue(ctx, evCtx, row, prevRow)
			require.NoError(t, err)

			if c.expectedKey != nil {
				assert.Equal(t, string(normalizeJson(t, c.expectedKey)), string(normalizeJson(t, key)))
			}
			if c.expectedValue != nil {
				assert.Equal(t, string(normalizeJson(t, c.expectedValue)), string(normalizeJson(t, value)))
			}
		})
	}
}

func TestJSONEncoderJSONNullAsObjectEdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts := hlc.Timestamp{WallTime: 1, Logical: 2}
	evCtx := eventContext{updated: ts}
	opts := changefeedbase.EncodingOptions{
		Format: changefeedbase.OptFormatJSON, Envelope: changefeedbase.OptEnvelopeWrapped, Diff: true, EncodeJSONValueNullAsObject: true,
	}
	require.NoError(t, opts.Validate())

	t.Run("table with column name __crdb_json_null__", func(t *testing.T) {
		tableDesc, err := parseTableDesc(`CREATE TABLE foo (a int PRIMARY KEY, __crdb_json_null__ bool, b jsonb)`)
		require.NoError(t, err)
		targets := mkTargets(tableDesc)

		eRow := rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(1))},
			rowenc.EncDatum{Datum: tree.DBoolTrue},
			rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
		}
		e, err := getEncoder(ctx, opts, targets, false, nil, nil,
			getTestingEnrichedSourceProvider(opts))
		require.NoError(t, err)

		row := cdcevent.TestingMakeEventRow(tableDesc, 0, eRow, false)
		prevRow := cdcevent.TestingMakeEventRow(tableDesc, 0, nil, false)

		value, err := e.EncodeValue(ctx, evCtx, row, prevRow)
		require.NoError(t, err)
		expected := `{"before": null, "after": {"a": 1, "__crdb_json_null__": true, "b": {"__crdb_json_null__": true}}}`
		assert.Equal(t, string(normalizeJson(t, []byte(expected))), string(normalizeJson(t, value)))
	})

	twoJSONsTableDesc, err := parseTableDesc(`CREATE TABLE foo (a int PRIMARY KEY, b jsonb, c jsonb)`)
	require.NoError(t, err)
	twoJSONsTargets := mkTargets(twoJSONsTableDesc)
	prevNilRow := cdcevent.TestingMakeEventRow(twoJSONsTableDesc, 0, nil, false)

	t.Run("test with sql null and json null in the same row", func(t *testing.T) {
		eRow := rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(1))},
			rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
			rowenc.EncDatum{Datum: tree.DNull},
		}
		e, err := getEncoder(ctx, opts, twoJSONsTargets, false, nil, nil,
			getTestingEnrichedSourceProvider(opts))
		require.NoError(t, err)

		row := cdcevent.TestingMakeEventRow(twoJSONsTableDesc, 0, eRow, false)

		value, err := e.EncodeValue(ctx, evCtx, row, prevNilRow)
		require.NoError(t, err)
		expected := `{"before": null, "after": {"a": 1, "b": {"__crdb_json_null__": true}, "c": null}}`
		assert.Equal(t, string(normalizeJson(t, []byte(expected))), string(normalizeJson(t, value)))
	})

	t.Run("test with two json nulls in the same row", func(t *testing.T) {
		eRow := rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(1))},
			rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
			rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
		}
		e, err := getEncoder(ctx, opts, twoJSONsTargets, false, nil, nil,
			getTestingEnrichedSourceProvider(opts))
		require.NoError(t, err)

		row := cdcevent.TestingMakeEventRow(twoJSONsTableDesc, 0, eRow, false)

		value, err := e.EncodeValue(ctx, evCtx, row, prevNilRow)
		require.NoError(t, err)
		expected := `{"before": null, "after": {"a": 1, "b": {"__crdb_json_null__": true}, "c": {"__crdb_json_null__": true}}}`
		assert.Equal(t, string(normalizeJson(t, []byte(expected))), string(normalizeJson(t, value)))
	})

	t.Run("demonstrate nulls encoded the same without this option look the same", func(t *testing.T) {
		disabledOpts := changefeedbase.EncodingOptions{
			Format: changefeedbase.OptFormatJSON, Envelope: changefeedbase.OptEnvelopeWrapped, Diff: true,
		}
		require.NoError(t, disabledOpts.Validate())
		eRow := rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(1))},
			rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
			rowenc.EncDatum{Datum: tree.DNull},
		}
		e, err := getEncoder(ctx, disabledOpts, twoJSONsTargets, false, nil, nil,
			getTestingEnrichedSourceProvider(disabledOpts))
		require.NoError(t, err)

		row := cdcevent.TestingMakeEventRow(twoJSONsTableDesc, 0, eRow, false)

		value, err := e.EncodeValue(ctx, evCtx, row, prevNilRow)
		require.NoError(t, err)
		expected := `{"before": null, "after": {"a": 1, "b": null, "c": null}}`
		assert.Equal(t, string(normalizeJson(t, []byte(expected))), string(normalizeJson(t, value)))
	})

	t.Run("test with json null and an object with the same format as our sentinel - confusing but correct", func(t *testing.T) {
		objb := json.NewObjectBuilder(1)
		objb.Add(jsonNullAsObjectKey, json.FromBool(true))
		obj := objb.Build()

		eRow := rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(1))},
			rowenc.EncDatum{Datum: tree.NewDJSON(json.NullJSONValue)},
			rowenc.EncDatum{Datum: tree.NewDJSON(obj)},
		}
		e, err := getEncoder(ctx, opts, twoJSONsTargets, false, nil, nil,
			getTestingEnrichedSourceProvider(opts))
		require.NoError(t, err)

		row := cdcevent.TestingMakeEventRow(twoJSONsTableDesc, 0, eRow, false)

		value, err := e.EncodeValue(ctx, evCtx, row, prevNilRow)
		require.NoError(t, err)
		expected := `{"before": null, "after": {"a": 1, "b": {"__crdb_json_null__": true}, "c": {"__crdb_json_null__": true}}}`
		assert.Equal(t, string(normalizeJson(t, []byte(expected))), string(normalizeJson(t, value)))
	})

}

func normalizeJson(t *testing.T, b []byte) []byte {
	var v interface{}
	require.NoError(t, gojson.Unmarshal(b, &v))
	norm, err := gojson.Marshal(v)
	require.NoError(t, err)
	return norm
}

func mkTargets(tableDesc catalog.TableDescriptor) changefeedbase.Targets {
	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{
		Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		TableID:           tableDesc.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(tableDesc.GetName()),
	})
	return targets
}

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
