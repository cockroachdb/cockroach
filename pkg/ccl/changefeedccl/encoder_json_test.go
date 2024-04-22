package changefeedccl

import (
	"context"
	stdjson "encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonEncoderJSONNullAsObject(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Parallel()

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

	fmt := changefeedbase.OptFormatJSON
	env := changefeedbase.OptEnvelopeWrapped

	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{
		Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		TableID:           tableDesc.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(tableDesc.GetName()),
	})

	cases := []struct {
		name                       string
		row, prevRow               rowenc.EncDatumRow
		expectedKey, expectedValue []byte
	}{
		{
			name:          "data",
			row:           rowWithData,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"after": {"a":1,"b":{"foo":"bar"}}, "before": null}`),
		},
		{
			name:          "sql null",
			row:           rowWithSQLNull,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"after": {"a":1,"b":null}, "before": null}`),
		},
		{
			name:          "json null",
			row:           rowWithJSONNull,
			expectedKey:   []byte(`[1]`),
			expectedValue: []byte(`{"after": {"a":1,"b":{"__crdb_null_json__":true}}, "before": null}`),
		},
		{
			name:          "json null key",
			row:           rowWithJSONNullKey,
			expectedKey:   []byte(`[{"__crdb_null_json__":true}]`),
			expectedValue: []byte(`{"after": {"a":{"__crdb_null_json__":true},"b":{"foo":"bar"}}, "before": null}`),
		},
		{
			name:          "prev sql null",
			row:           rowWithData,
			prevRow:       rowWithSQLNull,
			expectedValue: []byte(`{"before": {"a":1,"b":null}, "after": {"a":1,"b":{"foo":"bar"}}}`),
		},
		{
			name:          "prev json null",
			row:           rowWithData,
			prevRow:       rowWithJSONNull,
			expectedValue: []byte(`{"after": {"a":1,"b":{"foo":"bar"}}, "before": {"a":1,"b":{"__crdb_null_json__":true}}}`),
		},
	}

	opts := changefeedbase.EncodingOptions{Format: fmt, Envelope: env, Diff: true, JSONValueNullAsObject: true}
	require.NoError(t, opts.Validate())

	for _, c := range cases {
		c := c // i don't think this is necessary under go 1.22+ but bazel won't let me omit it..
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			e, err := getEncoder(opts, targets, false, nil, nil)
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

func normalizeJson(t *testing.T, b []byte) []byte {
	var v interface{}
	require.NoError(t, stdjson.Unmarshal(b, &v))
	norm, err := stdjson.Marshal(v)
	require.NoError(t, err)
	return norm
}
