// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/collatedstring"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func Test_ProtoEncoderAllTypes(t *testing.T) {
	// test set up
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	ctx := context.Background()

	rng, _ := randutil.NewTestRand()
	typesToTest := make([]*types.T, 0, 256)

	overrideRandGen := func(typ *types.T) (tree.Datum, *types.T) {
		switch typ.Family() {
		//google timestamps only support a specific range
		case types.TimestampFamily, types.TimestampTZFamily:
			t := timeutil.Unix(0, rng.Int63()).UTC()
			min := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			max := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

			if t.Before(min) {
				t = min
			}
			if t.After(max) {
				t = max
			}

			if typ.Family() == types.TimestampFamily {
				return tree.MustMakeDTimestamp(t, time.Microsecond), typ
			}
			return tree.MustMakeDTimestampTZ(t, time.Microsecond), typ

		default:
			return nil, typ
		}
	}

	var skipType func(typ *types.T) bool
	skipType = func(typ *types.T) bool {
		switch typ.Family() {
		case types.AnyFamily,
			types.OidFamily,
			types.TriggerFamily:
			// types.TupleFamily --> not exepcted to be needed for chnagefeeds but its in the encoder
			// types.JsonpathFamily,
			// types.PGVectorFamily:
			return true

		case types.BitFamily:
			return typ.Width() == 0
		case types.ArrayFamily:
			if skipType(typ.ArrayContents()) {
				return true
			}
			return !randgen.IsAllowedForArray(typ.ArrayContents())
		}
		switch typ.Oid() {
		case oid.T_int2vector, oid.T_oidvector:
			return true
		}
		return !randgen.IsLegalColumnType(typ)
	}

	for _, typ := range types.OidToType {
		if skipType(typ) {
			continue
		}
		datum, typ := overrideRandGen(typ)
		if datum == nil {
			datum = randgen.RandDatum(rng, typ, false /* nullOk */)
		}
		if datum == tree.DNull {
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
		tree.EnumValueList{tree.EnumValue("open"), tree.EnumValue("closed")},
		tree.MakeUnqualifiedTypeName("switch"),
	)
	typesToTest = append(typesToTest, testEnum)

	tests := []struct {
		sqlType string
		datum   tree.Datum
		typ     *types.T
	}{
		{"INT", tree.NewDInt(123), types.Int},
		{"STRING", tree.NewDString("test-val"), types.String},
		{"FLOAT8", tree.NewDFloat(3.1415), types.Float},
		{"BOOL", tree.DBoolTrue, types.Bool},
	}

	// When more envelopes are supported, add them
	envelopes := []changefeedbase.EnvelopeType{
		changefeedbase.OptEnvelopeBare,
	}

	for _, typ := range typesToTest {
		datum := randgen.RandDatum(rng, typ, false /* nullOk */)
		if datum == tree.DNull {
			continue // skip nulls
		}
		tests = append(tests, struct {
			sqlType string
			datum   tree.Datum
			typ     *types.T
		}{
			sqlType: typ.SQLString(),
			datum:   datum,
			typ:     typ,
		})
	}

	for _, tc := range tests {
		for _, envelope := range envelopes {
			t.Run(tc.sqlType, func(t *testing.T) {
				createStmt := fmt.Sprintf(`CREATE TABLE test (id INT PRIMARY KEY, val %s)`, tc.sqlType)
				tableDesc, err := parseTableDesc(createStmt)
				require.NoError(t, err)

				targets := mkTargets(tableDesc)
				row := cdcevent.TestingMakeEventRow(tableDesc, 0, rowenc.EncDatumRow{
					rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
					rowenc.DatumToEncDatum(tc.typ, tc.datum),
				}, false)

				opts := changefeedbase.EncodingOptions{Envelope: envelope}
				enc := newProtobufEncoder(ctx, protobufEncoderOptions{EncodingOptions: opts}, targets)

				evCtx := eventContext{updated: hlc.Timestamp{WallTime: 42}}
				valBytes, err := enc.EncodeValue(ctx, evCtx, row, cdcevent.Row{})
				require.NoError(t, err)

				var msg changefeedpb.Message
				require.NoError(t, protoutil.Unmarshal(valBytes, &msg))
				bare := msg.GetBare()
				require.NotNil(t, bare.Values)

				gotDatum := bare.Values["val"]

				expectedDatum, err := convertProtobufValueToDatum(gotDatum, tc.typ, evalCtx)
				require.NoError(t, err)

				cmp, err := tc.datum.Compare(ctx, evalCtx, expectedDatum)
				require.NoError(t, err)
				require.Equal(t, 0, cmp, "expected %v, got %v", tc.datum, expectedDatum)

			})
		}
	}
}

func convertProtobufValueToDatum(
	val *changefeedpb.Value, typ *types.T, evalCtx *eval.Context,
) (tree.Datum, error) {
	if val == nil {
		return tree.DNull, nil
	}

	switch typ.Family() {
	case types.GeographyFamily:
		return tree.ParseDGeography(val.GetStringValue())
	case types.GeometryFamily:
		return tree.ParseDGeometry(val.GetStringValue())
	case types.BitFamily:
		return tree.ParseDBitArray(val.GetStringValue())
	case types.INetFamily:
		return tree.ParseDIPAddrFromINetString(val.GetStringValue())
	case types.PGLSNFamily:
		return tree.ParseDPGLSN(val.GetStringValue())
	case types.Box2DFamily:
		return tree.ParseDBox2D(val.GetStringValue())
	case types.TSVectorFamily:
		return tree.ParseDTSVector(val.GetStringValue())
	case types.RefCursorFamily:
		return tree.NewDString(val.GetStringValue()), nil
	case types.TimeFamily:
		datum, _, err := tree.ParseDTime(nil, val.GetTimeValue(), time.Microsecond)
		return datum, err
	case types.TimeTZFamily:
		datum, _, err := tree.ParseDTimeTZ(nil, val.GetTimeValue(), time.Microsecond)
		return datum, err
	case types.TSQueryFamily:
		return tree.ParseDTSQuery(val.GetStringValue())
	case types.PGVectorFamily:
		return tree.ParseDPGVector(val.GetStringValue())
	case types.OidFamily:
		parsed, err := strconv.ParseUint(val.GetStringValue(), 10, 32)
		if err != nil {
			return nil, err
		}
		return tree.NewDOid(oid.Oid(parsed)), nil

	case types.VoidFamily:
		return tree.DNull, nil

	case types.CollatedStringFamily:
		return tree.NewDCollatedString(val.GetStringValue(), typ.Locale(), &tree.CollationEnvironment{})
	case types.IntervalFamily:
		return tree.ParseDInterval(duration.IntervalStyle_POSTGRES, val.GetIntervalValue())
	case types.IntFamily:
		return tree.NewDInt(tree.DInt(val.GetInt64Value())), nil
	case types.StringFamily:
		return tree.NewDString(val.GetStringValue()), nil
	case types.BoolFamily:
		return tree.MakeDBool(tree.DBool(val.GetBoolValue())), nil
	case types.FloatFamily:
		return tree.NewDFloat(tree.DFloat(val.GetDoubleValue())), nil
	case types.DecimalFamily:
		dec := &apd.Decimal{}
		if err := dec.UnmarshalText([]byte(val.GetDecimalValue().Value)); err != nil {
			return nil, err
		}
		return &tree.DDecimal{Decimal: *dec}, nil
	case types.BytesFamily:
		return tree.NewDBytes(tree.DBytes(val.GetBytesValue())), nil
	case types.DateFamily:
		d, _, err := tree.ParseDDate(nil, val.GetDateValue())
		if err != nil {
			return nil, err
		}
		return d, nil
	case types.TimestampFamily:
		ts := val.GetTimestampValue()
		if ts == nil {
			return nil, errors.New("missing timestamp_value")
		}
		t := timeutil.Unix(ts.Seconds, int64(ts.Nanos))
		return tree.MakeDTimestamp(t, time.Microsecond)

	case types.TimestampTZFamily:
		ts := val.GetTimestampValue()
		if ts == nil {
			return nil, errors.New("missing timestamp_value")
		}
		t := timeutil.Unix(ts.Seconds, int64(ts.Nanos))
		return tree.MakeDTimestampTZ(t, time.Microsecond)

	case types.UuidFamily:
		parsed, err := uuid.FromString(val.GetUuidValue())
		if err != nil {
			return nil, err
		}
		return tree.NewDUuid(tree.DUuid{UUID: parsed}), nil
	case types.JsonFamily:
		j, err := json.ParseJSON(val.GetStringValue())
		if err != nil {
			return nil, err
		}
		return tree.NewDJSON(j), nil
	case types.EnumFamily:
		logical := val.GetStringValue()
		return tree.NewDEnum(tree.DEnum{
			LogicalRep: logical,
			EnumTyp:    typ,
		}), nil

	case types.ArrayFamily:
		elemVals := val.GetArrayValue().Values
		darr := tree.NewDArray(typ.ArrayContents())
		for _, elem := range elemVals {
			elemDatum, err := convertProtobufValueToDatum(elem, typ.ArrayContents(), evalCtx)
			if err != nil {
				return nil, err
			}
			if err := darr.Append(elemDatum); err != nil {
				return nil, err
			}
		}
		return darr, nil
	case types.TupleFamily:
		// extract tuple
		rec := val.GetTupleValue()
		if rec == nil {
			return nil, errors.New("expected record for tuple, got nil")
		}
		// get labels
		labels := typ.TupleLabels()
		contents := typ.TupleContents()
		tupleElems := make(tree.Datums, len(contents))

		for i := range contents {
			var fieldName string
			if labels != nil && labels[i] != "" {
				fieldName = labels[i]
			} else {
				fieldName = fmt.Sprintf("f%d", i)
			}

			subVal, ok := rec.Values[fieldName]
			if !ok {
				return nil, errors.Errorf("missing tuple field: %s", fieldName)
			}

			subDatum, err := convertProtobufValueToDatum(subVal, contents[i], evalCtx)
			if err != nil {
				return nil, errors.Wrapf(err, "decoding tuple field %q", fieldName)
			}

			tupleElems[i] = subDatum
		}

		return tree.NewDTuple(typ, tupleElems...), nil

	default:
		return nil, errors.Newf("unsupported Protobuf → Datum conversion for type: %s", typ.SQLString())
	}
}
func Test_ProtoEncoder_Escaping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	createStmt := `CREATE TABLE "☃" ("🍦" INT PRIMARY KEY)`
	tableDesc, err := parseTableDesc(createStmt)
	require.NoError(t, err)

	targets := mkTargets(tableDesc)
	row := cdcevent.TestingMakeEventRow(tableDesc, 0, rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(123)),
	}, false)

	opts := changefeedbase.EncodingOptions{Envelope: changefeedbase.OptEnvelopeBare}
	enc := newProtobufEncoder(ctx, protobufEncoderOptions{EncodingOptions: opts}, targets)

	valBytes, err := enc.EncodeValue(ctx, eventContext{updated: hlc.Timestamp{WallTime: 42}}, row, cdcevent.Row{})
	require.NoError(t, err)

	var msg changefeedpb.Message
	require.NoError(t, protoutil.Unmarshal(valBytes, &msg))
	bare := msg.GetBare()
	require.NotNil(t, bare.Values)

	// Check that the key "🍦" exists and the value roundtrips
	val, ok := bare.Values["🍦"]
	require.True(t, ok, "Expected to find encoded column name 🍦")
	require.Equal(t, int64(123), val.GetInt64Value())
}

// func TestProtobufValueGoldens(t *testing.T) {
//  goldens := []struct {
//      sqlType  string
//      sql      string
//      expected *changefeedpb.Value
//  }{
//      {sqlType: "INT", sql: "1", expected: &changefeedpb.Value{Value: &changefeedpb.Value_Int64Value{Int64Value: 1}}},
//      {sqlType: "INT", sql: "NULL", expected: &changefeedpb.Value{}},
//      {sqlType: "STRING", sql: "'hello'", expected: &changefeedpb.Value{Value: &changefeedpb.Value_StringValue{StringValue: "hello"}}},
//      {sqlType: "STRING", sql: "'null'", expected: &changefeedpb.Value{}},
//      {sqlType: "BOOL", sql: "true", expected: &changefeedpb.Value{Value: &changefeedpb.Value_BoolValue{BoolValue: true}}},
//      {sqlType: "FLOAT", sql: "1.23", expected: &changefeedpb.Value{Value: &changefeedpb.Value_DoubleValue{DoubleValue: 1.23}}},

//      // Add more from supported types
//  }

//  for _, g := range goldens {
//      t.Run(g.sqlType+"_"+g.sql, func(t *testing.T) {
//          tableDesc, err := parseTableDesc(fmt.Sprintf(`CREATE TABLE t (a INT PRIMARY KEY, b %s)`, g.sqlType))
//          require.NoError(t, err)

//          encDatums, err := parseValues(tableDesc, fmt.Sprintf("VALUES (1, %s)", g.sql))
//          require.NoError(t, err)

//          val, err := datumToProtoValue(encDatums[0][1].Datum)
//          require.NoError(t, err)
//          require.Equal(t, g.expected, val)
//      })
//  }
// }
// func runProtoRoundtripTest(t *testing.T, sqlType string, datum tree.Datum) {
//  t.Helper()
//  createStmt := fmt.Sprintf(`CREATE TABLE test (id INT PRIMARY KEY, val %s)`, sqlType)
//  tableDesc, err := parseTableDesc(createStmt)
//  require.NoError(t, err)

//  targets := mkTargets(tableDesc)
//  row := cdcevent.TestingMakeEventRow(tableDesc, 0, rowenc.EncDatumRow{
//      rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
//      rowenc.DatumToEncDatum(datum.ResolvedType(), datum),
//  }, false)

//  opts := changefeedbase.EncodingOptions{Envelope: changefeedbase.OptEnvelopeBare}
//  enc := newProtobufEncoder(context.Background(), protobufEncoderOptions{EncodingOptions: opts}, targets)

//  valBytes, err := enc.EncodeValue(context.Background(), eventContext{updated: hlc.Timestamp{WallTime: 42}}, row, cdcevent.Row{})
//  require.NoError(t, err)

//  var msg changefeedpb.Message
//  require.NoError(t, protoutil.Unmarshal(valBytes, &msg))
//  gotDatum := msg.GetBare().Values["val"]

//  decoded, err := convertProtobufValueToDatum(gotDatum, datum.ResolvedType(), evalCtx)
//  require.NoError(t, err)

//  cmp, err := datum.Compare(context.Background(), evalCtx, decoded)
//  require.NoError(t, err)
//  require.Equal(t, 0, cmp, "mismatch for type %s", sqlType)
// }

func TestProtoEncoder_BareEnvelope_WithMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tableDesc, err := parseTableDesc(`CREATE TABLE test (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	encRow := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
		rowenc.DatumToEncDatum(types.String, tree.NewDString("Alice")),
	}
	row := cdcevent.TestingMakeEventRow(tableDesc, 0, encRow, false)

	evCtx := eventContext{
		updated: hlc.Timestamp{WallTime: 123},
		mvcc:    hlc.Timestamp{WallTime: 456},
		topic:   "test-topic",
	}

	encOpts := protobufEncoderOptions{
		EncodingOptions: changefeedbase.EncodingOptions{
			Envelope:          changefeedbase.OptEnvelopeBare,
			UpdatedTimestamps: true,
			MVCCTimestamps:    true,
			KeyInValue:        true,
			TopicInValue:      true,
		},
	}

	encoder := newProtobufEncoder(context.Background(), encOpts, mkTargets(tableDesc))

	valueBytes, err := encoder.EncodeValue(context.Background(), evCtx, row, cdcevent.Row{})
	require.NoError(t, err)

	msg := new(changefeedpb.Message)
	require.NoError(t, protoutil.Unmarshal(valueBytes, msg))

	bare := msg.GetBare()
	require.NotNil(t, bare)
	require.NotNil(t, bare.XCrdb__)

	require.Equal(t, evCtx.updated.AsOfSystemTime(), bare.XCrdb__.Updated)
	require.Equal(t, evCtx.mvcc.AsOfSystemTime(), bare.XCrdb__.MvccTimestamp)
	require.Equal(t, int64(1), bare.XCrdb__.Key.Key["id"].GetInt64Value())
	require.Equal(t, "test-topic", bare.XCrdb__.Topic)
	require.Equal(t, int64(1), bare.Values["id"].GetInt64Value())
	require.Equal(t, "Alice", bare.Values["name"].GetStringValue())
}
