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
	"github.com/cockroachdb/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func Test_ProtoEncoderAllTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	typesToTest := make([]*types.T, 0, 256)

	var overrideRandGen func(*types.T) (tree.Datum, *types.T)
	overrideRandGen = func(typ *types.T) (tree.Datum, *types.T) {
		switch typ.Family() {
		case types.TimestampFamily, types.TimestampTZFamily, types.DateFamily:
			datum, err := generateClampedTemporal(rng, typ.Family())
			require.NoError(t, err)
			return datum, typ

		case types.ArrayFamily:
			elemTyp := typ.ArrayContents()
			darr := tree.NewDArray(elemTyp)
			numElems := 2 + rng.Intn(3)
			for i := 0; i < numElems; i++ {
				elemDatum, _ := overrideRandGen(elemTyp)
				if elemDatum == nil {
					elemDatum = randgen.RandDatum(rng, elemTyp, false)
				}
				_ = darr.Append(elemDatum)
			}
			return darr, typ

		default:
			return nil, typ
		}
	}

	for _, typ := range types.OidToType {
		if skipType(typ) {
			continue
		}
		var datum tree.Datum
		var err error
		switch typ.Family() {
		case types.TimestampFamily, types.TimestampTZFamily, types.DateFamily:
			datum, err = generateClampedTemporal(rng, typ.Family())
			require.NoError(t, err)
		default:
			datum = randgen.RandDatum(rng, typ, false /* nullOk */)
		}

		// Skipping nulls as Protobuf omits null values during serialization.
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

	// Adding enums
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

	for _, typ := range typesToTest {
		datum, usedTyp := overrideRandGen(typ)
		if datum == nil {
			datum = randgen.RandDatum(rng, typ, false)
		}
		if datum == tree.DNull {
			continue
		}

		tests = append(tests, struct {
			sqlType string
			datum   tree.Datum
			typ     *types.T
		}{
			sqlType: usedTyp.SQLString(),
			datum:   datum,
			typ:     typ,
		})
	}
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.sqlType, func(t *testing.T) {
			tableDesc, err := parseTableDesc(fmt.Sprintf(`CREATE TABLE test (id INT PRIMARY KEY, val %s)`, tc.sqlType))
			require.NoError(t, err)

			targets := mkTargets(tableDesc)
			row := cdcevent.TestingMakeEventRow(tableDesc, 0, rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
				rowenc.DatumToEncDatum(tc.typ, tc.datum),
			}, false)

			opts := changefeedbase.EncodingOptions{Envelope: changefeedbase.OptEnvelopeBare}
			enc := newProtobufEncoder(ctx, protobufEncoderOptions{EncodingOptions: opts}, targets, nil)

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

			if dEnum, ok := tc.datum.(*tree.DEnum); ok {
				require.Equal(t, dEnum.LogicalRep, expectedDatum.(*tree.DEnum).LogicalRep)
			} else {
				cmp, err := tc.datum.Compare(ctx, evalCtx, expectedDatum)
				require.NoError(t, err)
				require.Equal(t, 0, cmp, "expected %v, got %v", tc.datum, expectedDatum)
			}
		})
	}
}

func generateClampedTemporal(rng *rand.Rand, family types.Family) (tree.Datum, error) {
	t := time.Unix(0, rng.Int63()).UTC().Truncate(time.Microsecond)

	var min, max time.Time
	switch family {
	case types.DateFamily:
		min = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
		max = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)

	case types.TimestampFamily, types.TimestampTZFamily:
		min = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
		max = time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC)
	default:
		return nil, errors.AssertionFailedf("family type %v unrecognized for date/timestamp", family)
	}
	if t.Before(min) {
		t = min
	}
	if t.After(max) {
		t = max
	}

	// Return appropriate Datum
	switch family {
	case types.DateFamily:
		pgd, _ := pgdate.MakeDateFromTime(t)
		d := tree.MakeDDate(pgd)
		return &d, nil

	case types.TimestampFamily:
		return tree.MustMakeDTimestamp(t, time.Microsecond), nil

	case types.TimestampTZFamily:
		return tree.MustMakeDTimestampTZ(t, time.Microsecond), nil
	}
	return nil, errors.AssertionFailedf("family type %v unrecognized for date/timestamp", family)
}

// skipType returns true for any type we don’t want to test.
func skipType(typ *types.T) bool {
	switch typ.Family() {
	case types.PGVectorFamily:
		return true
	}
	if typ.Oid() == oid.T_int2vector || typ.Oid() == oid.T_oidvector {
		return true
	}

	// 2) Exclude families we don’t support at all.
	switch typ.Family() {
	case types.AnyFamily, types.OidFamily, types.TriggerFamily:
		return true
	}

	// 3) Bit family: only zero-width is unsupportable.
	if typ.Family() == types.BitFamily {
		return typ.Width() == 0
	}

	// 4) Arrays: skip if element is JSON as these are not supported.
	if typ.Family() == types.ArrayFamily {
		elem := typ.ArrayContents()
		if elem.Family() == types.JsonFamily {
			return true
		}
		if skipType(elem) {
			return true
		}
		return !randgen.IsAllowedForArray(elem)
	}

	return !randgen.IsLegalColumnType(typ)
}

func convertProtobufValueToDatum(
	val *changefeedpb.Value, typ *types.T, evalCtx *eval.Context,
) (tree.Datum, error) {
	if val == nil {
		return tree.DNull, nil
	}
	switch typ.Family() {
	case types.GeographyFamily:
		ewkb := val.GetBytesValue()
		g, err := geo.ParseGeographyFromEWKBUnsafe(geopb.EWKB(ewkb))
		if err != nil {
			return nil, err
		}
		return tree.NewDGeography(g), nil

	case types.GeometryFamily:
		ewkb := val.GetBytesValue()
		g, err := geo.ParseGeometryFromEWKBUnsafe(geopb.EWKB(ewkb))
		if err != nil {
			return nil, err
		}
		return tree.NewDGeometry(g), nil
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
		return tree.NewDRefCursor(val.GetStringValue()), nil
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
		rec := val.GetTupleValue()
		if rec == nil {
			return nil, errors.New("expected record for tuple, got nil")
		}
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
	enc := newProtobufEncoder(ctx, protobufEncoderOptions{EncodingOptions: opts}, targets, nil)

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

	encoder := newProtobufEncoder(context.Background(), encOpts, mkTargets(tableDesc), nil)

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

func TestProtoEncoder_ResolvedEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tableDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	require.NoError(t, err)
	targets := mkTargets(tableDesc)

	ts := hlc.Timestamp{WallTime: 123, Logical: 456}

	tests := []struct {
		name          string
		envelopeType  changefeedbase.EnvelopeType
		expectWrapped bool
	}{
		{
			name:         "wrapped envelope",
			envelopeType: changefeedbase.OptEnvelopeWrapped,
		},
		{
			name:         "bare envelope",
			envelopeType: changefeedbase.OptEnvelopeBare,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := changefeedbase.EncodingOptions{
				Envelope: tc.envelopeType,
				Format:   changefeedbase.OptFormatProtobuf,
			}

			enc, err := getEncoder(context.Background(), opts, targets, false, nil, nil, nil)
			require.NoError(t, err)

			b, err := enc.EncodeResolvedTimestamp(context.Background(), "test-topic", ts)
			require.NoError(t, err)

			var msg changefeedpb.Message
			require.NoError(t, protoutil.Unmarshal(b, &msg))

			switch tc.envelopeType {
			case changefeedbase.OptEnvelopeWrapped:
				res := msg.GetResolved()
				require.NotNil(t, res, "wrapped envelope should populate Resolved field")
				require.Equal(t, ts.AsOfSystemTime(), res.Resolved)
			case changefeedbase.OptEnvelopeBare:
				res := msg.GetBareResolved()
				require.NotNil(t, res, "bare envelope should populate BareResolved field")
				require.Equal(t, ts.AsOfSystemTime(), res.XCrdb__.Resolved)
			default:
				t.Fatalf("unexpected envelope type: %v", tc.envelopeType)
			}
		})
	}
}
