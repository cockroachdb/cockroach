// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	a := &tree.DatumAlloc{}
	ctx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	var scratch []byte
	properties.Property("roundtrip", prop.ForAll(
		func(d tree.Datum) string {
			var (
				b   []byte
				err error
			)
			b, scratch, err = valueside.EncodeWithScratch(nil, 0, d, scratch[:0])
			if err != nil {
				return "error: " + err.Error()
			}
			newD, leftoverBytes, err := valueside.Decode(a, d.ResolvedType(), b)
			if len(leftoverBytes) > 0 {
				return "Leftover bytes"
			}
			if err != nil {
				return "error: " + err.Error()
			}
			if cmp, err := newD.Compare(context.Background(), ctx, d); err != nil {
				return "error: " + err.Error()
			} else if cmp != 0 {
				return "unequal"
			}
			return ""
		},
		genDatum(),
	))
	properties.TestingRun(t)
}

func TestDecode(t *testing.T) {
	a := &tree.DatumAlloc{}
	for _, tc := range []struct {
		in  tree.Datum
		typ *types.T
		err string
	}{
		// These test cases are not intended to be exhaustive, but rather exercise
		// the special casing and error handling of Decode.
		{tree.DNull, types.Bool, ""},
		{tree.DBoolTrue, types.Bool, ""},
		{tree.NewDInt(tree.DInt(4)), types.Bool, "value type is not True or False: Int"},
		{tree.DNull, types.Int, ""},
		{tree.NewDInt(tree.DInt(4)), types.Int, ""},
		{tree.DBoolTrue, types.Int, "decoding failed"},
	} {
		t.Run("", func(t *testing.T) {
			var prefix []byte
			buf, err := valueside.Encode(prefix, 0 /* colID */, tc.in)
			if err != nil {
				t.Fatal(err)
			}
			d, _, err := valueside.Decode(a, tc.typ, buf)
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("expected error %q, but got %v", tc.err, err)
			} else if err != nil {
				return
			}
			evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			if cmp, err := tc.in.Compare(context.Background(), evalCtx, d); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatalf("decoded datum %[1]v (%[1]T) does not match encoded datum %[2]v (%[2]T)", d, tc.in)
			}
		})
	}
}

// TestDecodeTableValueOutOfRangeTimestamp deliberately tests out of range timestamps
// can still be decoded from disk. See #46973.
func TestDecodeTableValueOutOfRangeTimestamp(t *testing.T) {
	for _, d := range []tree.Datum{
		&tree.DTimestamp{Time: timeutil.Unix(-9223372036854775808, 0).In(time.UTC)},
		&tree.DTimestampTZ{Time: timeutil.Unix(-9223372036854775808, 0).In(time.UTC)},
	} {
		t.Run(d.String(), func(t *testing.T) {
			var b []byte
			encoded, err := valueside.Encode(b, 1 /* colID */, d)
			require.NoError(t, err)
			a := &tree.DatumAlloc{}
			decoded, _, err := valueside.Decode(a, d.ResolvedType(), encoded)
			require.NoError(t, err)
			require.Equal(t, d, decoded)
		})
	}
}

// This test ensures that decoding a tuple value with a specific, labeled tuple
// type preserves the labels.
func TestDecodeTupleValueWithType(t *testing.T) {
	tupleType := types.MakeLabeledTuple([]*types.T{types.Int, types.String}, []string{"a", "b"})
	datum := tree.NewDTuple(tupleType, tree.NewDInt(tree.DInt(1)), tree.NewDString("foo"))
	buf, err := valueside.Encode(nil, valueside.NoColumnID, datum)
	if err != nil {
		t.Fatal(err)
	}
	da := tree.DatumAlloc{}
	var decoded tree.Datum
	decoded, _, err = valueside.Decode(&da, tupleType, buf)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, decoded, datum)
}

func TestLegacy(t *testing.T) {
	tests := []struct {
		typ   *types.T
		datum tree.Datum
		exp   roachpb.Value
	}{
		{
			typ:   types.Bool,
			datum: tree.MakeDBool(true),
			exp:   func() (v roachpb.Value) { v.SetBool(true); return }(),
		},
		{
			typ:   types.Bool,
			datum: tree.MakeDBool(false),
			exp:   func() (v roachpb.Value) { v.SetBool(false); return }(),
		},
		{
			typ:   types.Int,
			datum: tree.NewDInt(314159),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			typ:   types.Float,
			datum: tree.NewDFloat(3.14159),
			exp:   func() (v roachpb.Value) { v.SetFloat(3.14159); return }(),
		},
		{
			typ: types.Decimal,
			datum: func() (v tree.Datum) {
				v, err := tree.ParseDDecimal("1234567890.123456890")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
			exp: func() (v roachpb.Value) {
				dDecimal, err := tree.ParseDDecimal("1234567890.123456890")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				err = v.SetDecimal(&dDecimal.Decimal)
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
		},
		{
			typ:   types.Date,
			datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159)),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			typ:   types.Date,
			datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(math.MinInt64)),
			exp:   func() (v roachpb.Value) { v.SetInt(math.MinInt64); return }(),
		},
		{
			typ:   types.Date,
			datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(math.MaxInt64)),
			exp:   func() (v roachpb.Value) { v.SetInt(math.MaxInt64); return }(),
		},
		{
			typ:   types.Time,
			datum: tree.MakeDTime(timeofday.FromInt(314159)),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			typ:   types.Timestamp,
			datum: tree.MustMakeDTimestamp(timeutil.Unix(314159, 1000), time.Microsecond),
			exp:   func() (v roachpb.Value) { v.SetTime(timeutil.Unix(314159, 1000)); return }(),
		},
		{
			typ:   types.TimestampTZ,
			datum: tree.MustMakeDTimestampTZ(timeutil.Unix(314159, 1000), time.Microsecond),
			exp:   func() (v roachpb.Value) { v.SetTime(timeutil.Unix(314159, 1000)); return }(),
		},
		{
			typ:   types.String,
			datum: tree.NewDString("testing123"),
			exp:   func() (v roachpb.Value) { v.SetString("testing123"); return }(),
		},
		{
			typ:   types.Name,
			datum: tree.NewDName("testingname123"),
			exp:   func() (v roachpb.Value) { v.SetString("testingname123"); return }(),
		},
		{
			typ:   types.Bytes,
			datum: tree.NewDBytes(tree.DBytes([]byte{0x31, 0x41, 0x59})),
			exp:   func() (v roachpb.Value) { v.SetBytes([]byte{0x31, 0x41, 0x59}); return }(),
		},
		{
			typ: types.Uuid,
			datum: func() (v tree.Datum) {
				v, err := tree.ParseDUuidFromString("63616665-6630-3064-6465-616462656562")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
			exp: func() (v roachpb.Value) {
				dUUID, err := tree.ParseDUuidFromString("63616665-6630-3064-6465-616462656562")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				v.SetBytes(dUUID.GetBytes())
				return
			}(),
		},
		{
			typ: types.INet,
			datum: func() (v tree.Datum) {
				v, err := tree.ParseDIPAddrFromINetString("192.168.0.1")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
			exp: func() (v roachpb.Value) {
				ipAddr, err := tree.ParseDIPAddrFromINetString("192.168.0.1")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				data := ipAddr.ToBuffer(nil)
				v.SetBytes(data)
				return
			}(),
		},
	}

	for i, testCase := range tests {
		typ := testCase.typ
		if actual, err := valueside.MarshalLegacy(typ, testCase.datum); err != nil {
			t.Errorf("%d: unexpected error with column type %v: %v", i, typ, err)
		} else if !reflect.DeepEqual(actual, testCase.exp) {
			t.Errorf("%d: MarshalColumnValue() got %v, expected %v", i, actual, testCase.exp)
		}
	}
}

func TestLegacyRoundtrip(t *testing.T) {
	a := &tree.DatumAlloc{}
	ctx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)

	properties.Property("roundtrip",
		prop.ForAll(
			func(typ *types.T) string {
				d, ok := genDatumWithType(typ).Sample()
				if !ok {
					return "error generating datum"
				}
				datum := d.(tree.Datum)
				value, err := valueside.MarshalLegacy(typ, datum)
				if err != nil {
					return "error marshaling: " + err.Error()
				}
				outDatum, err := valueside.UnmarshalLegacy(a, typ, value)
				if err != nil {
					return "error unmarshaling: " + err.Error()
				}
				if cmp, err := datum.Compare(context.Background(), ctx, outDatum); err != nil {
					return "error: " + err.Error()
				} else if cmp != 0 {
					return fmt.Sprintf("datum didn't roundtrip.\ninput: %v\noutput: %v", datum, outDatum)
				}
				return ""
			},
			genColumnType(),
		),
	)
	properties.TestingRun(t)
}

func genColumnType() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		columnType := randgen.RandColumnType(genParams.Rng)
		return gopter.NewGenResult(columnType, gopter.NoShrinker)
	}
}

func genDatum() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(randgen.RandDatum(genParams.Rng, randgen.RandColumnType(genParams.Rng),
			false), gopter.NoShrinker)
	}
}

func genDatumWithType(columnType interface{}) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		datum := randgen.RandDatum(genParams.Rng, columnType.(*types.T), false)
		return gopter.NewGenResult(datum, gopter.NoShrinker)
	}
}
