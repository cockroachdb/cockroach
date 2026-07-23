// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyside_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	roundtripDatum := func(d tree.Datum, dir encoding.Direction) string {
		b, err := keyside.Encode(nil, d, dir)
		if err != nil {
			return "error: " + err.Error()
		}
		newD, leftoverBytes, err := keyside.Decode(a, d.ResolvedType(), b, dir)
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
	}
	properties.Property("roundtrip", prop.ForAll(
		roundtripDatum,
		genColumnType().
			SuchThat(hasKeyEncoding).
			FlatMap(genDatumWithType, reflect.TypeOf((*tree.Datum)(nil)).Elem()),
		genEncodingDirection(),
	))

	// Also run the property on arrays possibly containing NULL values.
	// The random generator in the property above does not generate NULLs.
	properties.Property("roundtrip-arrays", prop.ForAll(
		roundtripDatum,
		genRandomArrayType().
			SuchThat(hasKeyEncoding).
			FlatMap(genArrayDatumWithType, reflect.TypeOf((*tree.Datum)(nil)).Elem()),
		genEncodingDirection(),
	))

	generateAndCompareDatums := func(datums []tree.Datum, dir encoding.Direction) string {
		d1 := datums[0]
		d2 := datums[1]
		b1, err := keyside.Encode(nil, d1, dir)
		if err != nil {
			return "error: " + err.Error()
		}
		b2, err := keyside.Encode(nil, d2, dir)
		if err != nil {
			return "error: " + err.Error()
		}

		expectedCmp, err := d1.Compare(context.Background(), ctx, d2)
		if err != nil {
			return "error: " + err.Error()
		}
		cmp := bytes.Compare(b1, b2)

		if expectedCmp == 0 {
			if cmp != 0 {
				return fmt.Sprintf("equal inputs produced inequal outputs: \n%v\n%v", b1, b2)
			}
			// If the inputs are equal and so are the outputs, no more checking to do.
			return ""
		}

		cmpsMatch := expectedCmp == cmp
		dirIsAscending := dir == encoding.Ascending

		if cmpsMatch != dirIsAscending {
			return fmt.Sprintf("non-order preserving encoding: \n%v\n%v", b1, b2)
		}
		return ""
	}

	properties.Property("order-preserving", prop.ForAll(
		generateAndCompareDatums,
		// For each column type, generate two datums of that type.
		genColumnType().
			SuchThat(hasKeyEncoding).
			FlatMap(
				func(t interface{}) gopter.Gen {
					colTyp := t.(*types.T)
					return gopter.CombineGens(
						genDatumWithType(colTyp),
						genDatumWithType(colTyp))
				}, reflect.TypeOf([]interface{}{})).
			Map(func(datums []interface{}) []tree.Datum {
				ret := make([]tree.Datum, len(datums))
				for i, d := range datums {
					ret[i] = d.(tree.Datum)
				}
				return ret
			}),
		genEncodingDirection(),
	))

	// Also run the property on arrays possibly containing NULL values.
	// The random generator in the property above does not generate NULLs.
	properties.Property("order-preserving-arrays", prop.ForAll(
		generateAndCompareDatums,
		// For each column type, generate two datums of that type.
		genRandomArrayType().
			SuchThat(hasKeyEncoding).
			FlatMap(
				func(t interface{}) gopter.Gen {
					colTyp := t.(*types.T)
					return gopter.CombineGens(
						genArrayDatumWithType(colTyp),
						genArrayDatumWithType(colTyp))
				}, reflect.TypeOf([]interface{}{})).
			Map(func(datums []interface{}) []tree.Datum {
				ret := make([]tree.Datum, len(datums))
				for i, d := range datums {
					ret[i] = d.(tree.Datum)
				}
				return ret
			}),
		genEncodingDirection(),
	))

	properties.TestingRun(t)
}

func TestSkip(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	properties.Property("correctness", prop.ForAll(
		func(d tree.Datum, dir encoding.Direction) string {
			b, err := keyside.Encode(nil, d, dir)
			if err != nil {
				return "error: " + err.Error()
			}
			res, err := keyside.Skip(b)
			if err != nil {
				return "error: " + err.Error()
			}
			if len(res) != 0 {
				fmt.Println(res, len(res), d.ResolvedType(), d.ResolvedType().Family())
				return "expected 0 bytes remaining"
			}
			return ""
		},
		genColumnType().
			SuchThat(hasKeyEncoding).FlatMap(genDatumWithType, reflect.TypeOf((*tree.Datum)(nil)).Elem()),
		genEncodingDirection(),
	))
	properties.TestingRun(t)
}

// TestDecodeOutOfRangeTimestamp deliberately tests out of range timestamps
// can still be decoded from disk. See #46973.
func TestDecodeOutOfRangeTimestamp(t *testing.T) {
	for _, d := range []tree.Datum{
		&tree.DTimestamp{Time: timeutil.Unix(-9223372036854775808, 0).In(time.UTC)},
		&tree.DTimestampTZ{Time: timeutil.Unix(-9223372036854775808, 0).In(time.UTC)},
	} {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			t.Run(fmt.Sprintf("%s/direction:%d", d.String(), dir), func(t *testing.T) {
				encoded, err := keyside.Encode([]byte{}, d, dir)
				require.NoError(t, err)
				a := &tree.DatumAlloc{}
				decoded, _, err := keyside.Decode(a, d.ResolvedType(), encoded, dir)
				require.NoError(t, err)
				require.Equal(t, d, decoded)
			})
		}
	}
}

func genColumnType() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		columnType := randgen.RandColumnType(genParams.Rng)
		return gopter.NewGenResult(columnType, gopter.NoShrinker)
	}
}

func genRandomArrayType() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		arrType := randgen.RandArrayType(genParams.Rng)
		return gopter.NewGenResult(arrType, gopter.NoShrinker)
	}
}

func genDatumWithType(columnType interface{}) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		datum := randgen.RandDatum(genParams.Rng, columnType.(*types.T), false)
		return gopter.NewGenResult(datum, gopter.NoShrinker)
	}
}

func genArrayDatumWithType(arrTyp interface{}) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		// Mark the array contents to have a 1 in 10 chance of being null.
		datum := randgen.RandArray(genParams.Rng, arrTyp.(*types.T), 10)
		return gopter.NewGenResult(datum, gopter.NoShrinker)
	}
}

func genEncodingDirection() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(
			encoding.Direction((genParams.Rng.Int()%int(encoding.Descending))+1),
			gopter.NoShrinker)
	}
}

func hasKeyEncoding(typ *types.T) bool {
	switch typ.Family() {
	// Special case needed for CollatedStringFamily and DecimalFamily which do have
	// a key encoding but do not roundtrip.
	case types.CollatedStringFamily, types.DecimalFamily:
		return false
	case types.ArrayFamily:
		return hasKeyEncoding(typ.ArrayContents())
	}
	return !colinfo.MustBeValueEncoded(typ)
}
