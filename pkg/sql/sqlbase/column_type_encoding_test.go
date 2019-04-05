// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
)

func genColumnType() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(RandColumnType(genParams.Rng), gopter.NoShrinker)
	}
}

func genDatum() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(RandDatum(genParams.Rng, RandColumnType(genParams.Rng),
			false), gopter.NoShrinker)
	}
}

func genDatumWithType(columnType interface{}) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		datum := RandDatum(genParams.Rng, columnType.(ColumnType),
			false)
		if datum == nil {
			fmt.Println("WOT M8", columnType)
		}
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

func hasKeyEncoding(typ ColumnType) bool {
	// Only some types are round-trip key encodable.
	switch typ.SemanticType {
	case ColumnType_JSONB, ColumnType_ARRAY, ColumnType_COLLATEDSTRING, ColumnType_TUPLE, ColumnType_DECIMAL:
		return false
	}
	return true
}

func TestEncodeTableValue(t *testing.T) {
	a := &DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	var scratch []byte
	properties.Property("roundtrip", prop.ForAll(
		func(d tree.Datum) string {
			b, err := EncodeTableValue(nil, 0, d, scratch)
			if err != nil {
				return "error: " + err.Error()
			}
			newD, leftoverBytes, err := DecodeTableValue(a, d.ResolvedType(), b)
			if len(leftoverBytes) > 0 {
				return "Leftover bytes"
			}
			if err != nil {
				return "error: " + err.Error()
			}
			if newD.Compare(ctx, d) != 0 {
				return "unequal"
			}
			return ""
		},
		genDatum(),
	))
	properties.TestingRun(t)
}

func TestEncodeTableKey(t *testing.T) {
	a := &DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	properties.Property("roundtrip", prop.ForAll(
		func(d tree.Datum, dir encoding.Direction) string {
			b, err := EncodeTableKey(nil, d, dir)
			if err != nil {
				return "error: " + err.Error()
			}
			newD, leftoverBytes, err := DecodeTableKey(a, d.ResolvedType(), b, dir)
			if len(leftoverBytes) > 0 {
				return "Leftover bytes"
			}
			if err != nil {
				return "error: " + err.Error()
			}
			if newD.Compare(ctx, d) != 0 {
				return "unequal"
			}
			return ""
		},
		genColumnType().
			SuchThat(hasKeyEncoding).
			FlatMap(genDatumWithType, reflect.TypeOf((*tree.Datum)(nil)).Elem()),
		genEncodingDirection(),
	))
	properties.Property("order-preserving", prop.ForAll(
		func(datums []tree.Datum, dir encoding.Direction) string {
			d1 := datums[0]
			d2 := datums[1]
			b1, err := EncodeTableKey(nil, d1, dir)
			if err != nil {
				return "error: " + err.Error()
			}
			b2, err := EncodeTableKey(nil, d2, dir)
			if err != nil {
				return "error: " + err.Error()
			}

			expectedCmp := d1.Compare(ctx, d2)
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
		},
		// For each column type, generate two datums of that type.
		genColumnType().
			SuchThat(hasKeyEncoding).
			FlatMap(
				func(t interface{}) gopter.Gen {
					colTyp := t.(ColumnType)
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
	properties.TestingRun(t)
}
