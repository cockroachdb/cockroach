// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"math/rand"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

var (
	// SeedTypes includes the following types that form the basis of randomly
	// generated types:
	//   - All scalar types, except UNKNOWN and ANY
	//   - ARRAY of ANY, where the ANY will be replaced with one of the legal
	//     array element types in RandType
	//   - OIDVECTOR and INT2VECTOR types
	SeedTypes []*types.T

	// arrayContentsTypes contains all of the types that are valid to store within
	// an array.
	arrayContentsTypes []*types.T
	collationLocales   = [...]string{"da", "de", "en"}
)

func init() {
	for _, typ := range types.OidToType {
		switch typ.Oid() {
		case oid.T_unknown, oid.T_anyelement:
			// Don't include these.
		case oid.T_anyarray, oid.T_oidvector, oid.T_int2vector:
			// Include these.
			SeedTypes = append(SeedTypes, typ)
		default:
			// Only include scalar types.
			if typ.Family() != types.ArrayFamily {
				SeedTypes = append(SeedTypes, typ)
			}
		}
	}

	for _, typ := range types.OidToType {
		if IsAllowedForArray(typ) {
			arrayContentsTypes = append(arrayContentsTypes, typ)
		}
	}

	// Sort these so randomly chosen indexes always point to the same element.
	sort.Slice(SeedTypes, func(i, j int) bool {
		return SeedTypes[i].String() < SeedTypes[j].String()
	})
	sort.Slice(arrayContentsTypes, func(i, j int) bool {
		return arrayContentsTypes[i].String() < arrayContentsTypes[j].String()
	})
}

// IsAllowedForArray returns true iff the passed in type can be a valid ArrayContents()
func IsAllowedForArray(typ *types.T) bool {
	// Don't include un-encodable types.
	encTyp, err := rowenc.DatumTypeToArrayElementEncodingType(typ)
	if err != nil || encTyp == 0 {
		return false
	}

	// Don't include reg types, since parser currently doesn't allow them to
	// be declared as array element types.
	if typ.Family() == types.OidFamily && typ.Oid() != oid.T_oid {
		return false
	}

	return true
}

// RandType returns a random type value.
func RandType(rng *rand.Rand) *types.T {
	return RandTypeFromSlice(rng, SeedTypes)
}

// RandArrayContentsType returns a random type that's guaranteed to be valid to
// use as the contents of an array.
func RandArrayContentsType(rng *rand.Rand) *types.T {
	return RandTypeFromSlice(rng, arrayContentsTypes)
}

// RandTypeFromSlice returns a random type from the input slice of types.
func RandTypeFromSlice(rng *rand.Rand, typs []*types.T) *types.T {
	typ := typs[rng.Intn(len(typs))]
	switch typ.Family() {
	case types.BitFamily:
		return types.MakeBit(int32(rng.Intn(50)))
	case types.CollatedStringFamily:
		return types.MakeCollatedString(types.String, *RandCollationLocale(rng))
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.AnyFamily {
			inner := RandArrayContentsType(rng)
			if inner.Family() == types.CollatedStringFamily {
				// TODO(justin): change this when collated arrays are supported.
				inner = types.String
			}
			return types.MakeArray(inner)
		}
	case types.TupleFamily:
		// Generate tuples between 0 and 4 datums in length
		len := rng.Intn(5)
		contents := make([]*types.T, len)
		for i := range contents {
			contents[i] = RandType(rng)
		}
		return types.MakeTuple(contents)
	}
	return typ
}

// RandColumnType returns a random type that is a legal column type (e.g. no
// nested arrays or tuples).
func RandColumnType(rng *rand.Rand) *types.T {
	for {
		typ := RandType(rng)
		if IsLegalColumnType(typ) {
			return typ
		}
	}
}

// IsLegalColumnType returns true if the given type can be
// given to a column in a user-created table.
func IsLegalColumnType(typ *types.T) bool {
	switch typ.Oid() {
	case oid.T_int2vector, oid.T_oidvector:
		// OIDVECTOR and INT2VECTOR are not valid column types for
		// user-created tables.
		return false
	}
	return colinfo.ValidateColumnDefType(typ) == nil
}

// RandArrayType generates a random array type.
func RandArrayType(rng *rand.Rand) *types.T {
	for {
		typ := RandColumnType(rng)
		resTyp := types.MakeArray(typ)
		if err := colinfo.ValidateColumnDefType(resTyp); err == nil {
			return resTyp
		}
	}
}

// RandColumnTypes returns a slice of numCols random types. These types must be
// legal table column types.
func RandColumnTypes(rng *rand.Rand, numCols int) []*types.T {
	types := make([]*types.T, numCols)
	for i := range types {
		types[i] = RandColumnType(rng)
	}
	return types
}

// RandSortingType returns a column type which can be key-encoded.
func RandSortingType(rng *rand.Rand) *types.T {
	typ := RandType(rng)
	for colinfo.MustBeValueEncoded(typ) {
		typ = RandType(rng)
	}
	return typ
}

// RandSortingTypes returns a slice of numCols random ColumnType values
// which are key-encodable.
func RandSortingTypes(rng *rand.Rand, numCols int) []*types.T {
	types := make([]*types.T, numCols)
	for i := range types {
		types[i] = RandSortingType(rng)
	}
	return types
}

// RandCollationLocale returns a random element of collationLocales.
func RandCollationLocale(rng *rand.Rand) *string {
	return &collationLocales[rng.Intn(len(collationLocales))]
}

// RandEncodableType wraps RandType in order to workaround #36736, which fails
// when name[] (or other type using DTypeWrapper) is encoded.
//
// TODO(andyk): Remove this workaround once #36736 is resolved. Also, RandDatum
// really should be extended to create DTypeWrapper datums with alternate OIDs
// like oid.T_varchar for better testing.
func RandEncodableType(rng *rand.Rand) *types.T {
	var isEncodableType func(t *types.T) bool
	isEncodableType = func(t *types.T) bool {
		switch t.Family() {
		case types.ArrayFamily:
			// Due to #36736, any type returned by RandType that gets turned into
			// a DTypeWrapper random datum will not work. Currently, that's just
			// types.Name.
			if t.ArrayContents().Oid() == oid.T_name {
				return false
			}
			return isEncodableType(t.ArrayContents())

		case types.TupleFamily:
			for i := range t.TupleContents() {
				if !isEncodableType(t.TupleContents()[i]) {
					return false
				}
			}
		}
		return true
	}

	for {
		typ := RandType(rng)
		if isEncodableType(typ) {
			return typ
		}
	}
}

// RandEncodableColumnTypes works around #36736, which fails when name[] (or
// other type using DTypeWrapper) is encoded.
//
// TODO(andyk): Remove this workaround once #36736 is resolved. Replace calls to
// it with calls to RandColumnTypes.
func RandEncodableColumnTypes(rng *rand.Rand, numCols int) []*types.T {
	types := make([]*types.T, numCols)
	for i := range types {
		for {
			types[i] = RandEncodableType(rng)
			if err := colinfo.ValidateColumnDefType(types[i]); err == nil {
				break
			}
		}
	}
	return types
}
