// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colinfo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CheckDatumTypeFitsColumnType verifies that a given scalar value
// type is valid to be stored in a column of the given column type.
//
// For the purpose of this analysis, column type aliases are not
// considered to be different (eg. TEXT and VARCHAR will fit the same
// scalar type String).
//
// This is used by the UPDATE, INSERT and UPSERT code.
func CheckDatumTypeFitsColumnType(colName string, colTyp *types.T, typ *types.T) error {
	if typ.Family() == types.UnknownFamily {
		return nil
	}
	if !typ.Equivalent(colTyp) {
		return pgerror.Newf(pgcode.DatatypeMismatch,
			"value type %s doesn't match type %s of column %q",
			typ.String(), colTyp.String(), tree.ErrNameString(colName))
	}
	return nil
}

// CanHaveCompositeKeyEncoding returns true if key columns of the given kind can
// have a composite encoding. For such types, it can be decided on a
// case-by-base basis whether a given Datum requires the composite encoding.
//
// As an example of a composite encoding, collated string key columns are
// encoded partly as a key and partly as a value. The key part is the collation
// key, so that different strings that collate equal cannot both be used as
// keys. The value part is the usual UTF-8 encoding of the string, stored so
// that it can be recovered later for inspection/display.
func CanHaveCompositeKeyEncoding(typ *types.T) bool {
	switch typ.Family() {
	case types.FloatFamily,
		types.DecimalFamily,
		types.JsonFamily,
		types.CollatedStringFamily:
		return true
	case types.ArrayFamily:
		return CanHaveCompositeKeyEncoding(typ.ArrayContents())
	case types.TupleFamily:
		for _, t := range typ.TupleContents() {
			if CanHaveCompositeKeyEncoding(t) {
				return true
			}
		}
		return false
	case types.BoolFamily,
		types.IntFamily,
		types.DateFamily,
		types.TimestampFamily,
		types.IntervalFamily,
		types.StringFamily,
		types.BytesFamily,
		types.TimestampTZFamily,
		types.OidFamily,
		types.UuidFamily,
		types.INetFamily,
		types.TimeFamily,
		types.TimeTZFamily,
		types.BitFamily,
		types.GeometryFamily,
		types.GeographyFamily,
		types.EnumFamily,
		types.Box2DFamily,
		types.PGLSNFamily,
		types.PGVectorFamily,
		types.RefCursorFamily,
		types.VoidFamily,
		types.EncodedKeyFamily,
		types.TSQueryFamily,
		types.TSVectorFamily,
		types.JsonpathFamily:
		return false
	case types.UnknownFamily,
		types.AnyFamily:
		return true
	default:
		panic(errors.AssertionFailedf("unsupported column family for type %s", typ.SQLString()))
	}
}
