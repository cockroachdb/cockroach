// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"unicode/utf8"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// LimitValueWidth checks that the width (for strings, byte arrays, and bit
// strings) and scale (for decimals) of the value fits the specified column
// type. In case of decimals, it can truncate fractional digits in the input
// value in order to fit the target column. If the input value fits the target
// column, it is returned unchanged. If the input value can be truncated to fit,
// then a truncated copy is returned. Otherwise, an error is returned. This
// method is used by INSERT and UPDATE.
func LimitValueWidth(typ *types.T, inVal tree.Datum, name *string) (outVal tree.Datum, err error) {
	switch typ.Family() {
	case types.StringFamily, types.CollatedStringFamily:
		var sv string
		if v, ok := tree.AsDString(inVal); ok {
			sv = string(v)
		} else if v, ok := inVal.(*tree.DCollatedString); ok {
			sv = v.Contents
		}

		if typ.Width() > 0 && utf8.RuneCountInString(sv) > int(typ.Width()) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
				"value too long for type %s (column %q)",
				typ.SQLString(), tree.ErrNameStringP(name))
		}
	case types.IntFamily:
		if v, ok := tree.AsDInt(inVal); ok {
			if typ.Width() == 32 || typ.Width() == 64 || typ.Width() == 16 {
				// Width is defined in bits.
				width := uint(typ.Width() - 1)

				// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer out of range for type %s (column %q)",
						typ.Name(), tree.ErrNameStringP(name))
				}
			}
		}
	case types.BitFamily:
		if v, ok := tree.AsDBitArray(inVal); ok {
			if typ.Width() > 0 {
				bitLen := v.BitLen()
				switch typ.Oid() {
				case oid.T_varbit:
					if bitLen > uint(typ.Width()) {
						return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
							"bit string length %d too large for type %s", bitLen, typ.SQLString())
					}
				default:
					if bitLen != uint(typ.Width()) {
						return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"bit string length %d does not match type %s", bitLen, typ.SQLString())
					}
				}
			}
		}
	case types.DecimalFamily:
		if inDec, ok := inVal.(*tree.DDecimal); ok {
			if inDec.Form != apd.Finite || typ.Precision() == 0 {
				// Non-finite form or unlimited target precision, so no need to limit.
				break
			}
			if int64(typ.Precision()) >= inDec.NumDigits() && typ.Scale() == inDec.Exponent {
				// Precision and scale of target column are sufficient.
				break
			}

			var outDec tree.DDecimal
			outDec.Set(&inDec.Decimal)
			err := tree.LimitDecimalWidth(&outDec.Decimal, int(typ.Precision()), int(typ.Scale()))
			if err != nil {
				return nil, errors.Wrapf(err, "type %s (column %q)",
					typ.SQLString(), tree.ErrNameStringP(name))
			}
			return &outDec, nil
		}
	case types.ArrayFamily:
		if inArr, ok := inVal.(*tree.DArray); ok {
			var outArr *tree.DArray
			elementType := typ.ArrayContents()
			for i, inElem := range inArr.Array {
				outElem, err := LimitValueWidth(elementType, inElem, name)
				if err != nil {
					return nil, err
				}
				if outElem != inElem {
					if outArr == nil {
						outArr = &tree.DArray{}
						*outArr = *inArr
						outArr.Array = make(tree.Datums, len(inArr.Array))
						copy(outArr.Array, inArr.Array[:i])
					}
				}
				if outArr != nil {
					outArr.Array[i] = inElem
				}
			}
			if outArr != nil {
				return outArr, nil
			}
		}
	}
	return inVal, nil
}

// CheckDatumTypeFitsColumnType verifies that a given scalar value
// type is valid to be stored in a column of the given column type.
//
// For the purpose of this analysis, column type aliases are not
// considered to be different (eg. TEXT and VARCHAR will fit the same
// scalar type String).
//
// This is used by the UPDATE, INSERT and UPSERT code.
func CheckDatumTypeFitsColumnType(col *ColumnDescriptor, typ *types.T) error {
	if typ.Family() == types.UnknownFamily {
		return nil
	}
	if !typ.Equivalent(&col.Type) {
		return pgerror.Newf(pgcode.DatatypeMismatch,
			"value type %s doesn't match type %s of column %q",
			typ.String(), col.Type.String(), tree.ErrNameString(col.Name))
	}
	return nil
}
