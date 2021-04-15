// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var hashOverloads []*oneArgOverload

func populateHashOverloads() {
	hashOverloadBase := &overloadBase{
		kind:  hashOverload,
		Name:  "hash",
		OpStr: "uint64",
	}
	for _, family := range supportedCanonicalTypeFamilies {
		widths, found := supportedWidthsByCanonicalTypeFamily[family]
		if !found {
			colexecerror.InternalError(errors.AssertionFailedf("didn't find supported widths for %s", family))
		}
		ov := newLastArgTypeOverload(hashOverloadBase, family)
		for _, width := range widths {
			// Note that we pass in types.Bool as the return type just to make
			//
			// Note that we pass in types.Bool as the return type just to make
			// overloads initialization happy. We don't actually care about the
			// return type since we know that it will be represented physically
			// as uint64.
			lawo := newLastArgWidthOverload(ov, width, types.Bool)
			sameTypeCustomizer := typeCustomizers[typePair{family, width, family, width}]
			if sameTypeCustomizer != nil {
				if b, ok := sameTypeCustomizer.(hashTypeCustomizer); ok {
					lawo.AssignFunc = b.getHashAssignFunc()
				}
			}
		}
		hashOverloads = append(hashOverloads, &oneArgOverload{
			lastArgTypeOverload: ov,
		})
	}
}

// hashTypeCustomizer is a type customizer that changes how the templater
// produces hash output for a particular type.
type hashTypeCustomizer interface {
	getHashAssignFunc() assignFunc
}

func (boolCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		return fmt.Sprintf(`
			x := 0
			if %[2]s {
    		x = 1
			}
			%[1]s = %[1]s*31 + uintptr(x)
		`, targetElem, vElem)
	}
}

// hashByteSliceString is a templated code for hashing a byte slice. It is
// meant to be used as a format string for fmt.Sprintf with the first argument
// being the "target" (i.e. what variable to assign the hash to) and the second
// argument being the "value" (i.e. what is the name of a byte slice variable).
const hashByteSliceString = `
			sh := (*reflect.SliceHeader)(unsafe.Pointer(&%[2]s))
			%[1]s = memhash(unsafe.Pointer(sh.Data), %[1]s, uintptr(len(%[2]s)))
`

func (bytesCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		return fmt.Sprintf(hashByteSliceString, targetElem, vElem)
	}
}

func (decimalCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		return fmt.Sprintf(`
			// In order for equal decimals to hash to the same value we need to
			// remove the trailing zeroes if there are any.
			tmpDec := &_overloadHelper.TmpDec1
			tmpDec.Reduce(&%[1]s)
			b := []byte(tmpDec.String())`, vElem) +
			fmt.Sprintf(hashByteSliceString, targetElem, "b")
	}
}

func (c floatCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		// TODO(yuzefovich): think through whether this is appropriate way to hash
		// NaNs.
		return fmt.Sprintf(
			`
			f := %[2]s
			if math.IsNaN(float64(f)) {
				f = 0
			}
			%[1]s = f64hash(noescape(unsafe.Pointer(&f)), %[1]s)
`, targetElem, vElem)
	}
}

func (c intCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		return fmt.Sprintf(`
				// In order for integers with different widths but of the same value to
				// to hash to the same value, we upcast all of them to int64.
				asInt64 := int64(%[2]s)
				%[1]s = memhash64(noescape(unsafe.Pointer(&asInt64)), %[1]s)`,
			targetElem, vElem)
	}
}

func (c timestampCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		return fmt.Sprintf(`
		  s := %[2]s.UnixNano()
		  %[1]s = memhash64(noescape(unsafe.Pointer(&s)), %[1]s)
		`, targetElem, vElem)
	}
}

func (c intervalCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		return fmt.Sprintf(`
		  months, days, nanos := %[2]s.Months, %[2]s.Days, %[2]s.Nanos()
		  %[1]s = memhash64(noescape(unsafe.Pointer(&months)), %[1]s)
		  %[1]s = memhash64(noescape(unsafe.Pointer(&days)), %[1]s)
		  %[1]s = memhash64(noescape(unsafe.Pointer(&nanos)), %[1]s)
		`, targetElem, vElem)
	}
}

func (c jsonCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		// TODO(yuzefovich): consider refactoring this to avoid decoding-encoding of
		// JSON altogether. This will require changing `assignFunc` to also have an
		// access to the index of the current element and then some trickery to get
		// to the bytes underlying the JSON.
		return fmt.Sprintf(`
        scratch := _overloadHelper.ByteScratch[:0]
        _b, _err := json.EncodeJSON(scratch, %[2]s)
        if _err != nil {
          colexecerror.ExpectedError(_err)
        }
        _overloadHelper.ByteScratch = _b
        %[1]s`, fmt.Sprintf(hashByteSliceString, targetElem, "_b"), vElem)
	}
}

func (c datumCustomizer) getHashAssignFunc() assignFunc {
	return func(op *lastArgWidthOverload, targetElem, vElem, _, _, _, _ string) string {
		// Note that this overload assumes that there exists
		//   var datumAlloc *rowenc.DatumAlloc.
		// in the scope.
		return fmt.Sprintf(`b := %s.(*coldataext.Datum).Hash(datumAlloc)`, vElem) +
			fmt.Sprintf(hashByteSliceString, targetElem, "b")
	}
}
