// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
			// overloads initialization happy. We don't actually care about the
			// return type since we know that it will be represented physically
			// as uint64.
			lawo := newLastArgWidthOverload(ov, width, types.Bool)
			sameTypeCustomizer := typeCustomizers[typePair{family, width, family, width}]
			if sameTypeCustomizer != nil {
				if b, ok := sameTypeCustomizer.(hashTypeCustomizer); ok {
					lawo.HashFunc = b.getHashFunc()
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
	getHashFunc() hashFunc
}

func (boolCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
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

func (bytesCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
		return fmt.Sprintf(hashByteSliceString, targetElem, vElem)
	}
}

func (decimalCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
		return fmt.Sprintf(`
			// In order for equal decimals to hash to the same value we need to
			// remove the trailing zeroes if there are any.
			var tmpDec apd.Decimal //gcassert:noescape
			tmpDec.Reduce(&%[1]s)
			b := []byte(tmpDec.String())`, vElem) +
			fmt.Sprintf(hashByteSliceString, targetElem, "b")
	}
}

func (c floatCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
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

func (c intCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
		return fmt.Sprintf(`
				// In order for integers with different widths but of the same value to
				// to hash to the same value, we upcast all of them to int64.
				asInt64 := int64(%[2]s)
				%[1]s = memhash64(noescape(unsafe.Pointer(&asInt64)), %[1]s)`,
			targetElem, vElem)
	}
}

func (c timestampCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
		return fmt.Sprintf(`
		  s := %[2]s.UnixNano()
		  %[1]s = memhash64(noescape(unsafe.Pointer(&s)), %[1]s)
		`, targetElem, vElem)
	}
}

func (c intervalCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
		return fmt.Sprintf(`
		  months, days, nanos := %[2]s.Months, %[2]s.Days, %[2]s.Nanos()
		  %[1]s = memhash64(noescape(unsafe.Pointer(&months)), %[1]s)
		  %[1]s = memhash64(noescape(unsafe.Pointer(&days)), %[1]s)
		  %[1]s = memhash64(noescape(unsafe.Pointer(&nanos)), %[1]s)
		`, targetElem, vElem)
	}
}

func (c jsonCustomizer) getHashFunc() hashFunc {
	return func(targetElem, _, vVec, vIdx string) string {
		return fmt.Sprintf(`
        // Access the underlying []byte directly which allows us to skip
        // decoding-encoding of the JSON object.
        _b := %[2]s.Bytes.Get(%[3]s)
        %[1]s`, fmt.Sprintf(hashByteSliceString, targetElem, "_b"), vVec, vIdx)
	}
}

func (c datumCustomizer) getHashFunc() hashFunc {
	return func(targetElem, vElem, _, _ string) string {
		// Note that this overload assumes that there exists
		//   var datumAlloc *tree.DatumAlloc.
		// in the scope.
		return fmt.Sprintf(`b := coldataext.Hash(%s.(tree.Datum), datumAlloc)`, vElem) +
			fmt.Sprintf(hashByteSliceString, targetElem, "b")
	}
}
