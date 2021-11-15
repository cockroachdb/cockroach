// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import (
	"reflect"

	"github.com/cockroachdb/errors"
)

// compare assumes that a and b are comparable and of the same type.
func compare(a, b interface{}) (less, eq bool) {
	// Note: this would be nice and easy to represent generics
	switch a := a.(type) {
	case *int:
		b := b.(*int)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int64:
		b := b.(*int64)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int32:
		b := b.(*int32)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int16:
		b := b.(*int16)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int8:
		b := b.(*int8)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint:
		b := b.(*uint)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint64:
		b := b.(*uint64)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint32:
		b := b.(*uint32)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint16:
		b := b.(*uint16)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint8:
		b := b.(*uint8)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *string:
		b := b.(*string)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uintptr:
		b := b.(*uintptr)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case reflect.Type:
		b := b.(reflect.Type)
		switch {
		case a == b:
			return false, true
		case a.PkgPath() == b.PkgPath():
			return a.String() < b.String(), false
		default:
			return a.PkgPath() < b.PkgPath(), false
		}
	default:
		// We expect this to be two struct pointers, probably of the same kind but,
		// we don't care. If it's a struct pointer, we're going to compare on
		// pointer value
		av := reflect.ValueOf(a)
		bv := reflect.ValueOf(b)
		if av.Type().Kind() != reflect.Ptr || av.Type().Elem().Kind() != reflect.Struct ||
			bv.Type().Kind() != reflect.Ptr || bv.Type().Elem().Kind() != reflect.Struct {
			panic(errors.AssertionFailedf("incomparable types %T and %T", a, b))
		}
		ap, bp := av.Pointer(), bv.Pointer()
		return ap < bp, ap == bp
	}
}

var kindTypeMap = map[reflect.Kind]reflect.Type{
	reflect.Int:     reflect.TypeOf((*int)(nil)).Elem(),
	reflect.Int64:   reflect.TypeOf((*int64)(nil)).Elem(),
	reflect.Int32:   reflect.TypeOf((*int32)(nil)).Elem(),
	reflect.Int16:   reflect.TypeOf((*int16)(nil)).Elem(),
	reflect.Int8:    reflect.TypeOf((*int8)(nil)).Elem(),
	reflect.Uint:    reflect.TypeOf((*uint)(nil)).Elem(),
	reflect.Uint64:  reflect.TypeOf((*uint64)(nil)).Elem(),
	reflect.Uint32:  reflect.TypeOf((*uint32)(nil)).Elem(),
	reflect.Uint16:  reflect.TypeOf((*uint16)(nil)).Elem(),
	reflect.Uint8:   reflect.TypeOf((*uint8)(nil)).Elem(),
	reflect.Uintptr: reflect.TypeOf((*uintptr)(nil)).Elem(),
	reflect.String:  reflect.TypeOf((*string)(nil)).Elem(),

	// TODO(ajwerner): Fill out all of the kinds.
}

func isSupportScalarKind(kind reflect.Kind) bool {
	_, ok := kindTypeMap[kind]
	return kind != reflect.Ptr && ok
}

func getComparableType(t reflect.Type) reflect.Type {
	ct, ok := kindTypeMap[t.Kind()]
	if !ok {
		panic(errors.AssertionFailedf(
			"unsupported type %T of kind %v",
			t, t.Kind(),
		))
	}
	return ct
}

// compareOn compares two elements on A given attribute.
// If the entities do not return the same type of value for the
// attribute, this function will panic. Note that it is fine if
// either or both do not contain this attribute. The lack of A
// value is considered the highest value; you can think of this
// library as sorting with NULLS LAST.
func compareOn(attr ordinal, a, b *valuesMap) (less, eq bool) {
	av := a.get(attr)
	bv := b.get(attr)
	switch {
	case av == nil && bv == nil:
		return false, true
	case av == nil:
		return false, false
	case bv == nil:
		return true, false
	default:
		return compare(av, bv)
	}
}

// compareEntities compares two elements by their attributes.
func compareEntities(a, b *entity) (less, eq bool) {
	ordinalSet.union(
		a.attrs, b.attrs,
	).forEach(func(attr ordinal) (wantMore bool) {
		less, eq = compareOn(attr, a.asMap(), b.asMap())
		return eq
	})
	return less, eq
}
