// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package walkutil

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// Walk will use reflection to find all values which are either scalars
// types or pointers types and pass them to f as pointers. The
// expectation is that the input is a pointer to some structure so that
// everything is addressable.
func Walk[T any](toWalk interface{}, f func(T) error) (err error) {
	wantType := reflect.TypeOf((*T)(nil)).Elem()

	defer func() {
		switch r := recover().(type) {
		case nil:
		case error:
			err = r
		default:
			err = errors.AssertionFailedf("failed to do walk: %v", r)
		}
		err = iterutil.Map(err)
	}()

	visit := func(v reflect.Value) {
		if err := f(v.Interface().(T)); err != nil {
			panic(err)
		}
	}

	wantTypeIsPtr := wantType.Kind() == reflect.Ptr
	var wantTypeElem reflect.Type
	if wantTypeIsPtr {
		wantTypeElem = wantType.Elem()
	}
	maybeVisit := func(v reflect.Value) bool {
		if vt := v.Type(); vt == wantType && (!wantTypeIsPtr || !v.IsNil()) {
			visit(v)
		} else if wantTypeIsPtr && wantTypeElem == vt {
			visit(v.Addr())
		} else {
			return false
		}
		return true
	}

	var walk func(value reflect.Value)
	walk = func(value reflect.Value) {
		if maybeVisit(value) {
			return
		}
		switch value.Kind() {
		case reflect.Array, reflect.Slice:
			for i := 0; i < value.Len(); i++ {
				walk(value.Index(i).Addr().Elem())
			}
		case reflect.Ptr:
			if !value.IsNil() {
				walk(value.Elem())
			}
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if f := value.Field(i); f.CanAddr() && value.Type().Field(i).IsExported() {
					walk(f.Addr().Elem())
				}
			}
		case reflect.Interface:
			if !value.IsNil() {
				walk(value.Elem())
			}
		case reflect.Map:
			iter := value.MapRange()
			for iter.Next() {
				v := iter.Value()
				if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
					walk(v)
				}
			}
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		default:
			panic(errors.AssertionFailedf(
				"cannot walk values of kind %v, type %v", value.Kind(), value.Type(),
			))
		}
	}

	v := reflect.ValueOf(toWalk)
	if !v.IsValid() || v.Kind() != reflect.Ptr || v.IsNil() || !v.Elem().CanAddr() {
		return errors.Errorf("invalid value for walking of type %T", toWalk)
	}
	walk(v.Elem())
	return nil
}
