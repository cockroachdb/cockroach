// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package screl

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	types "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// WalkDescIDs calls f for every catid.DescID field in e.
func WalkDescIDs(e scpb.Element, f func(id *catid.DescID) error) error {
	return walk(reflect.TypeOf((*catid.DescID)(nil)), e, func(i interface{}) error {
		return f(i.(*catid.DescID))
	})
}

// WalkTypes calls f for every *types.T field in e.
func WalkTypes(e scpb.Element, f func(t *types.T) error) error {
	return walk(reflect.TypeOf((*types.T)(nil)), e, func(i interface{}) error {
		return f(i.(*types.T))
	})
}

// WalkExpressions calls f for every catpb.Expression field in e.
func WalkExpressions(e scpb.Element, f func(t *catpb.Expression) error) error {
	return walk(reflect.TypeOf((*catpb.Expression)(nil)), e, func(i interface{}) error {
		return f(i.(*catpb.Expression))
	})
}

// walk will use reflection to find all values which are either scalars
// types or pointers types and pass them to f as pointers. The
// expectation is that the input is a pointer to some structure so that
// everything is addressable.
//
// Note that it's a pretty general function, but that generality is not
// necessarily leveraged in any meaningful way at the present moment, and
// it's not tested tightly enough to expose this directly as it stands.
// It's plenty well tested for its use case of decomposing elements.
func walk(wantType reflect.Type, toWalk interface{}, f func(interface{}) error) (err error) {
	defer func() {
		switch r := recover().(type) {
		case nil:
		case error:
			err = r
		default:
			err = errors.AssertionFailedf("failed to do walk: %v", r)
		}
	}()

	visit := func(v reflect.Value) {
		if err := f(v.Interface()); err != nil {
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
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String,
			reflect.Interface:
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
