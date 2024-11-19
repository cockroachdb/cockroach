// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// EqualOn is like CompareOn but only returns the boolean corresponding to
// equality.
func (sc *Schema) EqualOn(attrs []Attr, a, b interface{}) (eq bool) {
	_, eq = sc.CompareOn(attrs, a, b)
	return eq
}

// CompareOn compares two entities. Note that it will panic if either variable
// is malformed.
func (sc *Schema) CompareOn(attrs []Attr, a, b interface{}) (less, eq bool) {
	at, av, err := getEntityValueInfo(sc, a)
	if err != nil {
		panic(err)
	}
	bt, bv, err := getEntityValueInfo(sc, b)
	if err != nil {
		panic(err)
	}
	getAttrValue := func(t *entityTypeSchema, ord ordinal, v reflect.Value) interface{} {
		for _, f := range at.attrFields[ord] {
			if ret := f.comparableValue(unsafe.Pointer(v.Pointer())); ret != nil {
				return ret
			}
		}
		return nil
	}
	for _, a := range attrs {
		var aav, bav interface{}
		switch a {
		case Self:
			aav, bav = a, b
		case Type:
			aav, bav = at.typ, bt.typ
		default:
			ord, err := sc.getOrdinal(a)
			if err != nil {
				panic(err)
			}
			if sc.sliceOrdinals.contains(ord) {
				continue
			}
			aav = getAttrValue(at, ord, av)
			bav = getAttrValue(bt, ord, bv)
		}
		less, eq = compareMaybeNil(aav, bav)
		if !eq {
			return less, false
		}
	}
	return false, true
}

// IterateAttributes calls the callback for each defined attribute of the
// passed entity. If the entity's type is not defined in the schema, an error
// will be returned.
func (sc *Schema) IterateAttributes(
	entityI interface{}, f func(attribute Attr, value interface{}) error,
) (err error) {
	ti, value, err := getEntityValueInfo(sc, entityI)
	if err != nil {
		return err
	}
	vp := unsafe.Pointer(value.Pointer())
	var seen ordinalSet
	for _, field := range ti.fields {
		v := field.value(vp)
		if v == nil {
			continue
		}
		attr := sc.attrs[field.attr]
		if seen.contains(field.attr) {
			return errors.Errorf(
				"%v contains second non-nil entry for %v at %s",
				ti.typ, attr, field.path,
			)
		}
		seen = seen.add(field.attr)
		if err := f(attr, v); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func getEntityValueInfo(s *Schema, v interface{}) (*entityTypeSchema, reflect.Value, error) {
	vv := reflect.ValueOf(v)
	if !vv.IsValid() {
		return nil, reflect.Value{}, errors.Errorf("invalid nil value")
	}
	t, ok := s.entityTypeSchemas[vv.Type()]
	if !ok {
		return nil, reflect.Value{}, errors.Errorf("unknown type handler for %T", v)
	}
	// Note that the fact that we have an entry for this variable type is
	// how we get to assume that this type must be a struct pointer.
	if vv.IsNil() {
		return nil, reflect.Value{}, errors.Errorf("invalid nil %T value", v)
	}
	return t, vv, nil
}
