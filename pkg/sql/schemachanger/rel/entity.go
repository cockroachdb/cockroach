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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// entity is the internal representation of a struct pointer.
// The idea is that ptr is the pointer itself and typ is a
// pointer to the entityTypeSchema.
type entity valuesMap

// EqualOn is like CompareOn but only returns the boolean corresponding to
// equality.
func (sc *Schema) EqualOn(attrs []Attr, a, b interface{}) (eq bool) {
	_, eq = sc.CompareOn(attrs, a, b)
	return eq
}

// CompareOn compares two entities. Note that it will panic if either variable
// is malformed.
func (sc *Schema) CompareOn(attrs []Attr, a, b interface{}) (less, eq bool) {
	ords, _, err := sc.attributesToOrdinals(attrs)
	if err != nil {
		panic(err)
	}
	ae, err := toEntity(sc, a)
	if err != nil {
		panic(err)
	}
	be, err := toEntity(sc, b)
	if err != nil {
		panic(err)
	}
	defer putValues((*valuesMap)(ae))
	defer putValues((*valuesMap)(be))
	for _, a := range ords {
		if less, eq = compareOn(a, (*valuesMap)(ae), (*valuesMap)(be)); !eq {
			return less, eq
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

	v, err := toEntity(sc, entityI)
	if err != nil {
		return err
	}
	v.attrs.forEach(func(ord ordinal) (wantMore bool) {
		a := sc.attrs[ord]
		if isSystemAttribute(a) {
			return true
		}
		tv, ok := v.getTypedValue(sc, ord)
		if !ok {
			err = errors.AssertionFailedf(
				"failed to get typed value for populated scalar attribute %v for %T",
				a, entityI,
			)
		} else {
			err = f(a, tv.toInterface())
		}
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
}

// getComparableValue gets the value in its type-erased form from the
// entity.
func (e *entity) getComparableValue(sc *Schema, attribute Attr) interface{} {
	return (*valuesMap)(e).get(sc.mustGetOrdinal(attribute))
}

func (e *entity) getTypeInfo(sc *Schema) *entityTypeSchema {
	return sc.entityTypeSchemas[e.getComparableValue(sc, Type).(reflect.Type)]
}

// getTypedValue returns the typedValue for the attribute of the entity.
// Recall that the entity stores in its values type-erased primitive values
// for comparison (so-called comparableValues). We annotate these comparable
// values in typedValue.
func (e *entity) getTypedValue(sc *Schema, attr ordinal) (typedValue, bool) {
	val := (*valuesMap)(e).get(attr)
	if val == nil {
		return typedValue{}, false
	}
	var typ reflect.Type

	// To set the type, there's some disparate logic based on what attribute
	// we're looking at. For the Type attribute, it's a reflect.Type.
	// For scalar fields we know the type because we do not permit oneOf behavior.
	// For entity fields, we don't store the type because it's dynamic. Instead we
	// know that if the field points to an entity, then the value has not had its
	// type erased for comparison.
	if sc.attrs[attr] == Type {
		typ = reflectTypeType
	} else if fi, ok := e.getTypeInfo(sc).attrFields[attr]; ok && !fi[0].isEntity {
		// This is a bit of a hack to deal with the fact that an attribute
		// might have multiple fields which can lead to its value.
		typ = fi[0].typ
	} else {
		typ = reflect.TypeOf(val)
	}
	return typedValue{
		typ:   typ,
		value: val,
	}, true
}

func (e *entity) asMap() *valuesMap {
	return (*valuesMap)(e)
}

func toEntity(s *Schema, v interface{}) (*entity, error) {
	ti, value, err := getEntityValueInfo(s, v)
	if err != nil {
		return nil, err
	}

	e := getValues()
	e.add(s.mustGetOrdinal(Type), value.Type())
	e.add(s.mustGetOrdinal(Self), v)
	for _, field := range ti.fields {
		// Note that this is the place where we type-erase scalar types.
		// This could well not be worth the hassle.
		var val interface{}
		if field.isEntity {
			val = field.value(unsafe.Pointer(value.Pointer()))
		} else {
			val = field.comparableValue(unsafe.Pointer(value.Pointer()))
		}
		if field.isPtr && val == nil {
			continue
		} else if val == nil {
			return nil, errors.AssertionFailedf(
				"got nil value for non-pointer scalar attribute %s of type %s",
				s.attrs[field.attr], ti.typ,
			)
		}
		if e.attrs.contains(field.attr) {
			return nil, errors.Errorf(
				"%v contains second non-nil entry for %v at %s",
				ti.typ, s.attrs[field.attr], field.path,
			)
		}
		e.add(field.attr, val)
	}

	return (*entity)(e), nil
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
