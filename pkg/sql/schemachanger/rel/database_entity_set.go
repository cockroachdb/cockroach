// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"reflect"
	"unsafe"

	"github.com/cockroachdb/errors"
)

// entitySet is an interning of entities. Its use provides a means to represent
// entities and strings as integers in the context of a database.
//
// TODO(ajwerner): Consider reworking the interning such that the values are
// self-describing and don't need the entitySet to be de-referenced. In
// particular, consider storing pointers in the inline values. For the strings,
// this is straightforward. For the entities, we may want to store a pointer
// to the entity itself in the inline value and then somewhere in the entity
// store the pointer to the raw value with a new system attribute.
type entitySet struct {
	schema *Schema

	stringIntern []string
	strings      map[string]int

	entities []entity
	objs     []interface{}
	hash     map[interface{}]int
}

// insert implements the entityStore interface. It inserts the entity if it
// does not already exist. If it does it exist, it inserts the children
// entities recursively into the passed entityStore before inserting the
// passed entity and returning its id.
func (t *entitySet) insert(v interface{}, es entityStore) (int, error) {
	if id, exists := t.hash[v]; exists {
		return id, nil
	}
	ti, value, err := getEntityValueInfo(t.schema, v)
	if err != nil {
		return 0, err
	}
	id := len(t.objs)
	t.objs = append(t.objs, v)
	t.hash[v] = id
	t.entities = append(t.entities, entity{})
	var ev values
	vp := unsafe.Pointer(value.Pointer())
	ev.add(t.schema.selfOrdinal, uintptr(id))
	ev.add(t.schema.typeOrdinal, ti.typID)
	for _, field := range ti.fields {
		var val uintptr
		switch {
		case field.isSlice():
			// In this case, we want to create slice members for each member in
			// the slice and then be done with it.
			fvi := field.value(vp)
			if fvi == nil {
				continue
			}
			fv := reflect.ValueOf(fvi)
			if fv.Kind() != reflect.Slice {
				return 0, errors.AssertionFailedf(
					"expected a slice type, got %v", fv.Type(),
				)
			}
			for i := 0; i < fv.Len(); i++ {
				smpv := reflect.New(field.sliceMemberType.Elem())
				smv := smpv.Elem()
				smv.Field(sliceMemberSourceFieldIndex).Set(value)
				smv.Field(sliceMemberIndexFieldIndex).Set(reflect.ValueOf(i))
				smv.Field(sliceMemberValueFieldIndex).Set(fv.Index(i))
				if _, err := es.insert(smpv.Interface(), es); err != nil {
					return 0, err
				}
			}
			// We do not directly store the slice anywhere in the entity; we only
			// index slice member entities for each slice member.
			continue
		case field.isStruct():
			fv := field.value(vp)
			if fv == nil {
				continue
			}
			fieldID, err := es.insert(fv, es)
			if err != nil {
				return 0, err
			}
			val = uintptr(fieldID)
		case field.isIntLike():
			var ok bool
			val, ok = field.inline(vp)
			if !ok {
				continue
			}
		case field.isString():
			s, ok := field.comparableValue(vp).(*string)
			if !ok {
				continue
			}
			val = t.internString(*s)
		default:
			return 0, errors.AssertionFailedf("invalid field in %v mapping for %v", field.path, ti.typ)
		}
		if !ev.add(field.attr, val) {
			var attrs []Attr
			ev.attrs.forEach(func(a ordinal) (wantMore bool) {
				attrs = append(attrs, t.schema.attrs[a])
				return true
			})
			attrs = append(attrs, t.schema.attrs[field.attr])
			return 0, errors.AssertionFailedf(
				"invalid entity %T has too many attributes: maximum allowed is %d, have at least %v",
				v, numAttrs, attrs)
		}
	}
	t.entities[id] = entity(ev)
	return id, nil
}

func (t *entitySet) compareOnAttrs(attrs ordinalSet, a, b *values) (less, eq bool) {
	attrs.forEach(func(attr ordinal) (wantMore bool) {
		less, eq = t.compareOn(attr, a, b)
		return eq
	})
	return less, eq
}

func (t *entitySet) compareOn(o ordinal, a, b *values) (less, eq bool) {
	av, aOk := a.get(o)
	bv, bOk := b.get(o)
	if aOk != bOk {
		return bOk, false
	}
	if t.schema.stringAttrs.contains(o) {
		as, bs := t.stringIntern[av], t.stringIntern[bv]
		return as < bs, as == bs
	}
	return av < bv, av == bv
}

func (t *entitySet) internString(s string) uintptr {
	id, exists := t.strings[s]
	if !exists {
		id = len(t.stringIntern)
		t.stringIntern = append(t.stringIntern, s)
		t.strings[s] = id
	}
	return uintptr(id)
}

// TODO(ajwerner): Does this really need to know about the ordinal?
// I guess the use case is about figuring out whether we want the
// type itself or the type-type.
func (t *entitySet) makeInlineValue(attr ordinal, vv reflect.Value) (uintptr, error) {
	if err := checkNotNil(vv); err != nil {
		return 0, err
	}
	typ := vv.Type()
	// TODO(ajwerner): Do we ever need to type check here?
	/*
		if err := checkType(typ, attrTyp); err != nil {
			return 0, err
		}
	*/
	switch {
	case attr == t.schema.typeOrdinal:
		typVal, ok := vv.Interface().(reflect.Type)
		if !ok {
			return 0, errors.Errorf(
				"cannot build value for type with non-type value of type %v", vv.Type(),
			)
		}
		ti, ok := t.schema.entityTypeSchemas[typVal]
		if !ok {
			return 0, errors.Errorf(
				"cannot build value for type %v not in database", typVal,
			)
		}
		return ti.typID, nil
	case typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct:
		id, ok := t.hash[vv.Interface()]
		if !ok {
			return 0, errors.Errorf(
				"cannot build value for entity object not in database",
			)
		}
		return uintptr(id), nil
	case isIntKind(typ.Kind()):
		return uintptr(vv.Int()), nil
	case isUintKind(typ.Kind()):
		return uintptr(vv.Uint()), nil
	case typ.Kind() == reflect.String:
		return t.internString(vv.String()), nil
	default:
		return 0, errors.Errorf(
			"unsupported query attribute for type %v", typ,
		)
	}
}
