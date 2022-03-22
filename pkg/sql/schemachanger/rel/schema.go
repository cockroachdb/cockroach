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
	"sort"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"
)

// Schema defines a mapping of entities to their attributes and decomposition.
type Schema struct {
	name              string
	attrs             []Attr
	attrTypes         []reflect.Type
	attrToOrdinal     map[Attr]ordinal
	entityTypeSchemas map[reflect.Type]*entityTypeSchema
}

// NewSchema constructs a new schema from mappings.
// The name parameter is just used for debugging and error messages.
func NewSchema(name string, m ...SchemaOption) (_ *Schema, err error) {
	defer func() {
		switch r := recover().(type) {
		case nil:
			return
		case error:
			err = errors.Wrap(r, "failed to construct schema")
		default:
			err = errors.AssertionFailedf("failed to construct schema: %v", r)
		}
	}()
	sc := buildSchema(name, m...)
	return sc, nil
}

// MustSchema is like NewSchema but any errors result in a panic.
func MustSchema(name string, m ...SchemaOption) *Schema {
	return buildSchema(name, m...)
}

type entityTypeSchema struct {
	typ        reflect.Type
	fields     []fieldInfo
	attrFields map[ordinal][]fieldInfo
}

type fieldInfo struct {
	path            string
	typ             reflect.Type
	attr            ordinal
	comparableValue func(unsafe.Pointer) interface{}
	value           func(unsafe.Pointer) interface{}
	isPtr, isEntity bool
}

func buildSchema(name string, opts ...SchemaOption) *Schema {
	var m schemaMappings
	for _, opt := range opts {
		opt.apply(&m)
	}
	sb := &schemaBuilder{
		Schema: &Schema{
			name:              name,
			attrToOrdinal:     make(map[Attr]ordinal),
			entityTypeSchemas: make(map[reflect.Type]*entityTypeSchema),
		},
		m: m,
	}

	sb.maybeAddAttribute(Self, emptyInterfaceType)
	sb.maybeAddAttribute(Type, reflectTypeType)
	for _, t := range m.attrTypes {
		sb.maybeAddAttribute(t.a, t.typ)
	}

	// We want to know what all the variable types are.
	for _, tm := range m.entityMappings {
		sb.maybeAddTypeMapping(tm.typ, tm.attrMappings)
	}
	return sb.Schema
}

type schemaBuilder struct {
	*Schema
	m schemaMappings
}

func (sb *schemaBuilder) maybeAddAttribute(a Attr, typ reflect.Type) ordinal {
	// TODO(ajwerner): Validate that t is an okay type for an attribute
	// to be.
	ord, exists := sb.attrToOrdinal[a]
	if !exists {
		ord = ordinal(len(sb.attrs))
		if ord >= maxUserAttribute {
			panic(errors.Errorf("too many attributes"))
		}
		sb.attrs = append(sb.attrs, a)
		sb.attrTypes = append(sb.attrTypes, typ)
		sb.attrToOrdinal[a] = ord
		return ord
	}
	prev := sb.attrTypes[ord]
	if err := checkType(typ, prev); err != nil {
		panic(errors.Wrapf(err, "type mismatch for %v", a))
	}
	return ord
}

// checkType determines whether, either, the typ matches exp or the typ
// implements exp which is an interface type.
func checkType(typ, exp reflect.Type) error {
	switch exp.Kind() {
	case reflect.Interface:
		if !typ.Implements(exp) {
			return errors.Errorf("%v does not implement %v", typ, exp)
		}
	default:
		if typ != exp {
			return errors.Errorf("%v is not %v", typ, exp)
		}
	}
	return nil
}

func (sb *schemaBuilder) maybeAddTypeMapping(t reflect.Type, attributeMappings []attrMapping) {
	isStructPointer := func(tt reflect.Type) bool {
		return tt.Kind() == reflect.Ptr && tt.Elem().Kind() == reflect.Struct
	}

	if !isStructPointer(t) {
		panic(errors.Errorf("%v is not a pointer to a struct", t))
	}
	var fieldInfos []fieldInfo
	for _, am := range attributeMappings {
		for _, sel := range am.selectors {
			fieldInfos = append(fieldInfos,
				sb.addTypeAttrMapping(am.a, t, sel))
		}
	}
	sort.Slice(fieldInfos, func(i, j int) bool {
		return fieldInfos[i].attr < fieldInfos[j].attr
	})
	attributeFields := make(map[ordinal][]fieldInfo)

	for i := 0; i < len(fieldInfos); {
		cur := fieldInfos[i].attr
		j := i + 1
		for ; j < len(fieldInfos); j++ {
			if fieldInfos[j].attr != cur {
				break
			}
		}
		attributeFields[cur] = fieldInfos[i:j]
		i = j
	}
	sb.entityTypeSchemas[t] = &entityTypeSchema{
		typ:        t,
		fields:     fieldInfos,
		attrFields: attributeFields,
	}
}

func (sb *schemaBuilder) addTypeAttrMapping(a Attr, t reflect.Type, sel string) fieldInfo {
	offset, cur := getOffsetAndTypeFromSelector(t, sel)

	// TODO(ajwerner): Deal with making entities out of structs themselves.
	// This gets complicated given the pointer equality used to determine
	// whether entities exist. We'd otherwise need some mechanism for interning
	// structs or something like that.
	isPtr := cur.Kind() == reflect.Ptr
	isStructPtr := isPtr && cur.Elem().Kind() == reflect.Struct
	isScalarPtr := isPtr && isSupportScalarKind(cur.Elem().Kind())
	if !isScalarPtr && !isStructPtr && !isSupportScalarKind(cur.Kind()) {
		panic(errors.Errorf(
			"selector %q of %v has unsupported type %v",
			sel, t, cur,
		))
	}

	typ := cur
	if isScalarPtr {
		typ = cur.Elem()
	}
	ord := sb.maybeAddAttribute(a, typ)

	f := fieldInfo{
		path:     sel,
		attr:     ord,
		isEntity: isStructPtr,
		isPtr:    isPtr,
		typ:      typ,
	}
	makeValueGetter := func(t reflect.Type, offset uintptr) func(u unsafe.Pointer) reflect.Value {
		return func(u unsafe.Pointer) reflect.Value {
			return reflect.NewAt(t, unsafe.Pointer(uintptr(u)+offset))
		}
	}
	getPtrValue := func(vg func(pointer unsafe.Pointer) reflect.Value) func(u unsafe.Pointer) interface{} {
		return func(u unsafe.Pointer) interface{} {
			got := vg(u)
			if got.Elem().IsNil() {
				return nil
			}
			return got.Elem().Interface()
		}
	}
	{
		vg := makeValueGetter(cur, offset)
		if isStructPtr {
			f.value = getPtrValue(vg)
		} else {
			if isScalarPtr {
				f.value = func(u unsafe.Pointer) interface{} {
					got := vg(u)
					if got.Elem().IsNil() {
						return nil
					}
					return got.Elem().Elem().Interface()
				}
			} else {
				f.value = func(u unsafe.Pointer) interface{} {
					return vg(u).Elem().Interface()
				}
			}
		}
	}
	{
		if isStructPtr {
			f.comparableValue = getPtrValue(makeValueGetter(cur, offset))
		} else {
			compType := getComparableType(typ)
			if isScalarPtr {
				compType = reflect.PtrTo(compType)
			}
			vg := makeValueGetter(compType, offset)
			if isScalarPtr {
				f.comparableValue = getPtrValue(vg)
			} else {
				f.comparableValue = func(u unsafe.Pointer) interface{} {
					return vg(u).Interface()
				}
			}
		}
	}
	return f
}

// getOffsetAndTypeFromSelector takes an entity (struct pointer) type and a
// selector string and finds its offset within the struct. Note that this
// allows one to select fields in struct members of the current struct but
// not in referenced structs.
func getOffsetAndTypeFromSelector(
	structPointer reflect.Type, selector string,
) (uintptr, reflect.Type) {
	names := strings.Split(selector, ".")
	var offset uintptr
	cur := structPointer.Elem()
	for _, n := range names {
		sf, ok := cur.FieldByName(n)
		if !ok {
			panic(errors.Errorf("%v.%s is not a field", structPointer, selector))
		}
		offset += sf.Offset
		cur = sf.Type
	}
	return offset, cur
}

func (sc *Schema) mustGetOrdinal(attribute Attr) ordinal {
	ord, err := sc.getOrdinal(attribute)
	if err != nil {
		panic(err)
	}
	return ord
}

func (sc *Schema) getOrdinal(attribute Attr) (ordinal, error) {
	ord, ok := sc.attrToOrdinal[attribute]
	if !ok {
		return 0, errors.Errorf("unknown attribute %s in schema %s", attribute, sc.name)
	}
	return ord, nil
}
