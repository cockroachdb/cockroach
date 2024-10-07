// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	name                                  string
	attrs                                 []Attr
	attrTypes                             []reflect.Type
	sliceOrdinals                         ordinalSet
	attrToOrdinal                         map[Attr]ordinal
	entityTypes                           []*entityTypeSchema
	entityTypeSchemas                     map[reflect.Type]*entityTypeSchema
	typeOrdinal, selfOrdinal              ordinal
	sliceIndexOrdinal, sliceSourceOrdinal ordinal
	stringAttrs                           ordinalSet
	rules                                 []*RuleDef
	rulesByName                           map[string]*RuleDef
}

type entityTypeSchemaSort Schema

func (e entityTypeSchemaSort) Len() int { return len(e.entityTypeSchemas) }
func (e entityTypeSchemaSort) Less(i, j int) bool {
	less, _ := compareTypes(e.entityTypes[i].typ, e.entityTypes[j].typ)
	return less
}
func (e entityTypeSchemaSort) Swap(i, j int) {
	e.entityTypes[i].typID = uintptr(j)
	e.entityTypes[j].typID = uintptr(i)
	e.entityTypes[i], e.entityTypes[j] = e.entityTypes[j], e.entityTypes[i]
}

var _ sort.Interface = (*entityTypeSchemaSort)(nil)

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

	// typID is the rank of the type of this entity in the schema.
	typID uintptr

	// isSliceMemberType is true if this type exists to support containment
	// operations over a slice type.
	isSliceMemberType bool
	sliceAttr         ordinal
}

type fieldInfo struct {
	path            string
	typ             reflect.Type
	attr            ordinal
	comparableValue func(unsafe.Pointer) interface{}
	value           func(unsafe.Pointer) interface{}
	inline          func(unsafe.Pointer) (uintptr, bool)
	fieldFlags

	sliceMemberType reflect.Type
}

type fieldFlags int8

func (f fieldFlags) isPtr() bool     { return f&pointerField != 0 }
func (f fieldFlags) isScalar() bool  { return f&(intField|stringField|uintField) != 0 }
func (f fieldFlags) isStruct() bool  { return f&structField != 0 }
func (f fieldFlags) isInt() bool     { return f&intField != 0 }
func (f fieldFlags) isUint() bool    { return f&uintField != 0 }
func (f fieldFlags) isIntLike() bool { return f&(intField|uintField) != 0 }
func (f fieldFlags) isString() bool  { return f&stringField != 0 }
func (f fieldFlags) isSlice() bool   { return f&sliceField != 0 }

const (
	intField fieldFlags = 1 << iota
	uintField
	stringField
	structField
	pointerField
	sliceField
)

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
			rulesByName:       make(map[string]*RuleDef),
		},
		m: m,
	}

	for _, t := range m.attrTypes {
		sb.maybeAddAttribute(t.a, t.typ)
	}

	// We want to know what all the variable types are.
	for _, tm := range m.entityMappings {
		sb.maybeAddTypeMapping(tm.typ, tm.attrMappings)
	}

	sb.maybeAddAttribute(Self, emptyInterfaceType)
	sb.selfOrdinal = sb.mustGetOrdinal(Self)
	sb.maybeAddAttribute(Type, reflectTypeType)
	sb.typeOrdinal = sb.mustGetOrdinal(Type)
	sort.Sort((*entityTypeSchemaSort)(sb.Schema))

	return sb.Schema
}

type schemaBuilder struct {
	*Schema
	m schemaMappings
}

func (sb *schemaBuilder) maybeInitializeSliceMemberAttributes() {
	if sb.sliceIndexOrdinal != 0 {
		return
	}
	sb.sliceIndexOrdinal = sb.maybeAddAttribute(sliceIndex, reflect.TypeOf((*int)(nil)).Elem())
	sb.sliceSourceOrdinal = sb.maybeAddAttribute(sliceSource, reflect.TypeOf((*interface{})(nil)).Elem())
}

func (sb *schemaBuilder) maybeAddAttribute(a Attr, typ reflect.Type) ordinal {
	// TODO(ajwerner): Validate that t is an okay type for an attribute
	// to be.
	ord, exists := sb.attrToOrdinal[a]
	if !exists {
		ord = ordinal(len(sb.attrs))
		if ord > maxOrdinal {
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

func (sb *schemaBuilder) maybeAddTypeMapping(
	t reflect.Type, attributeMappings []attrMapping,
) *entityTypeSchema {
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
				sb.addTypeAttrMapping(am.a, t, sel, am.selectorTypes, am.isOneOfElement)...)
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
	ts := &entityTypeSchema{
		typ:        t,
		fields:     fieldInfos,
		attrFields: attributeFields,
		typID:      uintptr(len(sb.entityTypes)),
	}
	sb.entityTypeSchemas[t] = ts
	sb.entityTypes = append(sb.entityTypes, ts)
	return ts
}

func makeFieldFlags(t reflect.Type) (fieldFlags, bool) {
	var f fieldFlags
	if t.Kind() == reflect.Ptr {
		f |= pointerField
		t = t.Elem()
	}
	kind := t.Kind()
	switch {
	case kind == reflect.Slice && !f.isPtr():
		f |= sliceField
	case kind == reflect.Struct && f.isPtr():
		f |= structField
	case kind == reflect.String:
		f |= stringField
	case isIntKind(kind):
		f |= intField
	case isUintKind(kind):
		f |= uintField
	default:
		return 0, false
	}
	return f, true
}

func (sb *schemaBuilder) addTypeAttrMapping(
	a Attr, t reflect.Type, sel string, selOneOfTypes []reflect.Type, isOneOf bool,
) (fields []fieldInfo) {
	var offset uintptr
	var oneOfType reflect.Type
	if isOneOf {
		offset, oneOfType = getOffsetAndTypeFromSelector(t, sel)
	} else {
		if len(selOneOfTypes) > 0 {
			panic("selector type are only allowed for one of attributes.")
		}
		var cur reflect.Type
		offset, cur = getOffsetAndTypeFromSelector(t, sel)
		selOneOfTypes = []reflect.Type{cur}
	}
	for _, cur := range selOneOfTypes {
		// For one of types, extract the value inside.
		castType := cur
		if isOneOf {
			cur = cur.Elem().Field(0).Type
		}
		flags, ok := makeFieldFlags(cur)
		if !ok {
			panic(errors.Errorf(
				"selector %q of %v has unsupported type %v",
				sel, t, cur,
			))
		}
		typ := cur
		if flags.isPtr() && flags.isScalar() {
			typ = cur.Elem()
		}
		var ord ordinal
		var sliceMemberType reflect.Type
		if !flags.isSlice() {
			ord = sb.maybeAddAttribute(a, typ)
		} else {
			// We need to add the slice type and then return, or
			// perhaps, add some annotation to the type that this
			// is a slice, and it refers to xyz.
			sb.maybeInitializeSliceMemberAttributes()
			// Give the generated struct a field name based on the attribute name.
			// We could use something generic like "Value" for all value fields of
			// such structs, but this makes debugging a tad easier because you can
			// look at the field names of the type in the debugger.
			fieldName := "F_" + a.String()
			sliceMemberType = makeSliceMemberType(t, typ, fieldName)
			st := sb.maybeAddTypeMapping(sliceMemberType, []attrMapping{
				{a: sliceSource, selectors: []string{"Source"}},
				{a: sliceIndex, selectors: []string{"Index"}},
				{a: a, selectors: []string{fieldName}},
			})
			st.isSliceMemberType = true
			ord = sb.attrToOrdinal[a]
			sb.sliceOrdinals = sb.sliceOrdinals.add(ord)
			st.sliceAttr = ord
		}
		f := fieldInfo{
			fieldFlags:      flags,
			path:            sel,
			attr:            ord,
			typ:             typ,
			sliceMemberType: sliceMemberType,
		}
		makeValueGetter := func(t reflect.Type, offset uintptr) func(u unsafe.Pointer) reflect.Value {
			return func(u unsafe.Pointer) reflect.Value {
				if !isOneOf {
					return reflect.NewAt(t, unsafe.Pointer(uintptr(u)+offset))
				} else {
					oneOfValue := reflect.NewAt(oneOfType, unsafe.Pointer(uintptr(u)+offset))
					innerElement := oneOfValue.Elem().Elem()
					if innerElement.Type() == castType {
						return innerElement.Elem().Field(0)
					}
					return reflect.Zero(cur)
				}
			}
		}
		getPtrValue := func(vg func(pointer unsafe.Pointer) reflect.Value) func(u unsafe.Pointer) interface{} {
			return func(u unsafe.Pointer) interface{} {
				got := vg(u)
				// Methods will return direct references without any indirection.
				if isOneOf {
					if got.IsNil() {
						return nil
					}
					return got.Interface()
				} else {
					// Otherwise, we will have the pointer wrapped in another pointer.
					if got.Elem().IsNil() {
						return nil
					}
					return got.Elem().Interface()
				}
			}
		}
		{
			vg := makeValueGetter(cur, offset)
			if f.isPtr() && f.isStruct() {
				f.value = getPtrValue(vg)
			} else if f.isSlice() {
				f.value = func(u unsafe.Pointer) interface{} {
					got := vg(u)
					ge := got.Elem()
					if ge.IsNil() || ge.Len() == 0 {
						return nil
					}
					return ge.Interface()
				}
			} else if f.isPtr() && f.isScalar() {
				f.value = func(u unsafe.Pointer) interface{} {
					got := vg(u)
					ge := got.Elem()
					if ge.IsNil() {
						return nil
					}
					return ge.Elem().Interface()
				}
			} else {
				f.value = func(u unsafe.Pointer) interface{} {
					return vg(u).Elem().Interface()
				}
			}
			switch {
			case f.isSlice():
				// f.inline is not defined
			case f.isPtr() && f.isInt():
				f.inline = func(u unsafe.Pointer) (uintptr, bool) {
					got := vg(u)
					if got.Elem().IsNil() {
						return 0, false
					}
					return uintptr(got.Elem().Elem().Int()), true
				}
			case f.isPtr() && f.isUint():
				f.inline = func(u unsafe.Pointer) (uintptr, bool) {
					got := vg(u)
					if got.Elem().IsNil() {
						return 0, false
					}
					return uintptr(got.Elem().Elem().Uint()), true
				}
			case f.isInt():
				f.inline = func(u unsafe.Pointer) (uintptr, bool) {
					return uintptr(vg(u).Elem().Int()), true
				}
			case f.isUint():
				f.inline = func(u unsafe.Pointer) (uintptr, bool) {
					return uintptr(vg(u).Elem().Uint()), true
				}
			case f.isString(), f.isStruct():
				f.inline = func(u unsafe.Pointer) (uintptr, bool) {
					return 0, false
				}
			}
		}
		{
			if f.isStruct() {
				f.comparableValue = getPtrValue(makeValueGetter(cur, offset))
			} else if !f.isSlice() {
				compType := getComparableType(typ)
				if f.isPtr() && f.isScalar() {
					compType = reflect.PointerTo(compType)
				}
				vg := makeValueGetter(compType, offset)
				if f.isPtr() && f.isScalar() {
					f.comparableValue = getPtrValue(vg)
				} else {
					f.comparableValue = func(u unsafe.Pointer) interface{} {
						return vg(u).Interface()
					}
				}
			}
		}
		fields = append(fields, f)
	}
	return fields
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
		// If this field is accessed by embedding, ensure
		// that all the offsets add up. Also, ensure that
		// there is no pointer field which needs to be
		// traversed.
		for i := 0; i <= len(sf.Index); i++ {
			f := cur.FieldByIndex(sf.Index[:i])
			// Ensure that the value we are looking for is actually inside the
			// struct. One can embed pointers, and, thus, go pointer chasing to
			// access some field. This is not currently implemented.
			//
			// TODO(ajwerner): Support pointer chasing for embedded fields.
			if i < len(sf.Index) && f.Type.Kind() != reflect.Struct {
				panic(errors.Errorf("%v.%s references an embedded pointer %s", structPointer, selector, f.Name))
			}
			offset += f.Offset
		}
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

// IsSliceAttr returns true if the Attr corresponds to a slice field.
func (sc *Schema) IsSliceAttr(a Attr) bool {
	ord, ok := sc.attrToOrdinal[a]
	return ok && sc.sliceOrdinals.contains(ord)
}

// Give the struct field indexes constants so that they can be used
// when setting the fields entity set inserts.
const (
	sliceMemberSourceFieldIndex = iota
	sliceMemberIndexFieldIndex
	sliceMemberValueFieldIndex
)

func makeSliceMemberType(srcType, sliceType reflect.Type, valueFieldName string) reflect.Type {
	fields := [...]reflect.StructField{
		sliceMemberSourceFieldIndex: {
			Name: "Source", Type: srcType,
		},
		sliceMemberIndexFieldIndex: {
			Name: "Index", Type: reflect.TypeOf((*int)(nil)).Elem(),
		},
		sliceMemberValueFieldIndex: {
			Name: valueFieldName, Type: sliceType.Elem(),
		},
	}
	return reflect.PointerTo(reflect.StructOf(fields[:]))
}
