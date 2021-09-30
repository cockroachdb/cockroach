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

import "reflect"

// SchemaOption is used to construct a schema.
type SchemaOption interface {
	apply(*schemaMappings)
}

// EntityMappingOption is used to build an EntityMapping.
type EntityMappingOption interface {
	apply(typeMappings *entityMapping)
}

// AttrType maps the Attr to the provided type. This can be useful to
// constrain the types of a oneOf attribute to a broader interface.
func AttrType(a Attr, typ reflect.Type) SchemaOption {
	return attrType{a: a, typ: typ}
}

// EntityMapping is used to specify how fields of a type are mapped to Attrs.
func EntityMapping(typ reflect.Type, opts ...EntityMappingOption) SchemaOption {
	tm := entityMapping{typ: typ}
	for _, o := range opts {
		o.apply(&tm)
	}
	return tm
}

// EntityAttr defines a mapping of selector[s] to Attr for an entity.
func EntityAttr(a Attr, selectors ...string) EntityMappingOption {
	return attrMapping{a: a, selectors: selectors}
}

// schemaMappings defines how to map data types to Attr.
type schemaMappings struct {

	// AttrTypes sets the type of values for an Attr. Values do not need to be
	// provided for most attribute; types will be inferred from selectors.
	//
	// Types must be defined for any attributes which are not in fields.
	// Otherwise, the schema will have no way of knowing about the attribute.
	//
	// It also must be defined for attributes which may take on more than one
	// type. In that case it must be defined to some interface type to which
	// all the possible types conform.
	attrTypes []attrType

	// entityMappings enumerate the entities of a schema and the way their fields
	// map to attributes. The selector fields must be exported and may be either
	// primitive types or struct pointers.
	//
	// For struct pointers, new entities will be added and the reference to
	// that type will be stored in the current variable. An attribute may appear
	// more than once in A mapping in the case that all of the times it appears
	// are for pointers and at most one of those pointers is non-nil.
	//
	// TODO(ajwerner): Support pointers to primitive types as well as interface
	// values. Interface values get tricky.
	entityMappings []entityMapping
}

type attrType struct {
	a   Attr
	typ reflect.Type
}

func (a attrType) apply(mappings *schemaMappings) {
	mappings.attrTypes = append(mappings.attrTypes, a)
}

type entityMapping struct {
	typ          reflect.Type
	attrMappings []attrMapping
}

func (t entityMapping) apply(mappings *schemaMappings) {
	mappings.entityMappings = append(mappings.entityMappings, t)
}

// attrMapping is used in mappings to describe how attributes are mapped to
// struct fields as part of an entityMapping.
type attrMapping struct {
	a         Attr
	selectors []string
}

func (a attrMapping) apply(tm *entityMapping) {
	tm.attrMappings = append(tm.attrMappings, a)
}
