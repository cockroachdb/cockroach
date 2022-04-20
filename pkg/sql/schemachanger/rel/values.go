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
)

// numAttrs is the number of attributes we allow ourselves to store inline.
// Schemas with entities which have more than this many attributes will need to
// increase this number.
const numAttrs = 8

// valuesMap is a container for attributes.
//
// It stores the data in a format which is convenient for performing
// comparisons and lookups. If you want strongly typed data out of it,
// you need to use a Schema to retrieve that data. Note that the library
// expects all values to be stored in the map in the comparable, primitive
// form and not in the strongly typed format.
type values struct {
	attrs ordinalSet
	m     [numAttrs]uintptr
}

// get retrieves the primitive valuesMap stores in the valuesMap
// struct.
func (vm *values) get(a ordinal) (uintptr, bool) {
	if !vm.attrs.contains(a) {
		return 0, false
	}
	return vm.m[vm.attrs.rank(a)], true
}

func (vm *values) add(a ordinal, v uintptr) {
	rank := vm.attrs.rank(a)
	if !vm.attrs.contains(a) {
		if l := vm.attrs.len(); rank < l {
			copy(vm.m[rank+1:l+1], vm.m[rank:l])
		}
		vm.attrs = vm.attrs.add(a)
	}
	vm.m[rank] = v
}

// entity is the internal representation of a struct pointer.
// The idea is that ptr is the pointer itself and typ is a
// pointer to the entityTypeSchema.
type entity values

func (e *entity) getTypeInfo(es *entitySet) *entityTypeSchema {
	tv, _ := (*values)(e).get(es.schema.typeOrdinal)
	return es.schema.entityTypes[tv]
}

func (e *entity) getSelf(es *entitySet) interface{} {
	sv, _ := (*values)(e).get(es.schema.selfOrdinal)
	return es.objs[sv]
}

// getTypedValue returns the typedValue for the attribute of the entity.
// Recall that the entity stores in its values type-erased primitive values
// for comparison (so-called comparableValues). We annotate these comparable
// values in typedValue.
func (e *entity) getTypedValue(es *entitySet, attr ordinal) (typedValue, bool) {
	ti := e.getTypeInfo(es)
	if es.schema.attrs[attr] == Type {
		return typedValue{typ: reflectTypeType, value: ti.typ}, true
	}
	sv := e.getSelf(es)
	if es.schema.attrs[attr] == Self {
		return typedValue{
			typ:   ti.typ,
			value: e.getSelf(es),
		}, true
	}
	for _, f := range ti.attrFields[attr] {
		if v := f.comparableValue(unsafe.Pointer(reflect.ValueOf(sv).Pointer())); v != nil {
			return typedValue{
				typ:   f.typ,
				value: v,
			}, true
		}
	}
	return typedValue{}, false
}
