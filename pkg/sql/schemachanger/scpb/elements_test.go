// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/stretchr/testify/require"
)

// TestElementAttributeValueTypesMatch ensure that for all elements which
// have a given Attribute, that the values all have the same type.
func TestElementAttributeValueTypesMatch(t *testing.T) {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	attributeMap := make(map[eav.Attribute]reflect.Type)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		elem := reflect.New(f.Type.Elem()).Interface().(Element)
		for i := eav.Ordinal(0); i < NumAttrs; i++ {
			attr := AttrSchema().At(i)
			av := elem.Get(attr)
			if av == nil {
				continue
			}
			avt := reflect.TypeOf(av)
			if exp, ok := attributeMap[attr]; ok {
				require.Equalf(t, exp, avt, "%v", attr)
			} else {
				attributeMap[attr] = avt
			}
		}
	}
}

func TestGetElement(t *testing.T) {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	elementInterfaceType := reflect.TypeOf((*Element)(nil)).Elem()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.Type.Implements(elementInterfaceType) {
			t.Errorf("%v does not implement %v", f.Type, elementInterfaceType)
		}
	}
}

// TestAllElementsHaveDescID ensures that all element types do carry an
// AttributeDescID.
func TestAllElementsHaveDescID(t *testing.T) {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		elem := reflect.New(f.Type.Elem()).Interface().(Element)
		require.NotNilf(t, elem.Get(AttrDescID), "%s", f.Type.Elem())
	}
}

// TestAllElementsHaveExpectedAttributes ensures that each element type
// broadcasts the correct set of attributes.
func TestAllElementsHaveExpectedAttributes(t *testing.T) {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	sc := AttrSchema()
	check := func(e Entity) {
		e.Attributes().ForEach(
			sc, func(a eav.Attribute) (wantMore bool) {
				require.NotNilf(t, e.Get(a), "%T/%T@a", e, e.GetElement(), a)
				return true
			})

		sc.Attributes().Without(e.Attributes()).ForEach(
			sc, func(a eav.Attribute) (wantMore bool) {
				require.Nilf(t, e.Get(a), "%T/%T@a", e, e.GetElement(), a)
				return true
			})
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		elem := reflect.New(f.Type.Elem()).Interface().(Element)
		check(elem)
		check(NewTarget(Target_ADD, elem))
		check(&Node{
			Target: NewTarget(Target_ADD, elem),
			Status: Status_ABSENT,
		})
	}
}
