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
)

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
