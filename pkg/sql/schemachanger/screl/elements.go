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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

// ForEachElementType invokes the function f with a nil value for each
// type of element.
func ForEachElementType(f func(e scpb.Element) error) error {
	for _, e := range elements {
		if err := f(e); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

var elements = func() (elements []scpb.Element) {
	typ := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		elem := reflect.Zero(f.Type).Interface().(scpb.Element)
		elements = append(elements, elem)
	}
	return elements
}()
