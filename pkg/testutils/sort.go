// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/cockroachdb/errors"
)

var _ sort.Interface = structSorter{}

// structSorter implements sort.Interface for a slice of structs, making heavy use of
// reflection.
type structSorter struct {
	v          reflect.Value
	fieldNames []string
}

// Len returns the length of the underlying slice.
func (ss structSorter) Len() int {
	return ss.v.Len()
}

// Less returns true iff if the sort fields at index i are less than the sort
// fields at index j.
func (ss structSorter) Less(i, j int) bool {
	v1 := reflect.Indirect(ss.v.Index(i))
	v2 := reflect.Indirect(ss.v.Index(j))
	return ss.fieldIsLess(v1, v2, 0)
}

func (ss structSorter) fieldIsLess(v1, v2 reflect.Value, fieldNum int) bool {
	fieldName := ss.fieldNames[fieldNum]
	lastField := len(ss.fieldNames) == fieldNum+1

	// Grab the appropriate field from both structs.
	//
	// TODO(cdo): This can be optimized by moving this next block of tests into
	// SortStructs, caching the index of the field, and using the more efficient
	// reflect.Value.FieldByIndex().
	f1 := v1.FieldByName(fieldName)
	if !f1.IsValid() {
		panic(fmt.Sprintf("couldn't get field %s", fieldName))
	}
	f2 := v2.FieldByName(fieldName)
	if !f2.IsValid() {
		panic(fmt.Sprintf("couldn't get field %s", fieldName))
	}

	// Do the appropriate < comparison based on the type of the fields.
	switch f1.Kind() {
	case reflect.String:
		if !lastField && f1.String() == f2.String() {
			return ss.fieldIsLess(v1, v2, fieldNum+1)
		}
		return f1.String() < f2.String()

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !lastField && f1.Int() == f2.Int() {
			return ss.fieldIsLess(v1, v2, fieldNum+1)
		}
		return f1.Int() < f2.Int()

	case reflect.Uint, reflect.Uintptr, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if !lastField && f1.Uint() == f2.Uint() {
			return ss.fieldIsLess(v1, v2, fieldNum+1)
		}
		return f1.Uint() < f2.Uint()

	case reflect.Float32, reflect.Float64:
		if !lastField && f1.Float() == f2.Float() {
			return ss.fieldIsLess(v1, v2, fieldNum+1)
		}
		return f1.Float() < f2.Float()

	case reflect.Bool:
		if !lastField && f1.Bool() == f2.Bool() {
			return ss.fieldIsLess(v1, v2, fieldNum+1)
		}
		return !f1.Bool() && f2.Bool()
	}

	panic(fmt.Sprintf("can't handle sort key type %d", uint(f1.Kind())))
}

// Swap swaps the elements at the provided indices.
func (ss structSorter) Swap(i, j int) {
	// Store the temp value in a new reflect.Value. Then, do a standard swap of the two slice
	// elements.
	t := reflect.ValueOf(ss.v.Index(i).Interface())
	ss.v.Index(i).Set(ss.v.Index(j))
	ss.v.Index(j).Set(t)
}

// SortStructs sorts the given slice of structs using the given fields as the ordered sort keys.
func SortStructs(s interface{}, fieldNames ...string) {
	// Verify that we've gotten a slice of structs or pointers to structs.
	structs := reflect.ValueOf(s)
	if structs.Kind() != reflect.Slice {
		panic(errors.AssertionFailedf("expected slice, got %T", s))
	}
	elemType := structs.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		panic(errors.AssertionFailedf("%s is not a struct or pointer to struct", structs.Elem()))
	}

	sort.Sort(structSorter{structs, fieldNames})
}
