// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Cuong Do (cdo@cockroachlabs.com)

package testutils

import (
	"fmt"
	"reflect"
	"sort"
)

var _ sort.Interface = structSorter{}

// structSorter implements sort.Interface for a slice of structs, making heavy use of
// reflection.
type structSorter struct {
	v         reflect.Value
	fieldName string
}

// Len returns the length of the underlying slice.
func (ss structSorter) Len() int {
	return ss.v.Len()
}

// Less returns true iff if the sort field at index i is less than the sort
// field at index j.
func (ss structSorter) Less(i, j int) bool {
	v1 := reflect.Indirect(ss.v.Index(i))
	v2 := reflect.Indirect(ss.v.Index(j))

	// Grab the appropriate field from both structs.
	//
	// TODO(cdo): This can be optimized by moving this next block of tests into
	// SortStructs, caching the index of the field, and using the more efficient
	// reflect.Value.FieldByIndex().
	f1 := v1.FieldByName(ss.fieldName)
	if !f1.IsValid() {
		panic(fmt.Sprintf("couldn't get field %s", ss.fieldName))
	}
	f2 := v2.FieldByName(ss.fieldName)
	if !f2.IsValid() {
		panic(fmt.Sprintf("couldn't get field %s", ss.fieldName))
	}

	// Do the appropriate < comparison based on the type of the fields.
	switch f1.Kind() {
	case reflect.String:
		return f1.String() < f2.String()

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return f1.Int() < f2.Int()

	case reflect.Uint, reflect.Uintptr, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return f1.Uint() < f2.Uint()

	case reflect.Float32, reflect.Float64:
		return f1.Float() < f2.Float()
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

// SortStructs sorts the given slice of structs using the given field as the sort key.
func SortStructs(s interface{}, fieldName string) {
	// Verify that we've gotten a slice of structs or pointers to structs.
	structs := reflect.ValueOf(s)
	if structs.Kind() != reflect.Slice {
		panic(fmt.Sprintf("expected slice, got %T", s))
	}
	elemType := structs.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		panic(fmt.Sprintf("%s is not a struct or pointer to struct", structs.Elem()))
	}

	sort.Sort(structSorter{structs, fieldName})
}
