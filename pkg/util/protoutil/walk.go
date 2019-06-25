// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoutil

import (
	"reflect"
	"unsafe"
)

// During visitReplace, must keep track of checks that are
// in progress. The comparison algorithm assumes that all
// checks in progress are true when it reencounters them.
// Visited comparisons are stored in a map indexed by visit.
type visit struct {
	a1  unsafe.Pointer
	typ reflect.Type
}

// Tests for deep equality using reflected types. The map argument tracks
// comparisons that have already been seen, which allows short circuiting on
// recursive types.
func visitReplace(v1 reflect.Value, f func(reflect.Value), visited map[visit]bool, depth int) {
	if !v1.IsValid() {
		return
	}

	// We want to avoid putting more in the visited map than we need to.
	// For any possible reference cycle that might be encountered,
	// hard(t) needs to return true for at least one of the types in the cycle.
	hard := func(k reflect.Kind) bool {
		switch k {
		case reflect.Map, reflect.Slice, reflect.Ptr, reflect.Interface:
			return true
		}
		return false
	}

	if v1.CanAddr() && hard(v1.Kind()) {
		addr1 := unsafe.Pointer(v1.UnsafeAddr())

		// Short circuit if references are already seen.
		typ := v1.Type()
		v := visit{addr1, typ}
		if visited[v] {
			return
		}

		// Remember for later.
		visited[v] = true
	}

	switch v1.Kind() {
	case reflect.Array:
		for i := 0; i < v1.Len(); i++ {
			visitReplace(v1.Index(i), f, visited, depth+1)
		}
	case reflect.Slice:
		for i := 0; i < v1.Len(); i++ {
			visitReplace(v1.Index(i), f, visited, depth+1)
		}
	case reflect.Interface:
		visitReplace(v1.Elem(), f, visited, depth+1)
	case reflect.Ptr:
		visitReplace(v1.Elem(), f, visited, depth+1)
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			visitReplace(v1.Field(i), f, visited, depth+1)
		}
	case reflect.Map:
		for _, k := range v1.MapKeys() {
			visitReplace(v1.MapIndex(k), f, visited, depth+1)
		}
	case reflect.Func:
	default:
		// Elementary types, no recursion necessary.
	}
	f(v1)
}

// Walk .
func Walk(x interface{}, replacingVisitor func(reflect.Value)) {
	if x == nil {
		return
	}
	v1 := reflect.ValueOf(x)
	visitReplace(v1, replacingVisitor, make(map[visit]bool), 0)
}
