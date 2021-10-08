// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import "reflect"

// TestingHook sets `*ptr = val` and returns a closure for restoring `*ptr` to
// its original value. A runtime panic will occur if `val` is not assignable to
// `*ptr`.
func TestingHook(ptr, val interface{}) func() {
	global := reflect.ValueOf(ptr).Elem()
	orig := reflect.New(global.Type()).Elem()
	orig.Set(global)
	global.Set(reflect.ValueOf(val))
	return func() { global.Set(orig) }
}
