// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// HookGlobal provides a way to temporarily set a package-global variable to a
// new value for the duration of a test. It returns a closure that restores the
// original value.
func HookGlobal[T any](ptr *T, val T) func() {
	orig := *ptr
	*ptr = val
	return func() { *ptr = orig }
}
