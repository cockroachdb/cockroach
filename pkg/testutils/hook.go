package testutils

import "reflect"

// HookGlobal sets `*ptr = val` and returns a closure for restoring `*ptr` to
// its original value. A runtime panic will occur if `val` is not assignable to
// `*ptr`.
func HookGlobal(ptr, val interface{}) func() {
	global := reflect.ValueOf(ptr).Elem()
	orig := reflect.New(global.Type()).Elem()
	orig.Set(global)
	global.Set(reflect.ValueOf(val))
	return func() { global.Set(orig) }
}
