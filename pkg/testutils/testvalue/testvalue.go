// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testvalue

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// enabled controls whether testvalue.Adjust will perform it's operation.
// Since the check against enabled happens from production code, we don't want to add additional
// synchronization overhead when checking this option.  Hence, the use of atomics.
var enabled int32

var mu sync.Mutex
var registry = make(map[string]interface{})

// Tests must call Enable() at least once.
// Enable() should be called from the tests init(): otherwise, due to parallel test execution,
// it's possible that the code under test may not observe modified value.
func Enable() {
	atomic.StoreInt32(&enabled, 1)
}

// Clears out all registered values.  Requires Enabled() has been called.
func Reset() {
	checkEnabled()
	mu.Lock()
	registry = make(map[string]interface{})
	mu.Unlock()
}

// A CallbackSetter is a callback that's responsible for returning the value to assign.
type CallbackSetter func() interface{}

type callback struct {
	setter CallbackSetter
}

// Set is called by the unit test to arrange an override for the specified 'key'.
// Requires Enable() has been called.
func Set(key string, val interface{}) {
	checkEnabled()
	mu.Lock()
	defer mu.Unlock()

	_, ok := registry[key]

	if ok {
		panic(fmt.Sprintf("Cannot register value with the key '%s'; it's already registered", key))
	}

	registry[key] = val
}

// Similar to Set() above, but the setter is specified as a function which takes in the original value
// and returns the new value for the override.
func SetCallback(key string, setter CallbackSetter) {
	Set(key, callback{setter})
}

// Adjust is called from production code to modify a value for the specified key.
// The passed in value must be a pointer type, and, if the override has been set, the types
// of the *ival and the override must match.
//
// This method is a no-op if enable has not been called, or if the override has not been set for the 'key'
func Adjust(key string, ival interface{}) {
	if atomic.LoadInt32(&enabled) == 0 {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	registered, ok := registry[key]
	if !ok {
		return
	}

	val := reflect.ValueOf(ival)
	var override reflect.Value

	if cb, ok := registered.(callback); ok {
		// If our override is a callback setter, execute the callback to get the override value.
		override = reflect.ValueOf(cb.setter())
	} else {
		override = reflect.ValueOf(registered)
	}

	// Perform various checks (via reflection) before updating the value.
	// The checks below are not strictly needed; the Set call at the end panics if the value is not assignable for
	// any reasons.  However, we try to provide a bit more interesting error messages to simplify debugging.

	// Value must be a pointer type so that we can assign to it.
	if val.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("Cannot adjust non-ptr values for key '%s'", key))
	}

	// Ensure we can assign override to *value.
	if !override.Type().AssignableTo(val.Elem().Type()) {
		msg := fmt.Sprintf("Cannot assing '%v %v' to variable of type '%v' for key '%s'",
			override, override.Type(), val.Elem().Type(), key)
		if override.Kind() == reflect.Func {
			msg += "; did you intend to call testvalue.SetCallback() instead of testvalue.Set()?"
		}
		panic(msg)
	}

	// If the *value is an interface, and the override is a function,
	// we want to ensure that the function signatures match, and if they don't we return an error.
	// Note: if the functions signatures do not match, we can still assign to *value (since it's an interface).
	// We would get a run time error when we try to invoke this function (with wrong arguments), but it's better
	// to return an error right at the Adjust() point instead of getting the error at (potentially much) later time.
	if val.Elem().Kind() == reflect.Interface && override.Kind() == reflect.Func {
		ptrT := reflect.TypeOf(val.Elem().Interface())
		if !override.Type().AssignableTo(ptrT) {
			panic(fmt.Sprintf("Cannot assing '%v %v' to interface of type '%v' for key '%s'",
				override, override.Type(), ptrT, key))
		}
	}

	val.Elem().Set(override)
}

func checkEnabled() {
	if atomic.LoadInt32(&enabled) == 0 {
		panic("Cannot set test value; Did you call 'defer testvalue.Enable()()' in your test?")
	}
}
