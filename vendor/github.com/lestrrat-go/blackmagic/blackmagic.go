package blackmagic

import (
	"fmt"
	"reflect"
)

// AssignIfCompatible is a convenience function to safely
// assign arbitrary values. dst must be a pointer to an
// empty interface, or it must be a pointer to a compatible
// variable type that can hold src.
func AssignIfCompatible(dst, src interface{}) error {
	orv := reflect.ValueOf(src) // save this value for error reporting
	result := orv

	// t can be a pointer or a slice, and the code will slightly change
	// depending on this
	var isSlice bool
	switch result.Kind() {
	case reflect.Ptr:
		// no op
	case reflect.Slice:
		isSlice = true
	default:
		return fmt.Errorf("argument t to AssignIfCompatible must be a pointer or a slice: %T", src)
	}

	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf(`argument to AssignIfCompatible() must be a pointer: %T`, dst)
	}

	actualDst := rv.Elem()
	switch actualDst.Kind() {
	case reflect.Interface:
		// If it's an interface, we can just assign the pointer to the interface{}
	default:
		// If it's a pointer to the struct we're looking for, we need to set
		// the de-referenced struct
		if !isSlice {
			result = result.Elem()
		}
	}
	if !result.Type().AssignableTo(actualDst.Type()) {
		return fmt.Errorf(`argument to AssignIfCompatible() must be compatible with %T (was %T)`, orv.Interface(), dst)
	}

	if !actualDst.CanSet() {
		return fmt.Errorf(`argument to AssignIfCompatible() must be settable`)
	}
	actualDst.Set(result)

	return nil
}
