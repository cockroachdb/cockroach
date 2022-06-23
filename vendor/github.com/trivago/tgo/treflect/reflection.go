// Copyright 2015-2018 trivago N.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package treflect

import (
	"fmt"
	"reflect"
	"unsafe"
)

// GetMissingMethods checks if a given object implements all methods of a
// given interface. It returns the interface coverage [0..1] as well as an array
// of error messages. If the interface is correctly implemented the coverage is
// 1 and the error message array is empty.
func GetMissingMethods(objType reflect.Type, ifaceType reflect.Type) (float32, []string) {
	missing := []string{}
	if objType.Implements(ifaceType) {
		return 1.0, missing
	}

	methodCount := ifaceType.NumMethod()
	for mIdx := 0; mIdx < methodCount; mIdx++ {
		ifaceMethod := ifaceType.Method(mIdx)
		objMethod, exists := objType.MethodByName(ifaceMethod.Name)
		signatureMismatch := false

		switch {
		case !exists:
			missing = append(missing, fmt.Sprintf("Missing: \"%s\" %v", ifaceMethod.Name, ifaceMethod.Type))
			continue // ### continue, error found ###

		case ifaceMethod.Type.NumOut() != objMethod.Type.NumOut():
			signatureMismatch = true

		case ifaceMethod.Type.NumIn()+1 != objMethod.Type.NumIn():
			signatureMismatch = true

		default:
			for oIdx := 0; !signatureMismatch && oIdx < ifaceMethod.Type.NumOut(); oIdx++ {
				signatureMismatch = ifaceMethod.Type.Out(oIdx) != objMethod.Type.Out(oIdx)
			}
			for iIdx := 0; !signatureMismatch && iIdx < ifaceMethod.Type.NumIn(); iIdx++ {
				signatureMismatch = ifaceMethod.Type.In(iIdx) != objMethod.Type.In(iIdx+1)
			}
		}

		if signatureMismatch {
			missing = append(missing, fmt.Sprintf("Invalid: \"%s\" %v is not %v", ifaceMethod.Name, objMethod.Type, ifaceMethod.Type))
		}
	}

	return float32(methodCount-len(missing)) / float32(methodCount), missing
}

// Int64 converts any signed number type to an int64.
// The second parameter is returned as false if a non-number type was given.
func Int64(v interface{}) (int64, bool) {

	switch reflect.TypeOf(v).Kind() {
	case reflect.Int:
		return int64(v.(int)), true
	case reflect.Int8:
		return int64(v.(int8)), true
	case reflect.Int16:
		return int64(v.(int16)), true
	case reflect.Int32:
		return int64(v.(int32)), true
	case reflect.Int64:
		return v.(int64), true
	case reflect.Float32:
		return int64(v.(float32)), true
	case reflect.Float64:
		return int64(v.(float64)), true
	}

	return 0, false
}

// Uint64 converts any unsigned number type to an uint64.
// The second parameter is returned as false if a non-number type was given.
func Uint64(v interface{}) (uint64, bool) {

	switch reflect.TypeOf(v).Kind() {
	case reflect.Uint:
		return uint64(v.(uint)), true
	case reflect.Uint8:
		return uint64(v.(uint8)), true
	case reflect.Uint16:
		return uint64(v.(uint16)), true
	case reflect.Uint32:
		return uint64(v.(uint32)), true
	case reflect.Uint64:
		return v.(uint64), true
	}

	return 0, false
}

// Float32 converts any number type to an float32.
// The second parameter is returned as false if a non-number type was given.
func Float32(v interface{}) (float32, bool) {

	switch reflect.TypeOf(v).Kind() {
	case reflect.Int:
		return float32(v.(int)), true
	case reflect.Uint:
		return float32(v.(uint)), true
	case reflect.Int8:
		return float32(v.(int8)), true
	case reflect.Uint8:
		return float32(v.(uint8)), true
	case reflect.Int16:
		return float32(v.(int16)), true
	case reflect.Uint16:
		return float32(v.(uint16)), true
	case reflect.Int32:
		return float32(v.(int32)), true
	case reflect.Uint32:
		return float32(v.(uint32)), true
	case reflect.Int64:
		return float32(v.(int64)), true
	case reflect.Uint64:
		return float32(v.(uint64)), true
	case reflect.Float32:
		return v.(float32), true
	case reflect.Float64:
		return float32(v.(float64)), true
	}

	return 0, false
}

// Float64 converts any number type to an float64.
// The second parameter is returned as false if a non-number type was given.
func Float64(v interface{}) (float64, bool) {

	switch reflect.TypeOf(v).Kind() {
	case reflect.Int:
		return float64(v.(int)), true
	case reflect.Uint:
		return float64(v.(uint)), true
	case reflect.Int8:
		return float64(v.(int8)), true
	case reflect.Uint8:
		return float64(v.(uint8)), true
	case reflect.Int16:
		return float64(v.(int16)), true
	case reflect.Uint16:
		return float64(v.(uint16)), true
	case reflect.Int32:
		return float64(v.(int32)), true
	case reflect.Uint32:
		return float64(v.(uint32)), true
	case reflect.Int64:
		return float64(v.(int64)), true
	case reflect.Uint64:
		return float64(v.(uint64)), true
	case reflect.Float32:
		return float64(v.(float32)), true
	case reflect.Float64:
		return v.(float64), true
	}

	return 0, false
}

// RemovePtrFromType will return the type of t and strips away any pointer(s)
// in front of the actual type.
func RemovePtrFromType(t interface{}) reflect.Type {
	var v reflect.Type
	if rt, isType := t.(reflect.Type); isType {
		v = rt
	} else {
		v = reflect.TypeOf(t)
	}
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

// RemovePtrFromValue will return the value of t and strips away any pointer(s)
// in front of the actual type.
func RemovePtrFromValue(t interface{}) reflect.Value {
	var v reflect.Value
	if rv, isValue := t.(reflect.Value); isValue {
		v = rv
	} else {
		v = reflect.ValueOf(t)
	}
	for v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

// UnsafeCopy will copy data from src to dst while ignoring type information.
// Both types need to be of the same size and dst and src have to be pointers.
// UnsafeCopy will panic if these requirements are not met.
func UnsafeCopy(dst, src interface{}) {
	dstValue := reflect.ValueOf(dst)
	srcValue := reflect.ValueOf(src)
	UnsafeCopyValue(dstValue, srcValue)
}

// UnsafeCopyValue will copy data from src to dst while ignoring type
// information. Both types need to be of the same size or this function will
// panic. Also both types must support dereferencing via reflect.Elem()
func UnsafeCopyValue(dstValue reflect.Value, srcValue reflect.Value) {
	dstType := dstValue.Elem().Type()
	srcType := srcValue.Type()

	var srcPtr uintptr
	if srcValue.Kind() != reflect.Ptr {
		// If we don't get a pointer to our source data we need to forcefully
		// retrieve it by accessing the interface pointer. This is ok as we
		// only read from it.
		iface := srcValue.Interface()
		srcPtr = reflect.ValueOf(&iface).Elem().InterfaceData()[1] // Pointer to data
	} else {
		srcType = srcValue.Elem().Type()
		srcPtr = srcValue.Pointer()
	}

	if dstType.Size() != srcType.Size() {
		panic("Type size mismatch between " + dstType.String() + " and " + srcType.String())
	}

	dstAsSlice := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: dstValue.Pointer(),
		Len:  int(dstType.Size()),
		Cap:  int(dstType.Size()),
	}))

	srcAsSlice := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: srcPtr,
		Len:  int(srcType.Size()),
		Cap:  int(srcType.Size()),
	}))

	copy(dstAsSlice, srcAsSlice)
}

// SetMemberByName sets member name of the given pointer-to-struct to the data
// passed to this function. The member may be private, too.
func SetMemberByName(ptrToStruct interface{}, name string, data interface{}) {
	structVal := reflect.Indirect(reflect.ValueOf(ptrToStruct))
	member := structVal.FieldByName(name)

	SetValue(member, data)
}

// SetMemberByIndex sets member idx of the given pointer-to-struct to the data
// passed to this function. The member may be private, too.
func SetMemberByIndex(ptrToStruct interface{}, idx int, data interface{}) {
	structVal := reflect.Indirect(reflect.ValueOf(ptrToStruct))
	member := structVal.Field(idx)

	SetValue(member, data)
}

// SetValue sets an addressable value to the data passed to this function.
// In contrast to golangs reflect package this will also work with private
// variables. Please note that this function may not support all types, yet.
func SetValue(member reflect.Value, data interface{}) {
	if member.CanSet() {
		member.Set(reflect.ValueOf(data).Convert(member.Type()))
		return // ### return, easy way ###
	}

	if !member.CanAddr() {
		panic("SetValue requires addressable member type")
	}

	ptrToMember := unsafe.Pointer(member.UnsafeAddr())
	dataValue := reflect.ValueOf(data)

	switch member.Kind() {
	case reflect.Bool:
		*(*bool)(ptrToMember) = dataValue.Bool()

	case reflect.Uint:
		*(*uint)(ptrToMember) = uint(dataValue.Uint())

	case reflect.Uint8:
		*(*uint8)(ptrToMember) = uint8(dataValue.Uint())

	case reflect.Uint16:
		*(*uint16)(ptrToMember) = uint16(dataValue.Uint())

	case reflect.Uint32:
		*(*uint32)(ptrToMember) = uint32(dataValue.Uint())

	case reflect.Uint64:
		*(*uint64)(ptrToMember) = dataValue.Uint()

	case reflect.Int:
		*(*int)(ptrToMember) = int(dataValue.Int())

	case reflect.Int8:
		*(*int8)(ptrToMember) = int8(dataValue.Int())

	case reflect.Int16:
		*(*int16)(ptrToMember) = int16(dataValue.Int())

	case reflect.Int32:
		*(*int32)(ptrToMember) = int32(dataValue.Int())

	case reflect.Int64:
		*(*int64)(ptrToMember) = dataValue.Int()

	case reflect.Float32:
		*(*float32)(ptrToMember) = float32(dataValue.Float())

	case reflect.Float64:
		*(*float64)(ptrToMember) = dataValue.Float()

	case reflect.Complex64:
		*(*complex64)(ptrToMember) = complex64(dataValue.Complex())

	case reflect.Complex128:
		*(*complex128)(ptrToMember) = dataValue.Complex()

	case reflect.String:
		*(*string)(ptrToMember) = dataValue.String()

	case reflect.Map, reflect.Chan:
		// Exploit the fact that "map" is actually "*runtime.hmap" and force
		// overwrite that pointer in the passed struct.
		// Same foes for "chan" which is actually "*runtime.hchan".

		// Note: Assigning a map or channel to another variable does NOT copy
		// the contents so copying the pointer follows go's standard behavior.
		dataAsPtr := unsafe.Pointer(dataValue.Pointer())
		*(**uintptr)(ptrToMember) = (*uintptr)(dataAsPtr)

	case reflect.Interface:
		// Interfaces are basically two pointers, see runtime.iface.
		// We want to modify exactly that data, which is returned by
		// the InterfaceData() method.

		if dataValue.Kind() != reflect.Interface {
			// A type reference was passed. In order to overwrite the memory
			// Representation of an interface we need to generate it first.
			// Reflect does not allow us to do that unless we use the
			// InterfaceData method which exposes the internal representation
			// of an interface.
			interfaceData := reflect.ValueOf(&data).Elem().InterfaceData()
			dataValue = reflect.ValueOf(interfaceData)
		}
		fallthrough

	default:
		// Complex types are assigned memcpy style.
		// Note: This should not break the garbage collector although we cannot
		// be 100% sure on this.
		UnsafeCopyValue(member.Addr(), dataValue)
	}
}
