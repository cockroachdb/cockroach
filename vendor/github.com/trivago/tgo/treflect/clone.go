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
	"reflect"
)

// Clone does a deep copy of the given value.
// Please note that field have to be public in order to be copied.
// Private fields will be ignored.
func Clone(v interface{}) interface{} {
	value := reflect.ValueOf(v)
	copy := clone(value)
	return copy.Interface()
}

func clone(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Struct:
		copy := reflect.New(v.Type())

		for i := 0; i < v.Type().NumField(); i++ {
			field := v.Field(i)
			targetField := copy.Elem().Field(i)
			if !targetField.CanSet() {
				continue // ignore private fields
			}
			fieldCopy := clone(field)
			targetField.Set(fieldCopy)
		}
		return copy.Elem()

	case reflect.Chan:
		copy := reflect.MakeChan(v.Type(), v.Len())
		return copy

	case reflect.Map:
		copy := reflect.MakeMap(v.Type())
		keys := v.MapKeys()
		for _, k := range keys {
			fieldCopy := clone(v.MapIndex(k))
			copy.SetMapIndex(k, fieldCopy)
		}
		return copy

	case reflect.Slice:
		copy := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			elementCopy := clone(v.Index(i))
			copy.Index(i).Set(elementCopy)
		}
		return copy

	case reflect.Array:
		copy := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			elementCopy := clone(v.Index(i))
			copy.Index(i).Set(elementCopy)
		}
		return copy

	default:
		copy := reflect.New(v.Type())
		copy.Elem().Set(v)
		return copy.Elem()
	}
}
