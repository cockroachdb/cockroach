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

package tcontainer

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/trivago/tgo/treflect"
)

// MarshalMap is a wrapper type to attach converter methods to maps normally
// returned by marshalling methods, i.e. key/value parsers.
// All methods that do a conversion will return an error if the value stored
// behind key is not of the expected type or if the key is not existing in the
// map.
type MarshalMap map[string]interface{}

const (
	// MarshalMapSeparator defines the rune used for path separation
	MarshalMapSeparator = '/'
	// MarshalMapArrayBegin defines the rune starting array index notation
	MarshalMapArrayBegin = '['
	// MarshalMapArrayEnd defines the rune ending array index notation
	MarshalMapArrayEnd = ']'
)

// NewMarshalMap creates a new marshal map (string -> interface{})
func NewMarshalMap() MarshalMap {
	return make(map[string]interface{})
}

// TryConvertToMarshalMap converts collections to MarshalMap if possible.
// This is a deep conversion, i.e. each element in the collection will be
// traversed. You can pass a formatKey function that will be applied to all
// string keys that are detected.
func TryConvertToMarshalMap(value interface{}, formatKey func(string) string) interface{} {
	valueMeta := reflect.ValueOf(value)
	switch valueMeta.Kind() {
	default:
		return value

	case reflect.Array, reflect.Slice:
		arrayLen := valueMeta.Len()
		converted := make([]interface{}, arrayLen)
		for i := 0; i < arrayLen; i++ {
			converted[i] = TryConvertToMarshalMap(valueMeta.Index(i).Interface(), formatKey)
		}
		return converted

	case reflect.Map:
		converted := NewMarshalMap()
		keys := valueMeta.MapKeys()

		for _, keyMeta := range keys {
			strKey, isString := keyMeta.Interface().(string)
			if !isString {
				continue
			}
			if formatKey != nil {
				strKey = formatKey(strKey)
			}
			val := valueMeta.MapIndex(keyMeta).Interface()
			converted[strKey] = TryConvertToMarshalMap(val, formatKey)
		}
		return converted // ### return, converted MarshalMap ###
	}
}

// ConvertToMarshalMap tries to convert a compatible map type to a marshal map.
// Compatible types are map[interface{}]interface{}, map[string]interface{} and of
// course MarshalMap. The same rules as for ConvertValueToMarshalMap apply.
func ConvertToMarshalMap(value interface{}, formatKey func(string) string) (MarshalMap, error) {
	converted := TryConvertToMarshalMap(value, formatKey)
	if result, isMap := converted.(MarshalMap); isMap {
		return result, nil
	}
	return nil, fmt.Errorf("Root value cannot be converted to MarshalMap")
}

// Clone creates a copy of the given MarshalMap.
func (mmap MarshalMap) Clone() MarshalMap {
	clone := cloneMap(reflect.ValueOf(mmap))
	return clone.Interface().(MarshalMap)
}

func cloneMap(mapValue reflect.Value) reflect.Value {
	clone := reflect.MakeMap(mapValue.Type())
	keys := mapValue.MapKeys()

	for _, k := range keys {
		v := mapValue.MapIndex(k)
		switch k.Kind() {
		default:
			clone.SetMapIndex(k, v)

		case reflect.Array, reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Map {
				sliceCopy := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
				for i := 0; i < v.Len(); i++ {
					element := v.Index(i)
					sliceCopy.Index(i).Set(cloneMap(element))
				}
			} else {
				sliceCopy := reflect.MakeSlice(v.Type(), 0, v.Len())
				reflect.Copy(sliceCopy, v)
				clone.SetMapIndex(k, sliceCopy)
			}

		case reflect.Map:
			vClone := cloneMap(v)
			clone.SetMapIndex(k, vClone)
		}
	}

	return clone
}

// Bool returns a value at key that is expected to be a boolean
func (mmap MarshalMap) Bool(key string) (bool, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return false, fmt.Errorf(`"%s" is not set`, key)
	}

	boolValue, isBool := val.(bool)
	if !isBool {
		return false, fmt.Errorf(`"%s" is expected to be a boolean`, key)
	}
	return boolValue, nil
}

// Uint returns a value at key that is expected to be an uint64 or compatible
// integer value.
func (mmap MarshalMap) Uint(key string) (uint64, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return 0, fmt.Errorf(`"%s" is not set`, key)
	}

	if intVal, isNumber := treflect.Uint64(val); isNumber {
		return intVal, nil
	}

	return 0, fmt.Errorf(`"%s" is expected to be an unsigned number type`, key)
}

// Int returns a value at key that is expected to be an int64 or compatible
// integer value.
func (mmap MarshalMap) Int(key string) (int64, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return 0, fmt.Errorf(`"%s" is not set`, key)
	}

	if intVal, isNumber := treflect.Int64(val); isNumber {
		return intVal, nil
	}

	return 0, fmt.Errorf(`"%s" is expected to be a signed number type`, key)
}

// Float returns a value at key that is expected to be a float64 or compatible
// float value.
func (mmap MarshalMap) Float(key string) (float64, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return 0, fmt.Errorf(`"%s" is not set`, key)
	}

	if floatVal, isNumber := treflect.Float64(val); isNumber {
		return floatVal, nil
	}

	return 0, fmt.Errorf(`"%s" is expected to be a signed number type`, key)
}

// Duration returns a value at key that is expected to be a string
func (mmap MarshalMap) Duration(key string) (time.Duration, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return time.Duration(0), fmt.Errorf(`"%s" is not set`, key)
	}

	switch val.(type) {
	case time.Duration:
		return val.(time.Duration), nil
	case string:
		return time.ParseDuration(val.(string))
	}

	return time.Duration(0), fmt.Errorf(`"%s" is expected to be a duration or string`, key)
}

// String returns a value at key that is expected to be a string
func (mmap MarshalMap) String(key string) (string, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return "", fmt.Errorf(`"%s" is not set`, key)
	}

	strValue, isString := val.(string)
	if !isString {
		return "", fmt.Errorf(`"%s" is expected to be a string`, key)
	}
	return strValue, nil
}

// Bytes returns a value at key that is expected to be a []byte
func (mmap MarshalMap) Bytes(key string) ([]byte, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return []byte{}, fmt.Errorf(`"%s" is not set`, key)
	}

	bytesValue, isBytes := val.([]byte)
	if !isBytes {
		return []byte{}, fmt.Errorf(`"%s" is expected to be a []byte`, key)
	}
	return bytesValue, nil
}

// Slice is an alias for Array
func (mmap MarshalMap) Slice(key string) ([]interface{}, error) {
	return mmap.Array(key)
}

// Array returns a value at key that is expected to be a []interface{}
func (mmap MarshalMap) Array(key string) ([]interface{}, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return nil, fmt.Errorf(`"%s" is not set`, key)
	}

	arrayValue, isArray := val.([]interface{})
	if !isArray {
		return nil, fmt.Errorf(`"%s" is expected to be an array`, key)
	}
	return arrayValue, nil
}

// Map returns a value at key that is expected to be a
// map[interface{}]interface{}.
func (mmap MarshalMap) Map(key string) (map[interface{}]interface{}, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return nil, fmt.Errorf(`"%s" is not set`, key)
	}

	mapValue, isMap := val.(map[interface{}]interface{})
	if !isMap {
		return nil, fmt.Errorf(`"%s" is expected to be a map`, key)
	}
	return mapValue, nil
}

func castToStringArray(key string, value interface{}) ([]string, error) {
	switch value.(type) {
	case string:
		return []string{value.(string)}, nil

	case []interface{}:
		arrayVal := value.([]interface{})
		stringArray := make([]string, 0, len(arrayVal))

		for _, val := range arrayVal {
			strValue, isString := val.(string)
			if !isString {
				return nil, fmt.Errorf(`"%s" does not contain string keys`, key)
			}
			stringArray = append(stringArray, strValue)
		}
		return stringArray, nil

	case []string:
		return value.([]string), nil

	default:
		return nil, fmt.Errorf(`"%s" is not a valid string array type`, key)
	}
}

// StringSlice is an alias for StringArray
func (mmap MarshalMap) StringSlice(key string) ([]string, error) {
	return mmap.StringArray(key)
}

// StringArray returns a value at key that is expected to be a []string
// This function supports conversion (by copy) from
//  * []interface{}
func (mmap MarshalMap) StringArray(key string) ([]string, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return nil, fmt.Errorf(`"%s" is not set`, key)
	}

	return castToStringArray(key, val)
}

func castToInt64Array(key string, value interface{}) ([]int64, error) {
	switch value.(type) {
	case int:
		return []int64{value.(int64)}, nil

	case []interface{}:
		arrayVal := value.([]interface{})
		intArray := make([]int64, 0, len(arrayVal))

		for _, val := range arrayVal {
			intValue, isInt := val.(int64)
			if !isInt {
				return nil, fmt.Errorf(`"%s" does not contain int keys`, key)
			}
			intArray = append(intArray, intValue)
		}
		return intArray, nil

	case []int64:
		return value.([]int64), nil

	default:
		return nil, fmt.Errorf(`"%s" is not a valid string array type`, key)
	}
}

// Int64Slice is an alias for Int64Array
func (mmap MarshalMap) Int64Slice(key string) ([]int64, error) {
	return mmap.Int64Array(key)
}

// Int64Array returns a value at key that is expected to be a []int64
// This function supports conversion (by copy) from
//  * []interface{}
func (mmap MarshalMap) Int64Array(key string) ([]int64, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return nil, fmt.Errorf(`"%s" is not set`, key)
	}

	return castToInt64Array(key, val)
}

// StringMap returns a value at key that is expected to be a map[string]string.
// This function supports conversion (by copy) from
//  * map[interface{}]interface{}
//  * map[string]interface{}
func (mmap MarshalMap) StringMap(key string) (map[string]string, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return nil, fmt.Errorf(`"%s" is not set`, key)
	}

	switch val.(type) {
	case map[string]string:
		return val.(map[string]string), nil

	default:
		valueMeta := reflect.ValueOf(val)
		if valueMeta.Kind() != reflect.Map {
			return nil, fmt.Errorf(`"%s" is expected to be a map[string]string but is %T`, key, val)
		}

		result := make(map[string]string)
		for _, keyMeta := range valueMeta.MapKeys() {
			strKey, isString := keyMeta.Interface().(string)
			if !isString {
				return nil, fmt.Errorf(`"%s" is expected to be a map[string]string. Key is not a string`, key)
			}

			value := valueMeta.MapIndex(keyMeta)
			strValue, isString := value.Interface().(string)
			if !isString {
				return nil, fmt.Errorf(`"%s" is expected to be a map[string]string. Value is not a string`, key)
			}

			result[strKey] = strValue
		}

		return result, nil
	}
}

// StringSliceMap is an alias for StringArrayMap
func (mmap MarshalMap) StringSliceMap(key string) (map[string][]string, error) {
	return mmap.StringArrayMap(key)
}

// StringArrayMap returns a value at key that is expected to be a
// map[string][]string. This function supports conversion (by copy) from
//  * map[interface{}][]interface{}
//  * map[interface{}]interface{}
//  * map[string]interface{}
func (mmap MarshalMap) StringArrayMap(key string) (map[string][]string, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return nil, fmt.Errorf(`"%s" is not set`, key)
	}

	switch val.(type) {
	case map[string][]string:
		return val.(map[string][]string), nil

	default:
		valueMeta := reflect.ValueOf(val)
		if valueMeta.Kind() != reflect.Map {
			return nil, fmt.Errorf(`"%s" is expected to be a map[string][]string but is %T`, key, val)
		}

		result := make(map[string][]string)
		for _, keyMeta := range valueMeta.MapKeys() {
			strKey, isString := keyMeta.Interface().(string)
			if !isString {
				return nil, fmt.Errorf(`"%s" is expected to be a map[string][]string. Key is not a string`, key)
			}

			value := valueMeta.MapIndex(keyMeta)
			arrayValue, err := castToStringArray(strKey, value.Interface())
			if err != nil {
				return nil, fmt.Errorf(`"%s" is expected to be a map[string][]string. Value is not a []string`, key)
			}

			result[strKey] = arrayValue
		}

		return result, nil
	}
}

// MarshalMap returns a value at key that is expected to be another MarshalMap
// This function supports conversion (by copy) from
//  * map[interface{}]interface{}
func (mmap MarshalMap) MarshalMap(key string) (MarshalMap, error) {
	val, exists := mmap.Value(key)
	if !exists {
		return nil, fmt.Errorf(`"%s" is not set`, key)
	}

	return ConvertToMarshalMap(val, nil)
}

// Value returns a value from a given value path.
// Fields can be accessed by their name. Nested fields can be accessed by using
// "/" as a separator. Arrays can be addressed using the standard array
// notation "[<index>]".
// Examples:
// "key"         -> mmap["key"]              single value
// "key1/key2"   -> mmap["key1"]["key2"]     nested map
// "key1[0]"     -> mmap["key1"][0]          nested array
// "key1[0]key2" -> mmap["key1"][0]["key2"]  nested array, nested map
func (mmap MarshalMap) Value(key string) (val interface{}, exists bool) {
	exists = mmap.resolvePath(key, mmap, func(p, k reflect.Value, v interface{}) {
		val = v
	})
	return val, exists
}

// Delete a value from a given path.
// The path must point to a map key. Deleting from arrays is not supported.
func (mmap MarshalMap) Delete(key string) {
	mmap.resolvePath(key, mmap, func(p, k reflect.Value, v interface{}) {
		if v != nil {
			p.SetMapIndex(k, reflect.Value{})
		}
	})
}

// Set a value for a given path.
// The path must point to a map key. Setting array elements is not supported.
func (mmap MarshalMap) Set(key string, val interface{}) {
	mmap.resolvePath(key, mmap, func(p, k reflect.Value, v interface{}) {
		p.SetMapIndex(k, reflect.ValueOf(val))
	})
}

func (mmap MarshalMap) resolvePathKey(key string) (int, int) {
	keyEnd := len(key)
	nextKeyStart := keyEnd
	pathIdx := strings.IndexRune(key, MarshalMapSeparator)
	arrayIdx := strings.IndexRune(key, MarshalMapArrayBegin)

	if pathIdx > -1 && pathIdx < keyEnd {
		keyEnd = pathIdx
		nextKeyStart = pathIdx + 1 // don't include slash
	}
	if arrayIdx > -1 && arrayIdx < keyEnd {
		keyEnd = arrayIdx
		nextKeyStart = arrayIdx // include bracket because of multidimensional arrays
	}

	// a       -> key: "a", remain: ""       -- value
	// a/b/c   -> key: "a", remain: "b/c"    -- nested map
	// a[1]b/c -> key: "a", remain: "[1]b/c" -- nested array

	return keyEnd, nextKeyStart
}

func (mmap MarshalMap) resolvePath(k string, v interface{}, action func(p, k reflect.Value, v interface{})) bool {
	if len(k) == 0 {
		action(reflect.Value{}, reflect.ValueOf(k), v) // ### return, found requested value ###
		return true
	}

	vValue := reflect.ValueOf(v)
	switch vValue.Kind() {
	case reflect.Array, reflect.Slice:
		startIdx := strings.IndexRune(k, MarshalMapArrayBegin) // Must be first char, otherwise malformed
		endIdx := strings.IndexRune(k, MarshalMapArrayEnd)     // Must be > startIdx, otherwise malformed

		if startIdx == -1 || endIdx == -1 {
			return false
		}

		if startIdx == 0 && endIdx > startIdx {
			index, err := strconv.Atoi(k[startIdx+1 : endIdx])

			// [1]    -> index: "1", remain: ""    -- value
			// [1]a/b -> index: "1", remain: "a/b" -- nested map
			// [1][2] -> index: "1", remain: "[2]" -- nested array

			if err == nil && index < vValue.Len() {
				item := vValue.Index(index).Interface()
				key := k[endIdx+1:]
				return mmap.resolvePath(key, item, action) // ### return, nested array ###
			}
		}

	case reflect.Map:
		kValue := reflect.ValueOf(k)
		if storedValue := vValue.MapIndex(kValue); storedValue.IsValid() {
			action(vValue, kValue, storedValue.Interface())
			return true
		}

		keyEnd, nextKeyStart := mmap.resolvePathKey(k)
		if keyEnd == len(k) {
			action(vValue, kValue, nil) // call action to support setting non-existing keys
			return false                // ### return, key not found ###
		}

		nextKey := k[:keyEnd]
		nkValue := reflect.ValueOf(nextKey)

		if storedValue := vValue.MapIndex(nkValue); storedValue.IsValid() {
			remain := k[nextKeyStart:]
			return mmap.resolvePath(remain, storedValue.Interface(), action) // ### return, nested map ###
		}
	}

	return false
}
