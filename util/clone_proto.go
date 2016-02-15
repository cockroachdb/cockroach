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
// Author: Tamir Duberstein (tamird@gmail.com)

package util

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/gogo/protobuf/proto"
)

var types struct {
	sync.Mutex
	known map[reflect.Type]bool
}

func init() {
	types.known = make(map[reflect.Type]bool)
}

// CloneProto returns a deep copy of a protocol buffer. If pb contains a
// util/uuid.UUID, CloneProto panics.
func CloneProto(pb proto.Message) proto.Message {
	if v := findArray(reflect.ValueOf(pb)); v != nil {
		panic(fmt.Sprintf("attempt to clone %+v, which contains %+v", pb, v))
	}

	return proto.Clone(pb)
}

func findArray(v reflect.Value) interface{} {
	if v.IsValid() && !typeIsOrContainsArray(v.Type()) {
		return nil
	}

	switch v.Kind() {
	case reflect.Ptr:
		return findArray(v.Elem())

	case reflect.Array:
		return v.Interface()

	case reflect.Map, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if elem := findArray(v.Index(i)); elem != nil {
				return elem
			}
		}

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if elem := findArray(v.Field(i)); elem != nil {
				return elem
			}
		}

	case reflect.Interface:
		// Not strictly correct, but cloning interfaces is not allowed.
		return v
	}

	return nil
}

func typeIsOrContainsArray(t reflect.Type) bool {
	types.Lock()
	defer types.Unlock()

	return typeIsOrContainsArrayLocked(t)
}

func typeIsOrContainsArrayLocked(t reflect.Type) bool {
	knownTypeIsOrContainsArray, ok := types.known[t]
	if !ok {
		knownTypeIsOrContainsArray = typeIsOrContainsArrayImpl(t)
		types.known[t] = knownTypeIsOrContainsArray
	}
	return knownTypeIsOrContainsArray
}

func typeIsOrContainsArrayImpl(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Array:
		return true
	case reflect.Map, reflect.Ptr, reflect.Slice:
		if typeIsOrContainsArrayLocked(t.Elem()) {
			return true
		}

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if typeIsOrContainsArrayLocked(t.Field(i).Type) {
				return true
			}
		}

	case reflect.Interface:
		// Not strictly correct, but cloning interfaces is not allowed.
		return true

	}

	return false
}
