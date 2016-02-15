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

	"github.com/cockroachdb/cockroach/util/uuid"
)

var uuidValueType = reflect.TypeOf(uuid.UUID{})

var types struct {
	sync.Mutex
	knownGood map[reflect.Type]struct{}
}

func init() {
	types.knownGood = make(map[reflect.Type]struct{})
}

// CloneProto returns a deep copy of a protocol buffer. If pb contains a
// util/uuid.UUID, CloneProto panics.
func CloneProto(pb proto.Message) proto.Message {
	t := reflect.TypeOf(pb)

	types.Lock()
	defer types.Unlock()

	if _, ok := types.knownGood[t]; !ok {
		if isOrContainsUUID(t) {
			if v := findUUIDValue(reflect.ValueOf(pb)); v != nil {
				panic(fmt.Sprintf("attempt to clone %+v, which contains %+v", pb, v))
			}
		} else {
			types.knownGood[t] = struct{}{}
		}
	}
	return proto.Clone(pb)
}

func isOrContainsUUID(t reflect.Type) bool {
	if t == uuidValueType {
		return true
	}

	switch t.Kind() {
	case reflect.Array, reflect.Map, reflect.Ptr, reflect.Slice:
		if isOrContainsUUID(t.Elem()) {
			return true
		}

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if isOrContainsUUID(t.Field(i).Type) {
				return true
			}
		}

	case reflect.Interface:
		// Not strictly correct, but cloning interfaces is not allowed.
		return true

	}

	return false
}

func findUUIDValue(v reflect.Value) interface{} {
	if v.IsValid() {
		if v.Type() == uuidValueType {
			return v.Interface()
		}
	}

	switch v.Kind() {
	case reflect.Ptr:
		return findUUIDValue(v.Elem())

	case reflect.Array, reflect.Map, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if elem := findUUIDValue(v.Index(i)); elem != nil {
				return elem
			}
		}

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if elem := findUUIDValue(v.Field(i)); elem != nil {
				return elem
			}
		}

	case reflect.Interface:
		// Not strictly correct, but cloning interfaces is not allowed.
		return v
	}

	return nil
}
