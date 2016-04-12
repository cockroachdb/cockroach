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

package protoutil

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/gogo/protobuf/proto"
)

var verbotenKinds = [...]reflect.Kind{
	reflect.Array,
}

type typeKey struct {
	typ      reflect.Type
	verboten reflect.Kind
}

var types struct {
	sync.Mutex
	known map[typeKey]bool
}

func init() {
	types.known = make(map[typeKey]bool)
}

// Clone uses proto.Clone to return a deep copy of pb. It panics if pb
// recursively contains any instances of types which are known to be
// unsupported by proto.Clone.
//
// This function and its associated lint (see `make check`) exist to ensure we
// do not attempt to proto.Clone types which are not supported by proto.Clone.
// This hackery is necessary because proto.Clone gives no direct indication
// that it has incompletely cloned a type; it merely logs to standard output
// (see https://github.com/golang/protobuf/blob/89238a3/proto/clone.go#L204).
//
// The concrete case against which this is currently guarding may be resolved
// upstream, see https://github.com/gogo/protobuf/issues/147.
func Clone(pb proto.Message) proto.Message {
	for _, verbotenKind := range verbotenKinds {
		if v := findVerboten(reflect.ValueOf(pb), verbotenKind); v != nil {
			panic(fmt.Sprintf("attempt to clone %+v, which contains %+v", pb, v))
		}
	}

	return proto.Clone(pb)
}

func findVerboten(v reflect.Value, verboten reflect.Kind) interface{} {
	// Check if v's type can ever contain anything verboten.
	if v.IsValid() && !typeIsOrContainsVerboten(v.Type(), verboten) {
		return nil
	}

	// OK, v might contain something verboten based on its type, now check if it
	// actually does.
	switch v.Kind() {
	case verboten:
		return v.Interface()

	case reflect.Ptr:
		return findVerboten(v.Elem(), verboten)

	case reflect.Map:
		for _, key := range v.MapKeys() {
			if elem := findVerboten(v.MapIndex(key), verboten); elem != nil {
				return elem
			}
		}

	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if elem := findVerboten(v.Index(i), verboten); elem != nil {
				return elem
			}
		}

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if elem := findVerboten(v.Field(i), verboten); elem != nil {
				return elem
			}
		}

	case reflect.Chan, reflect.Func, reflect.Interface:
		// Not strictly correct, but cloning these kinds is not allowed.
		return v
	}

	return nil
}

func typeIsOrContainsVerboten(t reflect.Type, verboten reflect.Kind) bool {
	types.Lock()
	defer types.Unlock()

	return typeIsOrContainsVerbotenLocked(t, verboten)
}

func typeIsOrContainsVerbotenLocked(t reflect.Type, verboten reflect.Kind) bool {
	key := typeKey{t, verboten}
	knownTypeIsOrContainsVerboten, ok := types.known[key]
	if !ok {
		knownTypeIsOrContainsVerboten = typeIsOrContainsVerbotenImpl(t, verboten)
		types.known[key] = knownTypeIsOrContainsVerboten
	}
	return knownTypeIsOrContainsVerboten
}

func typeIsOrContainsVerbotenImpl(t reflect.Type, verboten reflect.Kind) bool {
	switch t.Kind() {
	case verboten:
		return true

	case reflect.Map:
		if typeIsOrContainsVerbotenLocked(t.Key(), verboten) || typeIsOrContainsVerbotenLocked(t.Elem(), verboten) {
			return true
		}

	case reflect.Array, reflect.Ptr, reflect.Slice:
		if typeIsOrContainsVerbotenLocked(t.Elem(), verboten) {
			return true
		}

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if typeIsOrContainsVerbotenLocked(t.Field(i).Type, verboten) {
				return true
			}
		}

	case reflect.Chan, reflect.Func, reflect.Interface:
		// Not strictly correct, but cloning these kinds is not allowed.
		return true

	}

	return false
}
