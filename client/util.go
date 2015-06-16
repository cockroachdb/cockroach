// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"encoding"
	"fmt"
	"math"
	"reflect"

	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TODO(pmattis): The methods in this file needs tests.

func marshalKey(k interface{}) ([]byte, error) {
	// Note that the ordering here is important. In particular, proto.Key is also
	// a fmt.Stringer.
	switch t := k.(type) {
	case string:
		return []byte(t), nil
	case proto.Key:
		return []byte(t), nil
	case []byte:
		return t, nil
	case encoding.BinaryMarshaler:
		return t.MarshalBinary()
	case fmt.Stringer:
		return []byte(t.String()), nil
	}
	return nil, fmt.Errorf("unable to marshal key: %T", k)
}

// marshalValue returns a proto.Value initialized from the source
// reflect.Value, returning an error if the types are not compatible.
func marshalValue(v reflect.Value) (proto.Value, error) {
	var r proto.Value
	if !v.IsValid() {
		return r, nil
	}

	switch t := v.Interface().(type) {
	case nil:
		return r, nil

	case string:
		r.Bytes = []byte(t)
		return r, nil

	case []byte:
		r.Bytes = t
		return r, nil

	case proto.Key:
		r.Bytes = []byte(t)
		return r, nil

	case gogoproto.Message:
		var err error
		r.Bytes, err = gogoproto.Marshal(t)
		return r, err

	case encoding.BinaryMarshaler:
		var err error
		r.Bytes, err = t.MarshalBinary()
		return r, err
	}

	switch v.Kind() {
	case reflect.Bool:
		i := int64(0)
		if v.Bool() {
			i = 1
		}
		r.SetInteger(i)
		return r, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		r.SetInteger(v.Int())
		return r, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		r.SetInteger(int64(v.Uint()))
		return r, nil

	case reflect.Float32, reflect.Float64:
		r.SetInteger(int64(math.Float64bits(v.Float())))
		return r, nil

	case reflect.String:
		r.Bytes = []byte(v.String())
		return r, nil
	}

	return r, fmt.Errorf("unable to marshal value: %s", v)
}

// unmarshalValue sets the destination reflect.Value contents from the source
// proto.Value, returning an error if the types are not compatible.
func unmarshalValue(src *proto.Value, dest reflect.Value) error {
	if src == nil {
		dest.Set(reflect.Zero(dest.Type()))
		return nil
	}

	switch d := dest.Addr().Interface().(type) {
	case *string:
		if src.Bytes != nil {
			*d = string(src.Bytes)
		} else {
			*d = ""
		}
		return nil

	case *[]byte:
		if src.Bytes != nil {
			*d = src.Bytes
		} else {
			*d = nil
		}
		return nil

	case *gogoproto.Message:
		panic("TODO(pmattis): unimplemented")

	case *encoding.BinaryUnmarshaler:
		panic("TODO(pmattis): unimplemented")
	}

	switch dest.Kind() {
	case reflect.Bool:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetBool(i != 0)
		return nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetInt(i)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetUint(uint64(i))
		return nil

	case reflect.Float32, reflect.Float64:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetFloat(math.Float64frombits(uint64(i)))
		return nil

	case reflect.String:
		if src == nil || src.Bytes == nil {
			dest.SetString("")
			return nil
		}
		dest.SetString(string(src.Bytes))
		return nil
	}

	return fmt.Errorf("unable to unmarshal value: %s", dest.Type())
}
