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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/gogo/protobuf/proto"
	"github.com/shopspring/decimal"
)

// TODO(pmattis): The methods in this file needs tests.

func marshalKey(k interface{}) (roachpb.Key, error) {
	switch t := k.(type) {
	case string:
		return roachpb.Key(t), nil
	case roachpb.Key:
		return t, nil
	case []byte:
		return roachpb.Key(t), nil
	}
	return nil, fmt.Errorf("unable to marshal key: %T %q", k, k)
}

// marshalValue returns a roachpb.Value initialized from the source
// interface{}, returning an error if the types are not compatible.
func marshalValue(v interface{}) (roachpb.Value, error) {
	var r roachpb.Value

	// Handle a few common types via a type switch.
	switch t := v.(type) {
	case nil:
		return r, nil

	case bool:
		i := int64(0)
		if t {
			i = 1
		}
		r.SetInt(i)
		return r, nil

	case string:
		r.SetBytes([]byte(t))
		return r, nil

	case []byte:
		r.SetBytes(t)
		return r, nil

	case decimal.Decimal:
		r.SetDecimal(t)
		return r, nil

	case roachpb.Key:
		r.SetBytes([]byte(t))
		return r, nil

	case time.Time:
		r.SetTime(t)
		return r, nil

	case proto.Message:
		err := r.SetProto(t)
		return r, err
	}

	// Handle all of the Go primitive types besides struct and pointers. This
	// switch also handles types based on a primitive type (e.g. "type MyInt
	// int").
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.Bool:
		i := int64(0)
		if v.Bool() {
			i = 1
		}
		r.SetInt(i)
		return r, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		r.SetInt(v.Int())
		return r, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		r.SetInt(int64(v.Uint()))
		return r, nil

	case reflect.Float32, reflect.Float64:
		r.SetFloat(v.Float())
		return r, nil

	case reflect.String:
		r.SetBytes([]byte(v.String()))
		return r, nil
	}

	return r, fmt.Errorf("unable to marshal value: %v", v)
}
