// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TODO(pmattis): The methods in this file needs tests.

func marshalKey(k interface{}) (roachpb.Key, error) {
	switch t := k.(type) {
	case *roachpb.Key:
		return *t, nil
	case roachpb.Key:
		return t, nil
	case *roachpb.RKey:
		return t.AsRawKey(), nil
	case roachpb.RKey:
		return t.AsRawKey(), nil
	case string:
		return roachpb.Key(t), nil
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
	case *roachpb.Value:
		if err := t.VerifyHeader(); err != nil {
			panic(err)
		}
		return *t, nil

	case nil:
		return r, nil

	case bool:
		r.SetBool(t)
		return r, nil

	case string:
		r.SetBytes([]byte(t))
		return r, nil

	case []byte:
		r.SetBytes(t)
		return r, nil

	case apd.Decimal:
		err := r.SetDecimal(&t)
		return r, err

	case roachpb.Key:
		r.SetBytes([]byte(t))
		return r, nil

	case time.Time:
		r.SetTime(t)
		return r, nil

	case duration.Duration:
		err := r.SetDuration(t)
		return r, err

	case protoutil.Message:
		err := r.SetProto(t)
		return r, err

	case roachpb.Value:
		panic("unexpected type roachpb.Value (use *roachpb.Value)")
	}

	// Handle all of the Go primitive types besides struct and pointers. This
	// switch also handles types based on a primitive type (e.g. "type MyInt
	// int").
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.Bool:
		r.SetBool(v.Bool())
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

	return r, fmt.Errorf("unable to marshal %T: %v", v, v)
}
