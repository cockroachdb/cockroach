// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

// opsFunc are a fully-compiled and checked set of functions to emit operations
// given an element value.
type opsFunc func(element scpb.Element) []scop.Op

func makeOpsFunc(el scpb.Element, fns []interface{}) (opsFunc, error) {
	var funcValues []reflect.Value
	for _, fn := range fns {
		if err := checkOpFunc(el, fn); err != nil {
			return nil, err
		}
		funcValues = append(funcValues, reflect.ValueOf(fn))
	}
	return func(element scpb.Element) []scop.Op {
		ret := make([]scop.Op, 0, len(funcValues))
		in := []reflect.Value{reflect.ValueOf(element)}
		for _, fn := range funcValues {
			out := fn.Call(in)
			ret = append(ret, out[0].Interface().(scop.Op))
		}
		return ret
	}, nil
}

var opType = reflect.TypeOf((*scop.Op)(nil)).Elem()

func checkOpFunc(el scpb.Element, fn interface{}) error {
	fnV := reflect.ValueOf(fn)
	fnT := fnV.Type()
	if fnT.Kind() != reflect.Func {
		return errors.Errorf(
			"%v is a %s, expected %s", fnT, fnT.Kind(), reflect.Func,
		)
	}
	elType := reflect.TypeOf(el)
	if fnT.NumIn() != 1 || fnT.In(0) != elType {
		return errors.Errorf(
			"expected %v to be a func with one argument of type %s", fnT, elType,
		)
	}
	if fnT.NumOut() != 1 || !fnT.Out(0).Implements(opType) {
		return errors.Errorf(
			"expected %v to be a func with one return value of type %s", fnT, opType,
		)
	}
	return nil
}
