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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func newLogEventOp(e scpb.Element, ts scpb.TargetState) *scop.LogEvent {
	for _, t := range ts.Targets {
		if t.Element() == e {
			return &scop.LogEvent{
				TargetMetadata: *protoutil.Clone(&t.Metadata).(*scpb.TargetMetadata),
				Authorization:  *protoutil.Clone(&ts.Authorization).(*scpb.Authorization),
				Statement:      ts.Statements[t.Metadata.StatementID].RedactedStatement,
				StatementTag:   ts.Statements[t.Metadata.StatementID].StatementTag,
				Element:        *protoutil.Clone(&t.ElementProto).(*scpb.ElementProto),
				TargetStatus:   t.TargetStatus,
			}
		}
	}
	panic(errors.AssertionFailedf("could not find element %s in target state", screl.ElementString(e)))
}

// opsFunc are a fully-compiled and checked set of functions to emit operations
// given an element value.
type opsFunc func(element scpb.Element, targetState scpb.TargetState) []scop.Op

func makeOpsFunc(el scpb.Element, fns []interface{}) (opsFunc, error) {
	var funcValues []reflect.Value
	for _, fn := range fns {
		if err := checkOpFunc(el, fn); err != nil {
			return nil, err
		}
		funcValues = append(funcValues, reflect.ValueOf(fn))
	}
	return func(element scpb.Element, targetState scpb.TargetState) []scop.Op {
		ret := make([]scop.Op, 0, len(funcValues))
		in := []reflect.Value{reflect.ValueOf(element)}
		inWithMeta := []reflect.Value{reflect.ValueOf(element), reflect.ValueOf(targetState)}
		for _, fn := range funcValues {
			var out []reflect.Value
			if fn.Type().NumIn() == 1 {
				out = fn.Call(in)
			} else {
				out = fn.Call(inWithMeta)
			}
			if !out[0].IsNil() {
				ret = append(ret, out[0].Interface().(scop.Op))
			}
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
	if !(fnT.NumIn() == 1 && fnT.In(0) == elType) &&
		!(fnT.NumIn() == 2 && fnT.In(0) == elType &&
			fnT.In(1) == reflect.TypeOf(scpb.TargetState{})) {
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
