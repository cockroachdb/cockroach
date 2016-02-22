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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package log_test

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

type testArg struct {
	StrVal string
	IntVal int64
}

func (t testArg) String() string {
	return fmt.Sprintf("%d-->%s", t.IntVal, t.StrVal)
}

func testContext() context.Context {
	ctx := context.Background()
	return log.Add(ctx, log.NodeID, int32(1), log.StoreID, int32(2), log.RangeID, int64(3))
}

func TestSetLogEntry(t *testing.T) {
	ctx := testContext()

	nodeID := ctx.Value(log.NodeID).(int32)
	storeID := ctx.Value(log.StoreID).(int32)
	rangeID := ctx.Value(log.RangeID).(int64)

	testCases := []struct {
		ctx      context.Context
		format   string
		args     []interface{}
		expEntry log.LogEntry
	}{
		{nil, "", []interface{}{}, log.LogEntry{}},
		{ctx, "", []interface{}{}, log.LogEntry{
			NodeID: &nodeID, StoreID: &storeID, RangeID: &rangeID,
		}},
		{ctx, "no args", []interface{}{}, log.LogEntry{
			NodeID: &nodeID, StoreID: &storeID, RangeID: &rangeID,
			Format: "no args",
		}},
		{ctx, "1 arg %s", []interface{}{"foo"}, log.LogEntry{
			NodeID: &nodeID, StoreID: &storeID, RangeID: &rangeID,
			Format: "1 arg %s",
			Args: []log.LogEntry_Arg{
				{Str: "foo"},
			},
		}},
		// Try a float64 argument with width and precision specified.
		{nil, "float arg %10.4f", []interface{}{math.Pi}, log.LogEntry{
			Format: "float arg %s",
			Args: []log.LogEntry_Arg{
				{Str: "    3.1416", Json: []byte("3.141592653589793")},
			},
		}},
		// Try a roachpb.Key argument.
		{nil, "Key arg %s", []interface{}{roachpb.Key("\x00\xff")}, log.LogEntry{
			Format: "Key arg %s",
			Args: []log.LogEntry_Arg{
				{Str: "\"\\x00\\xff\""},
			},
		}},
		// Verify multiple args and set the formatting very particularly for int type.
		{nil, "2 args %s %010d", []interface{}{"foo", 1}, log.LogEntry{
			Format: "2 args %s %s",
			Args: []log.LogEntry_Arg{
				{Str: "foo"},
				{Str: "0000000001", Json: []byte("1")},
			},
		}},
		// Set argument to a non-simple type with custom stringer which will yield a JSON value in the Arg.
		{nil, "JSON arg %s", []interface{}{testArg{"foo", 10}}, log.LogEntry{
			Format: "JSON arg %s",
			Args: []log.LogEntry_Arg{
				{Str: "10-->foo", Json: []byte("{\"StrVal\":\"foo\",\"IntVal\":10}")},
			},
		}},
		// Error format test.
		{nil, "Error format s%", []interface{}{"foo"}, log.LogEntry{
			Format: "Error format s",
			Args: []log.LogEntry_Arg{
				{Str: "foo"},
			},
		}},
	}
	for i, test := range testCases {
		for i, arg := range test.args {
			test.expEntry.Args[i].Type = fmt.Sprintf("%T", arg)
		}

		entry := log.LogEntry{}
		entry.Set(test.ctx, test.format, test.args)
		if !reflect.DeepEqual(&entry, &test.expEntry) {
			t.Errorf("%d: expected:\n%+v\ngot:\n%+v", i, &test.expEntry, entry)
		}
	}
}
