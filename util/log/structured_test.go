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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package log

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
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
	return Add(ctx, NodeID, proto.NodeID(1), StoreID, proto.StoreID(2), RaftID, int64(3), Method, proto.Get, Key, proto.Key("key"))
}

func TestSetLogEntry(t *testing.T) {
	ctx := testContext()

	testCases := []struct {
		ctx      context.Context
		format   string
		args     []interface{}
		expEntry proto.LogEntry
	}{
		{nil, "", []interface{}{}, proto.LogEntry{}},
		{ctx, "", []interface{}{}, proto.LogEntry{
			NodeID: gogoproto.Int32(1), StoreID: gogoproto.Int32(2), RaftID: gogoproto.Int64(3), Method: gogoproto.Int32(1), Key: []byte("key"),
		}},
		{ctx, "no args", []interface{}{}, proto.LogEntry{
			NodeID: gogoproto.Int32(1), StoreID: gogoproto.Int32(2), RaftID: gogoproto.Int64(3), Method: gogoproto.Int32(1), Key: []byte("key"),
			Format: "no args",
		}},
		{ctx, "1 arg %s", []interface{}{"foo"}, proto.LogEntry{
			NodeID: gogoproto.Int32(1), StoreID: gogoproto.Int32(2), RaftID: gogoproto.Int64(3), Method: gogoproto.Int32(1), Key: []byte("key"),
			Format: "1 arg %s",
			Args: []proto.LogEntry_Arg{
				{Type: "string", Str: "foo"},
			},
		}},
		// Try a float64 argument with width and precision specified.
		{nil, "float arg %10.4f", []interface{}{math.Pi}, proto.LogEntry{
			Format: "float arg %s",
			Args: []proto.LogEntry_Arg{
				{Type: "float64", Str: "    3.1416", Json: []byte("3.141592653589793")},
			},
		}},
		// Try a proto.Key argument.
		{nil, "Key arg %s", []interface{}{proto.Key("\x00\xff")}, proto.LogEntry{
			Format: "Key arg %s",
			Args: []proto.LogEntry_Arg{
				{Type: "proto.Key", Str: "\"\\x00\\xff\""},
			},
		}},
		// Verify multiple args and set the formatting very particularly for int type.
		{nil, "2 args %s %010d", []interface{}{"foo", 1}, proto.LogEntry{
			Format: "2 args %s %s",
			Args: []proto.LogEntry_Arg{
				{Type: "string", Str: "foo"},
				{Type: "int", Str: "0000000001", Json: []byte("1")},
			},
		}},
		// Set argument to a non-simple type with custom stringer which will yield a JSON value in the Arg.
		{nil, "JSON arg %s", []interface{}{testArg{"foo", 10}}, proto.LogEntry{
			Format: "JSON arg %s",
			Args: []proto.LogEntry_Arg{
				{Type: "log.testArg", Str: "10-->foo", Json: []byte("{\"StrVal\":\"foo\",\"IntVal\":10}")},
			},
		}},
	}
	for i, test := range testCases {
		entry := &proto.LogEntry{}
		setLogEntry(test.ctx, test.format, test.args, entry)
		if !reflect.DeepEqual(entry, &test.expEntry) {
			t.Errorf("%d: expected %+v; got %+v", i, &test.expEntry, entry)
		}
	}
}
