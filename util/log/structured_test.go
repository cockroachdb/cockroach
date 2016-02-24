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

package log

import (
	"testing"

	"golang.org/x/net/context"
)

func TestMakeMessage(t *testing.T) {
	testCases := []struct {
		ctx      context.Context
		expected string
	}{
		{nil, "foo"},
		{Add(context.Background(), NodeID, 1), "[node=1] foo"},
		{Add(context.Background(), StoreID, 2), "[store=2] foo"},
		{Add(context.Background(), RangeID, 3), "[range=3] foo"},
		{Add(context.Background(), StoreID, 4, NodeID, 5), "[node=5,store=4] foo"},
	}
	for i, test := range testCases {
		msg := makeMessage(test.ctx, "foo", nil)
		if test.expected != msg {
			t.Fatalf("%d: expected %s, but found %s", i, test.expected, msg)
		}
	}
}
