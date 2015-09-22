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
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestClientCommandID verifies that client command ID is set
// on call.
func TestClientCommandID(t *testing.T) {
	defer leaktest.AfterTest(t)
	count := 0
	db := NewDB(newTestSender(func(ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		count++
		if ba.CmdID.WallTime == 0 {
			t.Errorf("expected client command ID to be initialized")
		}
		return &proto.BatchResponse{}, nil
	}, nil))
	if err := db.Put("a", "b"); err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("expected test sender to be invoked once; got %d", count)
	}
}
