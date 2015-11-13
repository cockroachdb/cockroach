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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestClientTxnSequenceNumber verifies that the sequence number is increased
// between calls.
func TestClientTxnSequenceNumber(t *testing.T) {
	defer leaktest.AfterTest(t)
	count := 0
	var curSeq uint32
	db := NewDB(newTestSender(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		count++
		if ba.Txn.Sequence <= curSeq {
			return nil, roachpb.NewError(util.Errorf("sequence number %d did not increase", curSeq))
		}
		curSeq = ba.Txn.Sequence
		return ba.CreateReply(), nil
	}, nil))
	if err := db.Txn(func(txn *Txn) error {
		for range []int{1, 2, 3} {
			if err := txn.Put("a", "b"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Errorf("expected test sender to be invoked four times; got %d", count)
	}
}
