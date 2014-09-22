// Copyright 2014 The Cockroach Authors.
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

package kv

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

// TestTxnDBBasics verifies that a simple transaction can be run and
// either committed or aborted. On commit, mutations are visible; on
// abort, mutations are never visible. During the txn, verify that
// uncommitted writes cannot be read outside of the txn but can be
// read from inside the txn.
func TestTxnDBBasics(t *testing.T) {
	db, clock, _ := createTestDB(t)
	value = []byte("value")

	for _, commit := range []bool{true, false} {
		// Use snapshot isolation so non-transactional read can always push.
		err := db.RunTransaction(storage.UserRoot, 1, proto.SNAPSHOT, func(txn storage.DB) error {
			key = []byte(fmt.Sprintf("key-%t", commit))

			// Put transactional value.
			if pr := <-txn.Put(proto.PutRequest{
				RequestHeader: proto.RequestHeader{Key: key},
				Value:         proto.Value{Bytes: value},
			}); pr.GoError() != nil {
				return nil
			}

			// Attempt to read outside of txn.
			if gr := <-db.Get(proto.GetRequest{
				RequestHeader: proto.RequestHeader{
					Key:       key,
					Timestamp: clock.Now(),
				},
			}); gr.GoError() != nil || gr.Value != nil {
				return util.Errorf("expected success reading nil value: %+v, %v", gr.Value, gr.GoError())
			}

			// Read within the transaction.
			if gr := <-txn.Get(proto.GetRequest{
				RequestHeader: proto.RequestHeader{Key: key},
			}); gr.GoError() != nil || gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
				return util.Errorf("expected success reading value %+v: %v", gr.Value, gr.GoError())
			}

			if !commit {
				return util.Errorf("purposefully failing transaction")
			}
		})

		if commit != (err == nil) {
			t.Errorf("expected success? %t; got %v", commit, err)
		} else if !commit && err.Error() != "purposefully failing transaction" {
			t.Errorf("unexpected failure with !commit: %v", err)
		}

		// Verify the value is now visible on commit == true, and not visible otherwise.
		gr := <-db.Get(proto.GetRequest{
			RequestHeader: proto.RequestHeader{
				Key:       key,
				Timestamp: clock.Now(),
			},
		})
		if commit {
			if gr.GoError() != nil || gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
				return util.Errorf("expected success reading value: %+v, %v", gr.Value, gr.GoError())
			}
		} else {
			if gr.GoError() != nil || gr.Value != nil {
				return util.Errorf("expected success and nil value: %+v, %v", gr.Value, gr.GoError())
			}
		}
	}
}
