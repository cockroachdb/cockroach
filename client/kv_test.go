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

package client

import "testing"

// TODO(spencer): Verify all error paths in KV.RunTransaction()
// using intermediary senders, as in txn_sender_test.go.

// TestKVNestedTransactions verifies that trying to create nested
// transactions returns an error.
func TestKVNestedTransactions(t *testing.T) {
	client := NewKV(newTestSender(func(call *Call) {}), nil)
	client.RunTransaction(&TransactionOptions{}, func(txn *KV) error {
		if err := txn.RunTransaction(&TransactionOptions{}, func(txn *KV) error { return nil }); err == nil {
			t.Errorf("expected error starting a nested transaction")
		}
		return nil
	})
}
