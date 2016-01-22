// Copyright 2016 The Cockroach Authors.
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
// Author: Kenji Kaneda (kenji.kaneda@gmail.com)

package roachpb

import (
	"strings"
	"testing"
)

// TestSetTxn vefifies that SetTxn updates the error message.
func TestSetTxn(t *testing.T) {
	e := NewError(NewTransactionAbortedError())
	txn := NewTransaction("test", Key("a"), 1, SERIALIZABLE, Timestamp{}, 0)
	e.SetTxn(txn)
	if !strings.HasPrefix(e.Message, "txn aborted \"test\"") {
		t.Errorf("unexpected message: %s", e.Message)
	}
}
