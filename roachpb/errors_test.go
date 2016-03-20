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

type testError struct{}

func (t *testError) Error() string              { return "test" }
func (t *testError) message(pErr *Error) string { return "test" }
func (t *testError) CanRetry() bool             { return true }

// TestNewError verifies that a test error that
// implements retryable or indexed is converted properly into a generic error.
func TestNewError(t *testing.T) {
	pErr := NewError(&testError{})
	if pErr.GoError().Error() != "test" {
		t.Errorf("unexpected error: %s", pErr)
	}
	if !pErr.Retryable {
		t.Error("expected generic error to be retryable")
	}
}

// TestNewErrorNil verifies that a nil error can be set
// and retrieved from a response header.
func TestNewErrorNil(t *testing.T) {
	pErr := NewError(nil)
	if pErr != nil {
		t.Errorf("expected nil error; got %s", pErr)
	}
}

// TestSetTxn vefifies that SetTxn updates the error message.
func TestSetTxn(t *testing.T) {
	e := NewError(NewTransactionAbortedError())
	txn := NewTransaction("test", Key("a"), 1, SERIALIZABLE, Timestamp{}, 0)
	e.SetTxn(txn)
	if !strings.HasPrefix(e.Message, "txn aborted \"test\"") {
		t.Errorf("unexpected message: %s", e.Message)
	}
}

func TestErrorTxn(t *testing.T) {
	var pErr *Error
	if txn := pErr.GetTxn(); txn != nil {
		t.Fatalf("wanted nil, unexpected: %+v", txn)
	}
	pErr = &Error{}
	const name = "X"
	pErr.SetTxn(&Transaction{Name: name})
	if txn := pErr.GetTxn(); txn == nil || txn.Name != name {
		t.Fatalf("wanted name %s, unexpected: %+v", name, txn)
	}
}

// TestStripErrorTransaction verifies that StripErrorTransaction
// strips the transaction information on an error, but it does not
// update the error message.
func TestStripErrorTransaction(t *testing.T) {
	txn := NewTransaction("test", Key("a"), 1, SERIALIZABLE, Timestamp{}, 0)
	pErr := NewErrorWithTxn(NewTransactionAbortedError(), txn)
	if pErr.GetTxn() == nil {
		t.Errorf("no txn on error: %s", pErr)
	}
	if !strings.HasPrefix(pErr.Message, "txn aborted \"test\"") {
		t.Errorf("unexpected message: %s", pErr.Message)
	}

	pErr.StripErrorTransaction()
	if pErr.GetTxn() != nil {
		t.Errorf("txn must be stripped: %s", pErr)
	}
	if !strings.HasPrefix(pErr.Message, "txn aborted \"test\"") {
		t.Errorf("unexpected message: %s", pErr.Message)
	}
}
