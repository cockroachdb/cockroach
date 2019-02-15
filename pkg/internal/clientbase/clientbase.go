// Copyright 2019 The Cockroach Authors.
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

package clientbase

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TxnRestartError indicates that a retriable error happened and so the
// transaction needs to be restarted.
type TxnRestartError struct {
	cause string
	// NewTxn, if set, means that the recepient of this error is supposed to
	// create a new transaction using the information in this proto.
	NewTxn *roachpb.Transaction
}

var _ error = TxnRestartError{}

// Error implements the error interface.
func (e TxnRestartError) Error() string {
	return fmt.Sprintf("transaction restart error (caused by: %s)", e.cause)
}

func (e TxnRestartError) RestartCause() string {
	return e.cause
}

func NewTxnRestartError(cause string, newTxn *roachpb.Transaction) TxnRestartError {
	return TxnRestartError{cause: cause, NewTxn: newTxn}
}
