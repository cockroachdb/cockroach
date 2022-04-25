// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvinterfaces

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
)

// ForceRetryableError implements tree.ForceRetryableError.
type ForceRetryableError struct {
	txn *kv.Txn
}

// NewForceRetryableError returns a new instance of
// ForceRetryableError.
func NewForceRetryableError(txn *kv.Txn) *ForceRetryableError {
	return &ForceRetryableError{
		txn: txn,
	}
}

// GenerateForcedRetryableError implements the tree.ForceRetryRunner interface.
func (r *ForceRetryableError) GenerateForcedRetryableError(ctx context.Context, msg string) error {
	return r.txn.GenerateForcedRetryableError(ctx, msg)
}
