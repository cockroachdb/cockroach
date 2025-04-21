// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/errors"
)

// withSavepoint is a utility function that runs the provided function within
// the context of the savepoint.
//
// If the caller wishes to recover from an error returned by an internal
// executor and keep the transaction, then the call to the internal executor
// must be scoped within a savepoint. Failed internal executor calls are not
// atomic and may have left behind some KV operations. Usually that is okay
// because the error is passed up the call stack and the transaction is rolled
// back.
func withSavepoint(ctx context.Context, txn *kv.Txn, fn func() error) error {
	// In the classic sql writer the txn is optional.
	if txn == nil {
		return fn()
	}
	// TODO(jeffswenson): consider changing the internal executor so all calls
	// implicitly create and apply/rollback savepoints.
	savepoint, err := txn.CreateSavepoint(ctx)
	if err != nil {
		return err
	}
	err = fn()
	if err != nil {
		// NOTE: we return the save point error if rollback fails because we do not
		// want something checking error types to attempt to handle the inner
		// error.
		if savePointErr := txn.RollbackToSavepoint(ctx, savepoint); savePointErr != nil {
			return errors.WithSecondaryError(savePointErr, err)
		}
		return err
	}
	if err := txn.ReleaseSavepoint(ctx, savepoint); err != nil {
		return err
	}
	return nil
}
