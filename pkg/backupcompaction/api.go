// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupcompaction

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// GetBackupCompactionManagerHook is the hook to get access to the backup compaction APIs.
// Used by builtin functions to trigger backup compaction.
var GetBackupCompactionManagerHook func(
	ctx context.Context,
	evalCtx *eval.Context,
) eval.BackupCompactionManager

// GetBackupCompactionManager returns a BackupCompactionManager.
func GetBackupCompactionManager(
	ctx context.Context, evalCtx *eval.Context,
) (eval.BackupCompactionManager, error) {
	if GetBackupCompactionManagerHook == nil {
		return nil, errors.New("missing backup compaction manager hook")
	}
	return GetBackupCompactionManagerHook(ctx, evalCtx), nil
}
