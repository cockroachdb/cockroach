// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/errwrap/testdata/src/github.com/cockroachdb/errors"
)

// ensureCommentsHaveNonDroppedIndexes cleans up any comments associated with
// indexes that no longer exist.
func ensureCommentsHaveNonDroppedIndexes(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Determine the comments that belong to non-existent indexes.
		rows, err := d.InternalExecutor.QueryBufferedEx(
			ctx,
			"select-comments-with-missing-indexes",
			txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			`SELECT object_id, sub_id
  FROM system.comments AS comment
 WHERE type = $1
       AND sub_id
		NOT IN (
				SELECT index_id
				  FROM crdb_internal.table_indexes
				 WHERE descriptor_id = comment.object_id
			);`,
			keys.IndexCommentType,
		)
		if err != nil || len(rows) == 0 {
			return err
		}
		// Clean up each comment with a missing index.
		for _, row := range rows {
			objectID := row[0].(*tree.DInt)
			subID := row[1].(*tree.DInt)
			deletedRows, err := d.InternalExecutor.ExecEx(
				ctx,
				"delete-index-comment",
				txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				"DELETE FROM system.comments WHERE object_id=$1 and sub_id=$2",
				objectID,
				subID,
			)
			if err != nil {
				return err
			}
			if deletedRows == 0 {
				return errors.AssertionFailedf("no comments were deleted with "+
					"table_id=%s and index_id=%s", objectID, subID)
			}
		}
		return txn.Commit(ctx)
	})
}
