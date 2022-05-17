// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// ensureCommentsHaveNonDroppedIndexes cleans up any comments associated with
// indexes that no longer exist.
func ensureCommentsHaveNonDroppedIndexes(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Delete the rows that don't belong to any indexes.
		_, err := d.InternalExecutor.QueryBufferedEx(
			ctx,
			"select-comments-with-missing-indexes",
			txn,
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			`DELETE FROM system.comments
      WHERE type = $1
            AND (object_id, sub_id)
				NOT IN (
						SELECT (descriptor_id, index_id)
						  FROM crdb_internal.table_indexes
					);`,
			keys.IndexCommentType,
		)
		if err != nil {
			return err
		}
		return txn.Commit(ctx)
	})
}
