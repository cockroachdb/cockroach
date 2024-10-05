// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func migrateOldStylePTSRecords(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Read deprecated PTS records in one sweep and buffer them. Avoid using an iterator
	// to reduce the lifetime of the transaction. This helps reduce contention on the table.
	ids, err := deps.InternalExecutor.QueryBufferedEx(ctx, "pts-migration-read-ids", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride, `
		SELECT id FROM system.protected_ts_records WHERE target IS NULL;
	`)
	if err != nil {
		return err
	}

	for _, dID := range ids {
		id := dID[0].(*tree.DUuid).UUID.GetBytes()
		// Use a separate transaction for each record to avoid a long-running transaction.
		err := deps.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			rows, err := txn.QueryBufferedEx(ctx, "pts-migration-read-record", txn.KV(), sessiondata.NodeUserSessionDataOverride,
				"SELECT spans FROM system.protected_ts_records WHERE id = $1", id)
			if err != nil {
				return err
			}
			// The PTS record may have been deleted since the read transaction above.
			if len(rows) == 0 {
				return nil
			}

			row := rows[0]

			var spans ptstorage.Spans
			if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &spans); err != nil {
				return errors.Wrapf(err, "failed to unmarshal span for %v", id)
			}

			var tableIDs descpb.IDs
			for _, sp := range spans.Spans {
				var startKey, endkey roachpb.Key
				var err error
				startKey, _, err = keys.DecodeTenantPrefix(sp.Key)
				if err != nil {
					return err
				}
				_, startTableID, err := encoding.DecodeUvarintAscending(startKey)
				if err != nil {
					return err
				}

				if !sp.EndKey.Equal(sp.Key.PrefixEnd()) {
					endkey, _, err = keys.DecodeTenantPrefix(sp.EndKey)
					if err != nil {
						return err
					}
					_, endTableID, err := encoding.DecodeUvarintAscending(endkey)
					if err != nil {
						return err
					}
					// Track all tables between the two keys.
					for tID := startTableID; tID <= endTableID; tID += 1 {
						tableIDs = append(tableIDs, descpb.ID(tID))
					}
				} else {
					tableIDs = append(tableIDs, descpb.ID(startTableID))
				}
			}

			encodedTarget, err := protoutil.Marshal(&ptpb.Target{
				Union: ptpb.MakeSchemaObjectsTarget(tableIDs).GetUnion(),
			})
			if err != nil {
				return err
			}

			_, err = txn.ExecEx(ctx, "pts-migration-write-record", txn.KV(), sessiondata.NodeUserSessionDataOverride,
				"UPDATE system.protected_ts_records SET spans = $1, target = $2 WHERE id = $3", encodedTarget, encodedTarget, id)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
