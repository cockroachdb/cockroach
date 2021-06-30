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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TODO(ajwerner): Add assertions in descriptor validation that is active once
// the migration has completed. Consider how this interacts with unsafe
// descriptor injection.

func foreignKeyRepresentationUpgrade(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	var lastUpgradedID descpb.ID
	for {
		done, idToUpgrade, err := findNextDescriptorToUpdate(ctx, d.InternalExecutor, lastUpgradedID)
		if err != nil || done {
			return err
		}
		if err := upgradeFKRepresentation(ctx, idToUpgrade, d); err != nil {
			return err
		}
		lastUpgradedID = idToUpgrade
	}
}

func upgradeFKRepresentation(ctx context.Context, upgrade descpb.ID, d migration.TenantDeps) error {
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		t, err := descriptors.GetMutableTableByID(ctx, txn, upgrade, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}
		// Must have already happened, so no-op.
		if !t.GetPostDeserializationChanges().UpgradedForeignKeyRepresentation {
			log.Infof(ctx, "discovered fk representation already occurred for %d, skipping", upgrade)
			return nil
		}
		t.MaybeIncrementVersion()
		return descriptors.WriteDesc(ctx, false /* kvTrace */, t, txn)
	})
}

func findNextDescriptorToUpdate(
	ctx context.Context, ie sqlutil.InternalExecutor, lastScannedID descpb.ID,
) (done bool, idToUpgrade descpb.ID, _ error) {
	rows, err := ie.QueryIterator(ctx, "upgrade-fk-find-desc", nil, /* txn */
		`
SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor WHERE id > $1 ORDER BY ID ASC
`, lastScannedID)
	if err != nil {
		return false, 0, err
	}
	defer func() { _ = rows.Close() }()
	ok, err := rows.Next(ctx)
	for ; ok; ok, err = rows.Next(ctx) {
		row := rows.Cur()
		id, desc, ts, err := unmarshalDescFromDescriptorRow(row)
		if err != nil {
			return false, id, err
		}
		t, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&desc, ts)
		if t != nil && !t.Dropped() && tableNeedsFKUpgrade(t) {
			return false, id, nil
		}
	}
	if err != nil {
		return false, 0, err
	}
	return true, 0, nil
}

func tableNeedsFKUpgrade(t *descpb.TableDescriptor) bool {
	indexNeedsFKUpgrade := func(idx *descpb.IndexDescriptor) bool {
		return idx.ForeignKey.IsSet() || len(idx.ReferencedBy) > 0
	}
	if indexNeedsFKUpgrade(&t.PrimaryIndex) {
		return true
	}
	for i := range t.Indexes {
		if indexNeedsFKUpgrade(&t.Indexes[i]) {
			return true
		}
	}
	return false
}
