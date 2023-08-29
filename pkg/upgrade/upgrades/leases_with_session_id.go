// Copyright 2023 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func leaseWaitForSessionIDAdoption(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Wait for all leases to be expired and after this timestamp, which will
	// guarantee an entry exists in the *new* index with session ID.
	now := d.DB.KV().Clock().Now().GoTime()
	var rowCount int64 = 1
	for r := retry.Start(retry.Options{MaxBackoff: time.Minute}); r.Next() && rowCount > 0; {
		if err := d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// FIXME: This isn't crash safe :eyes:
			// FIXME: Speed this up by using a settings hook
			res, err := txn.QueryRow(ctx, "count-leases",
				txn.KV(),
				`SELECT count(*) FROM system.lease WHERE  expiration > current_timestamp AND expiration < $1 + '5 minutes'`,
				now)

			if err != nil {
				return err
			}

			rows := res[0].(*tree.DInt)
			rowCount = int64(*rows)
			return nil
		}); err != nil {
			return err
		}
	}
	// At this point all the leases have an equivalent session leases.
	// Existing queries running at this time will either:
	// 1) Query the old tables
	// 2) Query the old and new tables
	// All executing queries will be using synthetic descriptors,
	// so they should be safe once the new binary is deployed. The
	// two version invariant will hold, since any renewals will be
	// after this timestamp.
	return nil
}

func updateLeaseTableDesc(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Make the new version of the descriptor public, which will
	// only have the sessionID column.
	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		leaseDesc, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, keys.LeaseTableID)
		if err != nil {
			return err
		}
		targetLeaseDesc := systemschema.LeaseTable().NewBuilder().BuildCreatedMutable().(*tabledesc.Mutable)
		// Copy the subset of fields that were modified.
		leaseDesc.PrimaryIndex = targetLeaseDesc.PrimaryIndex
		leaseDesc.Columns = targetLeaseDesc.Columns
		leaseDesc.Families = targetLeaseDesc.Families
		leaseDesc.NextIndexID = targetLeaseDesc.NextIndexID
		leaseDesc.NextColumnID = targetLeaseDesc.NextColumnID
		return txn.Descriptors().WriteDesc(ctx, false /* kv batch */, leaseDesc, txn.KV())
	})
}
