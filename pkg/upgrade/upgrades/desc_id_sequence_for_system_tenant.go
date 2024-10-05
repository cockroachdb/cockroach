// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func descIDSequenceForSystemTenant(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if !d.Codec.ForSystemTenant() {
		return nil
	}
	return d.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		oldEntry, err := txn.GetForUpdate(ctx, keys.LegacyDescIDGenerator, kvpb.BestEffort)
		if err != nil {
			return err
		}
		id, created, err := CreateSystemTableInTxn(
			ctx, d.Settings, txn, keys.SystemSQLCodec, systemschema.DescIDSequence,
		)
		if err != nil || !created {
			return err
		}
		// Install the appropriate value for the sequence. Note that we use the
		// existence of the sequence above to make this transaction idempotent.
		return txn.Put(ctx, d.Codec.SequenceKey(uint32(id)), oldEntry.ValueInt())
	})
}
