// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func descIDSequenceForSystemTenant(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	if !d.Codec.ForSystemTenant() {
		return nil
	}
	mut := tabledesc.NewBuilder(systemschema.DescIDSequence.TableDesc()).BuildCreatedMutableTable()
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		oldEntry, err := txn.GetForUpdate(ctx, keys.LegacyDescIDGenerator)
		if err != nil {
			return err
		}
		mut.SequenceOpts.Start = oldEntry.ValueInt()
		_, _, err = CreateSystemTableInTxn(ctx, d.Settings, txn, keys.SystemSQLCodec, mut)
		return err
	})
}
