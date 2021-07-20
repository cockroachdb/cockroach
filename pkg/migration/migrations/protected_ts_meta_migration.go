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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func protectedTsMetaPrivilegesMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	id := systemschema.ProtectedTimestampsMetaTable.GetID()
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB,
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			log.Infof(ctx, "%s", "updating privileges in system.protected_ts_meta descriptor")
			mut, err := descriptors.GetMutableTableByID(ctx, txn, id, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return err
			}
			if mut.GetVersion() > 1 {
				// Descriptor has already been upgraded, skip.
				return nil
			}
			// Privileges have already been fixed at this point by the descriptor
			// unwrapping logic in catalogkv which runs post-deserialization changes,
			// but we still need to bump the version number.
			mut.Version = 2
			return descriptors.WriteDesc(ctx, false /* kvTrace */, mut, txn)
		},
	)
}
