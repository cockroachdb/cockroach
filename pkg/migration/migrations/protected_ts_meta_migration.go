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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func protectedTsMetaPrivilegesMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, c migration.Cluster,
) error {
	return c.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		log.Infof(ctx, "%s", "updating privileges in system.protected_ts_meta descriptor")
		id := systemschema.ProtectedTimestampsMetaTable.GetID()
		// Read the system.protected_ts_meta table descriptor from storage.
		// We can't use the catalogkv API here, it's too high-level.
		// We specifically want to avoid running post-deserialization changes on
		// the descriptor here.
		descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, id)
		r, err := txn.Get(ctx, descKey)
		if err != nil {
			return err
		}
		var descProto descpb.Descriptor
		err = r.ValueProto(&descProto)
		if err != nil {
			return err
		}
		table, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&descProto, r.Value.Timestamp)
		// This will fix the privileges on the table descriptor, if necessary.
		isModified := descpb.MaybeFixPrivileges(id, &table.Privileges)
		if !isModified {
			return nil
		}
		// Write the fixed descriptor back to storage.
		b := txn.NewBatch()
		b.Put(descKey, &descProto)
		return txn.Run(ctx, b)
	})
}
