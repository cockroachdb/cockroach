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
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func deleteDeprecatedNamespaceTableDescriptorMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	deprecatedTablePrefix := d.Codec.TablePrefix(uint32(keys.DeprecatedNamespaceTableID))
	deprecatedTableDescKey := d.Codec.DescMetadataKey(keys.DeprecatedNamespaceTableID)
	namespaceNameKey := catalogkeys.MakeNameMetadataKey(
		d.Codec, keys.SystemDatabaseID, keys.PublicSchemaID, `namespace`)
	namespace2NameKey := catalogkeys.MakeNameMetadataKey(
		d.Codec, keys.SystemDatabaseID, keys.PublicSchemaID, `namespace2`)

	log.Info(ctx, "removing references to deprecated system.namespace table descriptor")
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		b.DelRange(deprecatedTablePrefix, deprecatedTablePrefix.PrefixEnd(), false /* returnKeys */)
		b.Del(deprecatedTableDescKey)
		b.Del(namespace2NameKey)
		b.Put(namespaceNameKey, descpb.ID(keys.NamespaceTableID))
		return txn.Run(ctx, b)
	})
}
