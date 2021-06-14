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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func deleteDeprecatedNamespaceTableDescriptorMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	const pageSize = 1000
	deprecatedTablePrefix := d.Codec.TablePrefix(uint32(keys.DeprecatedNamespaceTableID))
	deprecatedTableDescKey := d.Codec.DescMetadataKey(keys.DeprecatedNamespaceTableID)
	namespaceNameKey := catalogkeys.MakePublicObjectNameKey(d.Codec, keys.SystemDatabaseID, `namespace`)
	namespace2NameKey := catalogkeys.MakePublicObjectNameKey(d.Codec, keys.SystemDatabaseID, `namespace2`)

	log.Info(ctx, "removing references to deprecated system.namespace table descriptor")
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		err := txn.Iterate(
			ctx,
			deprecatedTablePrefix,
			deprecatedTablePrefix.PrefixEnd(),
			pageSize,
			func(oldResults []kv.KeyValue) error {
				return checkDeprecatedEntries(ctx, txn, d.Codec, oldResults)
			},
		)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		b.DelRange(deprecatedTablePrefix, deprecatedTablePrefix.PrefixEnd(), false /* returnKeys */)
		b.Del(deprecatedTableDescKey)
		b.Del(namespace2NameKey)
		b.Put(namespaceNameKey, descpb.ID(keys.NamespaceTableID))
		nsTable, err := descriptors.GetMutableTableByID(
			ctx, txn, keys.NamespaceTableID, tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		if nsTable.GetPostDeserializationChanges().UpgradedNamespaceName {
			const kvTrace = false
			if err := descriptors.WriteDescToBatch(ctx, kvTrace, nsTable, b); err != nil {
				return err
			}
		}
		return txn.Run(ctx, b)
	})
}

// checkDeprecatedEntries checks that for each entry in the deprecated namespace
// table, there exists a corresponding entry in the new namespace table.
// The deprecated namespace table is not necessarily empty by the time this
// migration runs: it may still contain entries created prior to the
// introduction of the new namespace table.
func checkDeprecatedEntries(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, oldResults []kv.KeyValue,
) error {
	// Retrieve the corresponding entries in the new namespace table.
	b := txn.NewBatch()
	for _, result := range oldResults {
		parentID, name, err := decodeDeprecatedNameMetadataKey(codec, result.Key)
		if err != nil {
			log.Fatalf(ctx, "unexpected error decoding deprecated namespace table key: %s", err)
		}
		var newKey roachpb.Key
		if parentID == keys.RootNamespaceID {
			// This is a database key, look for it in the new namespace table.
			newKey = catalogkeys.MakeDatabaseNameKey(codec, name)
		} else {
			// This is a table key, look for it in the new namespace table.
			newKey = catalogkeys.MakePublicObjectNameKey(codec, parentID, name)
		}
		b.Get(newKey)
	}
	err := txn.Run(ctx, b)
	if err != nil {
		return err
	}
	// Check that there's a new entry for every one of the old entries.
	// Panic when that's not the case.
	if len(b.Results) != len(oldResults) {
		log.Fatalf(ctx, "expected %d batch results, instead got %d", len(oldResults), len(b.Results))
	}
	for i, newResult := range b.Results {
		if len(newResult.Rows) != 1 {
			log.Fatalf(ctx, "expected 1 row in batch results, instead got %d", len(newResult.Rows))
		}
		for _, row := range newResult.Rows {
			if !row.Exists() {
				parentID, name, _ := decodeDeprecatedNameMetadataKey(codec, oldResults[i].Key)
				log.Fatalf(ctx, "deprecated namespace table entry (%d, %q) -> %d "+
					"has no corresponding entry in new namespace table",
					parentID, name, oldResults[i].ValueInt())
			}
		}
	}
	return nil
}

// decodeDeprecatedNameMetadataKey returns the components that make up the
// NameMetadataKey for version < 20.1.
func decodeDeprecatedNameMetadataKey(
	codec keys.SQLCodec, k roachpb.Key,
) (parentID descpb.ID, name string, err error) {
	const deprecatedNamespaceTablePrimaryIndexID = 1

	k, _, err = codec.DecodeTablePrefix(k)
	if err != nil {
		return 0, "", err
	}

	var buf uint64
	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, "", err
	}
	if buf != deprecatedNamespaceTablePrimaryIndexID {
		return 0, "", errors.Newf("expected primary index ID %d, but got %d", deprecatedNamespaceTablePrimaryIndexID, buf)
	}

	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, "", err
	}
	parentID = descpb.ID(buf)

	var bytesBuf []byte
	_, bytesBuf, err = encoding.DecodeBytesAscending(k, nil)
	if err != nil {
		return 0, "", err
	}
	name = string(bytesBuf)

	return parentID, name, nil
}
