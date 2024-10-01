// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// deleteDatabaseZoneConfig removes the zone config for a given database ID.
func deleteDatabaseZoneConfig(
	ctx context.Context,
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	databaseID descpb.ID,
) error {
	if databaseID == descpb.InvalidID {
		return nil
	}
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()

		// Delete the zone config entry for the dropped database associated with the
		// job, if it exists.
		dbZoneKeyPrefix := config.MakeZoneKeyPrefix(codec, databaseID)
		b.DelRange(dbZoneKeyPrefix, dbZoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		return txn.Run(ctx, b)
	})
}
