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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
)

// zonesTableForSecondaryTenants adds system.zones to secondary tenants and
// seeds it with the RANGE DEFAULT zone configuration.
func zonesTableForSecondaryTenants(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	if d.Codec.ForSystemTenant() {
		// We don't need to add system.zones for the host tenant as it should already
		// be present.
		return nil
	}
	if err := sqlmigrations.CreateSystemTable(
		ctx, d.DB, d.Codec, d.Settings, systemschema.ZonesTable,
	); err != nil {
		return err
	}
	defaultZoneConfig := zonepb.DefaultZoneConfig()
	zoneKV := bootstrap.CreateZoneConfigKV(keys.RootNamespaceID, d.Codec, &defaultZoneConfig)
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		b.Put(zoneKV.Key, &zoneKV.Value)
		return txn.Run(ctx, b)
	})
}
