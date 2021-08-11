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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func spanConfigurationsTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	if !d.Codec.ForSystemTenant() {
		return nil
	}

	if err := sqlmigrations.CreateSystemTable(
		ctx, d.DB, d.Codec, d.Settings, systemschema.SpanConfigurationsTable,
	); err != nil {
		return err
	}

	defaultSpanConfig := zonepb.DefaultZoneConfigRef().AsSpanConfig()
	buf, err := protoutil.Marshal(&defaultSpanConfig)
	if err != nil {
		return err
	}

	// XXX: DOC, need to populate it with initial values.
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := d.InternalExecutor.ExecEx(ctx, "populate-total-scfg", txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"UPSERT INTO system.span_configurations (start_key, end_key, config) VALUES ($1, $2, $3)",
			keys.MinKey, keys.MaxKey, buf,
		)
		return err
	})
}
