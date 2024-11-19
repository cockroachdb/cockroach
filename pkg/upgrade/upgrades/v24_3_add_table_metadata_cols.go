// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// addTableMetadataCols adds new columns to system.table_metadata table if they do not exist.
func addTableMetadataCols(
	ctx context.Context, version clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if _, err := txn.ExecEx(ctx,
			"truncate-table_metadata",
			txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"TRUNCATE table system.table_metadata"); err != nil {
			return errors.Wrap(err, "failed to truncate table before adding new column")
		}

		if _, err := txn.ExecEx(
			ctx,
			"add-table_metadata-details-col",
			txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`ALTER TABLE system.table_metadata 
  ADD COLUMN IF NOT EXISTS details JSONB NOT NULL FAMILY "primary",
  ADD COLUMN IF NOT EXISTS table_type string NOT NULL FAMILY "primary" 
  `,
		); err != nil {
			return errors.Wrap(err, "failed to add details col to table_metadata")
		}
		return nil
	})
}
