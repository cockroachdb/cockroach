// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// addTimeseriesZoneConfig creates a zone configuration for the timeseries
// range if one does not exist already. It will match the zone config that is
// created during bootstrap.
func addTimeseriesZoneConfig(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.SystemDeps,
) error {
	err := d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		row, err := txn.QueryRowEx(
			ctx, "check-timeseries-zone-config-exists", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"SELECT count(*) = 1 FROM [SHOW ZONE CONFIGURATIONS] WHERE target = 'RANGE timeseries'",
		)
		if err != nil {
			return err
		}
		hasTimeseriesRange, ok := tree.AsDBool(row[0])
		if !ok {
			return errors.New("unexpected result from SHOW ZONE CONFIGURATIONS")
		}
		if hasTimeseriesRange {
			return nil
		}
		if _, err := txn.ExecEx(
			ctx, "add-timeseries-zone-config", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"ALTER RANGE timeseries CONFIGURE ZONE USING gc.ttlseconds = 14400",
		); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
