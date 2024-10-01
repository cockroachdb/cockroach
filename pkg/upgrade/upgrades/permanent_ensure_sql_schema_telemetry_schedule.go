// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

func ensureSQLSchemaTelemetrySchedule(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := schematelemetrycontroller.CreateSchemaTelemetrySchedule(
			ctx, txn, d.Settings, d.ClusterID,
		)
		// If the schedule already exists, we have nothing more to do. This
		// logic makes the upgrade idempotent.
		if errors.Is(err, schematelemetrycontroller.ErrDuplicatedSchedules) {
			err = nil
		}
		return err
	})
}
