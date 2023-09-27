// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func ensureSQLSchemaTelemetrySchedule(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Since this Schedule Creation job is expected to fail before the schedule
		// is persisted to the schedules table, it is fine to instantiate this
		// schedule with an empty cluster ID, which would also cause schedule
		// creation to fail.
		_, err := schematelemetrycontroller.CreateSchemaTelemetrySchedule(
			ctx, txn, d.Settings, uuid.UUID{},
		)
		// If the schedule already exists, we have nothing more to do. This
		// logic makes the upgrade idempotent.
		if errors.Is(err, schematelemetrycontroller.ErrDuplicatedSchedules) {
			err = nil
		}
		return err
	})
}
