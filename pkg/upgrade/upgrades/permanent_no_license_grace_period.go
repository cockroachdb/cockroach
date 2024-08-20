// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var _ = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.license_grace_period_start.timestamp",
	"this marks the start of the license grace period when no license exists. It is set programmatically.",
	0,
	settings.WithInternalOnly(),
)

// setNoLicenseGracePeriodStartTime will record the current time as the starting
// point for the no-license grace period.
func setNoLicenseGracePeriodStartTime(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// We record the current time in an internal config setting. This is used by
	// the license check code to know how long to defer throttling if we are
	// running without a license installed.
	//
	// We only want to set the timestamp once. If it was set in a prior version,
	// then we preserve the existing value.
	const lookupStmt = `
			SELECT 1 FROM system.settings WHERE NAME = 'server.license_grace_period_start.timestamp'
	`
	var settingExists bool
	callback := func(ctx context.Context, t isql.Txn) error {
		if row, err := t.QueryRow(ctx, "Check if server.license_grace_period_start.timestamp is already set", t.KV(), lookupStmt); err != nil {
			return err
		} else {
			settingExists = row != nil
		}
		return nil
	}
	if err := deps.DB.Txn(ctx, callback); err != nil {
		return err
	}
	if settingExists {
		log.Info(ctx, "The no-license grace period is already set.")
		return nil
	}
	_, err := deps.InternalExecutor.Exec(
		ctx, "set starting time for grace period", nil, /* txn */
		`SET CLUSTER SETTING server.license_grace_period_start.timestamp = now()::INT64`)
	return err
}
