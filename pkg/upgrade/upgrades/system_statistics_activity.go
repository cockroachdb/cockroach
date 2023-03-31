// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// systemStatementStatisticsActivityTableMigration creates the
// system.statement_activity and system.transaction_activity table.
func systemStatisticsActivityTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	tables := []catalog.TableDescriptor{
		systemschema.StatementActivityTable,
		systemschema.TransactionActivityTable,
	}

	for _, table := range tables {
		err := createSystemTable(ctx, d.DB.KV(), d.Settings, d.Codec,
			table)
		if err != nil {
			return err
		}
	}

	if d.TestingKnobs != nil && d.TestingKnobs.SkipUpdateSQLActivityJobBootstrap {
		return nil
	}

	record := jobs.Record{
		JobID:         jobs.AutoConfigSqlActivityID,
		Description:   "sql activity job",
		Username:      username.NodeUserName(),
		Details:       jobspb.AutoUpdateSQLActivityDetails{},
		Progress:      jobspb.AutoConfigRunnerProgress{},
		NonCancelable: true, // The job can't be canceled, but it can be paused.
	}

	// Make sure job with id doesn't already exist in system.jobs.
	row, err := d.DB.Executor().QueryRowEx(
		ctx,
		"check for existing update sql activity job",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"SELECT * FROM system.jobs WHERE id = $1",
		record.JobID,
	)
	if err != nil {
		return err
	}

	// If there isn't a row for the update SQL Activity job, create the job.
	if row == nil {
		if _, err = d.JobRegistry.CreateAdoptableJobWithTxn(ctx, record, record.JobID, nil); err != nil {
			return err
		}
	}
	return nil
}
