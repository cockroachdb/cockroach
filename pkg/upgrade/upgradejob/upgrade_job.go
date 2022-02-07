// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgradejob contains the jobs.Resumer implementation
// used for long-running upgrades.
package upgradejob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func init() {
	// Do not include the cost of long-running migrations in tenant accounting.
	// NB: While the exemption excludes the cost of Storage I/O, it is not able
	// to exclude the CPU cost.
	jobs.RegisterConstructor(jobspb.TypeMigration, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &resumer{j: job}
	}, jobs.DisablesTenantCostControl)
}

// NewRecord constructs a new jobs.Record for this upgrade.
func NewRecord(
	version clusterversion.ClusterVersion, user username.SQLUsername, name string,
) jobs.Record {
	return jobs.Record{
		Description: name,
		Details: jobspb.MigrationDetails{
			ClusterVersion: &version,
		},
		Username:      user,
		Progress:      jobspb.MigrationProgress{},
		NonCancelable: true,
	}
}

type resumer struct {
	j *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

func (r resumer) Resume(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	pl := r.j.Payload()
	cv := *pl.GetMigration().ClusterVersion
	ie := execCtx.ExecCfg().InternalExecutor

	alreadyCompleted, err := CheckIfMigrationCompleted(ctx, nil /* txn */, ie, cv)
	if alreadyCompleted || err != nil {
		return errors.Wrapf(err, "checking migration completion for %v", cv)
	}
	mc := execCtx.MigrationJobDeps()
	m, ok := mc.GetUpgrade(cv)
	if !ok {
		// TODO(ajwerner): Consider treating this as an assertion failure. Jobs
		// should only be created for a cluster version if there is an associated
		// upgrade. It seems possible that an upgrade job could be launched by
		// a node running a older version where an upgrade then runs on a job
		// with a newer version where the upgrade has been re-ordered to be later.
		// This should only happen between alphas but is theoretically not illegal.
		return nil
	}
	switch m := m.(type) {
	case *upgrade.SystemUpgrade:
		err = m.Run(ctx, cv, mc.SystemDeps(), r.j)
	case *upgrade.TenantUpgrade:
		tenantDeps := upgrade.TenantDeps{
			DB:                execCtx.ExecCfg().DB,
			Codec:             execCtx.ExecCfg().Codec,
			Settings:          execCtx.ExecCfg().Settings,
			CollectionFactory: execCtx.ExecCfg().CollectionFactory,
			LeaseManager:      execCtx.ExecCfg().LeaseManager,
			InternalExecutor:  execCtx.ExecCfg().InternalExecutor,
			JobRegistry:       execCtx.ExecCfg().JobRegistry,
			TestingKnobs:      execCtx.ExecCfg().UpgradeTestingKnobs,
		}
		tenantDeps.SpanConfig.KVAccessor = execCtx.ExecCfg().SpanConfigKVAccessor
		tenantDeps.SpanConfig.Splitter = execCtx.ExecCfg().SpanConfigSplitter
		tenantDeps.SpanConfig.Default = execCtx.ExecCfg().DefaultZoneConfig.AsSpanConfig()

		tenantDeps.SchemaResolverConstructor = func(
			txn *kv.Txn, descriptors *descs.Collection, currDb string,
		) (resolver.SchemaResolver, func(), error) {
			internalPlanner, cleanup := sql.NewInternalPlanner("internal planner for upgrades",
				txn,
				execCtx.User(),
				&sql.MemoryMetrics{},
				execCtx.ExecCfg(),
				sessiondatapb.SessionData{Database: currDb},
				sql.WithDescCollection(descriptors),
			)
			sr, ok := internalPlanner.(resolver.SchemaResolver)
			if !ok {
				cleanup()
				return nil, nil, errors.New("expected SchemaResolver")
			}
			return sr, cleanup, nil
		}

		err = m.Run(ctx, cv, tenantDeps, r.j)
	default:
		return errors.AssertionFailedf("unknown migration type %T", m)
	}
	if err != nil {
		return errors.Wrapf(err, "running migration for %v", cv)
	}

	// Mark the upgrade as having been completed so that subsequent iterations
	// no-op and new jobs are not created.
	if err := markMigrationCompleted(ctx, ie, cv); err != nil {
		return errors.Wrapf(err, "marking migration complete for %v", cv)
	}
	return nil
}

// CheckIfMigrationCompleted queries the system.upgrades table to determine
// if the upgrade associated with this version has already been completed.
// The txn may be nil, in which case the check will be run in its own
// transaction.
func CheckIfMigrationCompleted(
	ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor, cv clusterversion.ClusterVersion,
) (alreadyCompleted bool, _ error) {
	row, err := ie.QueryRow(
		ctx,
		"migration-job-find-already-completed",
		txn,
		`
SELECT EXISTS(
        SELECT *
          FROM system.migrations
         WHERE major = $1
           AND minor = $2
           AND patch = $3
           AND internal = $4
       );
`,
		cv.Major,
		cv.Minor,
		cv.Patch,
		cv.Internal)
	if err != nil {
		return false, err
	}
	return bool(*row[0].(*tree.DBool)), nil
}

func markMigrationCompleted(
	ctx context.Context, ie sqlutil.InternalExecutor, cv clusterversion.ClusterVersion,
) error {
	_, err := ie.ExecEx(
		ctx,
		"migration-job-mark-job-succeeded",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
INSERT
  INTO system.migrations
        (
            major,
            minor,
            patch,
            internal,
            completed_at
        )
VALUES ($1, $2, $3, $4, $5)`,
		cv.Major,
		cv.Minor,
		cv.Patch,
		cv.Internal,
		timeutil.Now())
	return err
}

// The long-running upgrade resumer has no reverting logic.
func (r resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, _ error) error {
	return nil
}
