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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/migrationstable"
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
func NewRecord(version roachpb.Version, user username.SQLUsername, name string) jobs.Record {
	return jobs.Record{
		Description: name,
		Details: jobspb.MigrationDetails{
			ClusterVersion: &clusterversion.ClusterVersion{Version: version},
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
	v := pl.GetMigration().ClusterVersion.Version
	db := execCtx.ExecCfg().InternalDB
	ex := db.Executor()
	enterpriseEnabled := base.CCLDistributionAndEnterpriseEnabled(
		execCtx.ExecCfg().Settings, execCtx.ExecCfg().NodeInfo.LogicalClusterID())
	alreadyCompleted, err := migrationstable.CheckIfMigrationCompleted(
		ctx, v, nil /* txn */, ex,
		enterpriseEnabled, migrationstable.ConsistentRead,
	)
	if alreadyCompleted || err != nil {
		return errors.Wrapf(err, "checking migration completion for %v", v)
	}
	if alreadyCompleted {
		return nil
	}
	mc := execCtx.MigrationJobDeps()
	m, ok := mc.GetUpgrade(v)
	if !ok {
		return errors.AssertionFailedf("found job for unknown upgrade at version: %s", v)
	}
	switch m := m.(type) {
	case *upgrade.SystemUpgrade:
		err = m.Run(ctx, v, mc.SystemDeps())
	case *upgrade.TenantUpgrade:
		tenantDeps := upgrade.TenantDeps{
			Codec:            execCtx.ExecCfg().Codec,
			Settings:         execCtx.ExecCfg().Settings,
			DB:               execCtx.ExecCfg().InternalDB,
			LeaseManager:     execCtx.ExecCfg().LeaseManager,
			InternalExecutor: ex,
			JobRegistry:      execCtx.ExecCfg().JobRegistry,
			TestingKnobs:     execCtx.ExecCfg().UpgradeTestingKnobs,
			SessionData:      execCtx.SessionData(),
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

		err = m.Run(ctx, v, tenantDeps)
	default:
		return errors.AssertionFailedf("unknown migration type %T", m)
	}
	if err != nil {
		return errors.Wrapf(err, "running migration for %v", v)
	}

	// Mark the upgrade as having been completed so that subsequent iterations
	// no-op and new jobs are not created.
	if err := migrationstable.MarkMigrationCompleted(ctx, ex, v); err != nil {
		return errors.Wrapf(err, "marking migration complete for %v", v)
	}
	return nil
}

// The long-running upgrade resumer has no reverting logic.
func (r resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, _ error) error {
	return nil
}
