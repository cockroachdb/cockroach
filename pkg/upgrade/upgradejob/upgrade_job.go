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
	"fmt"

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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
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
	ie := execCtx.ExecCfg().InternalExecutor

	st := execCtx.ExecCfg().Settings
	org := sql.ClusterOrganization.Get(&st.SV)
	enterpriseEnabled := base.CCLDistributionAndEnterpriseEnabled(
		st, execCtx.ExecCfg().NodeInfo.LogicalClusterID(), org)
	alreadyCompleted, err := CheckIfMigrationCompleted(
		ctx, v, nil /* txn */, ie, enterpriseEnabled, ConsistentRead,
	)
	if alreadyCompleted || err != nil {
		return errors.Wrapf(err, "checking migration completion for %v", v)
	}
	mc := execCtx.MigrationJobDeps()
	m, ok := mc.GetUpgrade(v)
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
		err = m.Run(ctx, v, mc.SystemDeps())
	case *upgrade.TenantUpgrade:
		tenantDeps := upgrade.TenantDeps{
			DB:                      execCtx.ExecCfg().DB,
			Codec:                   execCtx.ExecCfg().Codec,
			Settings:                execCtx.ExecCfg().Settings,
			InternalExecutorFactory: execCtx.ExecCfg().InternalExecutorFactory,
			LeaseManager:            execCtx.ExecCfg().LeaseManager,
			InternalExecutor:        execCtx.ExecCfg().InternalExecutor,
			JobRegistry:             execCtx.ExecCfg().JobRegistry,
			TestingKnobs:            execCtx.ExecCfg().UpgradeTestingKnobs,
			SessionData:             execCtx.SessionData(),
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
	if err := upgradebase.MarkMigrationCompleted(ctx, ie, v); err != nil {
		return errors.Wrapf(err, "marking migration complete for %v", v)
	}
	return nil
}

type StaleReadOpt bool

const (
	StaleRead      StaleReadOpt = true
	ConsistentRead              = false
)

// CheckIfMigrationCompleted queries the system.upgrades table to determine
// if the upgrade associated with this version has already been completed.
// The txn may be nil, in which case the check will be run in its own
// transaction.
//
// staleOpt dictates whether the check will run a consistent read or a stale,
// follower read. If txn is not nil, only ConsistentRead can be specified.
func CheckIfMigrationCompleted(
	ctx context.Context,
	v roachpb.Version,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	enterpriseEnabled bool,
	staleOpt StaleReadOpt,
) (alreadyCompleted bool, _ error) {
	if txn != nil && staleOpt == StaleRead {
		return false, errors.AssertionFailedf(
			"CheckIfMigrationCompleted: cannot ask for stale read when running in a txn")
	}
	queryFormat := `
SELECT count(*)
	FROM system.migrations
  %s
 WHERE major = $1
	 AND minor = $2
	 AND patch = $3
	 AND internal = $4
`
	var query string
	if staleOpt == StaleRead && enterpriseEnabled {
		query = fmt.Sprintf(queryFormat, "AS OF SYSTEM TIME with_max_staleness('1h')")
	} else {
		query = fmt.Sprintf(queryFormat, "")
	}

	row, err := ie.QueryRow(
		ctx,
		"migration-job-find-already-completed",
		txn,
		query,
		v.Major,
		v.Minor,
		v.Patch,
		v.Internal)
	if err != nil {
		return false, err
	}
	count := *row[0].(*tree.DInt)
	return count != 0, nil
}

// The long-running upgrade resumer has no reverting logic.
func (r resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, _ error) error {
	return nil
}
