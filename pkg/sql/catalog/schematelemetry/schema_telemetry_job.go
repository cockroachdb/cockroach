// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schematelemetry

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

var (
	jobEnabled = settings.RegisterBoolSetting(
		settings.TenantWritable,
		"sql.schema_telemetry.job.enabled",
		"whether the schema telemetry job is enabled",
		true,
	).WithPublic()
)

type schemaTelemetryResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*schemaTelemetryResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (t schemaTelemetryResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)

	if enabled := jobEnabled.Get(p.ExecCfg().SV()); !enabled {
		return errors.Newf("schema telemetry jobs are currently disabled by CLUSTER SETTING %s", jobEnabled.Key())
	}

	telemetry.Inc(sqltelemetry.SchemaTelemetryExecuted)

	var knobs sql.SchemaTelemetryTestingKnobs
	if k := p.ExecCfg().SchemaTelemetryTestingKnobs; k != nil {
		knobs = *k
	}

	aostDuration := -time.Second * 30
	if knobs.AOSTDuration != nil {
		aostDuration = *knobs.AOSTDuration
	}
	asOf := p.ExecCfg().Clock.Now().Add(aostDuration.Nanoseconds(), 0)
	events, err := CollectClusterSchemaForTelemetry(ctx, p.ExecCfg(), asOf)
	if err != nil || len(events) == 0 {
		return err
	}

	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return sql.InsertEventRecord(
			ctx,
			p.ExecCfg().InternalExecutor,
			txn,
			int32(p.ExecCfg().NodeID.SQLInstanceID()), /* reportingID */
			sql.LogEverywhere,
			0, /* targetID */
			events...,
		)
	})
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t schemaTelemetryResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeAutoSchemaTelemetry,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &schemaTelemetryResumer{
				job: job,
				st:  settings,
			}
		},
		jobs.DisablesTenantCostControl,
	)
}
