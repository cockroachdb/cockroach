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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type schemaTelemetryResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*schemaTelemetryResumer)(nil)

// Resume is part of the jobs.Resumer interface.
func (t schemaTelemetryResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)

	telemetry.Inc(sqltelemetry.SchemaTelemetryExecuted)

	var knobs sql.SchemaTelemetryTestingKnobs
	if k := p.ExecCfg().SchemaTelemetryTestingKnobs; k != nil {
		knobs = *k
	}

	// Outside of tests, scan the catalog tables AS OF SYSTEM TIME slightly in the
	// past. Schema telemetry is not latency-sensitive to the point where a few
	// seconds matter.
	aostDuration := builtinconstants.DefaultFollowerReadDuration
	if knobs.AOSTDuration != nil {
		aostDuration = *knobs.AOSTDuration
	} else if fn := builtins.EvalFollowerReadOffset; fn != nil {
		if d, err := fn(p.ExtendedEvalContext().ClusterID, p.ExecCfg().Settings); err == nil {
			aostDuration = d
		}
	}
	asOf := p.ExecCfg().Clock.Now().Add(aostDuration.Nanoseconds(), 0)
	const maxRecords = 10000
	events, err := CollectClusterSchemaForTelemetry(ctx, p.ExecCfg(), asOf, uuid.FastMakeV4(), maxRecords)
	if err != nil || len(events) == 0 {
		return err
	}

	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return sql.InsertEventRecords(
			ctx,
			p.ExecCfg().InternalExecutor,
			p.ExecCfg().EventsExporter,
			txn,
			int32(p.ExecCfg().NodeInfo.NodeID.SQLInstanceID()), /* reportingID */
			sql.LogExternally,
			0, /* targetID */
			events...,
		)
	})
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (t schemaTelemetryResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
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
