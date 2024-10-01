// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schematelemetry

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type Metrics struct {
	InvalidObjects *metric.Gauge
}

func newMetrics() Metrics {
	return Metrics{
		InvalidObjects: metric.NewGauge(metric.Metadata{
			Name:        "sql.schema.invalid_objects",
			Help:        "Gauge of detected invalid objects within the system.descriptor table (measured by querying crdb_internal.invalid_objects)",
			Measurement: "Objects",
			Unit:        metric.Unit_COUNT,
		}),
	}
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

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
		if d, err := fn(p.ExecCfg().Settings); err == nil {
			aostDuration = d
		}
	}

	const maxRecords = 10000
	asOf := p.ExecCfg().Clock.Now().Add(aostDuration.Nanoseconds(), 0)
	metrics := p.ExecCfg().JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeAutoSchemaTelemetry].(Metrics)

	if err := processInvalidObjects(ctx, p.ExecCfg(), asOf, &metrics, maxRecords); err != nil {
		return err
	}

	events, err := CollectClusterSchemaForTelemetry(ctx, p.ExecCfg(), asOf, uuid.MakeV4(), maxRecords)
	if err != nil || len(events) == 0 {
		return err
	}

	sql.InsertEventRecords(
		ctx,
		p.ExecCfg(),
		sql.LogExternally,
		events...,
	)

	return nil
}

func processInvalidObjects(
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	asOf hlc.Timestamp,
	metrics *Metrics,
	maxRecords int,
) error {
	return sql.DescsTxn(ctx, cfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (retErr error) {
		err := txn.KV().SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}

		rows, err := txn.QueryIteratorEx(
			ctx, "sql-telemetry-invalid-objects", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			`SELECT id, error_redactable FROM "".crdb_internal.invalid_objects LIMIT $1`,
			maxRecords,
		)
		if err != nil {
			return err
		}

		defer func(it isql.Rows) {
			retErr = errors.CombineErrors(retErr, it.Close())
		}(rows)

		count := int64(0)
		for {
			ok, err := rows.Next(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}

			count++
			row := rows.Cur()

			descID, ok := row[0].(*tree.DInt)
			if !ok {
				return errors.AssertionFailedf("expected id to be int (was %T)", row[0])
			}

			validationErr, ok := row[1].(*tree.DString)
			if !ok {
				return errors.AssertionFailedf("expected err to be string (was %T)", row[1])
			}

			// IDs are always non-sensitive, and the validationErr is written to the
			// table with redact.Sprint, so it's a RedactableString.
			log.Warningf(ctx, "found invalid object with ID %d: %s",
				redact.SafeInt(*descID), redact.RedactableString(*validationErr),
			)
		}

		metrics.InvalidObjects.Update(count)
		if count == 0 {
			log.Infof(ctx, "schema telemetry job found no invalid objects")
		}

		return nil
	})
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (t schemaTelemetryResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	return nil
}

// CollectProfile is part of the jobs.Resumer interface.
func (t schemaTelemetryResumer) CollectProfile(_ context.Context, _ interface{}) error {
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
		jobs.WithJobMetrics(newMetrics()),
	)
}
