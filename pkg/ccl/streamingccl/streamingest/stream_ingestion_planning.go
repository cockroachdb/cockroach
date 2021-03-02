// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func streamIngestionJobDescription(
	p sql.PlanHookState, streamIngestion *tree.StreamIngestion,
) (string, error) {
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(streamIngestion, ann), nil
}

func ingestionPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	ingestionStmt, ok := stmt.(*tree.StreamIngestion)
	if !ok {
		return nil, nil, nil, false, nil
	}

	// Check if the experimental feature is enabled.
	if !p.SessionData().EnableStreamReplication {
		return nil, nil, nil, false, errors.WithTelemetry(
			pgerror.WithCandidateCode(
				errors.WithHint(
					errors.Newf("stream replication is only supported experimentally"),
					"You can enable stream replication by running `SET enable_experimental_stream_replication = true`.",
				),
				pgcode.FeatureNotSupported,
			),
			"replication.ingest.disabled",
		)
	}

	fromFn, err := p.TypeAsStringArray(ctx, tree.Exprs(ingestionStmt.From), "INGESTION")
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
			"RESTORE FROM REPLICATION STREAM",
		); err != nil {
			return err
		}

		from, err := fromFn()
		if err != nil {
			return err
		}

		// We only support a TENANT target, so error out if that is nil.
		if ingestionStmt.Targets.Tenant == (roachpb.TenantID{}) {
			return errors.Newf("no tenant specified in ingestion query: %s", ingestionStmt.String())
		}

		streamAddress := streamingccl.StreamAddress(from[0])
		url, err := streamAddress.URL()
		if err != nil {
			return err
		}
		q := url.Query()
		q.Set("TENANT_ID", ingestionStmt.Targets.Tenant.String())
		url.RawQuery = q.Encode()
		streamAddress = streamingccl.StreamAddress(url.String())

		if ingestionStmt.Targets.Types != nil || ingestionStmt.Targets.Databases != nil ||
			ingestionStmt.Targets.Tables != nil || ingestionStmt.Targets.Schemas != nil {
			return errors.Newf("unsupported target in ingestion query, "+
				"only tenant ingestion is supported: %s", ingestionStmt.String())
		}

		// TODO(adityamaru): Add privileges checks. Probably the same as RESTORE.

		prefix := keys.MakeTenantPrefix(ingestionStmt.Targets.Tenant)
		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if ingestionStmt.AsOf.Expr != nil {
			var err error
			startTime, err = p.EvalAsOfTimestamp(ctx, ingestionStmt.AsOf)
			if err != nil {
				return err
			}
		}

		streamIngestionDetails := jobspb.StreamIngestionDetails{
			StreamAddress: string(streamAddress),
			Span:          roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()},
			StartTime:     startTime,
		}

		jobDescription, err := streamIngestionJobDescription(p, ingestionStmt)
		if err != nil {
			return err
		}

		jr := jobs.Record{
			Description:   jobDescription,
			Username:      p.User(),
			Progress:      jobspb.StreamIngestionProgress{},
			Details:       streamIngestionDetails,
			NonCancelable: true,
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		sj, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, jr,
			jobID, p.ExtendedEvalContext().Txn)
		if err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(sj.ID()))}
		return nil
	}

	return fn, utilccl.DetachedJobExecutionResultHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook(ingestionPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeStreamIngestion,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &streamIngestionResumer{
				job: job,
			}
		},
	)
}
