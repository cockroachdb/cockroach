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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
				pgcode.ExperimentalFeature,
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
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), p.ExecCfg().Organization(),
			"RESTORE FROM REPLICATION STREAM",
		); err != nil {
			return err
		}

		from, err := fromFn()
		if err != nil {
			return err
		}

		// We only support a TENANT target, so error out if that is nil.
		if !ingestionStmt.Targets.TenantID.IsSet() {
			return errors.Newf("no tenant specified in ingestion query: %s", ingestionStmt.String())
		}

		streamAddress := streamingccl.StreamAddress(from[0])
		streamURL, err := streamAddress.URL()
		if err != nil {
			return err
		}
		q := streamURL.Query()

		// Operator should specify a postgres scheme address with cert authentication.
		if hasPostgresAuthentication := (q.Get("sslmode") == "verify-full") &&
			q.Has("sslrootcert") && q.Has("sslkey") && q.Has("sslcert"); (streamURL.Scheme == "postgres") &&
			!hasPostgresAuthentication {
			return errors.Errorf(
				"stream replication address should have cert authentication if in postgres scheme: %s", streamAddress)
		}

		streamAddress = streamingccl.StreamAddress(streamURL.String())
		if ingestionStmt.Targets.Databases != nil ||
			ingestionStmt.Targets.Tables.TablePatterns != nil || ingestionStmt.Targets.Schemas != nil {
			return errors.Newf("unsupported target in ingestion query, "+
				"only tenant ingestion is supported: %s", ingestionStmt.String())
		}

		// TODO(adityamaru): Add privileges checks. Probably the same as RESTORE.
		// TODO(casper): make target to be tenant-only.
		oldTenantID := roachpb.MakeTenantID(ingestionStmt.Targets.TenantID.ID)
		newTenantID := oldTenantID
		if ingestionStmt.AsTenant.Specified {
			newTenantID = roachpb.MakeTenantID(ingestionStmt.AsTenant.ID)
		}
		if oldTenantID == roachpb.SystemTenantID || newTenantID == roachpb.SystemTenantID {
			return errors.Newf("either old tenant ID %d or the new tenant ID %d cannot be system tenant",
				oldTenantID.ToUint64(), newTenantID.ToUint64())
		}

		// Create a new tenant for the replication stream
		if _, err := sql.GetTenantRecord(ctx, p.ExecCfg(), nil, newTenantID.ToUint64()); err == nil {
			return errors.Newf("tenant with id %s already exists", newTenantID)
		}
		tenantInfo := &descpb.TenantInfoWithUsage{
			TenantInfo: descpb.TenantInfo{
				ID:    newTenantID.ToUint64(),
				State: descpb.TenantInfo_ADD,
			},
		}
		if err := sql.CreateTenantRecord(ctx, p.ExecCfg(), nil, tenantInfo); err != nil {
			return err
		}

		// Create a new stream with stream client.
		client, err := streamclient.NewStreamClient(ctx, streamAddress)
		if err != nil {
			return err
		}
		// Create the producer job first for the purpose of observability,
		// user is able to know the producer job id immediately after executing the RESTORE.
		streamID, err := client.Create(ctx, oldTenantID)
		if err != nil {
			return err
		}
		if err := client.Close(ctx); err != nil {
			return err
		}

		prefix := keys.MakeTenantPrefix(newTenantID)
		streamIngestionDetails := jobspb.StreamIngestionDetails{
			StreamAddress: string(streamAddress),
			StreamID:      uint64(streamID),
			TenantID:      oldTenantID,
			Span:          roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()},
			NewTenantID:   newTenantID,
		}

		jobDescription, err := streamIngestionJobDescription(p, ingestionStmt)
		if err != nil {
			return err
		}

		jr := jobs.Record{
			Description: jobDescription,
			Username:    p.User(),
			Progress:    jobspb.StreamIngestionProgress{},
			Details:     streamIngestionDetails,
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		sj, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, jr,
			jobID, p.Txn())
		if err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(sj.ID())), tree.NewDInt(tree.DInt(streamID))}
		return nil
	}

	return fn, colinfo.ResultColumns{
		{Name: "ingestion_job_id", Typ: types.Int},
		{Name: "producer_job_id", Typ: types.Int},
	}, nil, false, nil
}

func init() {
	sql.AddPlanHook("ingestion", ingestionPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeStreamIngestion,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &streamIngestionResumer{
				job: job,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
