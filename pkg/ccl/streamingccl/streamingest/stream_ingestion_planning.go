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
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// defaultRetentionTTLSeconds is the default value for how long
// replicated data will be retained.
const defaultRetentionTTLSeconds = 25 * 60 * 60

func streamIngestionJobDescription(
	p sql.PlanHookState, streamIngestion *tree.CreateTenantFromReplication,
) (string, error) {
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(streamIngestion, ann), nil
}

var resultColumns = colinfo.ResultColumns{
	{Name: "ingestion_job_id", Typ: types.Int},
	{Name: "producer_job_id", Typ: types.Int},
}

func ingestionTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	ingestionStmt, ok := stmt.(*tree.CreateTenantFromReplication)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(ctx, "INGESTION", p.SemaCtx(),
		exprutil.Strings{
			ingestionStmt.ReplicationSourceAddress,
			ingestionStmt.Options.Retention}); err != nil {
		return false, nil, err
	}

	return true, resultColumns, nil
}

func ingestionPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	ingestionStmt, ok := stmt.(*tree.CreateTenantFromReplication)
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

	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, nil, nil, false, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can create other tenants")
	}

	exprEval := p.ExprEvaluator("INGESTION")

	from, err := exprEval.String(ctx, ingestionStmt.ReplicationSourceAddress)
	if err != nil {
		return nil, nil, nil, false, err
	}

	retentionTTLSeconds := defaultRetentionTTLSeconds
	if ingestionStmt.Options.Retention != nil {
		retentionStr, err := exprEval.String(ctx, ingestionStmt.Options.Retention)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if retentionStr != "" {
			r, err := time.ParseDuration(retentionStr)
			if err != nil {
				return nil, nil, nil, false, err
			}
			retentionTTLSeconds = int(r.Seconds())
		}
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(),
			"CREATE TENANT FROM REPLICATION",
		); err != nil {
			return err
		}

		streamAddress := streamingccl.StreamAddress(from)
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
		sourceTenant := ingestionStmt.ReplicationSourceTenantName
		destinationTenant := ingestionStmt.Name

		// TODO(adityamaru): Add privileges checks. Probably the same as RESTORE.
		if roachpb.IsSystemTenantName(roachpb.TenantName(sourceTenant)) ||
			roachpb.IsSystemTenantName(roachpb.TenantName(destinationTenant)) {
			return errors.Newf("neither the source tenant %q nor the destination tenant %q can be the system tenant",
				sourceTenant, destinationTenant)
		}

		// Create a new tenant for the replication stream
		if _, err := sql.GetTenantRecordByName(ctx, p.ExecCfg(), p.Txn(), roachpb.TenantName(destinationTenant)); err == nil {
			return errors.Newf("tenant with name %q already exists", destinationTenant)
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		tenantInfo := &descpb.TenantInfoWithUsage{
			TenantInfo: descpb.TenantInfo{
				// We leave the ID field unset so that the tenant is assigned the next
				// available tenant ID.
				State:                  descpb.TenantInfo_ADD,
				Name:                   roachpb.TenantName(destinationTenant),
				TenantReplicationJobID: jobID,
			},
		}

		initialTenantZoneConfig, err := sql.GetHydratedZoneConfigForTenantsRange(ctx, p.Txn(), p.ExtendedEvalContext().Descs)
		if err != nil {
			return err
		}
		destinationTenantID, err := sql.CreateTenantRecord(ctx, p.ExecCfg(), p.Txn(), tenantInfo, initialTenantZoneConfig)
		if err != nil {
			return err
		}

		// Create a new stream with stream client.
		client, err := streamclient.NewStreamClient(ctx, streamAddress)
		if err != nil {
			return err
		}
		// Create the producer job first for the purpose of observability, user is
		// able to know the producer job id immediately after executing
		// CREATE TENANT ... FROM REPLICATION.
		replicationProducerSpec, err := client.Create(ctx, roachpb.TenantName(sourceTenant))
		if err != nil {
			return err
		}
		if err := client.Close(ctx); err != nil {
			return err
		}

		prefix := keys.MakeTenantPrefix(destinationTenantID)
		streamIngestionDetails := jobspb.StreamIngestionDetails{
			StreamAddress:         string(streamAddress),
			StreamID:              uint64(replicationProducerSpec.StreamID),
			Span:                  roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()},
			DestinationTenantID:   destinationTenantID,
			SourceTenantName:      roachpb.TenantName(sourceTenant),
			DestinationTenantName: roachpb.TenantName(destinationTenant),
			ReplicationTTLSeconds: int32(retentionTTLSeconds),
			ReplicationStartTime:  replicationProducerSpec.ReplicationStartTime,
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

		sj, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, jr,
			jobID, p.Txn())
		if err != nil {
			return err
		}

		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(sj.ID())),
			tree.NewDInt(tree.DInt(replicationProducerSpec.StreamID))}
		return nil
	}

	return fn, resultColumns, nil, false, nil
}

func init() {
	sql.AddPlanHook("ingestion", ingestionPlanHook, ingestionTypeCheck)
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
