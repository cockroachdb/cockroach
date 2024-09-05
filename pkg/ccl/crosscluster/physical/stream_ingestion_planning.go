// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package physical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// defaultRetentionTTLSeconds is the default value for how long
// replicated data will be retained.
const defaultRetentionTTLSeconds = int32(4 * 60 * 60)

// CannotSetExpirationWindowErr get returned if the user attempts to specify the
// EXPIRATION WINDOW option to create a replication stream, as this job setting
// should only be set from the producer cluster.
var CannotSetExpirationWindowErr = errors.New("cannot specify EXPIRATION WINDOW option while starting a physical replication stream")

func streamIngestionJobDescription(
	p sql.PlanHookState, sourceAddr string, streamIngestion *tree.CreateTenantFromReplication,
) (string, error) {
	redactedSourceAddr, err := streamclient.RedactSourceURI(sourceAddr)
	if err != nil {
		return "", err
	}

	redactedCreateStmt := &tree.CreateTenantFromReplication{
		TenantSpec:                  streamIngestion.TenantSpec,
		ReplicationSourceTenantName: streamIngestion.ReplicationSourceTenantName,
		ReplicationSourceAddress:    tree.NewDString(redactedSourceAddr),
		Options:                     streamIngestion.Options,
	}
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(redactedCreateStmt, ann), nil
}

func ingestionTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, _ colinfo.ResultColumns, _ error) {
	ingestionStmt, ok := stmt.(*tree.CreateTenantFromReplication)
	if !ok {
		return false, nil, nil
	}
	toTypeCheck := []exprutil.ToTypeCheck{
		exprutil.TenantSpec{TenantSpec: ingestionStmt.TenantSpec},
		exprutil.TenantSpec{TenantSpec: ingestionStmt.ReplicationSourceTenantName},
		exprutil.Strings{
			ingestionStmt.ReplicationSourceAddress,
			ingestionStmt.Options.Retention},
	}

	if err := exprutil.TypeCheck(ctx, "INGESTION", p.SemaCtx(), toTypeCheck...); err != nil {
		return false, nil, err
	}

	return true, nil, nil
}

func ingestionPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	ingestionStmt, ok := stmt.(*tree.CreateTenantFromReplication)
	if !ok {
		return nil, nil, nil, false, nil
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

	_, _, sourceTenant, err := exprEval.TenantSpec(ctx, ingestionStmt.ReplicationSourceTenantName)
	if err != nil {
		return nil, nil, nil, false, err
	}

	_, dstTenantID, dstTenantName, err := exprEval.TenantSpec(ctx, ingestionStmt.TenantSpec)
	if err != nil {
		return nil, nil, nil, false, err
	}

	evalCtx := &p.ExtendedEvalContext().Context
	options, err := evalTenantReplicationOptions(ctx, ingestionStmt.Options, exprEval, evalCtx, p.SemaCtx(), createReplicationOp)
	if err != nil {
		return nil, nil, nil, false, err
	}
	retentionTTLSeconds := defaultRetentionTTLSeconds
	if ret, ok := options.GetRetention(); ok {
		retentionTTLSeconds = ret
	}
	if _, ok := options.GetExpirationWindow(); ok {
		return nil, nil, nil, false, CannotSetExpirationWindowErr
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) (err error) {
		defer func() {
			if err == nil {
				telemetry.Count("physical_replication.started")
			}
		}()
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings,
			"CREATE VIRTUAL CLUSTER FROM REPLICATION",
		); err != nil {
			return err
		}

		if err := sql.CanManageTenant(ctx, p); err != nil {
			return err
		}

		streamAddress := crosscluster.StreamAddress(from)
		streamURL, err := streamAddress.URL()
		if err != nil {
			return err
		}
		streamAddress = crosscluster.StreamAddress(streamURL.String())

		if roachpb.IsSystemTenantName(roachpb.TenantName(dstTenantName)) ||
			roachpb.IsSystemTenantID(dstTenantID) {
			return errors.Newf("the destination tenant %q (%d) cannot be the system tenant",
				dstTenantName, dstTenantID)
		}

		// If we don't have a resume timestamp, make a new tenant
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		var destinationTenantID roachpb.TenantID

		var tenantInfo mtinfopb.TenantInfoWithUsage

		// Create a new tenant for the replication stream.
		tenantInfo.PhysicalReplicationConsumerJobID = jobID
		// dstTenantID may be zero which will cause auto-allocation.
		tenantInfo.ID = dstTenantID
		tenantInfo.DataState = mtinfopb.DataStateAdd
		tenantInfo.Name = roachpb.TenantName(dstTenantName)

		initialTenantZoneConfig, err := sql.GetHydratedZoneConfigForTenantsRange(ctx, p.Txn(), p.ExtendedEvalContext().Descs)
		if err != nil {
			return err
		}
		destinationTenantID, err = sql.CreateTenantRecord(
			ctx, p.ExecCfg().Codec, p.ExecCfg().Settings,
			p.InternalSQLTxn(),
			p.ExecCfg().SpanConfigKVAccessor.WithISQLTxn(ctx, p.InternalSQLTxn()),
			&tenantInfo, initialTenantZoneConfig,
			ingestionStmt.IfNotExists,
			p.ExecCfg().TenantTestingKnobs,
		)
		if err != nil {
			return err
		} else if !destinationTenantID.IsSet() {
			// No error but no valid tenant ID: there was an IF NOT EXISTS
			// clause and the tenant already existed. Nothing else to do.
			return nil
		}

		// TODO(dt): on by default? behind an option?
		const makeReader = false

		var readerID roachpb.TenantID
		if makeReader {
			var readerInfo mtinfopb.TenantInfoWithUsage
			readerInfo.DataState = mtinfopb.DataStateAdd
			readerInfo.Name = tenantInfo.Name + "-readonly"
			readerInfo.ReadFromTenant = &destinationTenantID

			readerZcfg, err := sql.GetHydratedZoneConfigForTenantsRange(ctx, p.Txn(), p.ExtendedEvalContext().Descs)
			if err != nil {
				return err
			}

			readerID, err = sql.CreateTenantRecord(
				ctx, p.ExecCfg().Codec, p.ExecCfg().Settings,
				p.InternalSQLTxn(),
				p.ExecCfg().SpanConfigKVAccessor.WithISQLTxn(ctx, p.InternalSQLTxn()),
				&readerInfo, readerZcfg,
				false, p.ExecCfg().TenantTestingKnobs,
			)
			if err != nil {
				return err
			}

			readerInfo.ID = readerID.ToUint64()
			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, err := sql.BootstrapTenant(ctx, p.ExecCfg(), txn, readerInfo, readerZcfg)
				return err
			}); err != nil {
				return err
			}
		}
		// No revert required since this is a new tenant.
		const noRevertFirst = false

		return createReplicationJob(
			ctx,
			p,
			streamAddress,
			sourceTenant,
			destinationTenantID,
			retentionTTLSeconds,
			options.resumeTimestamp,
			hlc.Timestamp{},
			noRevertFirst,
			jobID,
			ingestionStmt,
			readerID,
		)
	}

	return fn, nil, nil, false, nil
}

func createReplicationJob(
	ctx context.Context,
	p sql.PlanHookState,
	streamAddress crosscluster.StreamAddress,
	sourceTenant string,
	destinationTenantID roachpb.TenantID,
	retentionTTLSeconds int32,
	resumeTimestamp hlc.Timestamp,
	revertToTimestamp hlc.Timestamp,
	revertFirst bool,
	jobID jobspb.JobID,
	stmt *tree.CreateTenantFromReplication,
	readID roachpb.TenantID,
) error {

	// Create a new stream with stream client.
	client, err := streamclient.NewStreamClient(ctx, streamAddress, p.ExecCfg().InternalDB)
	if err != nil {
		return err
	}

	// Create the producer job first for the purpose of observability, user is
	// able to know the producer job id immediately after executing
	// CREATE VIRTUAL CLUSTER ... FROM REPLICATION.
	req := streampb.ReplicationProducerRequest{}
	if !resumeTimestamp.IsEmpty() {
		req = streampb.ReplicationProducerRequest{
			ReplicationStartTime: resumeTimestamp,

			// NB: These are checked against any
			// PreviousSourceTenant on the source's tenant
			// record.
			TenantID:  destinationTenantID,
			ClusterID: p.ExtendedEvalContext().ClusterID,
		}
	}

	replicationProducerSpec, err := client.CreateForTenant(ctx, roachpb.TenantName(sourceTenant), req)
	if err != nil {
		return err
	}
	if err := client.Close(ctx); err != nil {
		return err
	}

	streamIngestionDetails := jobspb.StreamIngestionDetails{
		StreamAddress:         string(streamAddress),
		StreamID:              uint64(replicationProducerSpec.StreamID),
		Span:                  keys.MakeTenantSpan(destinationTenantID),
		ReplicationTTLSeconds: retentionTTLSeconds,

		DestinationTenantID:  destinationTenantID,
		SourceTenantName:     roachpb.TenantName(sourceTenant),
		SourceTenantID:       replicationProducerSpec.SourceTenantID,
		SourceClusterID:      replicationProducerSpec.SourceClusterID,
		ReplicationStartTime: replicationProducerSpec.ReplicationStartTime,
		ReadTenantID:         readID,
	}

	jobDescription, err := streamIngestionJobDescription(p, string(streamAddress), stmt)
	if err != nil {
		return err
	}

	jr := jobs.Record{
		Description: jobDescription,
		Username:    p.User(),
		Progress: jobspb.StreamIngestionProgress{
			ReplicatedTime:        resumeTimestamp,
			InitialSplitComplete:  revertFirst,
			InitialRevertRequired: revertFirst,
			InitialRevertTo:       revertToTimestamp,
		},
		Details: streamIngestionDetails,
	}

	_, err = p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
		ctx, jr, jobID, p.InternalSQLTxn(),
	)
	return err
}

func init() {
	sql.AddPlanHook("ingestion", ingestionPlanHook, ingestionTypeCheck)
}
