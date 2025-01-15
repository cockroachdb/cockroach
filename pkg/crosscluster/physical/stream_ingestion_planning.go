// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	p sql.PlanHookState,
	source streamclient.ConfigUri,
	streamIngestion *tree.CreateTenantFromReplication,
) (string, error) {
	redactedCreateStmt := &tree.CreateTenantFromReplication{
		TenantSpec:                  streamIngestion.TenantSpec,
		ReplicationSourceTenantName: streamIngestion.ReplicationSourceTenantName,
		ReplicationSourceConnUri:    tree.NewDString(source.Redacted()),
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
			ingestionStmt.ReplicationSourceConnUri,
			ingestionStmt.Options.Retention},
	}

	if err := exprutil.TypeCheck(ctx, "INGESTION", p.SemaCtx(), toTypeCheck...); err != nil {
		return false, nil, err
	}

	return true, nil, nil
}

func ingestionPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	ingestionStmt, ok := stmt.(*tree.CreateTenantFromReplication)
	if !ok {
		return nil, nil, false, nil
	}

	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, nil, false, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can create other tenants")
	}

	exprEval := p.ExprEvaluator("INGESTION")

	from, err := exprEval.String(ctx, ingestionStmt.ReplicationSourceConnUri)
	if err != nil {
		return nil, nil, false, err
	}

	_, _, sourceTenant, err := exprEval.TenantSpec(ctx, ingestionStmt.ReplicationSourceTenantName)
	if err != nil {
		return nil, nil, false, err
	}

	_, dstTenantID, dstTenantName, err := exprEval.TenantSpec(ctx, ingestionStmt.TenantSpec)
	if err != nil {
		return nil, nil, false, err
	}

	evalCtx := &p.ExtendedEvalContext().Context
	options, err := evalTenantReplicationOptions(ctx, ingestionStmt.Options, exprEval, evalCtx, p.SemaCtx(), createReplicationOp)
	if err != nil {
		return nil, nil, false, err
	}
	retentionTTLSeconds := defaultRetentionTTLSeconds
	if ret, ok := options.GetRetention(); ok {
		retentionTTLSeconds = ret
	}
	if _, ok := options.GetExpirationWindow(); ok {
		return nil, nil, false, CannotSetExpirationWindowErr
	}

	fn := func(ctx context.Context, _ chan<- tree.Datums) (err error) {
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

		configUri, err := streamclient.ParseConfigUri(from)
		if err != nil {
			return err
		}

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

		readerID, err := createReaderTenant(ctx, p, tenantInfo.Name, destinationTenantID, options)
		if err != nil {
			return err
		}

		// No revert required since this is a new tenant.
		const noRevertFirst = false

		return createReplicationJob(
			ctx,
			p,
			configUri,
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

	return fn, nil, false, nil
}

func createReplicationJob(
	ctx context.Context,
	p sql.PlanHookState,
	configUri streamclient.ConfigUri,
	sourceTenant string,
	destinationTenantID roachpb.TenantID,
	retentionTTLSeconds int32,
	resumeTimestamp hlc.Timestamp,
	revertToTimestamp hlc.Timestamp,
	revertFirst bool,
	jobID jobspb.JobID,
	stmt *tree.CreateTenantFromReplication,
	readerID roachpb.TenantID,
) error {
	clusterUri, err := configUri.AsClusterUri(ctx, p.ExecCfg().InternalDB)
	if err != nil {
		return err
	}

	// Create a new stream with stream client.
	client, err := streamclient.NewStreamClient(ctx, clusterUri, p.ExecCfg().InternalDB)
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
		SourceClusterConnUri:  configUri.Serialize(),
		StreamID:              uint64(replicationProducerSpec.StreamID),
		Span:                  keys.MakeTenantSpan(destinationTenantID),
		ReplicationTTLSeconds: retentionTTLSeconds,

		DestinationTenantID:  destinationTenantID,
		SourceTenantName:     roachpb.TenantName(sourceTenant),
		SourceTenantID:       replicationProducerSpec.SourceTenantID,
		SourceClusterID:      replicationProducerSpec.SourceClusterID,
		ReplicationStartTime: replicationProducerSpec.ReplicationStartTime,
		ReadTenantID:         readerID,
	}

	jobDescription, err := streamIngestionJobDescription(p, configUri, stmt)
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

func createReaderTenant(
	ctx context.Context,
	p sql.PlanHookState,
	tenantName roachpb.TenantName,
	destinationTenantID roachpb.TenantID,
	options *resolvedTenantReplicationOptions,
) (roachpb.TenantID, error) {
	var readerID roachpb.TenantID
	if options.ReaderTenantEnabled() {
		var readerInfo mtinfopb.TenantInfoWithUsage
		readerInfo.DataState = mtinfopb.DataStateAdd
		readerInfo.Name = tenantName + "-readonly"
		readerInfo.ReadFromTenant = &destinationTenantID

		readerZcfg, err := sql.GetHydratedZoneConfigForTenantsRange(ctx, p.Txn(), p.ExtendedEvalContext().Descs)
		if err != nil {
			return readerID, err
		}

		readerID, err = sql.CreateTenantRecord(
			ctx, p.ExecCfg().Codec, p.ExecCfg().Settings,
			p.InternalSQLTxn(),
			p.ExecCfg().SpanConfigKVAccessor.WithISQLTxn(ctx, p.InternalSQLTxn()),
			&readerInfo, readerZcfg,
			false, p.ExecCfg().TenantTestingKnobs,
		)
		if err != nil {
			return readerID, err
		}

		readerInfo.ID = readerID.ToUint64()
		_, err = sql.BootstrapTenant(ctx, p.ExecCfg(), p.Txn(), readerInfo, readerZcfg)
		if err != nil {
			return readerID, err
		}
	}
	return readerID, nil
}

func init() {
	sql.AddPlanHook("ingestion", ingestionPlanHook, ingestionTypeCheck)
}
