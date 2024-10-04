// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
const defaultRetentionTTLSeconds = int32(25 * 60 * 60)

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
		Like:                        streamIngestion.Like,
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
	if ingestionStmt.Options.ResumeTimestamp != nil {
		if _, err := typeCheckSystemTimeExpr(ctx,
			&p.ExtendedEvalContext().Context,
			p.SemaCtx(),
			ingestionStmt.Options.ResumeTimestamp,
			createReplicationOp); err != nil {
			return false, nil, err
		}
	}
	if ingestionStmt.Like.OtherTenant != nil {
		toTypeCheck = append(toTypeCheck,
			exprutil.TenantSpec{TenantSpec: ingestionStmt.Like.OtherTenant},
		)
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

	if !streamingccl.CrossClusterReplicationEnabled.Get(&p.ExecCfg().Settings.SV) {
		return nil, nil, nil, false, physicalReplicationDisabledErr
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

	var likeTenantID uint64
	var likeTenantName string
	if ingestionStmt.Like.OtherTenant != nil {
		_, likeTenantID, likeTenantName, err = exprEval.TenantSpec(ctx, ingestionStmt.Like.OtherTenant)
		if err != nil {
			return nil, nil, nil, false, err
		}
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

	fn := func(ctx context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(),
			"CREATE VIRTUAL CLUSTER FROM REPLICATION",
		); err != nil {
			return err
		}

		if err := sql.CanManageTenant(ctx, p); err != nil {
			return err
		}

		streamAddress := streamingccl.StreamAddress(from)
		streamURL, err := streamAddress.URL()
		if err != nil {
			return err
		}
		streamAddress = streamingccl.StreamAddress(streamURL.String())

		if roachpb.IsSystemTenantName(roachpb.TenantName(sourceTenant)) ||
			roachpb.IsSystemTenantName(roachpb.TenantName(dstTenantName)) ||
			roachpb.IsSystemTenantID(dstTenantID) {
			return errors.Newf("neither the source tenant %q nor the destination tenant %q (%d) can be the system tenant",
				sourceTenant, dstTenantName, dstTenantID)
		}

		// If we don't have a resume timestamp, make a new tenant
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		var destinationTenantID roachpb.TenantID
		if options.resumeTimestamp.IsEmpty() {
			// Determine which template will be used as config template to
			// create the new tenant below.
			tenantInfo, err := sql.GetTenantTemplate(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), nil, likeTenantID, likeTenantName)
			if err != nil {
				return err
			}

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
				p.ExecCfg().SpanConfigKVAccessor.WithTxn(ctx, p.Txn()),
				tenantInfo, initialTenantZoneConfig,
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
		} else {
			tenantRecord, err := sql.GetTenantRecordByName(
				ctx, p.ExecCfg().Settings,
				p.InternalSQLTxn(),
				roachpb.TenantName(dstTenantName),
			)
			if err != nil {
				return err
			}

			// Here, we try to prevent the user from making a few
			// mistakes. Starting a replication stream into an
			// existing tenant requires both that it is offline and
			// that it is consistent as of the provided timestamp.
			if tenantRecord.ServiceMode != mtinfopb.ServiceModeNone {
				return errors.Newf("cannot start replication for tenant %q (%d) in service mode %s; service mode must be %s",
					tenantRecord.Name,
					tenantRecord.ID,
					tenantRecord.ServiceMode,
					mtinfopb.ServiceModeNone,
				)
			}
			if tenantRecord.LastRevertTenantTimestamp.IsEmpty() {
				return errors.Newf("cannot start replication for tenant %q (%d) with no last revert timestamp found; likely that this tenant cannot be safely streamed into",
					tenantRecord.Name,
					tenantRecord.ID,
				)
			}
			if !tenantRecord.LastRevertTenantTimestamp.Equal(options.resumeTimestamp) {
				return errors.Newf("cannot start replication for tenant %q (%d) with resume timestamp %s that doesn't match last revert timestamp %s",
					tenantRecord.Name,
					tenantRecord.ID,
					options.resumeTimestamp,
					tenantRecord.LastRevertTenantTimestamp,
				)
			}

			// Reset the last revert timestamp.
			tenantRecord.LastRevertTenantTimestamp = hlc.Timestamp{}
			tenantRecord.PhysicalReplicationConsumerJobID = jobID
			tenantRecord.DataState = mtinfopb.DataStateAdd
			if err := sql.UpdateTenantRecord(ctx, p.ExecCfg().Settings,
				p.InternalSQLTxn(), tenantRecord); err != nil {
				return err
			}
			destinationTenantID, err = roachpb.MakeTenantID(tenantRecord.ID)
			if err != nil {
				return err
			}
		}

		// Create a new stream with stream client.
		client, err := streamclient.NewStreamClient(ctx, streamAddress, p.ExecCfg().InternalDB)
		if err != nil {
			return err
		}

		// Create the producer job first for the purpose of observability, user is
		// able to know the producer job id immediately after executing
		// CREATE VIRTUAL CLUSTER ... FROM REPLICATION.
		req := streampb.ReplicationProducerRequest{}
		if !options.resumeTimestamp.IsEmpty() {
			req = streampb.ReplicationProducerRequest{
				ReplicationStartTime: options.resumeTimestamp,

				// NB: These are checked against any
				// PreviousSourceTenant on the source's tenant
				// record.
				TenantID:  destinationTenantID,
				ClusterID: p.ExtendedEvalContext().ClusterID,
			}
		}

		replicationProducerSpec, err := client.Create(ctx, roachpb.TenantName(sourceTenant), req)
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

			DestinationTenantID:   destinationTenantID,
			DestinationTenantName: roachpb.TenantName(dstTenantName),

			SourceTenantName:     roachpb.TenantName(sourceTenant),
			SourceTenantID:       replicationProducerSpec.SourceTenantID,
			SourceClusterID:      replicationProducerSpec.SourceClusterID,
			ReplicationStartTime: replicationProducerSpec.ReplicationStartTime,
		}

		jobDescription, err := streamIngestionJobDescription(p, from, ingestionStmt)
		if err != nil {
			return err
		}

		jr := jobs.Record{
			Description: jobDescription,
			Username:    p.User(),
			Progress: jobspb.StreamIngestionProgress{
				ReplicatedTime: options.resumeTimestamp,
			},
			Details: streamIngestionDetails,
		}

		_, err = p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
			ctx, jr, jobID, p.InternalSQLTxn(),
		)
		return err
	}

	return fn, nil, nil, false, nil
}

func init() {
	sql.AddPlanHook("ingestion", ingestionPlanHook, ingestionTypeCheck)
}
