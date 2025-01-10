// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	alterReplicationJobOp = "ALTER VIRTUAL CLUSTER REPLICATION"
	createReplicationOp   = "CREATE VIRTUAL CLUSTER FROM REPLICATION"
)

var alterReplicationCutoverHeader = colinfo.ResultColumns{
	{Name: "failover_time", Typ: types.Decimal},
}

// ResolvedTenantReplicationOptions represents options from an
// evaluated CREATE/ALTER VIRTUAL CLUSTER FROM REPLICATION command.
type resolvedTenantReplicationOptions struct {
	resumeTimestamp    hlc.Timestamp
	retention          *int32
	expirationWindow   *time.Duration
	enableReaderTenant bool
}

func evalTenantReplicationOptions(
	ctx context.Context,
	options tree.TenantReplicationOptions,
	eval exprutil.Evaluator,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	op string,
) (*resolvedTenantReplicationOptions, error) {
	r := &resolvedTenantReplicationOptions{}
	if options.Retention != nil {
		dur, err := eval.Duration(ctx, options.Retention)
		if err != nil {
			return nil, err
		}
		retSeconds64, ok := dur.AsInt64()
		if !ok {
			return nil, errors.Newf("interval conversion error: %v", dur)
		}
		if retSeconds64 > math.MaxInt32 || retSeconds64 < 0 {
			return nil, errors.Newf("retention should result in a number of seconds between 0 and %d",
				math.MaxInt32)
		}
		retSeconds := int32(retSeconds64)
		r.retention = &retSeconds
	}
	if options.ExpirationWindow != nil {
		dur, err := eval.Duration(ctx, options.ExpirationWindow)
		if err != nil {
			return nil, err
		}
		expirationWindow := time.Duration(dur.Nanos())
		r.expirationWindow = &expirationWindow
	}
	if options.EnableReaderTenant != nil {
		enabled, err := eval.Bool(ctx, options.EnableReaderTenant)
		if err != nil {
			return nil, err
		}
		r.enableReaderTenant = enabled
	}
	return r, nil
}

func (r *resolvedTenantReplicationOptions) GetRetention() (int32, bool) {
	if r == nil || r.retention == nil {
		return 0, false
	}
	return *r.retention, true
}

func (r *resolvedTenantReplicationOptions) GetExpirationWindow() (time.Duration, bool) {
	if r == nil || r.expirationWindow == nil {
		return 0, false
	}
	return *r.expirationWindow, true
}

func (r *resolvedTenantReplicationOptions) DestinationOptionsSet() bool {
	return r != nil && (r.retention != nil || r.resumeTimestamp.IsSet())
}

func (r *resolvedTenantReplicationOptions) ReaderTenantEnabled() bool {
	if r == nil || !r.enableReaderTenant {
		return false
	}
	return true
}

func alterReplicationJobTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	alterStmt, ok := stmt.(*tree.AlterTenantReplication)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(
		ctx, alterReplicationJobOp, p.SemaCtx(),
		exprutil.TenantSpec{TenantSpec: alterStmt.TenantSpec},
		exprutil.TenantSpec{TenantSpec: alterStmt.ReplicationSourceTenantName},
		exprutil.Strings{alterStmt.Options.Retention, alterStmt.ReplicationSourceConnUri},
	); err != nil {
		return false, nil, err
	}

	if cutoverTime := alterStmt.Cutover; cutoverTime != nil {
		if cutoverTime.Timestamp != nil {
			if _, err := asof.TypeCheckSystemTimeExpr(ctx, p.SemaCtx(),
				cutoverTime.Timestamp, alterReplicationJobOp); err != nil {
				return false, nil, err
			}
		}
		return true, alterReplicationCutoverHeader, nil
	}

	return true, nil, nil
}

func alterReplicationJobHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	alterTenantStmt, ok := stmt.(*tree.AlterTenantReplication)
	if !ok {
		return nil, nil, false, nil
	}

	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, nil, false, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can alter tenant")
	}

	evalCtx := &p.ExtendedEvalContext().Context
	var cutoverTime hlc.Timestamp
	if alterTenantStmt.Cutover != nil {
		if !alterTenantStmt.Cutover.Latest {
			if alterTenantStmt.Cutover.Timestamp == nil {
				return nil, nil, false, errors.AssertionFailedf("unexpected nil cutover expression")
			}

			ct, err := asof.EvalSystemTimeExpr(ctx, evalCtx, p.SemaCtx(), alterTenantStmt.Cutover.Timestamp,
				alterReplicationJobOp, asof.ReplicationCutover)
			if err != nil {
				return nil, nil, false, err
			}
			cutoverTime = ct
		}
	}

	exprEval := p.ExprEvaluator(alterReplicationJobOp)
	options, err := evalTenantReplicationOptions(ctx, alterTenantStmt.Options, exprEval, evalCtx, p.SemaCtx(), alterReplicationJobOp)
	if err != nil {
		return nil, nil, false, err
	}

	var srcUri, srcTenant string
	if alterTenantStmt.ReplicationSourceConnUri != nil {
		srcUri, err = exprEval.String(ctx, alterTenantStmt.ReplicationSourceConnUri)
		if err != nil {
			return nil, nil, false, err
		}

		_, _, srcTenant, err = exprEval.TenantSpec(ctx, alterTenantStmt.ReplicationSourceTenantName)
		if err != nil {
			return nil, nil, false, err
		}
	}

	// Ensure the TenantSpec is type checked, even if we don't use the result.
	_, _, _, err = exprEval.TenantSpec(ctx, alterTenantStmt.TenantSpec)
	if err != nil {
		return nil, nil, false, err
	}

	retentionTTLSeconds := defaultRetentionTTLSeconds
	if ret, ok := options.GetRetention(); ok {
		retentionTTLSeconds = ret
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings,
			alterReplicationJobOp,
		); err != nil {
			return err
		}

		if err := sql.CanManageTenant(ctx, p); err != nil {
			return err
		}

		tenInfo, err := p.LookupTenantInfo(ctx, alterTenantStmt.TenantSpec, alterReplicationJobOp)
		if err != nil {
			return err
		}

		// If a source uri is being provided, we're enabling replication into an
		// existing virtual cluster. It must be inactive, and we'll verify that it
		// was the cluster from which the one it will replicate was replicated, i.e.
		// that we're reversing the direction of replication. We will then revert it
		// to the time they diverged and pick up from there.
		if alterTenantStmt.ReplicationSourceConnUri != nil {
			return alterTenantRestartReplication(
				ctx,
				p,
				tenInfo,
				srcUri,
				srcTenant,
				retentionTTLSeconds,
				alterTenantStmt,
				options,
			)
		}
		jobRegistry := p.ExecCfg().JobRegistry
		if !alterTenantStmt.Options.IsDefault() {
			// If the statement contains options, then the user provided the ALTER
			// TENANT ... SET REPLICATION [options] form of the command.
			return alterTenantSetReplication(ctx, p.InternalSQLTxn(), jobRegistry, options, tenInfo)
		}
		if err := checkForActiveIngestionJob(tenInfo); err != nil {
			return err
		}
		if alterTenantStmt.Cutover != nil {
			pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(p.InternalSQLTxn())
			actualCutoverTime, err := alterTenantJobCutover(
				ctx, p.InternalSQLTxn(), jobRegistry, pts, alterTenantStmt, tenInfo, cutoverTime)
			if err != nil {
				return err
			}
			resultsCh <- tree.Datums{eval.TimestampToDecimalDatum(actualCutoverTime)}
		} else {
			switch alterTenantStmt.Command {
			case tree.ResumeJob:
				if err := jobRegistry.Unpause(ctx, p.InternalSQLTxn(), tenInfo.PhysicalReplicationConsumerJobID); err != nil {
					return err
				}
			case tree.PauseJob:
				if err := jobRegistry.PauseRequested(ctx, p.InternalSQLTxn(), tenInfo.PhysicalReplicationConsumerJobID,
					"ALTER VIRTUAL CLUSTER PAUSE REPLICATION"); err != nil {
					return err
				}
			default:
				return errors.New("unsupported job command in ALTER VIRTUAL CLUSTER REPLICATION")
			}
		}
		return nil
	}
	if alterTenantStmt.Cutover != nil {
		return fn, alterReplicationCutoverHeader, false, nil
	}
	return fn, nil, false, nil
}

func alterTenantSetReplication(
	ctx context.Context,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	options *resolvedTenantReplicationOptions,
	tenInfo *mtinfopb.TenantInfo,
) error {

	if expirationWindow, ok := options.GetExpirationWindow(); ok {
		if err := alterTenantExpirationWindow(ctx, txn, jobRegistry, expirationWindow, tenInfo); err != nil {
			return err
		}
	}
	if options.DestinationOptionsSet() {
		if err := checkForActiveIngestionJob(tenInfo); err != nil {
			return err
		}
		if err := alterTenantConsumerOptions(ctx, txn, jobRegistry, options, tenInfo); err != nil {
			return err
		}
	}
	return nil
}

func checkForActiveIngestionJob(tenInfo *mtinfopb.TenantInfo) error {
	if tenInfo.PhysicalReplicationConsumerJobID == 0 {
		return errors.Newf("tenant %q (%d) does not have an active replication consumer job",
			tenInfo.Name, tenInfo.ID)
	}
	return nil
}

func alterTenantRestartReplication(
	ctx context.Context,
	p sql.PlanHookState,
	tenInfo *mtinfopb.TenantInfo,
	srcUri string,
	srcTenant string,
	retentionTTLSeconds int32,
	alterTenantStmt *tree.AlterTenantReplication,
	options *resolvedTenantReplicationOptions,
) error {
	dstTenantID, err := roachpb.MakeTenantID(tenInfo.ID)
	if err != nil {
		return err
	}

	// Here, we try to prevent the user from making a few
	// mistakes. Starting a replication stream into an
	// existing tenant requires both that it is offline and
	// that it is consistent as of the provided timestamp.
	if tenInfo.ServiceMode != mtinfopb.ServiceModeNone {
		return errors.Newf("cannot start replication for tenant %q (%s) in service mode %s; service mode must be %s",
			tenInfo.Name,
			dstTenantID,
			tenInfo.ServiceMode,
			mtinfopb.ServiceModeNone,
		)
	}

	if tenInfo.DataState != mtinfopb.DataStateReady {
		return errors.Newf("cannot start replication for tenant %q (%s) in state %s (is replication or a restore already running?)",
			tenInfo.Name,
			dstTenantID,
			tenInfo.DataState,
		)
	}

	if alterTenantStmt.Options.ExpirationWindowSet() {
		return CannotSetExpirationWindowErr
	}

	configUri, err := streamclient.ParseConfigUri(srcUri)
	if err != nil {
		return errors.Wrap(err, "url")
	}

	clusterUri, err := configUri.AsClusterUri(ctx, p.ExecCfg().InternalDB)
	if err != nil {
		return err
	}

	client, err := streamclient.NewStreamClient(ctx, clusterUri, p.ExecCfg().InternalDB)
	if err != nil {
		return errors.Wrap(err, "creating client")
	}

	srcID, srcReplicatedFrom, srcActivatedAt, err := client.PriorReplicationDetails(ctx, roachpb.TenantName(srcTenant))
	if err != nil {
		return errors.CombineErrors(errors.Wrap(err, "fetching prior replication details"), client.Close(ctx))
	}
	if err := client.Close(ctx); err != nil {
		return err
	}

	dstID := fmt.Sprintf("%s:%s", p.ExtendedEvalContext().ClusterID, dstTenantID)
	resumeTS, err := pickReplicationResume(ctx, srcID, srcReplicatedFrom, srcActivatedAt, dstID, tenInfo.PreviousSourceTenant)
	if err != nil {
		return err
	}

	if err := checkReplicationStartTime(ctx, p, tenInfo, resumeTS); err != nil {
		return err
	}

	const revertFirst = true

	jobID := p.ExecCfg().JobRegistry.MakeJobID()
	// Reset the last revert timestamp.
	tenInfo.LastRevertTenantTimestamp = hlc.Timestamp{}
	tenInfo.PhysicalReplicationConsumerJobID = jobID
	tenInfo.DataState = mtinfopb.DataStateAdd
	if err := sql.UpdateTenantRecord(ctx, p.ExecCfg().Settings,
		p.InternalSQLTxn(), tenInfo); err != nil {
		return err
	}

	var revertTo hlc.Timestamp
	if tenInfo.PreviousSourceTenant != nil {
		revertTo = tenInfo.PreviousSourceTenant.CutoverAsOf
	}

	readerID, err := createReaderTenant(ctx, p, tenInfo.Name, dstTenantID, options)
	if err != nil {
		return err
	}

	return errors.Wrap(createReplicationJob(
		ctx,
		p,
		configUri,
		srcTenant,
		dstTenantID,
		retentionTTLSeconds,
		resumeTS,
		revertTo,
		revertFirst,
		jobID,
		&tree.CreateTenantFromReplication{
			TenantSpec:                  alterTenantStmt.TenantSpec,
			ReplicationSourceTenantName: alterTenantStmt.ReplicationSourceTenantName,
			ReplicationSourceConnUri:    alterTenantStmt.ReplicationSourceConnUri,
			Options:                     alterTenantStmt.Options,
		},
		readerID,
	), "creating replication job")
}

// pickReplicationResume picks the timestamp to which dst can be reverted to
// begin replication into dst from src.
//
// This timestamp must be either the time as of which dst previously concluded
// replication from src -- replication which it is now resuming --, or the
// timestamp at which src previously concluded replication from dst (which it is
// now reversing).
//
// If neither of these conditions are determined to have previously held, no
// such resumption timestamp exists and and error is returned. If both held, the
// more recent of the two is used. This could arise e.g. if we first reverse
// replication, then cutover, but then resume the (reversed) replication.
func pickReplicationResume(
	ctx context.Context,
	srcHistoryID string,
	srcReplicatedFrom string,
	srcActivatedAt hlc.Timestamp,
	dstHistoryID string,
	dstPreviousReplicatedFrom *mtinfopb.PreviousSourceTenant,
) (hlc.Timestamp, error) {

	var resumeTS, reversalTS hlc.Timestamp

	var wasReplicatingFrom string

	// If the destination tenant was previously replicating from some some source,
	// if it is the same source that it will replicate from now, time it cutover
	// could be used to (re-)start replication.
	if from := dstPreviousReplicatedFrom; from != nil {
		wasReplicatingFrom = fmt.Sprintf("%s:%s", from.ClusterID, from.TenantID)
		if wasReplicatingFrom == srcHistoryID {
			resumeTS = from.CutoverTimestamp
		}
	}

	// If the remote source from which we will replicate was replicated from the
	// tenant into which we are about to replicate, i.e. we are reversing the
	// direction, then we can resume from the time the other side was activated.
	if srcReplicatedFrom == dstHistoryID && !srcActivatedAt.IsEmpty() {
		reversalTS = srcActivatedAt
	}

	if resumeTS.IsEmpty() && reversalTS.IsEmpty() {
		return hlc.Timestamp{}, errors.Newf(
			`cannot reconfigure existing tenant to replicate from specified source: it was neither previously replicating from this source (prior replication source was %q), nor was the source previously replicating from it (%q)`,
			wasReplicatingFrom,
			srcReplicatedFrom,
		)
	}

	if resumeTS.Less(reversalTS) {
		return reversalTS, nil
	}
	return resumeTS, nil
}

func checkReplicationStartTime(
	ctx context.Context, p sql.PlanHookState, tenInfo *mtinfopb.TenantInfo, ts hlc.Timestamp,
) error {
	// TODO(az): remove the conditional once we figure out how to validate PTS on the
	// source cluster when it has no producer jobs.
	// When starting a replication job for a tenant with BACKUP and RESTORE instead
	// of an initial scan, the source will not have producer jobs associated with
	// the tenant, thus no PTS to validate.
	if tenInfo.PreviousSourceTenant != nil && tenInfo.PreviousSourceTenant.CutoverTimestamp.Equal(ts) {
		return nil
	}

	pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(p.InternalSQLTxn())

	protected := hlc.MaxTimestamp
	for _, id := range tenInfo.PhysicalReplicationProducerJobIDs {
		j, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, id, p.InternalSQLTxn())
		if err != nil {
			return err
		}
		record, err := pts.GetRecord(ctx, j.Details().(jobspb.StreamReplicationDetails).ProtectedTimestampRecordID)
		if err != nil {
			if errors.Is(err, protectedts.ErrNotExists) {
				continue
			}
			return err
		}
		if record.Timestamp.LessEq(ts) {
			return nil
		}
		protected.Backward(record.Timestamp)
	}

	if protected == hlc.MaxTimestamp {
		return errors.Newf("cannot resume replication into tenant %q by reverting to %s (no history is retained by %d producer jobs)",
			tenInfo.Name, ts.GoTime(), len(tenInfo.PhysicalReplicationProducerJobIDs))
	}

	return errors.Newf("cannot resume replication into tenant %q by reverting to %s (history retained since %s)",
		tenInfo.Name, ts.GoTime(), protected.GoTime())
}

// alterTenantJobCutover returns the cutover timestamp that was used to initiate
// the cutover process - if the command is 'ALTER VIRTUAL CLUSTER .. COMPLETE REPLICATION
// TO LATEST' then the frontier high water timestamp is used.
func alterTenantJobCutover(
	ctx context.Context,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	ptp protectedts.Storage,
	alterTenantStmt *tree.AlterTenantReplication,
	tenInfo *mtinfopb.TenantInfo,
	cutoverTime hlc.Timestamp,
) (_ hlc.Timestamp, err error) {
	if alterTenantStmt == nil || alterTenantStmt.Cutover == nil {
		return hlc.Timestamp{}, errors.AssertionFailedf("unexpected nil ALTER VIRTUAL CLUSTER cutover expression")
	}

	defer func() {
		if err == nil {
			telemetry.Count("physical_replication.cutover")
		}
	}()

	tenantName := tenInfo.Name
	job, err := jobRegistry.LoadJobWithTxn(ctx, tenInfo.PhysicalReplicationConsumerJobID, txn)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	details, ok := job.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return hlc.Timestamp{}, errors.Newf("job with id %d is not a stream ingestion job", job.ID())
	}
	progress := job.Progress()

	replicatedTimeAtCutover := replicationutils.ReplicatedTimeFromProgress(&progress)
	if replicatedTimeAtCutover.IsEmpty() {
		replicatedTimeAtCutover = details.ReplicationStartTime
	}

	if alterTenantStmt.Cutover.Latest {
		cutoverTime = replicatedTimeAtCutover
	}

	// TODO(ssd): We could use the replication manager here, but
	// that embeds a priviledge check which is already completed.
	//
	// Check that the timestamp is above our retained timestamp.
	stats, err := replicationutils.GetStreamIngestionStats(ctx, details, progress)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if stats.IngestionDetails.ProtectedTimestampRecordID == nil {
		return hlc.Timestamp{}, errors.Newf("replicated tenant %q (%d) has not yet recorded a retained timestamp",
			tenantName, tenInfo.ID)
	} else {
		record, err := ptp.GetRecord(ctx, *stats.IngestionDetails.ProtectedTimestampRecordID)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		if cutoverTime.Less(record.Timestamp) {
			return hlc.Timestamp{}, errors.Newf("cutover time %s is before earliest safe cutover time %s",
				cutoverTime, record.Timestamp)
		}
	}
	if err := applyCutoverTime(ctx, job, txn, cutoverTime, replicatedTimeAtCutover); err != nil {
		return hlc.Timestamp{}, err
	}

	return cutoverTime, nil
}

// applyCutoverTime modifies the consumer job record with a cutover time and
// unpauses the job if necessary.
func applyCutoverTime(
	ctx context.Context,
	job *jobs.Job,
	txn isql.Txn,
	cutoverTimestamp hlc.Timestamp,
	replicatedTimeAtCutover hlc.Timestamp,
) error {
	log.Infof(ctx, "adding cutover time %s to job record", cutoverTimestamp)
	return job.WithTxn(txn).Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress := md.Progress.GetStreamIngest()
		details := md.Payload.GetStreamIngestion()
		if progress.ReplicationStatus == jobspb.ReplicationFailingOver {
			return errors.Newf("job %d already started cutting over to timestamp %s",
				job.ID(), progress.CutoverTime)
		}

		progress.ReplicationStatus = jobspb.ReplicationPendingFailover
		// Update the sentinel being polled by the stream ingestion job to
		// check if a complete has been signaled.
		progress.CutoverTime = cutoverTimestamp
		progress.ReplicatedTimeAtCutover = replicatedTimeAtCutover
		progress.RemainingCutoverSpans = roachpb.Spans{details.Span}
		ju.UpdateProgress(md.Progress)
		return ju.Unpaused(ctx, md)
	})
}

func alterTenantExpirationWindow(
	ctx context.Context,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	expirationWindow time.Duration,
	tenInfo *mtinfopb.TenantInfo,
) error {
	for _, producerJobID := range tenInfo.PhysicalReplicationProducerJobIDs {
		if err := jobRegistry.UpdateJobWithTxn(ctx, producerJobID, txn,
			func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {

				streamProducerDetails := md.Payload.GetStreamReplication()
				previousExpirationWindow := streamProducerDetails.ExpirationWindow
				streamProducerDetails.ExpirationWindow = expirationWindow
				ju.UpdatePayload(md.Payload)

				difference := expirationWindow - previousExpirationWindow
				currentExpiration := md.Progress.GetStreamReplication().Expiration
				newExpiration := currentExpiration.Add(difference)
				md.Progress.GetStreamReplication().Expiration = newExpiration
				ju.UpdateProgress(md.Progress)
				return nil
			}); err != nil {
			return err
		}
	}
	return nil
}

func alterTenantConsumerOptions(
	ctx context.Context,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	options *resolvedTenantReplicationOptions,
	tenInfo *mtinfopb.TenantInfo,
) error {
	return jobRegistry.UpdateJobWithTxn(ctx, tenInfo.PhysicalReplicationConsumerJobID, txn,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			streamIngestionDetails := md.Payload.GetStreamIngestion()
			if ret, ok := options.GetRetention(); ok {
				streamIngestionDetails.ReplicationTTLSeconds = ret
			}
			ju.UpdatePayload(md.Payload)
			return nil
		})

}

func init() {
	sql.AddPlanHook("alter replication job", alterReplicationJobHook, alterReplicationJobTypeCheck)
}
