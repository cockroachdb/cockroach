// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprint

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/followerreads"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type resumer jobspb.JobID

var _ jobs.Resumer = (*resumer)(nil)

func (r *resumer) ID() jobspb.JobID {
	return jobspb.JobID(*r)
}

func (r *resumer) LookupDetails(
	ctx context.Context, execCtx sql.JobExecContext,
) (jobspb.FingerprintDetails, error) {
	var payload jobspb.Payload
	if err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ok, err := jobs.InfoStorageForJob(txn, r.ID()).GetProto(ctx, jobs.LegacyPayloadKey, &payload)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("job payload not found")
		}
		return nil
	}); err != nil {
		return jobspb.FingerprintDetails{}, err
	}
	p, ok := payload.UnwrapDetails().(jobspb.FingerprintDetails)
	if !ok {
		return jobspb.FingerprintDetails{}, errors.AssertionFailedf("unexpected job payload type: %T", payload.UnwrapDetails())
	}
	return p, nil
}

func (r *resumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)

	details, err := r.LookupDetails(ctx, jobExecCtx)
	if err != nil {
		return err
	}

	gatherer := &gatherer{
		spans: details.Spans,
		persister: &persist{
			id:      jobspb.JobID(*r),
			execCtx: jobExecCtx,
		},
		partitioner: partitionSpans{jobExecCtx, false},
		fn: kvFingerprinter{
			sender:   jobExecCtx.ExecCfg().DB.NonTransactionalSender(),
			asOf:     details.AsOf,
			start:    details.Start,
			stripped: details.Stripped,
		}.fingerprintSpan,
		// TODO: make this configurable.
		chkptFreq: func() time.Duration { return 30 * time.Second },
	}

	// Retry the gatherer run in case of transient errors (e.g., node draining,
	// connection errors, ambiguous results). The gatherer maintains checkpointed
	// progress, so retries will resume from the last checkpoint.
	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     15 * time.Second,
		Multiplier:     2,
	}

	var lastErr error
	for retrier := retry.Start(retryOpts); retrier.Next(); {
		_, lastErr = gatherer.run(ctx)
		if lastErr == nil {
			break
		}

		// If the error is permanent (non-retryable), fail immediately.
		if joberror.IsPermanentBulkJobError(lastErr) {
			return lastErr
		}

		// Log retry attempt.
		log.Eventf(ctx, "fingerprint job %d: gatherer.run failed with retryable error, will retry: %v", r.ID(), lastErr)

		// Check if the context is done (e.g., job canceled or timeout).
		if ctx.Err() != nil {
			return errors.CombineErrors(ctx.Err(), lastErr)
		}
	}

	if lastErr != nil {
		return lastErr
	}

	// Final checkpoint to ensure all progress is saved.
	if err := gatherer.checkpoint(ctx); err != nil {
		return err
	}
	return unprotect(ctx, execCtx.(sql.JobExecContext).ExecCfg(), details.PTS)
}

func (r *resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, _ error) error {
	details, err := r.LookupDetails(ctx, execCtx.(sql.JobExecContext))
	if err != nil {
		return err
	}
	return unprotect(ctx, execCtx.(sql.JobExecContext).ExecCfg(), details.PTS)
}

func (r *resumer) CollectProfile(ctx context.Context, _ interface{}) error {
	return nil
}

type persist struct {
	id      jobspb.JobID
	execCtx sql.JobExecContext
}

var _ persister = (*persist)(nil)

const (
	fingerprintInfoKey, spanCountInfoKey, frontierName = "fingerprint", "spans", "done"
)

func (p *persist) load(ctx context.Context) (checkpointState, bool, error) {
	var state checkpointState
	var found bool

	err := p.execCtx.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		found = false
		state = checkpointState{}

		infoStorage := jobs.InfoStorageForJob(txn, p.id)
		hash, ok, err := infoStorage.GetUint64(ctx, fingerprintInfoKey)
		if err != nil {
			return err
		}
		if !ok {
			// No checkpoint exists yet.
			return nil
		}

		found = true
		state.fingerprint = hash
		done, ok, err := jobfrontier.Get(ctx, txn, p.id, frontierName)
		if err != nil {
			return err
		}
		if ok {
			state.done = done
		}
		spanCount, ok, err := infoStorage.GetUint64(ctx, spanCountInfoKey)
		if err != nil {
			return err
		}
		if ok {
			state.totalParts = int(spanCount)
		}
		return nil
	})

	if err != nil {
		return checkpointState{}, false, err
	}

	return state, found, nil
}

func (p *persist) store(ctx context.Context, state checkpointState, frac float64) error {
	return p.execCtx.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := jobs.InfoStorageForJob(txn, p.id)
		if err := infoStorage.WriteUint64(ctx, fingerprintInfoKey, state.fingerprint); err != nil {
			return errors.Wrap(err, "failed to store fingerprint hash")
		}

		if state.done != nil && !state.done.Frontier().IsEmpty() {
			if err := jobfrontier.Store(ctx, txn, p.id, frontierName, state.done); err != nil {
				return errors.Wrap(err, "failed to store done frontier")
			}
		} else {
			if err := jobfrontier.Delete(ctx, txn, p.id, frontierName); err != nil {
				return errors.Wrap(err, "failed to delete done frontier")
			}
		}
		if err := infoStorage.WriteUint64(ctx, spanCountInfoKey, uint64(state.totalParts)); err != nil {
			return errors.Wrap(err, "failed to store span count")
		}
		if err := jobs.ProgressStorage(p.id).Set(ctx, txn, frac, hlc.Timestamp{}); err != nil {
			return errors.Wrap(err, "failed to update job progress")
		}
		return nil
	})
}

type partitionSpans struct {
	sql.JobExecContext
	rangeSized bool
}

var _ partitioner = partitionSpans{}

func (p partitionSpans) partition(
	ctx context.Context, spans []roachpb.Span,
) ([]sql.SpanPartition, error) {
	dsp := p.DistSQLPlanner()
	planCtx, _, err := dsp.SetupAllNodesPlanningWithOracle(
		ctx, p.ExtendedEvalContext(),
		p.ExecCfg(),
		followerreads.NewStreakBulkOracle(
			dsp.ReplicaOracleConfig(p.ExecCfg().Locality), followerreads.StreakConfig{},
		),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	spanPartitions, err := p.DistSQLPlanner().PartitionSpans(ctx, planCtx, spans, sql.PartitionSpansBoundDefault)
	if err != nil {
		return nil, err
	}
	if p.rangeSized {
		// Sub-divide each partition into indivially range-aligned sub-spans.
		for part := range spanPartitions {
			subdivided := make([]roachpb.Span, 0, len(spanPartitions[part].Spans))
			for _, sp := range spanPartitions[part].Spans {
				rdi, err := p.JobExecContext.ExecCfg().RangeDescIteratorFactory.NewLazyIterator(ctx, sp, 64)
				if err != nil {
					return nil, err
				}
				remaining := sp
				for ; rdi.Valid(); rdi.Next() {
					rangeDesc := rdi.CurRangeDescriptor()
					rangeSpan := roachpb.Span{Key: rangeDesc.StartKey.AsRawKey(), EndKey: rangeDesc.EndKey.AsRawKey()}
					subspan := remaining.Intersect(rangeSpan)
					if !subspan.Valid() {
						return nil, errors.AssertionFailedf("%s not in %s of %s", rangeSpan, remaining, sp)
					}
					subdivided = append(subdivided, subspan)
					remaining.Key = subspan.EndKey
				}
				if err := rdi.Error(); err != nil {
					return nil, err
				}
				if remaining.Valid() {
					subdivided = append(subdivided, remaining)
				}
			}
			spanPartitions[part].Spans = subdivided
		}
	}
	return spanPartitions, nil
}

type kvFingerprinter struct {
	asOf     hlc.Timestamp
	start    hlc.Timestamp
	stripped bool
	sender   kv.Sender
}

var _ spanFingerprinter = kvFingerprinter{}.fingerprintSpan

func (k kvFingerprinter) fingerprintSpan(
	ctx context.Context, span roachpb.Span,
) (spanFingerprintResult, error) {
	res := spanFingerprintResult{span: span}

	header := kvpb.Header{Timestamp: k.asOf, ReturnElasticCPUResumeSpans: true}

	admissionHeader := kvpb.AdmissionHeader{
		Priority:                 int32(admissionpb.BulkNormalPri),
		CreateTime:               timeutil.Now().UnixNano(),
		Source:                   kvpb.AdmissionHeader_FROM_SQL,
		NoMemoryReservedAtSource: true,
	}
	for len(span.Key) != 0 {
		req := &kvpb.ExportRequest{
			RequestHeader:      kvpb.RequestHeader{Key: span.Key, EndKey: span.EndKey},
			StartTime:          k.start,
			ExportFingerprint:  true,
			FingerprintOptions: kvpb.FingerprintOptions{StripIndexPrefixAndTimestamp: k.stripped}}
		var rawResp kvpb.Response
		var recording tracingpb.Recording
		var pErr *kvpb.Error
		exportRequestErr := timeutil.RunWithTimeout(ctx,
			redact.Sprintf("ExportRequest fingerprint for span %s", roachpb.Span{Key: span.Key,
				EndKey: span.EndKey}),
			5*time.Minute, func(ctx context.Context) error {
				sp := tracing.SpanFromContext(ctx)
				ctx, exportSpan := sp.Tracer().StartSpanCtx(ctx, "fingerprint.ExportRequest", tracing.WithParent(sp))
				rawResp, pErr = kv.SendWrappedWithAdmission(ctx, k.sender, header, admissionHeader, req)
				recording = exportSpan.FinishAndGetConfiguredRecording()
				if pErr != nil {
					return pErr.GoError()
				}
				return nil
			})
		if exportRequestErr != nil {
			if recording != nil {
				log.Dev.Errorf(ctx, "failed export request trace:\n%s", recording)
			}
			return res, exportRequestErr
		}

		resp := rawResp.(*kvpb.ExportResponse)
		for _, file := range resp.Files {
			res.fingerprint = res.fingerprint ^ file.Fingerprint
			if len(file.SST) != 0 {
				// We shouldn't be receiving any SSTs when requesting fingerprints since
				// a fingerprint request would only return an SST along side the hash
				// if it encountered rangefeed fragments, but the latest-only (rather
				// than all-revision) scan we requested would just see nothing when a
				// rangekey i.e. a range-delete is covering a span.
				return res, errors.AssertionFailedf("unexpected SST reply to fingerprint request")
			}
		}
		var resumeSpan roachpb.Span
		if resp.ResumeSpan != nil {
			if !resp.ResumeSpan.Valid() {
				return res, errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
			}
			resumeSpan = *resp.ResumeSpan
		}
		span = resumeSpan
	}
	return res, nil
}

func unprotect(ctx context.Context, execCfg *sql.ExecutorConfig, id uuid.UUID) error {
	if id == uuid.Nil {
		return nil
	}
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
		err := pts.Release(ctx, id)
		if errors.Is(err, protectedts.ErrNotExists) {
			log.Dev.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
			return nil
		}
		return err
	})
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeFingerprint, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		id := resumer(job.ID())
		return &id
	}, jobs.DisablesTenantCostControl)
}
