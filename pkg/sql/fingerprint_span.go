// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var maxFingerprintNumWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.fingerprint.max_span_parallelism",
	"the maximum number of workers per partition used to issue fingerprint ExportRequests",
	5,
	settings.PositiveInt,
)

// FingerprintSpan calculated a fingerprint for the given span using ExportRequest.
//
// The caller is responsible for authorization checks.
func (p *planner) FingerprintSpan(
	ctx context.Context, span roachpb.Span, startTime hlc.Timestamp, allRevisions, stripped bool,
) (uint64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "sql.FingerprintSpan")
	defer sp.Finish()
	evalCtx := p.EvalContext()
	fingerprint, ssts, err := p.fingerprintSpanFanout(ctx, span, startTime, allRevisions, stripped)
	if err != nil {
		return 0, err
	}

	// No ExportRequests left to send. We've aggregated range keys
	// across all ExportRequests and can now fingerprint them.
	//
	// NB: We aggregate rangekeys across ExportRequests and then
	// fingerprint them on the client, instead of fingerprinting them as
	// part of the ExportRequest command evaluation, because range keys
	// do not have a stable, discrete identity. Their fragmentation can
	// be influenced by rangekeys outside the time interval that we are
	// fingerprinting, or by range splits. So, we need to "defragment"
	// all the rangekey stacks we observe such that the fragmentation is
	// deterministic on only the data we want to fingerprint in our key
	// and time interval.
	//
	// Egs:
	//
	// t2  				[-----)[----)
	//
	// t1 	[----)[-----)
	//			a			b			c			d
	//
	// Assume we have two rangekeys [a, c)@t1 and [b, d)@t2. They will
	// fragment as shown in the diagram above. If we wish to fingerprint
	// key [a-d) in time interval (t1, t2] the fragmented rangekey
	// [a, c)@t1 is outside our time interval and should not influence our
	// fingerprint. The iterator in `fingerprintRangekeys` will
	// "defragment" the rangekey stacks [b-c)@t2 and [c-d)@t2 and
	// fingerprint them as a single rangekey with bounds [b-d)@t2.
	rangekeyFingerprint, err := storage.FingerprintRangekeys(ctx, evalCtx.Settings,
		storage.MVCCExportFingerprintOptions{
			StripTenantPrefix:            true,
			StripValueChecksum:           true,
			StripIndexPrefixAndTimestamp: stripped,
		}, ssts)
	if err != nil {
		return 0, err
	}
	fingerprint = fingerprint ^ rangekeyFingerprint
	return fingerprint, nil
}

// fingerprintSpanFanout sends appropriately configured ExportRequests
// in parallel. The span is divided using DistSQL's PartitionSpans.
//
// We do this to get parallel execution of ExportRequest even in the
// case of a non-zero batch size. DistSender will not parallelize
// requests with non-zero MaxSpanRequestKeys set.
func (p *planner) fingerprintSpanFanout(
	ctx context.Context, span roachpb.Span, startTime hlc.Timestamp, allRevisions, stripped bool,
) (uint64, [][]byte, error) {
	ctx, sp := tracing.ChildSpan(ctx, "fingerprintSpanFanout")
	defer sp.Finish()

	var (
		txn        = p.EvalContext().Txn
		execCfg    = p.ExecutorConfig().(*ExecutorConfig)
		dsp        = p.DistSQLPlanner()
		extEvalCtx = p.ExtendedEvalContext()
	)

	maxWorkerCount := int(maxFingerprintNumWorkers.Get(execCfg.SV()))
	if maxWorkerCount == 1 {
		return fingerprintSpanImpl(ctx, txn, span, startTime, allRevisions, stripped)
	}

	planCtx, _, err := dsp.SetupAllNodesPlanning(ctx, extEvalCtx, execCfg)
	if err != nil {
		return 0, nil, err
	}

	spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, []roachpb.Span{span}, PartitionSpansBoundDefault)
	if err != nil {
		return 0, nil, err
	}

	rv := struct {
		syncutil.Mutex
		ssts        [][]byte
		fingerprint uint64
	}{
		ssts: make([][]byte, 0, len(spanPartitions)),
	}

	fingerprintPartition := func(
		partition roachpb.Spans,
	) func(ctx context.Context) error {
		return func(ctx context.Context) (retErr error) {
			// workCh is used to divide up the partition between workers. It is
			// closed whenever there is no work to do. It might not be closed if
			// the coordinator encounters an error.
			workCh := make(chan roachpb.Span)
			ctx, cancel := context.WithCancel(ctx)

			grp := ctxgroup.WithContext(ctx)
			for range maxWorkerCount {
				grp.GoCtx(func(ctx context.Context) error {
					// Run until the work channel is empty or the coordinator
					// exits.
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case sp, ok := <-workCh:
							if !ok {
								// No more work.
								return nil
							}
							localFingerprint, localSSTs, err := fingerprintSpanImpl(ctx, txn, sp, startTime, allRevisions, stripped)
							if err != nil {
								return err
							}
							rv.Lock()
							rv.ssts = append(rv.ssts, localSSTs...) // nolint:deferunlockcheck
							rv.fingerprint = rv.fingerprint ^ localFingerprint
							rv.Unlock()
						}
					}
				})
			}
			defer func() {
				// Either workCh is closed (meaning that we've processed all the
				// work), or we hit an error in the loop below. If it's the
				// latter, we need to cancel the context to signal to the
				// workers to shutdown ASAP.
				if retErr != nil {
					cancel()
				}
				// Regardless of how we got here, ensure that we always block
				// until all workers exit.
				// TODO(yuzefovich): refactor the logic here so that the
				// coordinator goroutine had a single return point. This will
				// also allow us to prevent a hypothetical scenario where we're
				// blocked forever (i.e. until the context is canceled) writing
				// into workCh which can happen if all worker goroutines exit
				// due to an error.
				grpErr := grp.Wait()
				if retErr == nil {
					retErr = grpErr
				}
			}()

			for _, part := range partition {
				rdi, err := p.execCfg.RangeDescIteratorFactory.NewLazyIterator(ctx, part, 64)
				if err != nil {
					return err
				}
				remainingSpan := part
				for ; rdi.Valid(); rdi.Next() {
					rangeDesc := rdi.CurRangeDescriptor()
					rangeSpan := roachpb.Span{Key: rangeDesc.StartKey.AsRawKey(), EndKey: rangeDesc.EndKey.AsRawKey()}
					subspan := remainingSpan.Intersect(rangeSpan)
					if !subspan.Valid() {
						return errors.AssertionFailedf("%s not in %s of %s", rangeSpan, remainingSpan, part)
					}
					select {
					case workCh <- subspan:
					case <-ctx.Done():
						return ctx.Err()
					}
					remainingSpan.Key = subspan.EndKey
				}
				if err := rdi.Error(); err != nil {
					return err
				}
			}
			close(workCh)
			return nil
		}
	}

	// Start one span splitter/group of workers per partition, each of which waits
	// for all its workers to finish before returning, and then wait for them all.
	grp := ctxgroup.WithContext(ctx)
	for _, part := range spanPartitions {
		grp.GoCtx(fingerprintPartition(part.Spans))
	}
	if err := grp.Wait(); err != nil {
		return 0, nil, err
	}

	return rv.fingerprint, rv.ssts, nil
}

func fingerprintSpanImpl(
	ctx context.Context,
	txn *kv.Txn,
	span roachpb.Span,
	startTime hlc.Timestamp,
	allRevisions, stripped bool,
) (uint64, [][]byte, error) {

	filter := kvpb.MVCCFilter_Latest
	if allRevisions {
		filter = kvpb.MVCCFilter_All
	}
	header := kvpb.Header{
		Timestamp: txn.ReadTimestamp(),
		// NOTE(ssd): Setting this disables async sending in
		// DistSender.
		ReturnElasticCPUResumeSpans: true,
	}
	admissionHeader := kvpb.AdmissionHeader{
		Priority:                 int32(admissionpb.BulkNormalPri),
		CreateTime:               timeutil.Now().UnixNano(),
		Source:                   kvpb.AdmissionHeader_FROM_SQL,
		NoMemoryReservedAtSource: true,
	}
	var (
		fingerprint uint64
		// TODO(adityamaru): Memory monitor this slice of buffered SSTs that
		// contain range keys across ExportRequests.
		ssts = make([][]byte, 0)
	)
	for len(span.Key) != 0 {
		req := &kvpb.ExportRequest{
			RequestHeader:      kvpb.RequestHeader{Key: span.Key, EndKey: span.EndKey},
			StartTime:          startTime,
			MVCCFilter:         filter,
			ExportFingerprint:  true,
			FingerprintOptions: kvpb.FingerprintOptions{StripIndexPrefixAndTimestamp: stripped}}
		var rawResp kvpb.Response
		var recording tracingpb.Recording
		var pErr *kvpb.Error
		exportRequestErr := timeutil.RunWithTimeout(ctx,
			redact.Sprintf("ExportRequest fingerprint for span %s", roachpb.Span{Key: span.Key,
				EndKey: span.EndKey}),
			5*time.Minute, func(ctx context.Context) error {
				sp := tracing.SpanFromContext(ctx)
				ctx, exportSpan := sp.Tracer().StartSpanCtx(ctx, "fingerprint.ExportRequest", tracing.WithParent(sp))
				rawResp, pErr = kv.SendWrappedWithAdmission(ctx, txn.DB().NonTransactionalSender(), header, admissionHeader, req)
				recording = exportSpan.FinishAndGetConfiguredRecording()
				if pErr != nil {
					return pErr.GoError()
				}
				return nil
			})
		if exportRequestErr != nil {
			if recording != nil {
				log.Errorf(ctx, "failed export request trace:\n%s", recording)
			}
			return 0, nil, exportRequestErr
		}

		resp := rawResp.(*kvpb.ExportResponse)
		for _, file := range resp.Files {
			fingerprint = fingerprint ^ file.Fingerprint

			// Aggregate all the range keys that need fingerprinting once all
			// ExportRequests have been completed.
			if len(file.SST) != 0 {
				ssts = append(ssts, file.SST)
			}
		}
		var resumeSpan roachpb.Span
		if resp.ResumeSpan != nil {
			if !resp.ResumeSpan.Valid() {
				return 0, nil, errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
			}
			resumeSpan = *resp.ResumeSpan
		}
		span = resumeSpan
	}
	return fingerprint, ssts, nil
}
