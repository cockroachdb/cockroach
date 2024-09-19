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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
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
	"the maximum number of workers used to issue fingerprint ExportRequests",
	64,
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
		evalCtx    = p.EvalContext()
		execCfg    = p.ExecutorConfig().(*ExecutorConfig)
		dsp        = p.DistSQLPlanner()
		extEvalCtx = p.ExtendedEvalContext()
	)

	maxWorkerCount := int(maxFingerprintNumWorkers.Get(execCfg.SV()))
	if maxWorkerCount == 1 {
		return fingerprintSpanImpl(ctx, evalCtx, span, startTime, allRevisions, stripped)
	}

	planCtx, _, err := dsp.SetupAllNodesPlanning(ctx, extEvalCtx, execCfg)
	if err != nil {
		return 0, nil, err
	}

	spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, []roachpb.Span{span})
	if err != nil {
		return 0, nil, err
	}

	maxLen := 0
	count := 0
	for _, partition := range spanPartitions {
		length := len(partition.Spans)
		maxLen = max(maxLen, length)
		count += length
	}
	// Take as many workers as partitions to efficiently distribute work
	maxWorkerCount = min(maxWorkerCount, count)

	// Each span partition contains spans that are likely to be served by
	// the same node. By round robin grabbing a span from each partition
	// and pushing to the channel, ideally sequential workers won't attempt
	// to read from the same node at the same time.
	spanChannel := make(chan roachpb.Span, count)
	for i := range maxLen {
		for _, partition := range spanPartitions {
			if i < len(partition.Spans) {
				spanChannel <- partition.Spans[i]
			}
		}
	}
	close(spanChannel)

	rv := struct {
		syncutil.Mutex
		ssts        [][]byte
		fingerprint uint64
	}{
		ssts: make([][]byte, 0, len(spanPartitions)),
	}

	grp := ctxgroup.WithContext(ctx)
	for range maxWorkerCount {
		grp.GoCtx(func(ctx context.Context) error {
			// Run until channel is empty
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case sp, ok := <-spanChannel:
					if !ok {
						return nil
					}
					localFingerprint, localSSTs, err := fingerprintSpanImpl(ctx, evalCtx, sp, startTime, allRevisions, stripped)
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

	if err := grp.Wait(); err != nil {
		return 0, nil, err
	}
	return rv.fingerprint, rv.ssts, nil
}

func fingerprintSpanImpl(
	ctx context.Context,
	evalCtx *eval.Context,
	span roachpb.Span,
	startTime hlc.Timestamp,
	allRevisions, stripped bool,
) (uint64, [][]byte, error) {

	filter := kvpb.MVCCFilter_Latest
	if allRevisions {
		filter = kvpb.MVCCFilter_All
	}
	header := kvpb.Header{
		Timestamp: evalCtx.Txn.ReadTimestamp(),
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
				rawResp, pErr = kv.SendWrappedWithAdmission(ctx, evalCtx.Txn.DB().NonTransactionalSender(), header, admissionHeader, req)
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
