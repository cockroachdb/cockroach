// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

func verboseFingerprint(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (tree.Datum, error) {
	if len(args) != 3 {
		return nil, errors.New("argument list must have three elements")
	}
	span, err := parseSpan(args[0])
	if err != nil {
		return nil, err
	}

	// The startTime can either be a timestampTZ or a decimal.
	var startTimestamp hlc.Timestamp
	if parsedDecimal, ok := tree.AsDDecimal(args[1]); ok {
		startTimestamp, err = hlc.DecimalToHLC(&parsedDecimal.Decimal)
		if err != nil {
			return nil, err
		}
	} else {
		startTime := tree.MustBeDTimestampTZ(args[1]).Time
		startTimestamp = hlc.Timestamp{WallTime: startTime.UnixNano()}
	}

	allRevisions := bool(tree.MustBeDBool(args[2]))
	return fingerprint(ctx, evalCtx, span, startTimestamp, allRevisions /* stripped */, false)
}

func fingerprint(
	ctx context.Context,
	evalCtx *eval.Context,
	span roachpb.Span,
	startTime hlc.Timestamp,
	allRevisions, stripped bool,
) (tree.Datum, error) {
	ctx, sp := tracing.ChildSpan(ctx, "crdb_internal.fingerprint")
	defer sp.Finish()

	if !evalCtx.Settings.Version.IsActive(ctx, clusterversion.V23_1) {
		return nil, errors.Errorf("cannot use crdb_internal.fingerprint until the cluster version is at least %s",
			clusterversion.V23_1.String())
	}

	isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		return nil, errors.New("crdb_internal.fingerprint() requires admin privilege")
	}

	filter := kvpb.MVCCFilter_Latest
	if allRevisions {
		filter = kvpb.MVCCFilter_All
	}
	header := kvpb.Header{
		Timestamp: evalCtx.Txn.ReadTimestamp(),
		// TODO(ssd): Setting this disables async sending in
		// DistSender so it likely substantially impacts
		// performance.
		ReturnElasticCPUResumeSpans: true,
	}
	admissionHeader := kvpb.AdmissionHeader{
		Priority:                 int32(admissionpb.BulkNormalPri),
		CreateTime:               timeutil.Now().UnixNano(),
		Source:                   kvpb.AdmissionHeader_FROM_SQL,
		NoMemoryReservedAtSource: true,
	}

	todo := make(chan kvpb.RequestHeader, 1)
	todo <- kvpb.RequestHeader{Key: span.Key, EndKey: span.EndKey}
	ctxDone := ctx.Done()
	var fingerprint uint64
	// TODO(adityamaru): Memory monitor this slice of buffered SSTs that
	// contain range keys across ExportRequests.
	ssts := make([][]byte, 0)
	for {
		select {
		case <-ctxDone:
			return nil, ctx.Err()
		case reqHeader := <-todo:
			req := &kvpb.ExportRequest{
				RequestHeader:      reqHeader,
				StartTime:          startTime,
				MVCCFilter:         filter,
				ExportFingerprint:  true,
				FingerprintOptions: kvpb.FingerprintOptions{StripIndexPrefixAndTimestamp: stripped}}
			var rawResp kvpb.Response
			var recording tracingpb.Recording
			var pErr *kvpb.Error
			exportRequestErr := timeutil.RunWithTimeout(ctx,
				fmt.Sprintf("ExportRequest fingerprint for span %s", roachpb.Span{Key: span.Key,
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
				return nil, exportRequestErr
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
			if resp.ResumeSpan != nil {
				if !resp.ResumeSpan.Valid() {
					return nil, errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
				}
				todo <- kvpb.RequestHeaderFromSpan(*resp.ResumeSpan)
			}
		default:
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
				return nil, err
			}
			fingerprint = fingerprint ^ rangekeyFingerprint
			return tree.NewDInt(tree.DInt(fingerprint)), nil
		}
	}
}
