// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// SSTTargetSizeSetting is the cluster setting name for the
// ExportRequestTargetFileSize setting.
const SSTTargetSizeSetting = "kv.bulk_sst.target_size"

// ExportRequestTargetFileSize controls the target file size for SSTs created
// during backups.
var ExportRequestTargetFileSize = settings.RegisterByteSizeSetting(
	SSTTargetSizeSetting,
	fmt.Sprintf("target size for SSTs emitted from export requests; "+
		"export requests (i.e. BACKUP) may buffer up to the sum of %s and %s in memory",
		SSTTargetSizeSetting, MaxExportOverageSetting,
	),
	16<<20,
).WithPublic()

// MaxExportOverageSetting is the cluster setting name for the
// ExportRequestMaxAllowedFileSizeOverage setting.
const MaxExportOverageSetting = "kv.bulk_sst.max_allowed_overage"

// ExportRequestMaxAllowedFileSizeOverage controls the maximum size in excess of
// the target file size which an exported SST may be. If this value is positive
// and an SST would exceed this size (due to large rows or large numbers of
// versions), then the export will fail.
var ExportRequestMaxAllowedFileSizeOverage = settings.RegisterByteSizeSetting(
	MaxExportOverageSetting,
	fmt.Sprintf("if positive, allowed size in excess of target size for SSTs from export requests; "+
		"export requests (i.e. BACKUP) may buffer up to the sum of %s and %s in memory",
		SSTTargetSizeSetting, MaxExportOverageSetting,
	),
	64<<20, /* 64 MiB */
).WithPublic()

func init() {
	RegisterReadOnlyCommand(roachpb.Export, declareKeysExport, evalExport)
}

func declareKeysExport(
	rs ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeGCThresholdKey(header.RangeID)})
}

// evalExport dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalExport(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ExportRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ExportResponse)

	ctx, evalExportSpan := tracing.ChildSpan(ctx, fmt.Sprintf("Export [%s,%s)", args.Key, args.EndKey))
	defer evalExportSpan.Finish()

	var evalExportTrace types.StringValue
	if cArgs.EvalCtx.NodeID() == h.GatewayNodeID {
		evalExportTrace.Value = fmt.Sprintf("evaluating Export [%s, %s) on local node %d",
			args.Key, args.EndKey, cArgs.EvalCtx.NodeID())
	} else {
		evalExportTrace.Value = fmt.Sprintf("evaluating Export [%s, %s) on remote node %d",
			args.Key, args.EndKey, cArgs.EvalCtx.NodeID())
	}
	evalExportSpan.RecordStructured(&evalExportTrace)

	if !args.ReturnSST {
		return result.Result{}, errors.New("ReturnSST is required")
	}

	if args.Encryption != nil {
		return result.Result{}, errors.New("returned SSTs cannot be encrypted")
	}

	// For MVCC_All backups with no start time, they'll only be capturing the
	// *revisions* since the gc threshold, so noting that in the reply allows the
	// BACKUP to correctly note the supported time bounds for RESTORE AS OF SYSTEM
	// TIME.
	if args.MVCCFilter == roachpb.MVCCFilter_All {
		reply.StartTime = cArgs.EvalCtx.GetGCThreshold()
	}

	var exportAllRevisions bool
	switch args.MVCCFilter {
	case roachpb.MVCCFilter_Latest:
		exportAllRevisions = false
	case roachpb.MVCCFilter_All:
		exportAllRevisions = true
	default:
		return result.Result{}, errors.Errorf("unknown MVCC filter: %s", args.MVCCFilter)
	}

	targetSize := uint64(args.TargetFileSize)
	// TODO(adityamaru): Remove this once we are able to set tenant specific
	// cluster settings. This takes the minimum of the system tenant's cluster
	// setting and the target size sent as part of the ExportRequest from the
	// tenant.
	clusterSettingTargetSize := uint64(ExportRequestTargetFileSize.Get(&cArgs.EvalCtx.ClusterSettings().SV))
	if targetSize > clusterSettingTargetSize {
		targetSize = clusterSettingTargetSize
	}

	var maxSize uint64
	allowedOverage := ExportRequestMaxAllowedFileSizeOverage.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	if targetSize > 0 && allowedOverage > 0 {
		maxSize = targetSize + uint64(allowedOverage)
	}

	// Time-bound iterators only make sense to use if the start time is set.
	useTBI := args.EnableTimeBoundIteratorOptimization && !args.StartTime.IsEmpty()
	// Only use resume timestamp if splitting mid key is enabled.
	resumeKeyTS := hlc.Timestamp{}
	if args.SplitMidKey {
		resumeKeyTS = args.ResumeKeyTS
	}

	var curSizeOfExportedSSTs int64
	for start := args.Key; start != nil; {
		destFile := &storage.MemFile{}
		summary, resume, resumeTS, err := reader.ExportMVCCToSst(ctx, start, args.EndKey, args.StartTime,
			h.Timestamp, resumeKeyTS, exportAllRevisions, targetSize, maxSize, args.SplitMidKey, useTBI, destFile)
		if err != nil {
			if errors.HasType(err, (*storage.ExceedMaxSizeError)(nil)) {
				err = errors.WithHintf(err,
					"consider increasing cluster setting %q", MaxExportOverageSetting)
			}
			return result.Result{}, err
		}
		data := destFile.Data()

		// NB: This should only happen on the first page of results. If there were
		// more data to be read that lead to pagination then we'd see it in this
		// page. Break out of the loop because there must be no data to export.
		if summary.DataSize == 0 {
			break
		}

		span := roachpb.Span{Key: start}
		if resume != nil {
			span.EndKey = resume
		} else {
			span.EndKey = args.EndKey
		}
		exported := roachpb.ExportResponse_File{
			Span:     span,
			EndKeyTS: resumeTS,
			Exported: summary,
			SST:      data,
		}
		reply.Files = append(reply.Files, exported)
		start = resume
		resumeKeyTS = resumeTS

		if h.TargetBytes > 0 {
			curSizeOfExportedSSTs += summary.DataSize
			// There could be a situation where the size of exported SSTs is larger
			// than the TargetBytes. In such a scenario, we want to report back
			// TargetBytes as the size of the processed SSTs otherwise the DistSender
			// will error out with an "exceeded limit". In every other case we want to
			// report back the actual size so that the DistSender can shrink the limit
			// for subsequent range requests.
			// This is semantically OK for two reasons:
			//
			// - DistSender does not parallelize requests with TargetBytes > 0.
			//
			// - DistSender uses NumBytes to shrink the limit for subsequent requests.
			// By returning TargetBytes, no more requests will be processed (and there
			// are no parallel running requests) which is what we expect.
			//
			// The ResumeSpan is what is used as the source of truth by the caller
			// issuing the request, and that contains accurate information about what
			// is left to be exported.
			targetSize := h.TargetBytes
			if curSizeOfExportedSSTs < targetSize {
				targetSize = curSizeOfExportedSSTs
			}
			reply.NumBytes = targetSize
			reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
			// NB: This condition means that we will allow another SST to be created
			// even if we have less room in our TargetBytes than the target size of
			// the next SST. In the worst case this could lead to us exceeding our
			// TargetBytes by SST target size + overage.
			if reply.NumBytes == h.TargetBytes {
				if resume != nil {
					reply.ResumeSpan = &roachpb.Span{
						Key:    resume,
						EndKey: args.EndKey,
					}
				}
				break
			}
		}
	}

	return result.Result{}, nil
}
