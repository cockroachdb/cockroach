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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// SSTTargetSizeSetting is the cluster setting name for the
// ExportRequestTargetFileSize setting.
const SSTTargetSizeSetting = "kv.bulk_sst.target_size"

// ExportRequestTargetFileSize controls the target file size for SSTs created
// during backups.
var ExportRequestTargetFileSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
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
	settings.TenantWritable,
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
	header *roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
	maxOffset time.Duration,
) {
	DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeGCThresholdKey(header.RangeID)})
	// Export requests will usually not hold latches during their evaluation.
	//
	// See call to `AssertAllowed()` in GetGCThreshold() to understand why we need
	// to disable these assertions for export requests.
	latchSpans.DisableUndeclaredAccessAssertions()
}

// evalExport dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalExport(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ExportRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ExportResponse)

	ctx, evalExportSpan := tracing.ChildSpan(ctx, "evalExport")
	defer evalExportSpan.Finish()

	// Table's marked to be excluded from backup are expected to be configured
	// with a short GC TTL. Additionally, backup excludes such table's from being
	// protected from GC when writing ProtectedTimestamp records. The
	// ExportRequest is likely to find its target data has been GC'ed at this
	// point, and so if the range being exported is part of such a table, we do
	// not want to send back any row data to be backed up.
	if cArgs.EvalCtx.ExcludeDataFromBackup() {
		log.Infof(ctx, "[%s, %s) is part of a table excluded from backup, returning empty ExportResponse", args.Key, args.EndKey)
		return result.Result{}, nil
	}

	if args.Encryption != nil {
		return result.Result{}, errors.New("returned SSTs cannot be encrypted")
	}

	// For MVCC_All backups with no start time, they'll only be capturing the
	// *revisions* since the gc threshold, so noting that in the reply allows the
	// BACKUP to correctly note the supported time bounds for RESTORE AS OF SYSTEM
	// TIME.
	//
	// NOTE: Since export requests may not be holding latches during evaluation,
	// this `GetGCThreshold()` call is going to potentially return a higher GC
	// threshold than the pebble state we're evaluating over. This is copacetic.
	if args.MVCCFilter == roachpb.MVCCFilter_All {
		reply.StartTime = args.StartTime
		if args.StartTime.IsEmpty() {
			reply.StartTime = cArgs.EvalCtx.GetGCThreshold()
		}
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

	var maxIntents uint64
	if m := storage.MaxIntentsPerWriteIntentError.Get(&cArgs.EvalCtx.ClusterSettings().SV); m > 0 {
		maxIntents = uint64(m)
	}

	// Only use resume timestamp if splitting mid key is enabled.
	resumeKeyTS := hlc.Timestamp{}
	if args.SplitMidKey {
		resumeKeyTS = args.ResumeKeyTS
	}

	var curSizeOfExportedSSTs int64
	for start := args.Key; start != nil; {
		destFile := &storage.MemFile{}
		opts := storage.MVCCExportOptions{
			StartKey:           storage.MVCCKey{Key: start, Timestamp: resumeKeyTS},
			EndKey:             args.EndKey,
			StartTS:            args.StartTime,
			EndTS:              h.Timestamp,
			ExportAllRevisions: exportAllRevisions,
			TargetSize:         targetSize,
			MaxSize:            maxSize,
			MaxIntents:         maxIntents,
			StopMidKey:         args.SplitMidKey,
		}
		var summary roachpb.BulkOpSummary
		var resume storage.MVCCKey
		var fingerprint uint64
		var err error
		if args.ExportFingerprint {
			// Default to stripping the tenant prefix from keys, and checksum from
			// values before fingerprinting so that the fingerprint is tenant
			// agnostic.
			opts.FingerprintOptions = storage.MVCCExportFingerprintOptions{
				StripTenantPrefix:  true,
				StripValueChecksum: true,
			}
			var hasRangeKeys bool
			summary, resume, fingerprint, hasRangeKeys, err = storage.MVCCExportFingerprint(ctx,
				cArgs.EvalCtx.ClusterSettings(), reader, opts, destFile)
			if err != nil {
				if errors.HasType(err, (*storage.ExceedMaxSizeError)(nil)) {
					err = errors.WithHintf(err,
						"consider increasing cluster setting %q", MaxExportOverageSetting)
				}
				return result.Result{}, err
			}

			// If no range keys were encountered during fingerprinting then we zero
			// out the underlying SST file as there is no use in sending an empty file
			// part of the ExportResponse. This frees up the memory used by the empty
			// SST file.
			if !hasRangeKeys {
				destFile = &storage.MemFile{}
			}
		} else {
			summary, resume, err = storage.MVCCExportToSST(ctx, cArgs.EvalCtx.ClusterSettings(), reader,
				opts, destFile)
			if err != nil {
				if errors.HasType(err, (*storage.ExceedMaxSizeError)(nil)) {
					err = errors.WithHintf(err,
						"consider increasing cluster setting %q", MaxExportOverageSetting)
				}
				return result.Result{}, err
			}
		}
		data := destFile.Data()

		// NB: This should only happen in two cases:
		//
		// 1. There was nothing to export for this span.
		//
		// 2. We hit a resource constraint that led to an
		//    early exit and thus have a resume key despite
		//    not having data.
		if summary.DataSize == 0 {
			if resume.Key != nil {
				start = resume.Key
				resumeKeyTS = resume.Timestamp
				continue
			} else {
				break
			}
		}

		span := roachpb.Span{Key: start}
		if resume.Key != nil {
			span.EndKey = resume.Key
		} else {
			span.EndKey = args.EndKey
		}

		var exported roachpb.ExportResponse_File
		if args.ExportFingerprint {
			exported = roachpb.ExportResponse_File{
				EndKeyTS:    resume.Timestamp,
				SST:         data,
				Fingerprint: fingerprint,
			}
		} else {
			exported = roachpb.ExportResponse_File{
				Span:     span,
				EndKeyTS: resume.Timestamp,
				Exported: summary,
				SST:      data,
			}
		}
		reply.Files = append(reply.Files, exported)
		start = resume.Key
		resumeKeyTS = resume.Timestamp

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
			// NB: This condition means that we will allow another SST to be created
			// even if we have less room in our TargetBytes than the target size of
			// the next SST. In the worst case this could lead to us exceeding our
			// TargetBytes by SST target size + overage.
			if reply.NumBytes == h.TargetBytes {
				if resume.Key != nil {
					reply.ResumeSpan = &roachpb.Span{
						Key:    resume.Key,
						EndKey: args.EndKey,
					}
					reply.ResumeReason = roachpb.RESUME_BYTE_LIMIT
				}
				break
			}
		}
	}

	return result.Result{}, nil
}
