// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
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
	settings.SystemVisible, // used by BACKUP
	SSTTargetSizeSetting,
	fmt.Sprintf("target size for SSTs emitted from export requests; "+
		"export requests (i.e. BACKUP) may buffer up to the sum of %s and %s in memory",
		SSTTargetSizeSetting, MaxExportOverageSetting,
	),
	16<<20,
	settings.WithPublic)

// MaxExportOverageSetting is the cluster setting name for the
// ExportRequestMaxAllowedFileSizeOverage setting.
const MaxExportOverageSetting = "kv.bulk_sst.max_allowed_overage"

// ExportRequestMaxAllowedFileSizeOverage controls the maximum size in excess of
// the target file size which an exported SST may be. If this value is positive
// and an SST would exceed this size (due to large rows or large numbers of
// versions), then the export will fail.
var ExportRequestMaxAllowedFileSizeOverage = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	MaxExportOverageSetting,
	fmt.Sprintf("if positive, allowed size in excess of target size for SSTs from export requests; "+
		"export requests (i.e. BACKUP) may buffer up to the sum of %s and %s in memory",
		SSTTargetSizeSetting, MaxExportOverageSetting,
	),
	64<<20, /* 64 MiB */
	settings.WithPublic)

func init() {
	RegisterReadOnlyCommand(kvpb.Export, declareKeysExport, evalExport)
}

func declareKeysExport(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	err := DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	if err != nil {
		return err
	}
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeGCThresholdKey(header.RangeID)})
	// Export requests will usually not hold latches during their evaluation.
	//
	// See call to `AssertAllowed()` in GetGCThreshold() to understand why we need
	// to disable these assertions for export requests.
	latchSpans.DisableUndeclaredAccessAssertions()
	return nil
}

// evalExport dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalExport(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ExportRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.ExportResponse)

	ctx, evalExportSpan := tracing.ChildSpan(ctx, "evalExport")
	defer evalExportSpan.Finish()

	// Table's marked to be excluded from backup are expected to be configured
	// with a short GC TTL. Additionally, backup excludes such table's from being
	// protected from GC when writing ProtectedTimestamp records. The
	// ExportRequest is likely to find its target data has been GC'ed at this
	// point, and so if the range being exported is part of such a table, we do
	// not want to send back any row data to be backed up.
	excludeFromBackup, err := cArgs.EvalCtx.ExcludeDataFromBackup(ctx,
		roachpb.Span{Key: args.Key, EndKey: args.EndKey})
	if err != nil {
		return result.Result{}, err
	}
	if excludeFromBackup {
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
	if args.MVCCFilter == kvpb.MVCCFilter_All {
		reply.StartTime = args.StartTime
		if args.StartTime.IsEmpty() {
			reply.StartTime = cArgs.EvalCtx.GetGCThreshold()
		}
	}

	var exportAllRevisions bool
	switch args.MVCCFilter {
	case kvpb.MVCCFilter_Latest:
		exportAllRevisions = false
	case kvpb.MVCCFilter_All:
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

	var maxLockConflicts uint64
	if m := storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV); m > 0 {
		maxLockConflicts = uint64(m)
	}

	var targetLockConflictBytes uint64
	if m := storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV); m > 0 {
		targetLockConflictBytes = uint64(m)
	}

	// Only use resume timestamp if splitting mid key is enabled.
	resumeKeyTS := args.ResumeKeyTS

	maybeAnnotateExceedMaxSizeError := func(err error) error {
		if errors.HasType(err, (*storage.ExceedMaxSizeError)(nil)) {
			return errors.WithHintf(err,
				"consider increasing cluster setting %q", MaxExportOverageSetting)
		}
		return err
	}

	var curSizeOfExportedSSTs int64
	for start := args.Key; start != nil; {
		var destFile bytes.Buffer
		opts := storage.MVCCExportOptions{
			StartKey:                storage.MVCCKey{Key: start, Timestamp: resumeKeyTS},
			EndKey:                  args.EndKey,
			StartTS:                 args.StartTime,
			EndTS:                   h.Timestamp,
			ExportAllRevisions:      exportAllRevisions,
			TargetSize:              targetSize,
			MaxSize:                 maxSize,
			MaxLockConflicts:        maxLockConflicts,
			TargetLockConflictBytes: targetLockConflictBytes,
			StopMidKey:              args.SplitMidKey,
			ScanStats:               cArgs.ScanStats,
			IncludeMVCCValueHeader:  args.IncludeMVCCValueHeader,
		}
		var summary kvpb.BulkOpSummary
		var resumeInfo storage.ExportRequestResumeInfo
		var fingerprint uint64
		var err error
		if args.ExportFingerprint {
			// Default to stripping the tenant prefix from keys, and checksum from
			// values before fingerprinting so that the fingerprint is tenant
			// agnostic.
			opts.FingerprintOptions = storage.MVCCExportFingerprintOptions{
				StripTenantPrefix:            true,
				StripValueChecksum:           true,
				StripIndexPrefixAndTimestamp: args.FingerprintOptions.StripIndexPrefixAndTimestamp,
			}
			if opts.FingerprintOptions.StripIndexPrefixAndTimestamp && args.MVCCFilter == kvpb.MVCCFilter_All {
				// If a key's value were updated from a to b, the xor hash without
				// timestamps of those two mvcc values would look the same if the key
				// were updated from b to a. In other words, the order of key value
				// updates without timestamps does not affect the xor hash; but this
				// order clearly presents different mvcc history, therefore, do not
				// strip timestamps if fingerprinting all mvcc history.
				return result.Result{}, errors.New("cannot fingerprint without mvcc timestamps and with mvcc history")
			}
			if opts.FingerprintOptions.StripIndexPrefixAndTimestamp && !args.StartTime.IsEmpty() {
				// Supplying a startKey only complicates results (e.g. it surfaces
				// tombstones), given that only the latest keys are surfaced.
				return result.Result{}, errors.New("cannot fingerprint without mvcc timestamps and with a start time")
			}
			var hasRangeKeys bool
			summary, resumeInfo, fingerprint, hasRangeKeys, err = storage.MVCCExportFingerprint(ctx,
				cArgs.EvalCtx.ClusterSettings(), reader, opts, &destFile)
			if err != nil {
				return result.Result{}, maybeAnnotateExceedMaxSizeError(err)
			}

			// If no range keys were encountered during fingerprinting then we zero
			// out the underlying SST file as there is no use in sending an empty file
			// part of the ExportResponse. This frees up the memory used by the empty
			// SST file.
			if !hasRangeKeys {
				destFile = bytes.Buffer{}
			}
		} else {
			summary, resumeInfo, err = storage.MVCCExportToSST(ctx, cArgs.EvalCtx.ClusterSettings(), reader,
				opts, &destFile)
			if err != nil {
				return result.Result{}, maybeAnnotateExceedMaxSizeError(err)
			}
		}

		// Check if our ctx has been cancelled while we were constructing the SST.
		// If it has been cancelled the client is no longer expecting a response.
		select {
		case <-ctx.Done():
			return result.Result{}, ctx.Err()
		default:
		}

		data := destFile.Bytes()

		// NB: This should only happen in two cases:
		//
		// 1. There was nothing to export for this span.
		//
		// 2. We hit a resource constraint that led to an
		//    early exit and thus have a resume key despite
		//    not having data.
		if summary.DataSize == 0 {
			hasResumeKey := resumeInfo.ResumeKey.Key != nil

			// If we have a resumeKey, it means that we must have hit a resource
			// constraint before exporting any data.
			if hasResumeKey {
				// If we hit our CPU limit we must return the response to the client
				// instead of retrying immediately. This will give the scheduler a
				// chance to move the goroutine off CPU allowing other processes to make
				// progress. The client is responsible for handling pagination of
				// ExportRequests.
				if resumeInfo.CPUOverlimit && h.ReturnElasticCPUResumeSpans {
					// Note, since we have not exported any data we do not populate the
					// `Files` field of the ExportResponse.
					reply.ResumeSpan = &roachpb.Span{
						Key:    resumeInfo.ResumeKey.Key,
						EndKey: args.EndKey,
					}
					reply.ResumeReason = kvpb.RESUME_ELASTIC_CPU_LIMIT
					break
				} else {
					if !resumeInfo.CPUOverlimit {
						// We should never come here. There should be no condition aside from
						// resource constraints that results in an early exit without
						// exporting any data. Regardless, if we have a resumeKey we
						// immediately retry the ExportRequest from that key and timestamp
						// onwards.
						if !build.IsRelease() {
							return result.Result{}, errors.AssertionFailedf("ExportRequest exited without " +
								"exporting any data for an unknown reason; programming error")
						} else {
							log.Warningf(ctx, "unexpected resume span from ExportRequest without exporting any data for an unknown reason: %v", resumeInfo)
						}
					}
					start = resumeInfo.ResumeKey.Key
					resumeKeyTS = resumeInfo.ResumeKey.Timestamp
					continue
				}
			}
			// If we do not have a resumeKey it indicates that there is no data to be
			// exported in this span.
			break
		}

		span := roachpb.Span{Key: start}
		if resumeInfo.ResumeKey.Key != nil {
			span.EndKey = resumeInfo.ResumeKey.Key
		} else {
			span.EndKey = args.EndKey
		}

		var exported kvpb.ExportResponse_File
		if args.ExportFingerprint {
			// A fingerprinting ExportRequest does not need to return the
			// BulkOpSummary or the exported Span. This is because we do not expect
			// the sender of a fingerprint ExportRequest to use anything but the
			// `Fingerprint` for point-keys and the SST file that contains the
			// rangekeys we encountered during ExportRequest evaluation.
			exported = kvpb.ExportResponse_File{
				EndKeyTS:    resumeInfo.ResumeKey.Timestamp,
				SST:         data,
				Fingerprint: fingerprint,
			}
		} else {
			exported = kvpb.ExportResponse_File{
				Span:     span,
				EndKeyTS: resumeInfo.ResumeKey.Timestamp,
				Exported: summary,
				SST:      data,
			}
		}
		reply.Files = append(reply.Files, exported)
		start = resumeInfo.ResumeKey.Key
		resumeKeyTS = resumeInfo.ResumeKey.Timestamp

		// If we paginated because we are over our allotted CPU limit, we must break
		// from command evaluation and return a response to the client before
		// resuming our export from the resume key. This gives the scheduler a
		// chance to take the current goroutine off CPU and allow other processes to
		// progress.
		if resumeInfo.CPUOverlimit && h.ReturnElasticCPUResumeSpans {
			if resumeInfo.ResumeKey.Key != nil {
				reply.ResumeSpan = &roachpb.Span{
					Key:    resumeInfo.ResumeKey.Key,
					EndKey: args.EndKey,
				}
				reply.ResumeReason = kvpb.RESUME_ELASTIC_CPU_LIMIT
			}
			break
		}

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
				if resumeInfo.ResumeKey.Key != nil {
					reply.ResumeSpan = &roachpb.Span{
						Key:    resumeInfo.ResumeKey.Key,
						EndKey: args.EndKey,
					}
					reply.ResumeReason = kvpb.RESUME_BYTE_LIMIT
				}
				break
			}
		}
	}

	return result.Result{}, nil
}
