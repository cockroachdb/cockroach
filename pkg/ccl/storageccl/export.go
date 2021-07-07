// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// ExportRequestTargetFileSize controls the target file size for SSTs created
// during backups.
var ExportRequestTargetFileSize = settings.RegisterByteSizeSetting(
	"kv.bulk_sst.target_size",
	"target size for SSTs emitted from export requests",
	64<<20, /* 64 MiB */
)

// ExportRequestMaxAllowedFileSizeOverage controls the maximum size in excess of
// the target file size which an exported SST may be. If this value is positive
// and an SST would exceed this size (due to large rows or large numbers of
// versions), then the export will fail.
var ExportRequestMaxAllowedFileSizeOverage = settings.RegisterByteSizeSetting(
	"kv.bulk_sst.max_allowed_overage",
	"if positive, allowed size in excess of target size for SSTs from export requests",
	64<<20, /* 64 MiB */
)

const maxUploadRetries = 5

func init() {
	batcheval.RegisterReadOnlyCommand(roachpb.Export, declareKeysExport, evalExport)
	ExportRequestTargetFileSize.SetVisibility(settings.Reserved)
	ExportRequestMaxAllowedFileSizeOverage.SetVisibility(settings.Reserved)
}

func declareKeysExport(
	rs batcheval.ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	batcheval.DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeGCThresholdKey(header.RangeID)})
}

// evalExport dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalExport(
	ctx context.Context, reader storage.Reader, cArgs batcheval.CommandArgs, resp roachpb.Response,
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

	// For MVCC_All backups with no start time, they'll only be capturing the
	// *revisions* since the gc threshold, so noting that in the reply allows the
	// BACKUP to correctly note the supported time bounds for RESTORE AS OF SYSTEM
	// TIME.
	if args.MVCCFilter == roachpb.MVCCFilter_All {
		reply.StartTime = cArgs.EvalCtx.GetGCThreshold()
	}

	makeExternalStorage := !args.ReturnSST || args.Storage != roachpb.ExternalStorage{} ||
		(args.StorageByLocalityKV != nil && len(args.StorageByLocalityKV) > 0)
	if makeExternalStorage || log.V(1) {
		log.Infof(ctx, "export [%s,%s)", args.Key, args.EndKey)
	} else {
		// Requests that don't write to export storage are expected to be small.
		log.Eventf(ctx, "export [%s,%s)", args.Key, args.EndKey)
	}

	if makeExternalStorage {
		if _, ok := roachpb.TenantFromContext(ctx); ok {
			if args.Storage.Provider == roachpb.ExternalStorageProvider_userfile {
				return result.Result{}, errors.Errorf("requests to userfile on behalf of tenants must be made by the tenant's SQL process")
			}
		}
	}

	// To get the store to export to, first try to match the locality of this node
	// to the locality KVs in args.StorageByLocalityKV (used for partitioned
	// backups). If that map isn't set or there's no match, fall back to
	// args.Storage.
	var localityKV string
	var exportStore cloud.ExternalStorage
	if makeExternalStorage {
		var storeConf roachpb.ExternalStorage
		var err error
		foundStoreByLocality := false
		if args.StorageByLocalityKV != nil && len(args.StorageByLocalityKV) > 0 {
			locality := cArgs.EvalCtx.GetNodeLocality()
			localityKV, storeConf, foundStoreByLocality = getMatchingStore(&locality, args.StorageByLocalityKV)
		}
		if !foundStoreByLocality {
			storeConf = args.Storage
		}
		exportStore, err = cArgs.EvalCtx.GetExternalStorage(ctx, storeConf)
		if err != nil {
			return result.Result{}, err
		}
		defer exportStore.Close()
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
	var curSizeOfExportedSSTs int64
	for start := args.Key; start != nil; {
		destFile := &storage.MemFile{}
		summary, resume, err := reader.ExportMVCCToSst(start, args.EndKey, args.StartTime,
			h.Timestamp, exportAllRevisions, targetSize, maxSize, useTBI, destFile)
		if err != nil {
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
			Span:       span,
			Exported:   summary,
			LocalityKV: localityKV,
		}

		returnSST := args.ReturnSST
		if args.ReturnSstBelowSize > 0 && len(data) < int(args.ReturnSstBelowSize) {
			returnSST = true
		}

		if returnSST {
			exported.SST = data
		} else {
			if args.Encryption != nil {
				data, err = EncryptFile(data, args.Encryption.Key)
				if err != nil {
					return result.Result{}, err
				}
			}

			exported.Path = GenerateUniqueSSTName(base.SQLInstanceID(cArgs.EvalCtx.NodeID()))
			var attemptNum int
			if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxUploadRetries, func() error {
				attemptNum++
				retryTracingEvent := roachpb.RetryTracingEvent{
					Operation:     fmt.Sprintf("%s.ExportRequest.WriteFile", exportStore.Conf().Provider.String()),
					AttemptNumber: int32(attemptNum),
				}
				// We blindly retry any error here because we expect the caller to have
				// verified the target is writable before sending ExportRequests for it.
				if err := cloud.WriteFile(ctx, exportStore, exported.Path, bytes.NewReader(data)); err != nil {
					log.VEventf(ctx, 1, "failed to put file: %+v", err)
					retryTracingEvent.RetryError = fmt.Sprintf("failed to put file: %s", tracing.RedactAndTruncateError(err))
					evalExportSpan.RecordStructured(&retryTracingEvent)
					return err
				}
				evalExportSpan.RecordStructured(&retryTracingEvent)
				return nil
			}); err != nil {
				return result.Result{}, err
			}
		}
		reply.Files = append(reply.Files, exported)
		start = resume

		// If we are not returning the SSTs to the processor, there is no need to
		// paginate the ExportRequest since the reply size will not grow large
		// enough to cause an OOM.
		if args.ReturnSST && h.TargetBytes > 0 {
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

func getMatchingStore(
	locality *roachpb.Locality, storageByLocalityKV map[string]*roachpb.ExternalStorage,
) (string, roachpb.ExternalStorage, bool) {
	kvs := locality.Tiers
	// When matching, more specific KVs in the node locality take precedence
	// over less specific ones.
	for i := len(kvs) - 1; i >= 0; i-- {
		if store, ok := storageByLocalityKV[kvs[i].String()]; ok {
			return kvs[i].String(), *store, true
		}
	}
	return "", roachpb.ExternalStorage{}, false
}

// GenerateUniqueSSTName generates a name for a backup SST that will not collide
// with another name generated by this node or another node.
func GenerateUniqueSSTName(nodeID base.SQLInstanceID) string {
	// The data/ prefix, including a /, is intended to group SSTs in most of the
	// common file/bucket browse UIs.
	// TODO(dt): don't reach out into a SQL builtin here; this code lives in KV.
	// Create a unique int differently.
	return fmt.Sprintf("data/%d.sst", builtins.GenerateUniqueInt(nodeID))
}
