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
	"crypto/sha512"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func init() {
	batcheval.RegisterCommand(roachpb.Export, declareKeysExport, evalExport)
}

func declareKeysExport(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	batcheval.DefaultDeclareKeys(desc, header, req, spans)
	spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
}

// evalExport dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalExport(
	ctx context.Context, batch engine.ReadWriter, cArgs batcheval.CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ExportRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ExportResponse)

	ctx, span := tracing.ChildSpan(ctx, fmt.Sprintf("Export [%s,%s)", args.Key, args.EndKey))
	defer tracing.FinishSpan(span)

	// If the startTime is zero, then we're doing a full backup and the gc
	// threshold is irrelevant for MVCC_Lastest backups. Otherwise, make sure
	// startTime is after the gc threshold. If it's not, the mvcc tombstones could
	// have been deleted and the resulting RocksDB tombstones compacted, which
	// means we'd miss deletions in the incremental backup. For MVCC_All backups
	// with no start time, they'll only be capturing the *revisions* since the
	// gc threshold, so noting that in the reply allows the BACKUP to correctly
	// note the supported time bounds for RESTORE AS OF SYSTEM TIME.
	gcThreshold := cArgs.EvalCtx.GetGCThreshold()
	if !args.StartTime.IsEmpty() {
		if args.StartTime.LessEq(gcThreshold) {
			return result.Result{}, errors.Errorf("start timestamp %v must be after replica GC threshold %v", args.StartTime, gcThreshold)
		}
	} else if args.MVCCFilter == roachpb.MVCCFilter_All {
		reply.StartTime = gcThreshold
	}

	if err := cArgs.EvalCtx.GetLimiters().ConcurrentExportRequests.Begin(ctx); err != nil {
		return result.Result{}, err
	}
	defer cArgs.EvalCtx.GetLimiters().ConcurrentExportRequests.Finish()

	makeExternalStorage := !args.ReturnSST || args.Storage != roachpb.ExternalStorage{} ||
		(args.StorageByLocalityKV != nil && len(args.StorageByLocalityKV) > 0)
	if makeExternalStorage || log.V(1) {
		log.Infof(ctx, "export [%s,%s)", args.Key, args.EndKey)
	} else {
		// Requests that don't write to export storage are expected to be small.
		log.Eventf(ctx, "export [%s,%s)", args.Key, args.EndKey)
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

	io := engine.IterOptions{
		UpperBound: args.EndKey,
	}

	// Time-bound iterators only make sense to use if the start time is set.
	if args.EnableTimeBoundIteratorOptimization && !args.StartTime.IsEmpty() {
		// The call to startTime.Next() converts our exclusive start bound into the
		// inclusive start bound that MinTimestampHint expects. This is strictly a
		// performance optimization; omitting the call would still return correct
		// results.
		io.MinTimestampHint = args.StartTime.Next()
		io.MaxTimestampHint = h.Timestamp
	}

	e := spanset.GetDBEngine(batch, roachpb.Span{Key: args.Key, EndKey: args.EndKey})

	data, summary, err := e.ExportToSst(args.Key, args.EndKey, args.StartTime, h.Timestamp, exportAllRevisions, io)

	if err != nil {
		return result.Result{}, err
	}

	if summary.DataSize == 0 {
		reply.Files = []roachpb.ExportResponse_File{}
		return result.Result{}, nil
	}

	var checksum []byte
	if !args.OmitChecksum {
		// Compute the checksum before we upload and remove the local file.
		checksum, err = SHA512ChecksumData(data)
		if err != nil {
			return result.Result{}, err
		}
	}

	exported := roachpb.ExportResponse_File{
		Span:       args.Span(),
		Exported:   summary,
		Sha512:     checksum,
		LocalityKV: localityKV,
	}

	if exportStore != nil {
		exported.Path = fmt.Sprintf("%d.sst", builtins.GenerateUniqueInt(cArgs.EvalCtx.NodeID()))
		if err := exportStore.WriteFile(ctx, exported.Path, bytes.NewReader(data)); err != nil {
			return result.Result{}, err
		}
	}

	if args.ReturnSST {
		exported.SST = data
	}

	reply.Files = []roachpb.ExportResponse_File{exported}
	return result.Result{}, nil
}

// SHA512ChecksumData returns the SHA512 checksum of data.
func SHA512ChecksumData(data []byte) ([]byte, error) {
	h := sha512.New()
	if _, err := h.Write(data); err != nil {
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return h.Sum(nil), nil
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
