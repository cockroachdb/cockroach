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
	"crypto/sha512"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// ExportRequestLimit is the number of Export requests that can run at once.
// Each extracts data from RocksDB to a temp file and then uploads it to cloud
// storage. In order to not exhaust the disk or memory, or saturate the network,
// limit the number of these that can be run in parallel. This number was chosen
// by a guess. If SST files are likely to not be over 200MB, then 5 parallel
// workers hopefully won't use more than 1GB of space in the temp directory. It
// could be improved by more measured heuristics.
const ExportRequestLimit = 5

var exportRequestLimiter = makeConcurrentRequestLimiter(ExportRequestLimit)

var exportCpp = settings.RegisterBoolSetting(
	"experimental.export.cpp",
	"use pure c++ mvcc export implementation to export key ranges in BACKUP",
	false,
)

func init() {
	storage.SetExportCmd(storage.Command{
		DeclareKeys: declareKeysExport,
		Eval:        evalExport,
	})
}

func declareKeysExport(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	batcheval.DefaultDeclareKeys(desc, header, req, spans)
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
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
	// threshold is irrelevant. Otherwise, make sure startTime is after the gc
	// threshold. If it's not, the mvcc tombstones could have been deleted and
	// the resulting RocksDB tombstones compacted, which means we'd miss
	// deletions in the incremental backup.
	gcThreshold := cArgs.EvalCtx.GetGCThreshold()
	if args.StartTime != (hlc.Timestamp{}) {
		if !gcThreshold.Less(args.StartTime) {
			return result.Result{}, errors.Errorf("start timestamp %v must be after replica GC threshold %v", args.StartTime, gcThreshold)
		}
	}

	if err := exportRequestLimiter.beginLimitedRequest(ctx); err != nil {
		return result.Result{}, err
	}
	defer exportRequestLimiter.endLimitedRequest()
	log.Infof(ctx, "export [%s,%s)", args.Key, args.EndKey)

	exportStore, err := MakeExportStorage(ctx, args.Storage, cArgs.EvalCtx.ClusterSettings())
	if err != nil {
		return result.Result{}, err
	}
	defer exportStore.Close()

	var allRevisions bool
	switch args.MVCCFilter {
	case roachpb.MVCCFilter_Latest:
		allRevisions = false
	case roachpb.MVCCFilter_All:
		allRevisions = true
	default:
		return result.Result{}, errors.Errorf("unknown MVCC filter: %s", args.MVCCFilter)
	}

	var summary roachpb.BulkOpSummary
	var sstContents []byte
	if exportCpp.Get(&cArgs.EvalCtx.ClusterSettings().SV) {
		// TODO(dan): Consider checking ctx periodically during the MVCCIterate call.
		sstContents, summary, err = engineccl.ExportSST(
			ctx, batch, args.Key, args.EndKey, args.StartTime, h.Timestamp, allRevisions,
		)
		if err != nil {
			return result.Result{}, err
		}
	} else {
		sstContents, summary, err = engineccl.MVCCIncIterExport(
			ctx, batch, args.Key, args.EndKey, args.StartTime, h.Timestamp, allRevisions,
		)
		if err != nil {
			return result.Result{}, err
		}
	}

	if summary.DataSize == 0 {
		// Let the defer Close the sstable.
		reply.Files = []roachpb.ExportResponse_File{}
		return result.Result{}, nil
	}

	// Compute the checksum before we upload and remove the local file.
	checksum, err := SHA512ChecksumData(sstContents)
	if err != nil {
		return result.Result{}, err
	}

	filename := fmt.Sprintf("%d.sst", builtins.GenerateUniqueInt(cArgs.EvalCtx.NodeID()))
	if err := exportStore.WriteFile(ctx, filename, bytes.NewReader(sstContents)); err != nil {
		return result.Result{}, err
	}

	reply.Files = []roachpb.ExportResponse_File{{
		Span:     args.Span,
		Path:     filename,
		Exported: summary,
		Sha512:   checksum,
	}}

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
