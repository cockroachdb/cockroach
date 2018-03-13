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

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func init() {
	batcheval.RegisterCommand(roachpb.Export, declareKeysExport, evalExport)
}

func declareKeysExport(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	batcheval.DefaultDeclareKeys(desc, header, req, spans)
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
}

type rowCounter struct {
	prev roachpb.Key
	roachpb.BulkOpSummary
}

func (r *rowCounter) count(key roachpb.Key) error {
	// EnsureSafeSplitKey is usually used to avoid splitting a row across ranges,
	// by returning the row's key prefix.
	// We reuse it here to count "rows" by counting when it changes.
	// Non-SQL keys are returned unchanged or may error -- we ignore them, since
	// non-SQL keys are obviously thus not SQL rows.
	row, err := keys.EnsureSafeSplitKey(key)
	if err != nil || len(key) == len(row) {
		return nil
	}

	// no change key prefix => no new row.
	if bytes.Equal(row, r.prev) {
		return nil
	}
	r.prev = append(r.prev[:0], row...)

	rest, tbl, err := keys.DecodeTablePrefix(row)
	if err != nil {
		return err
	}

	if tbl < keys.MaxReservedDescID {
		r.SystemRecords++
	} else {
		if _, indexID, err := encoding.DecodeUvarintAscending(rest); err != nil {
			return err
		} else if indexID == 1 {
			r.Rows++
		} else {
			r.IndexEntries++
		}
	}

	return nil
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
		if !gcThreshold.Less(args.StartTime) {
			return result.Result{}, errors.Errorf("start timestamp %v must be after replica GC threshold %v", args.StartTime, gcThreshold)
		}
	} else if args.MVCCFilter == roachpb.MVCCFilter_All {
		reply.StartTime = gcThreshold
	}

	if err := cArgs.EvalCtx.GetLimiters().ConcurrentExports.Begin(ctx); err != nil {
		return result.Result{}, err
	}
	defer cArgs.EvalCtx.GetLimiters().ConcurrentExports.Finish()

	log.Infof(ctx, "export [%s,%s)", args.Key, args.EndKey)

	var exportStore ExportStorage
	if !args.ReturnSST || (args.Storage != roachpb.ExportStorage{}) {
		var err error
		exportStore, err = MakeExportStorage(ctx, args.Storage, cArgs.EvalCtx.ClusterSettings())
		if err != nil {
			return result.Result{}, err
		}
		defer exportStore.Close()
	}

	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return result.Result{}, err
	}
	defer sst.Close()

	var skipTombstones bool
	var iterFn func(*engineccl.MVCCIncrementalIterator)
	switch args.MVCCFilter {
	case roachpb.MVCCFilter_Latest:
		skipTombstones = true
		iterFn = (*engineccl.MVCCIncrementalIterator).NextKey
	case roachpb.MVCCFilter_All:
		skipTombstones = false
		iterFn = (*engineccl.MVCCIncrementalIterator).Next
	default:
		return result.Result{}, errors.Errorf("unknown MVCC filter: %s", args.MVCCFilter)
	}

	var rows rowCounter
	// TODO(dan): Move all this iteration into cpp to avoid the cgo calls.
	// TODO(dan): Consider checking ctx periodically during the MVCCIterate call.
	iter := engineccl.NewMVCCIncrementalIterator(batch, args.StartTime, h.Timestamp)
	defer iter.Close()
	for iter.Seek(engine.MakeMVCCMetadataKey(args.Key)); ; iterFn(iter) {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return result.Result{}, err
		}
		if !ok || iter.UnsafeKey().Key.Compare(args.EndKey) >= 0 {
			break
		}

		// Skip tombstone (len=0) records when startTime is zero
		// (non-incremental) and we're not exporting all versions.
		if skipTombstones && args.StartTime.IsEmpty() && len(iter.UnsafeValue()) == 0 {
			iter.NextKey()
			if ok, err := iter.Valid(); err != nil {
				return result.Result{}, err
			} else if !ok {
				break
			}
			continue
		}

		if log.V(3) {
			v := roachpb.Value{RawBytes: iter.UnsafeValue()}
			log.Infof(ctx, "Export %s %s", iter.UnsafeKey(), v.PrettyPrint())
		}

		if err := rows.count(iter.UnsafeKey().Key); err != nil {
			return result.Result{}, errors.Wrapf(err, "decoding %s", iter.UnsafeKey())
		}
		if err := sst.Add(engine.MVCCKeyValue{Key: iter.UnsafeKey(), Value: iter.UnsafeValue()}); err != nil {
			return result.Result{}, errors.Wrapf(err, "adding key %s", iter.UnsafeKey())
		}
	}

	if sst.DataSize == 0 {
		// Let the defer Close the sstable.
		reply.Files = []roachpb.ExportResponse_File{}
		return result.Result{}, nil
	}
	rows.BulkOpSummary.DataSize = sst.DataSize

	sstContents, err := sst.Finish()
	if err != nil {
		return result.Result{}, err
	}

	// Compute the checksum before we upload and remove the local file.
	checksum, err := SHA512ChecksumData(sstContents)
	if err != nil {
		return result.Result{}, err
	}

	exported := roachpb.ExportResponse_File{
		Span:     args.Span,
		Exported: rows.BulkOpSummary,
		Sha512:   checksum,
	}

	if exportStore != nil {
		exported.Path = fmt.Sprintf("%d.sst", builtins.GenerateUniqueInt(cArgs.EvalCtx.NodeID()))
		if err := exportStore.WriteFile(ctx, exported.Path, bytes.NewReader(sstContents)); err != nil {
			return result.Result{}, err
		}
	}

	if args.ReturnSST {
		exported.SST = sstContents
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
