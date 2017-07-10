// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
	"bytes"
	"crypto/sha512"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

func init() {
	storage.SetExportCmd(storage.Command{
		DeclareKeys: declareKeysExport,
		Eval:        evalExport,
	})
}

func declareKeysExport(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *storage.SpanSet,
) {
	storage.DefaultDeclareKeys(desc, header, req, spans)
	spans.Add(storage.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
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
	ctx context.Context, batch engine.ReadWriter, cArgs storage.CommandArgs, resp roachpb.Response,
) (storage.EvalResult, error) {
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
	gcThreshold, err := cArgs.EvalCtx.GCThreshold()
	if err != nil {
		return storage.EvalResult{}, err
	}
	if args.StartTime != (hlc.Timestamp{}) {
		if !gcThreshold.Less(args.StartTime) {
			return storage.EvalResult{}, errors.Errorf("start timestamp %v must be after replica GC threshold %v", args.StartTime, gcThreshold)
		}
	}

	if err := exportRequestLimiter.beginLimitedRequest(ctx); err != nil {
		return storage.EvalResult{}, err
	}
	defer exportRequestLimiter.endLimitedRequest()
	log.Infof(ctx, "export [%s,%s)", args.Key, args.EndKey)

	exportStore, err := MakeExportStorage(ctx, args.Storage)
	if err != nil {
		return storage.EvalResult{}, err
	}
	defer exportStore.Close()

	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return storage.EvalResult{}, err
	}
	defer sst.Close()

	var rows rowCounter
	// TODO(dan): Move all this iteration into cpp to avoid the cgo calls.
	// TODO(dan): Consider checking ctx periodically during the MVCCIterate call.
	iter := engineccl.NewMVCCIncrementalIterator(batch, args.StartTime, h.Timestamp)
	defer iter.Close()
	for iter.Seek(engine.MakeMVCCMetadataKey(args.Key)); ; iter.NextKey() {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return storage.EvalResult{}, err
		}
		if !ok || iter.UnsafeKey().Key.Compare(args.EndKey) >= 0 {
			break
		}

		if log.V(3) {
			v := roachpb.Value{RawBytes: iter.UnsafeValue()}
			log.Infof(ctx, "Export %s %s", iter.UnsafeKey(), v.PrettyPrint())
		}

		if err := rows.count(iter.UnsafeKey().Key); err != nil {
			return storage.EvalResult{}, errors.Wrapf(err, "decoding %s", iter.UnsafeKey())
		}
		if err := sst.Add(engine.MVCCKeyValue{Key: iter.UnsafeKey(), Value: iter.UnsafeValue()}); err != nil {
			return storage.EvalResult{}, errors.Wrapf(err, "adding key %s", iter.UnsafeKey())
		}
	}

	if sst.DataSize == 0 {
		// Let the defer Close the sstable.
		reply.Files = []roachpb.ExportResponse_File{}
		return storage.EvalResult{}, nil
	}
	rows.BulkOpSummary.DataSize = sst.DataSize

	sstContents, err := sst.Finish()
	if err != nil {
		return storage.EvalResult{}, err
	}

	// Compute the checksum before we upload and remove the local file.
	checksum, err := SHA512ChecksumData(sstContents)
	if err != nil {
		return storage.EvalResult{}, err
	}

	filename := fmt.Sprintf("%d.sst", parser.GenerateUniqueInt(cArgs.EvalCtx.NodeID()))
	if err := exportStore.WriteFile(ctx, filename, bytes.NewReader(sstContents)); err != nil {
		return storage.EvalResult{}, err
	}

	reply.Files = []roachpb.ExportResponse_File{{
		Span:     args.Span,
		Path:     filename,
		Exported: rows.BulkOpSummary,
		Sha512:   checksum,
	}}

	return storage.EvalResult{}, nil
}

// SHA512ChecksumData returns the SHA512 checksum of data.
func SHA512ChecksumData(data []byte) ([]byte, error) {
	h := sha512.New()
	if _, err := h.Write(data); err != nil {
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return h.Sum(nil), nil
}
