// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package storageccl

import (
	"crypto/sha512"
	"fmt"
	"io"
	"os"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

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

// evalExport dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalExport(
	ctx context.Context, batch engine.ReadWriter, cArgs storage.CommandArgs, resp roachpb.Response,
) (pd storage.EvalResult, retErr error) {
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

	if err := beginLimitedRequest(ctx); err != nil {
		return storage.EvalResult{}, err
	}
	defer endLimitedRequest()
	log.Infof(ctx, "export [%s,%s)", args.Key, args.EndKey)

	exportStore, err := MakeExportStorage(ctx, args.Storage)
	if err != nil {
		return storage.EvalResult{}, err
	}
	defer exportStore.Close()

	filename := fmt.Sprintf("%d.sst", parser.GenerateUniqueInt(cArgs.EvalCtx.NodeID()))
	temp, err := MakeExportFileTmpWriter(ctx, cArgs.EvalCtx.GetTempPrefix(), exportStore, filename)
	if err != nil {
		return storage.EvalResult{}, err
	}
	localPath := temp.LocalFile()
	defer temp.Close(ctx)

	sstWriter := engine.MakeRocksDBSstFileWriter()
	sst := &sstWriter
	if err := sst.Open(localPath); err != nil {
		return storage.EvalResult{}, err
	}
	defer func() {
		if sst != nil {
			if closeErr := sst.Close(); closeErr != nil {
				log.Warningf(ctx, "could not close sst writer %s: %+v", localPath, closeErr)
			}
		}
	}()

	// TODO(dan): Move all this iteration into cpp to avoid the cgo calls.
	// TODO(dan): Consider checking ctx periodically during the MVCCIterate call.
	var entries int64
	iter := engineccl.NewMVCCIncrementalIterator(batch)
	defer iter.Close()
	iter.Reset(args.Key, args.EndKey, args.StartTime, h.Timestamp)
	for ; iter.Valid(); iter.Next() {
		if log.V(3) {
			v := roachpb.Value{RawBytes: iter.UnsafeValue()}
			log.Infof(ctx, "Export %s %s", iter.UnsafeKey(), v.PrettyPrint())
		}
		entries++
		if err := sst.Add(engine.MVCCKeyValue{Key: iter.UnsafeKey(), Value: iter.UnsafeValue()}); err != nil {
			return storage.EvalResult{}, errors.Wrapf(err, "adding key %s", iter.UnsafeKey())
		}
	}
	if err := iter.Error(); err != nil {
		// The error may be a WriteIntentError. In which case, returning it will
		// cause this command to be retried.
		return storage.EvalResult{}, err
	}

	if entries == 0 {
		// The defer above `Close`s sst if it's not nil, which in turn finishes
		// the SSTable. However, our SSTable library errors if there is not at
		// least one entry, so set `sst = nil` to avoid the `Close` and the log
		// spam when it fails.
		sst = nil
		reply.Files = []roachpb.ExportResponse_File{}
		return storage.EvalResult{}, nil
	}

	if err := sst.Close(); err != nil {
		return storage.EvalResult{}, err
	}
	size := sst.DataSize
	sst = nil

	// Compute the checksum before we upload and remove the local file.
	checksum, err := sha512ChecksumFile(localPath)
	if err != nil {
		return storage.EvalResult{}, err
	}

	if err := temp.Finish(ctx); err != nil {
		return storage.EvalResult{}, err
	}

	reply.Files = []roachpb.ExportResponse_File{{
		Span:     args.Span,
		Path:     filename,
		DataSize: size,
		Sha512:   checksum,
	}}

	return storage.EvalResult{}, nil
}

func sha512ChecksumFile(path string) ([]byte, error) {
	h := sha512.New()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}
