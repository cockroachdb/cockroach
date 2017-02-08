// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package storageccl

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
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
		DeclareKeys: storage.DefaultDeclareKeys,
		Eval:        evalExport,
	})
}

// evalExport dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalExport(
	ctx context.Context, batch engine.ReadWriter, cArgs storage.CommandArgs, resp roachpb.Response,
) (pd storage.EvalResult, retErr error) {
	args := cArgs.Args.(*roachpb.ExportRequest)
	h := cArgs.Header
	r := cArgs.Repl
	reply := resp.(*roachpb.ExportResponse)

	ctx, span := tracing.ChildSpan(ctx, fmt.Sprintf("Export %s-%s", args.Key, args.EndKey))
	defer tracing.FinishSpan(span)

	// If the startTime is zero, then we're doing a full backup and the gc
	// threshold is irrelevant. Otherwise, make sure startTime is after the gc
	// threshold. If it's not, the mvcc tombstones could have been deleted and
	// the resulting RocksDB tombstones compacted, which means we'd miss
	// deletions in the incremental backup.
	gcThreshold := r.GCThreshold()
	if !args.StartTime.Equal(hlc.ZeroTimestamp) {
		if !gcThreshold.Less(args.StartTime) {
			return storage.EvalResult{}, errors.Errorf("start timestamp %v must be after replica GC threshold %v", args.StartTime, gcThreshold)
		}
	}

	exportStore, err := MakeExportStorage(ctx, args.Storage)
	if err != nil {
		return storage.EvalResult{}, err
	}
	defer exportStore.Close()

	filename := fmt.Sprintf("%d.sst", parser.GenerateUniqueInt(r.NodeID()))
	writer, err := exportStore.PutFile(ctx, filename)
	if err != nil {
		return storage.EvalResult{}, err
	}
	path := writer.LocalFile()
	defer writer.Cleanup()

	sstWriter := engine.MakeRocksDBSstFileWriter()
	sst := &sstWriter
	if err := sst.Open(path); err != nil {
		return storage.EvalResult{}, err
	}
	defer func() {
		if sst != nil {
			if closeErr := sst.Close(); closeErr != nil {
				log.Warningf(ctx, "could not close sst writer %s: %+v", path, closeErr)
			}
		}
	}()

	// TODO(dan): Move all this iteration into cpp to avoid the cgo calls.
	// TODO(dan): Consider checking ctx periodically during the MVCCIterate call.
	var entries int64
	for {
		entries = 0
		iter := engineccl.NewMVCCIncrementalIterator(batch)
		defer iter.Close()
		iter.Reset(args.Key, args.EndKey, args.StartTime, h.Timestamp)
		for ; iter.Valid(); iter.Next() {
			key, value := iter.Key(), iter.Value()
			if log.V(3) {
				log.Infof(ctx, "Export %+v %+v", key, value)
			}
			entries++
			if err := sst.Add(engine.MVCCKeyValue{Key: key, Value: value}); err != nil {
				return storage.EvalResult{}, errors.Wrapf(err, "adding key %s", key)
			}
		}
		err := iter.Error()
		if _, ok := err.(*roachpb.WriteIntentError); ok {
			continue
		}
		if err != nil {
			return storage.EvalResult{}, err
		}
		break
	}

	if entries == 0 {
		// SSTables require at least one entry. It's silly to save an empty one,
		// anyway.
		sst = nil
		reply.Files = []roachpb.ExportResponse_File{}
		return storage.EvalResult{}, nil
	}

	if err := sst.Close(); err != nil {
		return storage.EvalResult{}, err
	}
	size := sst.DataSize
	sst = nil

	// TODO(dan): Compute a checksum of the file before upload.
	// https://github.com/cockroachdb/cockroach/issues/13482.

	if err := writer.Finish(); err != nil {
		return storage.EvalResult{}, err
	}

	reply.Files = []roachpb.ExportResponse_File{{
		Span:     args.Span,
		Path:     filename,
		DataSize: size,
	}}

	return storage.EvalResult{}, nil
}
