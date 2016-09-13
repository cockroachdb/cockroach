// Copyright 2016 The Cockroach Authors.
//
// TODO(dan): BEFORE MERGE Replace this with Cockroach Common Licence.

// +build ccl

package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

const dataSSTableName = "data.sst"

// ExportKeys dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func (r *Replica) ExportKeys(
	ctx context.Context,
	batch engine.ReadWriter,
	h roachpb.Header,
	args roachpb.ExportKeysRequest,
) (res roachpb.ExportKeysResponse, trigger *PostCommitTrigger, retErr error) {
	if args.StartTime != hlc.ZeroTimestamp {
		// TODO(dan): Support non-zero start timestamps aka incremental backups.
		return roachpb.ExportKeysResponse{}, nil, errors.Errorf("unsupported start time: %s", args.StartTime)
	}

	path := filepath.Join(
		args.Basepath, fmt.Sprintf("%03d", r.RangeID), fmt.Sprintf("%x-%x", []byte(args.Key), []byte(args.EndKey)),
		dataSSTableName)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return roachpb.ExportKeysResponse{}, nil, err
	}

	sst := engine.MakeRocksDBSstFileWriter()
	if err := sst.Open(path); err != nil {
		return roachpb.ExportKeysResponse{}, nil, err
	}
	defer func() {
		if retErr != nil {
			if delErr := os.Remove(path); delErr != nil {
				log.Warningf(ctx, "Could not clean up %s", path)
			}
		}
	}()

	// TODO(dan): Move all this iteration into cpp to avoid the cgo calls.
	var entries int64
	intents, err := engine.MVCCIterate(ctx, batch, args.Key, args.EndKey, h.Timestamp, true, h.Txn, false,
		func(kv roachpb.KeyValue) (bool, error) {
			entries++
			mvccKV := engine.MVCCKeyValue{
				Key:   engine.MVCCKey{Key: kv.Key, Timestamp: kv.Value.Timestamp},
				Value: kv.Value.RawBytes,
			}
			return false, sst.Add(mvccKV)
		})
	if err != nil {
		return roachpb.ExportKeysResponse{}, nil, err
	}
	if entries > 0 {
		// SSTables require at least one entry. It's silly to save an empty one,
		// anyway.
		if err := sst.Close(); err != nil {
			return roachpb.ExportKeysResponse{}, nil, err
		}
		res = roachpb.ExportKeysResponse{
			Batches: []roachpb.ExportKeysBatch{{Path: path, DataSize: sst.DataSize}},
		}
	}
	return res, intentsToTrigger(intents, &args), nil
}
