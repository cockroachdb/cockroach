// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/LICENSE

package storageccl

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// importRequestLimit is the number of Import requests that can run at once.
// Each downloads a file from cloud storage to a temp file, iterates it, and
// sends WriteBatch requests to batch insert it. After accounting for write
// amplification, a single ImportRequest and the resulting WriteBatch
// requests is enough to keep an SSD busy. Any more and we risk contending
// RocksDB, which slows down heartbeats, which can cause mass lease
// transfers.
const importRequestLimit = 1

var importRequestLimiter = makeConcurrentRequestLimiter(importRequestLimit)

func init() {
	storage.SetImportCmd(evalImport)
}

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs storage.CommandArgs) (*roachpb.ImportResponse, error) {
	args := cArgs.Args.(*roachpb.ImportRequest)
	db := cArgs.EvalCtx.DB()
	kr, err := MakeKeyRewriter(args.Rekeys)
	if err != nil {
		return nil, errors.Wrap(err, "make key rewriter")
	}

	var importStart, importEnd roachpb.Key
	{
		var ok bool
		importStart, ok, _ = kr.RewriteKey(append([]byte(nil), args.DataSpan.Key...))
		if !ok {
			return nil, errors.Errorf("could not rewrite span start key: %s", importStart)
		}
		importEnd, ok, _ = kr.RewriteKey(append([]byte(nil), args.DataSpan.EndKey...))
		if !ok {
			return nil, errors.Errorf("could not rewrite span end key: %s", importEnd)
		}
	}

	ctx, span := tracing.ChildSpan(ctx, fmt.Sprintf("Import [%s,%s)", importStart, importEnd))
	defer tracing.FinishSpan(span)

	if err := importRequestLimiter.beginLimitedRequest(ctx); err != nil {
		return nil, err
	}
	defer importRequestLimiter.endLimitedRequest()
	log.Infof(ctx, "import [%s,%s)", importStart, importEnd)

	// Arrived at by tuning and watching the effect on BenchmarkRestore.
	const batchSizeBytes = 1000000

	type batchBuilder struct {
		batch         engine.RocksDBBatchBuilder
		batchStartKey []byte
		batchEndKey   []byte
	}
	b := batchBuilder{}
	g, gCtx := errgroup.WithContext(ctx)
	sendWriteBatch := func() {
		start := roachpb.Key(b.batchStartKey)
		// The end key of the WriteBatch request is exclusive, but batchEndKey is
		// currently the largest key in the batch. Increment it.
		end := roachpb.Key(b.batchEndKey).Next()

		repr := b.batch.Finish()
		g.Go(func() error {
			if log.V(1) {
				log.Infof(gCtx, "writebatch [%s,%s)", start, end)
			}

			const maxWriteBatchRetries = 10
			for i := 0; ; i++ {
				err := db.WriteBatch(gCtx, start, end, repr)
				if err == nil {
					return nil
				}
				if _, ok := err.(*roachpb.AmbiguousResultError); i == maxWriteBatchRetries || !ok {
					return errors.Wrapf(err, "writebatch [%s,%s)", start, end)
				}
				log.Warningf(ctx, "writebatch [%s,%s) attempt %d failed: %+v",
					start, end, i, err)
				continue
			}
		})
		b = batchBuilder{}
	}

	var dataSize int64
	var iters []engine.Iterator
	for _, file := range args.Files {
		if log.V(2) {
			log.Infof(ctx, "import file [%s,%s) %s", importStart, importEnd, file.Path)
		}

		dir, err := MakeExportStorage(ctx, file.Dir)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := dir.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed %v", err)
			}
		}()
		tempPrefix := cArgs.EvalCtx.GetTempPrefix()
		localPath, cleanup, err := FetchFile(ctx, tempPrefix, dir, file.Path)
		if err != nil {
			return nil, err
		}
		defer cleanup()

		if len(file.Sha512) > 0 {
			checksum, err := sha512ChecksumFile(localPath)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(checksum, file.Sha512) {
				return nil, errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}

		fileInfo, err := os.Lstat(localPath)
		if err != nil {
			return nil, err
		}
		dataSize += fileInfo.Size()

		readerTempDir, err := ioutil.TempDir(tempPrefix, "import-sstreader")
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := os.RemoveAll(readerTempDir); err != nil {
				log.Warning(ctx, err)
			}
		}()

		sst, err := engine.MakeRocksDBSstFileReader(readerTempDir)
		if err != nil {
			return nil, err
		}
		defer sst.Close()

		// Add each file in its own sst reader because AddFile requires the
		// affected keyrange be empty and the keys in these files might overlap.
		// This becomes less heavyweight when we figure out how to use RocksDB's
		// TableReader directly.
		if err := sst.AddFile(localPath); err != nil {
			return nil, err
		}

		iter := sst.NewIterator(false)
		defer iter.Close()
		iters = append(iters, iter)
	}

	startKeyMVCC, endKeyMVCC := engine.MVCCKey{Key: args.DataSpan.Key}, engine.MVCCKey{Key: args.DataSpan.EndKey}
	iter := engineccl.MakeMultiIterator(iters)
	var keyScratch, valueScratch []byte
	for iter.Seek(startKeyMVCC); iter.Valid() && iter.UnsafeKey().Less(endKeyMVCC); iter.NextKey() {
		if len(iter.UnsafeValue()) == 0 {
			// Value is deleted.
			continue
		}

		keyScratch = append(keyScratch[:0], iter.UnsafeKey().Key...)
		valueScratch = append(valueScratch[:0], iter.UnsafeValue()...)
		key := engine.MVCCKey{Key: keyScratch, Timestamp: iter.UnsafeKey().Timestamp}
		value := roachpb.Value{RawBytes: valueScratch}

		var ok bool
		var err error
		key.Key, ok, err = kr.RewriteKey(key.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			// If the key rewriter didn't match this key, it's not data for the
			// table(s) we're interested in.
			if log.V(3) {
				log.Infof(ctx, "skipping %s %s", key.Key, value.PrettyPrint())
			}
			continue
		}

		// Rewriting the key means the checksum needs to be updated.
		value.ClearChecksum()
		value.InitChecksum(key.Key)

		if log.V(3) {
			log.Infof(ctx, "Put %s -> %s", key.Key, value.PrettyPrint())
		}
		b.batch.Put(key, value.RawBytes)

		// Update the range currently represented in this batch, as
		// necessary.
		if len(b.batchStartKey) == 0 || bytes.Compare(key.Key, b.batchStartKey) < 0 {
			b.batchStartKey = append(b.batchStartKey[:0], key.Key...)
		}
		if len(b.batchEndKey) == 0 || bytes.Compare(key.Key, b.batchEndKey) > 0 {
			b.batchEndKey = append(b.batchEndKey[:0], key.Key...)
		}

		if b.batch.Len() > batchSizeBytes {
			sendWriteBatch()
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	// Flush out the last batch.
	if b.batch.Len() > 0 {
		sendWriteBatch()
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &roachpb.ImportResponse{DataSize: dataSize}, nil
}
