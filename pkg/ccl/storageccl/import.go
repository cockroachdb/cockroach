// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
	"bytes"
	"io/ioutil"
	"runtime"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// importRequestLimit is the number of Import requests that can run at once.
// Each downloads a file from cloud storage to a temp file, iterates it, and
// sends AddSSTable requests to batch insert it. Import and the resulting
// AddSSTable calls are mostly cpu-bound at this point so allow NumCPU of them
// to be running concurrently, which will hopefully hit the sweet spot between
// maximizing throughput and minimizing thrashing.
var importRequestLimit = runtime.NumCPU()

var importRequestLimiter = makeConcurrentRequestLimiter(importRequestLimit)

func init() {
	storage.SetImportCmd(evalImport)
}

type importBatcher interface {
	Add(engine.MVCCKey, []byte) error
	Size() int64
	Finish(context.Context, *client.DB) error
	Close()
}

type writeBatcher struct {
	batch         engine.RocksDBBatchBuilder
	batchStartKey []byte
	batchEndKey   []byte
}

var _ importBatcher = &writeBatcher{}

func (b *writeBatcher) Add(key engine.MVCCKey, value []byte) error {
	// Update the range currently represented in this batch, as
	// necessary.
	if len(b.batchStartKey) == 0 || bytes.Compare(key.Key, b.batchStartKey) < 0 {
		b.batchStartKey = append(b.batchStartKey[:0], key.Key...)
	}
	if len(b.batchEndKey) == 0 || bytes.Compare(key.Key, b.batchEndKey) > 0 {
		b.batchEndKey = append(b.batchEndKey[:0], key.Key...)
	}

	b.batch.Put(key, value)
	return nil
}

func (b *writeBatcher) Size() int64 {
	return int64(b.batch.Len())
}

func (b *writeBatcher) Finish(ctx context.Context, db *client.DB) error {
	start := roachpb.Key(b.batchStartKey)
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(b.batchEndKey).Next()

	repr := b.batch.Finish()
	if log.V(1) {
		log.Infof(ctx, "writebatch [%s,%s)", start, end)
	}

	const maxWriteBatchRetries = 10
	for i := 0; ; i++ {
		err := db.WriteBatch(ctx, start, end, repr)
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
}

func (b *writeBatcher) Close() {}

type sstBatcher struct {
	sstWriter     engine.RocksDBSstFileWriter
	batchStartKey []byte
	batchEndKey   []byte
}

var _ importBatcher = &sstBatcher{}

func (b *sstBatcher) Add(key engine.MVCCKey, value []byte) error {
	// Update the range currently represented in this batch, as
	// necessary.
	if len(b.batchStartKey) == 0 || bytes.Compare(key.Key, b.batchStartKey) < 0 {
		b.batchStartKey = append(b.batchStartKey[:0], key.Key...)
	}
	if len(b.batchEndKey) == 0 || bytes.Compare(key.Key, b.batchEndKey) > 0 {
		b.batchEndKey = append(b.batchEndKey[:0], key.Key...)
	}

	return b.sstWriter.Add(engine.MVCCKeyValue{Key: key, Value: value})
}

func (b *sstBatcher) Size() int64 {
	return b.sstWriter.DataSize
}

func (b *sstBatcher) Finish(ctx context.Context, db *client.DB) error {
	start := roachpb.Key(b.batchStartKey)
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(b.batchEndKey).Next()

	sstBytes, err := b.sstWriter.Finish()
	if err != nil {
		return errors.Wrapf(err, "finishing constructed sstable")
	}

	const maxAddSSTableRetries = 10
	for i := 0; ; i++ {
		log.VEventf(ctx, 2, "sending AddSSTable [%s,%s)", start, end)
		// TODO(dan): This will fail if the range has split.
		err := db.ExperimentalAddSSTable(ctx, start, end, sstBytes)
		if err == nil {
			return nil
		}
		if _, ok := err.(*roachpb.AmbiguousResultError); i == maxAddSSTableRetries || !ok {
			return errors.Wrapf(err, "addsstable [%s,%s)", start, end)
		}
		log.Warningf(ctx, "addsstable [%s,%s) attempt %d failed: %+v",
			start, end, i, err)
		continue
	}
}

func (b *sstBatcher) Close() {
	b.sstWriter.Close()
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

	if err := importRequestLimiter.beginLimitedRequest(ctx); err != nil {
		return nil, err
	}
	defer importRequestLimiter.endLimitedRequest()

	var rows rowCounter
	var iters []engine.SimpleIterator
	for _, file := range args.Files {
		log.VEventf(ctx, 2, "import file %s %s", file.Path, args.Span.Key)

		dir, err := MakeExportStorage(ctx, file.Dir)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := dir.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed %v", err)
			}
		}()

		const maxAttempts = 3
		var fileContents []byte
		if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			f, err := dir.ReadFile(ctx, file.Path)
			if err != nil {
				return err
			}
			defer f.Close()
			fileContents, err = ioutil.ReadAll(f)
			return err
		}); err != nil {
			return nil, errors.Wrapf(err, "fetching %q", file.Path)
		}
		dataSize := int64(len(fileContents))
		log.Eventf(ctx, "fetched file (%s)", humanizeutil.IBytes(dataSize))

		rows.BulkOpSummary.DataSize += dataSize

		if len(file.Sha512) > 0 {
			checksum, err := SHA512ChecksumData(fileContents)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(checksum, file.Sha512) {
				return nil, errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}

		iter, err := engineccl.NewMemSSTIterator(fileContents)
		if err != nil {
			return nil, err
		}

		defer iter.Close()
		iters = append(iters, iter)
	}

	var batcher importBatcher
	makeBatcher := func() error {
		if batcher != nil {
			return errors.New("cannot overwrite a batcher")
		}
		if cArgs.EvalCtx.ClusterSettings().AddSSTableEnabled.Get() {
			sstWriter, err := engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return errors.Wrapf(err, "making sstBatcher")
			}
			batcher = &sstBatcher{sstWriter: sstWriter}
			return nil
		}
		batcher = &writeBatcher{}
		return nil
	}
	if err := makeBatcher(); err != nil {
		return nil, err
	}
	defer batcher.Close()

	g, gCtx := errgroup.WithContext(ctx)
	startKeyMVCC, endKeyMVCC := engine.MVCCKey{Key: args.DataSpan.Key}, engine.MVCCKey{Key: args.DataSpan.EndKey}
	iter := engineccl.MakeMultiIterator(iters)
	defer iter.Close()
	var keyScratch, valueScratch []byte
	for iter.Seek(startKeyMVCC); ; iter.NextKey() {
		ok, err := iter.Valid()
		if err != nil {
			return nil, err
		}
		if !ok || !iter.UnsafeKey().Less(endKeyMVCC) {
			break
		}
		if len(iter.UnsafeValue()) == 0 {
			// Value is deleted.
			continue
		}

		keyScratch = append(keyScratch[:0], iter.UnsafeKey().Key...)
		valueScratch = append(valueScratch[:0], iter.UnsafeValue()...)
		key := engine.MVCCKey{Key: keyScratch, Timestamp: iter.UnsafeKey().Timestamp}
		value := roachpb.Value{RawBytes: valueScratch}

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

		if err := rows.count(key.Key); err != nil {
			return nil, errors.Wrapf(err, "decoding %s", key.Key)
		}

		if err := batcher.Add(key, value.RawBytes); err != nil {
			return nil, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}

		if size := batcher.Size(); size > cArgs.EvalCtx.ClusterSettings().ImportBatchSize.Get() {
			finishBatcher := batcher
			batcher = nil
			log.Eventf(gCtx, "triggering finish of batch of size %s", humanizeutil.IBytes(size))
			g.Go(func() error {
				defer log.Event(ctx, "finished batch")
				defer finishBatcher.Close()
				return finishBatcher.Finish(gCtx, db)
			})
			if err := makeBatcher(); err != nil {
				return nil, err
			}
		}
	}
	// Flush out the last batch.
	if batcher.Size() > 0 {
		g.Go(func() error {
			defer log.Event(ctx, "finished batch")
			defer batcher.Close()
			return batcher.Finish(gCtx, db)
		})
	}
	log.Event(ctx, "waiting for batchers to finish")

	if err := g.Wait(); err != nil {
		return nil, err
	}
	log.Event(ctx, "done")

	return &roachpb.ImportResponse{Imported: rows.BulkOpSummary}, nil
}
