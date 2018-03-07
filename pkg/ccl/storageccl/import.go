// Copyright 2017 The Cockroach Authors.
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
	"io/ioutil"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

var importBatchSize = settings.RegisterByteSizeSetting(
	"kv.import.batch_size",
	"",
	32<<20,
)

// commandMetadataEstimate is an estimate of how much metadata Raft will add to
// an AddSSTable command. It is intentionally a vast overestimate to avoid
// embedding intricate knowledge of the Raft encoding scheme here.
const commandMetadataEstimate = 1 << 20 // 1 MB

func init() {
	importBatchSize.Hide()
	storage.SetImportCmd(evalImport)

	// Ensure that the user cannot set the maximum raft command size so low that
	// more than half of an Import or AddSSTable command will be taken up by Raft
	// metadata.
	if commandMetadataEstimate > storage.MaxCommandSizeFloor/2 {
		panic(fmt.Sprintf("raft command size floor (%s) is too small for import commands",
			humanizeutil.IBytes(storage.MaxCommandSizeFloor)))
	}
}

// MaxImportBatchSize determines the maximum size of the payload in an
// AddSSTable request. It uses the ImportBatchSize setting directly unless the
// specified value would exceed the maximum Raft command size, in which case it
// returns the maximum batch size that will fit within a Raft command.
func MaxImportBatchSize(st *cluster.Settings) int64 {
	desiredSize := importBatchSize.Get(&st.SV)
	maxCommandSize := storage.MaxCommandSize.Get(&st.SV)
	if desiredSize+commandMetadataEstimate > maxCommandSize {
		return maxCommandSize - commandMetadataEstimate
	}
	return desiredSize
}

type sstBatcher struct {
	sstWriter     engine.RocksDBSstFileWriter
	batchStartKey []byte
	batchEndKey   []byte
}

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

	return AddSSTable(ctx, db, start, end, sstBytes)
}

func (b *sstBatcher) Close() {
	b.sstWriter.Close()
}

// AddSSTable retries db.AddSSTable if retryable errors occur.
func AddSSTable(ctx context.Context, db *client.DB, start, end roachpb.Key, sstBytes []byte) error {
	const maxAddSSTableRetries = 10
	for i := 0; ; i++ {
		log.VEventf(ctx, 2, "sending AddSSTable [%s,%s)", start, end)
		// TODO(dan): This will fail if the range has split.
		err := db.AddSSTable(ctx, start, end, sstBytes)
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

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs batcheval.CommandArgs) (*roachpb.ImportResponse, error) {
	args := cArgs.Args.(*roachpb.ImportRequest)
	db := cArgs.EvalCtx.DB()
	kr, err := MakeKeyRewriter(args.Rekeys)
	if err != nil {
		return nil, errors.Wrap(err, "make key rewriter")
	}

	if err := cArgs.EvalCtx.GetLimiters().ConcurrentImports.Begin(ctx); err != nil {
		return nil, err
	}
	defer cArgs.EvalCtx.GetLimiters().ConcurrentImports.Finish()

	var rows rowCounter
	var iters []engine.SimpleIterator
	for _, file := range args.Files {
		log.VEventf(ctx, 2, "import file %s %s", file.Path, args.Span.Key)

		dir, err := MakeExportStorage(ctx, file.Dir, cArgs.EvalCtx.ClusterSettings())
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

		iter, err := engineccl.NewMemSSTIterator(fileContents, false)
		if err != nil {
			return nil, err
		}

		defer iter.Close()
		iters = append(iters, iter)
	}

	var batcher *sstBatcher
	makeBatcher := func() error {
		if batcher != nil {
			return errors.New("cannot overwrite a batcher")
		}
		sstWriter, err := engine.MakeRocksDBSstFileWriter()
		if err != nil {
			return errors.Wrapf(err, "making sstBatcher")
		}
		batcher = &sstBatcher{sstWriter: sstWriter}
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
	maxSize := MaxImportBatchSize(cArgs.EvalCtx.ClusterSettings())

	for iter.Seek(startKeyMVCC); ; {
		ok, err := iter.Valid()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		if args.EndTime != (hlc.Timestamp{}) {
			// TODO(dan): If we have to skip past a lot of versions to find the
			// latest one before args.EndTime, then this could be slow.
			if args.EndTime.Less(iter.UnsafeKey().Timestamp) {
				iter.Next()
				continue
			}
		}

		if !ok || !iter.UnsafeKey().Less(endKeyMVCC) {
			break
		}
		if len(iter.UnsafeValue()) == 0 {
			// Value is deleted.
			iter.NextKey()
			continue
		}

		keyScratch = append(keyScratch[:0], iter.UnsafeKey().Key...)
		valueScratch = append(valueScratch[:0], iter.UnsafeValue()...)
		key := engine.MVCCKey{Key: keyScratch, Timestamp: iter.UnsafeKey().Timestamp}
		value := roachpb.Value{RawBytes: valueScratch}
		iter.NextKey()

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

		if size := batcher.Size(); size > maxSize {
			finishBatcher := batcher
			batcher = nil
			log.Eventf(gCtx, "triggering finish of batch of size %s", humanizeutil.IBytes(size))
			g.Go(func() error {
				defer log.Event(ctx, "finished batch")
				defer finishBatcher.Close()
				return errors.Wrapf(finishBatcher.Finish(gCtx, db), "import [%s, %s)", startKeyMVCC.Key, endKeyMVCC.Key)
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
