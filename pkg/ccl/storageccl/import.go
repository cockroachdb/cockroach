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

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"the maximum size of the payload in an AddSSTable request",
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
	maxSize int64
	// rows written in the current batch.
	rowCounter RowCounter
	totalRows  roachpb.BulkOpSummary

	sstWriter     engine.RocksDBSstFileWriter
	batchStartKey []byte
	batchEndKey   []byte
}

func (b *sstBatcher) add(key engine.MVCCKey, value []byte) error {
	// Update the range currently represented in this batch, as
	// necessary.
	if len(b.batchStartKey) == 0 || bytes.Compare(key.Key, b.batchStartKey) < 0 {
		b.batchStartKey = append(b.batchStartKey[:0], key.Key...)
	}
	if len(b.batchEndKey) == 0 || bytes.Compare(key.Key, b.batchEndKey) > 0 {
		b.batchEndKey = append(b.batchEndKey[:0], key.Key...)
	}
	if err := b.rowCounter.Count(key.Key); err != nil {
		return err
	}
	return b.sstWriter.Add(engine.MVCCKeyValue{Key: key, Value: value})
}

func (b *sstBatcher) reset() error {
	b.sstWriter.Close()
	w, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	b.sstWriter = w
	b.batchStartKey = b.batchStartKey[:0]
	b.batchEndKey = b.batchEndKey[:0]
	b.rowCounter.BulkOpSummary.Reset()
	return nil
}

func (b *sstBatcher) shouldFlush(nextKey roachpb.Key) bool {
	return b.sstWriter.DataSize >= b.maxSize
}

func (b *sstBatcher) flush(ctx context.Context, db *client.DB) error {
	if b.sstWriter.DataSize == 0 {
		return nil
	}
	start := roachpb.Key(append([]byte(nil), b.batchStartKey...))
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(append([]byte(nil), b.batchEndKey...)).Next()

	sstBytes, err := b.sstWriter.Finish()
	if err != nil {
		return errors.Wrapf(err, "finishing constructed sstable")
	}
	if err := AddSSTable(ctx, db, start, end, sstBytes); err != nil {
		// TODO(dt): if we get a RangeKeyMismatchError, update batching split points
		// and then tell the caller to try again.
		return err
	}
	b.rowCounter.DataSize = b.sstWriter.DataSize
	b.totalRows.Add(b.rowCounter.BulkOpSummary)
	return nil
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
		if m, ok := errors.Cause(err).(*roachpb.RangeKeyMismatchError); ok {
			return addSplitSSTable(ctx, db, sstBytes, start, m.MismatchedRange.EndKey.AsRawKey())
		}
		if _, ok := err.(*roachpb.AmbiguousResultError); i == maxAddSSTableRetries || !ok {
			return errors.Wrapf(err, "addsstable [%s,%s)", start, end)
		}

		log.Warningf(ctx, "addsstable [%s,%s) attempt %d failed: %+v",
			start, end, i, err)
		continue
	}
}

func addSplitSSTable(
	ctx context.Context, db *client.DB, sstBytes []byte, start, splitKey roachpb.Key,
) error {
	iter, err := engine.NewMemSSTIterator(sstBytes, false)
	if err != nil {
		return err
	}
	defer iter.Close()

	w, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	defer w.Close()

	split := false
	var first, last roachpb.Key

	iter.Seek(engine.MVCCKey{Key: start})
	for {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		key := iter.UnsafeKey()

		if !split && key.Key.Compare(splitKey) >= 0 {
			res, err := w.Finish()
			if err != nil {
				return err
			}
			if err := AddSSTable(ctx, db, first, last.PrefixEnd(), res); err != nil {
				return err
			}
			w.Close()
			w, err = engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return err
			}

			split = true
			first = nil
			last = nil
		}

		if len(first) == 0 {
			first = append(first[:0], key.Key...)
		}
		last = append(last[:0], key.Key...)

		if err := w.Add(engine.MVCCKeyValue{Key: key, Value: iter.UnsafeValue()}); err != nil {
			return err
		}

		iter.Next()
	}

	res, err := w.Finish()
	if err != nil {
		return err
	}
	return AddSSTable(ctx, db, first, last.PrefixEnd(), res)
}

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs batcheval.CommandArgs) (*roachpb.ImportResponse, error) {
	args := cArgs.Args.(*roachpb.ImportRequest)
	db := cArgs.EvalCtx.DB()
	kr, err := MakeKeyRewriterFromRekeys(args.Rekeys)
	if err != nil {
		return nil, errors.Wrap(err, "make key rewriter")
	}

	if err := cArgs.EvalCtx.GetLimiters().ConcurrentImports.Begin(ctx); err != nil {
		return nil, err
	}
	defer cArgs.EvalCtx.GetLimiters().ConcurrentImports.Finish()

	var iters []engine.SimpleIterator
	for _, file := range args.Files {
		log.VEventf(ctx, 2, "import file %s %s", file.Path, args.Key)

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

		if len(file.Sha512) > 0 {
			checksum, err := SHA512ChecksumData(fileContents)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(checksum, file.Sha512) {
				return nil, errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}

		iter, err := engine.NewMemSSTIterator(fileContents, false)
		if err != nil {
			return nil, err
		}

		defer iter.Close()
		iters = append(iters, iter)
	}

	batcher := &sstBatcher{maxSize: MaxImportBatchSize(cArgs.EvalCtx.ClusterSettings())}
	if err := batcher.reset(); err != nil {
		return nil, err
	}
	defer batcher.Close()

	startKeyMVCC, endKeyMVCC := engine.MVCCKey{Key: args.DataSpan.Key}, engine.MVCCKey{Key: args.DataSpan.EndKey}
	iter := engine.MakeMultiIterator(iters)
	defer iter.Close()
	var keyScratch, valueScratch []byte

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

		// Check if we need to flush current batch *before* adding the next k/v --
		// the batcher may want to flush the keys it already has, either because it
		// is full or because it wants this key in a separate batch due to splits.
		if batcher.shouldFlush(key.Key) {
			if err := batcher.flush(ctx, db); err != nil {
				return nil, errors.Wrapf(err, "import [%s, %s)", startKeyMVCC.Key, endKeyMVCC.Key)
			}
			if err := batcher.reset(); err != nil {
				return nil, err
			}
		}

		// Rewriting the key means the checksum needs to be updated.
		value.ClearChecksum()
		value.InitChecksum(key.Key)

		if log.V(3) {
			log.Infof(ctx, "Put %s -> %s", key.Key, value.PrettyPrint())
		}
		if err := batcher.add(key, value.RawBytes); err != nil {
			return nil, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}
	}
	// Flush out the last batch.
	if err := batcher.flush(ctx, db); err != nil {
		return nil, err
	}
	log.Event(ctx, "done")
	return &roachpb.ImportResponse{Imported: batcher.totalRows}, nil
}
