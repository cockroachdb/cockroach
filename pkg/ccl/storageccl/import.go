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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

// TODO(dan): BEFORE MERGE change this default to 2MB
var importBatchSize = func() *settings.ByteSizeSetting {
	name := "kv.import.batch_size"
	s := settings.RegisterByteSizeSetting(name, "", 2<<30)
	settings.Hide(name)
	return s
}()

// AddSSTableEnabled is exposed for testing.
var AddSSTableEnabled = func() *settings.BoolSetting {
	name := "kv.import.addsstable.enabled"
	s := settings.RegisterBoolSetting(name, "set to true to use the AddSSTable command in Import", false)
	settings.Hide(name)
	return s
}()

type importBatcher interface {
	Add(engine.MVCCKey, []byte) error
	Size() int64
	Finish(context.Context, *client.DB) error
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

type sstBatcher struct {
	sstWriter     engine.RocksDBSstFileWriter
	sstWriterPath string
	batchStartKey []byte
	batchEndKey   []byte
}

var _ importBatcher = &sstBatcher{}

func makeSSTBatcher(tempPath string) (*sstBatcher, error) {
	b := &sstBatcher{
		sstWriterPath: filepath.Join(tempPath, uuid.MakeV4().String()),
		sstWriter:     engine.MakeRocksDBSstFileWriter(),
	}
	if err := b.sstWriter.Open(b.sstWriterPath); err != nil {
		return nil, err
	}
	return b, nil
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

	if err := b.sstWriter.Close(); err != nil {
		return errors.Wrapf(err, "closing sstable writer %d", b.sstWriterPath)
	}

	sstBytes, err := ioutil.ReadFile(b.sstWriterPath)
	if err != nil {
		return errors.Wrapf(err, "reading sstable %s", b.sstWriterPath)
	}

	// TODO(dan): This will fail if the range has split.
	if err := db.AddSSTable(ctx, start, end, sstBytes); err != nil {
		return errors.Wrapf(err, "linking sstable into rocksdb")
	}
	return nil
}

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs storage.CommandArgs) (*roachpb.ImportResponse, error) {
	args := cArgs.Args.(*roachpb.ImportRequest)
	db := cArgs.EvalCtx.DB()
	kr, err := MakeKeyRewriter(args.Rekeys)
	if err != nil {
		return nil, errors.Wrap(err, "make key rewriter")
	}

	// TODO(dan): sstBatcher doesn't clean up after itself. In the meantime,
	// make a tempdir that we clean up in a defer.
	tempPrefix := filepath.Join(
		cArgs.EvalCtx.GetTempPrefix(),
		strconv.Itoa(int(parser.GenerateUniqueInt(cArgs.EvalCtx.NodeID()))),
	)
	if err := os.MkdirAll(tempPrefix, 0755); err != nil {
		return nil, err
	}
	defer func() {
		if err := os.RemoveAll(tempPrefix); err != nil {
			log.Warningf(ctx, "could not remove %s: %+v", tempPrefix, err)
		}
	}()

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

		dataSize += int64(len(fileContents))

		if len(file.Sha512) > 0 {
			checksum, err := sha512ChecksumData(fileContents)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(checksum, file.Sha512) {
				return nil, errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}

		sst := engine.MakeRocksDBSstFileReader()
		defer sst.Close()

		// Add each file in its own sst reader because IngestExternalFile
		// requires the affected keyrange be empty and the keys in these files
		// might overlap.
		if err := sst.IngestExternalFile(fileContents); err != nil {
			return nil, err
		}

		iter := sst.NewIterator(false)
		defer iter.Close()
		iters = append(iters, iter)
	}

	var batcher importBatcher
	resetBatcher := func() error {
		if AddSSTableEnabled.Get() {
			var err error
			batcher, err = makeSSTBatcher(tempPrefix)
			if err != nil {
				return errors.Wrapf(err, "making sstBatcher: %s", tempPrefix)
			}
			return nil
		}
		batcher = &writeBatcher{}
		return nil
	}
	if err := resetBatcher(); err != nil {
		return nil, err
	}

	g, gCtx := errgroup.WithContext(ctx)
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
		if err := batcher.Add(key, value.RawBytes); err != nil {
			return nil, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}

		if batcher.Size() > importBatchSize.Get() {
			finishBatcher := batcher
			g.Go(func() error {
				return finishBatcher.Finish(gCtx, db)
			})
			if err := resetBatcher(); err != nil {
				return nil, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	// Flush out the last batch.
	if batcher.Size() > 0 {
		g.Go(func() error {
			return batcher.Finish(gCtx, db)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &roachpb.ImportResponse{DataSize: dataSize}, nil
}
