// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/LICENSE

package storageccl

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func init() {
	storage.SetImportCmd(evalImport)
}

func validateImportRangeEmpty(
	ctx context.Context, db *client.DB, datarange roachpb.Span, kr KeyRewriter,
) error {
	startKey, ok := kr.RewriteKey(append([]byte(nil), datarange.Key...))
	if !ok {
		return errors.Errorf("could not rewrite key: %s", startKey)
	}
	endKey, ok := kr.RewriteKey(append([]byte(nil), datarange.EndKey...))
	if !ok {
		return errors.Errorf("could not rewrite key: %s", endKey)
	}

	kvs, err := db.Scan(ctx, startKey, endKey, 1)
	if err != nil {
		return err
	}
	if len(kvs) > 0 {
		return errors.New("import can only be called on empty ranges")
	}
	return nil
}

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs storage.CommandArgs) error {
	args := cArgs.Args.(*roachpb.ImportRequest)
	db := cArgs.Repl.DB()
	kr := KeyRewriter(args.KeyRewrites)

	ctx, span := tracing.ChildSpan(ctx, fmt.Sprintf("import [%s,%s)", args.DataSpan.Key, args.DataSpan.EndKey))
	defer tracing.FinishSpan(span)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The WriteBatch requests this sends will fail if the ranges they affect
	// are non-empty, so error early (before the request limiting) if that's the
	// case. This gets rechecked, so it's okay that this doesn't guarantee that
	// the range will _stay_ empty.
	if err := validateImportRangeEmpty(ctx, db, args.DataSpan, kr); err != nil {
		return err
	}

	if err := beginLimitedRequest(ctx); err != nil {
		return err
	}
	defer endLimitedRequest()

	// Arrived at by tuning and watching the effect on BenchmarkRestore.
	const batchSizeBytes = 1000000

	var wg util.WaitGroupWithError
	type batchBuilder struct {
		batch         engine.RocksDBBatchBuilder
		batchStartKey []byte
		batchEndKey   []byte
	}
	b := batchBuilder{}
	sendWriteBatch := func() {
		batchStartKey := roachpb.Key(b.batchStartKey)
		// The end key of the WriteBatch request is exclusive, but batchEndKey
		// is currently the largest key in the batch. Increment it.
		batchEndKey := roachpb.Key(b.batchEndKey).Next()
		if log.V(1) {
			log.Infof(ctx, "writebatch [%s,%s)", batchStartKey, batchEndKey)
		}

		wg.Add(1)
		go func(start, end roachpb.Key, repr []byte) {
			if err := db.WriteBatch(ctx, start, end, repr); err != nil {
				log.Errorf(ctx, "writebatch [%s,%s): %+v", start, end, err)
				wg.Done(err)
				cancel()
				return
			}
			wg.Done(nil)
		}(batchStartKey, batchEndKey, b.batch.Finish())
		b = batchBuilder{}
	}

	startKeyMVCC, endKeyMVCC := engine.MVCCKey{Key: args.DataSpan.Key}, engine.MVCCKey{Key: args.DataSpan.EndKey}
	for _, file := range args.Files {
		if log.V(1) {
			log.Infof(ctx, "import file [%s,%s) %s", args.DataSpan.Key, args.DataSpan.EndKey, file.Path)
		}

		dir, err := MakeExportStorage(ctx, file.Dir)
		if err != nil {
			return err
		}

		localPath, err := dir.FetchFile(ctx, file.Path)
		if err != nil {
			return err
		}

		if len(file.Sha512) > 0 {
			checksum, err := sha512ChecksumFile(localPath)
			if err != nil {
				return err
			}
			if !bytes.Equal(checksum, file.Sha512) {
				return errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}

		sst, err := engine.MakeRocksDBSstFileReader()
		if err != nil {
			return err
		}
		defer sst.Close()

		// Add each file in its own sst reader because AddFile requires the
		// affected keyrange be empty and the keys in these files might overlap.
		// This becomes less heavyweight when we figure out how to use RocksDB's
		// TableReader directly.
		if err := sst.AddFile(localPath); err != nil {
			return err
		}

		iter := sst.NewIterator(false)
		defer iter.Close()
		iter.Seek(startKeyMVCC)
		for ; iter.Valid(); iter.Next() {
			key := iter.Key()
			if endKeyMVCC.Less(key) {
				break
			}
			value := roachpb.Value{RawBytes: iter.Value()}

			var ok bool
			key.Key, ok = kr.RewriteKey(key.Key)
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
			if len(b.batchStartKey) == 0 {
				b.batchStartKey = append(b.batchStartKey, key.Key...)
			}
			b.batchEndKey = append(b.batchEndKey[:0], key.Key...)

			if b.batch.Len() > batchSizeBytes {
				sendWriteBatch()
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
	}
	// Flush out the last batch.
	if b.batch.Len() > 0 {
		sendWriteBatch()
	}

	err := wg.Wait()
	return err
}
