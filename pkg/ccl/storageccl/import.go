// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/LICENSE

package storageccl

import (
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
	storage.SetImportCmd(storage.Command{
		DeclareKeys: storage.DefaultDeclareKeys,
		Eval:        evalImport,
	})
}

func validateRangeEmpty(ctx context.Context, db *client.DB, keyrange roachpb.Span) error {
	kvs, err := db.Scan(ctx, keyrange.Key, keyrange.EndKey, 1)
	if err != nil {
		return err
	}
	if len(kvs) > 0 {
		return errors.New("Import can only be called on empty ranges")
	}
	return nil
}

// evalImport bulk loads key/value entries.
func evalImport(
	ctx context.Context, _ engine.ReadWriter, cArgs storage.CommandArgs, resp roachpb.Response,
) (storage.EvalResult, error) {
	args := cArgs.Args.(*roachpb.ImportRequest)
	_ = resp.(*roachpb.ImportResponse)
	db := cArgs.Repl.DB()

	ctx, span := tracing.ChildSpan(ctx, fmt.Sprintf("import %s-%s", args.DataSpan.Key, args.DataSpan.EndKey))
	defer tracing.FinishSpan(span)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := validateRangeEmpty(ctx, db, args.Span); err != nil {
		return storage.EvalResult{}, err
	}

	if err := beginLimitedRequest(ctx); err != nil {
		return storage.EvalResult{}, err
	}
	defer endLimitedRequest()

	// Arrived at by tuning and watching the effect on BenchmarkRestore.
	const batchSizeBytes = 1000000

	var wg util.WaitGroupWithError
	b := struct {
		engine.RocksDBBatchBuilder
		batchStartKey []byte
		batchEndKey   []byte
	}{}
	sendWriteBatch := func() {
		batchStartKey := roachpb.Key(b.batchStartKey)
		// The end key of the WriteBatch request is exclusive, but batchEndKey
		// is currently the largest key in the batch. Increment it.
		batchEndKey := roachpb.Key(b.batchEndKey).Next()
		if log.V(1) {
			log.Infof(ctx, "writebatch %s-%s", batchStartKey, batchEndKey)
		}

		wg.Add(1)
		go func(start, end roachpb.Key, repr []byte) {
			if err := db.WriteBatch(ctx, start, end, repr); err != nil {
				log.Errorf(ctx, "writebatch %s-%s: %+v", start, end, err)
				wg.Done(err)
				cancel()
				return
			}
			wg.Done(nil)
		}(batchStartKey, batchEndKey, b.Finish())
		b.batchStartKey = nil
		b.batchEndKey = nil
		b.RocksDBBatchBuilder = engine.RocksDBBatchBuilder{}
	}

	kr := KeyRewriter(args.KeyRewrite)
	startKeyMVCC, endKeyMVCC := engine.MVCCKey{Key: args.DataSpan.Key}, engine.MVCCKey{Key: args.DataSpan.EndKey}
	for _, file := range args.File {
		if log.V(1) {
			log.Infof(ctx, "import file %s-%s %s", args.DataSpan.Key, args.DataSpan.EndKey, file.Path)
		}

		dir, err := MakeExportStorage(ctx, file.Dir)
		if err != nil {
			return storage.EvalResult{}, err
		}

		localPath, err := dir.FetchFile(ctx, file.Path)
		if err != nil {
			return storage.EvalResult{}, err
		}

		sst, err := engine.MakeRocksDBSstFileReader()
		if err != nil {
			return storage.EvalResult{}, err
		}
		defer sst.Close()

		// Add each file in its own sst reader because AddFile requires the
		// affected keyrange be empty and the keys in these files might overlap.
		// This becomes less heavyweight when we figure out how to use RocksDB's
		// TableReader directly.
		if err := sst.AddFile(localPath); err != nil {
			return storage.EvalResult{}, err
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
			b.Put(key, value.RawBytes)

			// Update the range currently represented in this batch, as
			// necessary.
			if len(b.batchStartKey) == 0 {
				b.batchStartKey = append(b.batchStartKey, key.Key...)
			}
			b.batchEndKey = append(b.batchEndKey[:0], key.Key...)

			if b.Len() > batchSizeBytes {
				sendWriteBatch()
			}
		}
		if err := iter.Error(); err != nil {
			return storage.EvalResult{}, err
		}
	}
	// Flush out the last batch.
	if b.Len() > 0 {
		sendWriteBatch()
	}

	err := wg.Wait()
	return storage.EvalResult{}, err
}
