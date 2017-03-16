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

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func init() {
	storage.SetImportCmd(evalImport)
}

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs storage.CommandArgs) error {
	args := cArgs.Args.(*roachpb.ImportRequest)
	db := cArgs.Repl.DB()
	kr := KeyRewriter(args.KeyRewrites)

	ctx, span := tracing.ChildSpan(ctx, fmt.Sprintf("import [%s,%s)", args.DataSpan.Key, args.DataSpan.EndKey))
	defer tracing.FinishSpan(span)

	if err := beginLimitedRequest(ctx); err != nil {
		return err
	}
	defer endLimitedRequest()

	// Arrived at by tuning and watching the effect on BenchmarkRestore.
	const batchSizeBytes = 1000000

	type batchBuilder struct {
		batch         engine.RocksDBBatchBuilder
		batchStartKey []byte
		batchEndKey   []byte
	}
	b := batchBuilder{}
	g, wgCtx := errgroup.WithContext(ctx)
	sendWriteBatch := func() {
		batchStartKey := roachpb.Key(b.batchStartKey)
		// The end key of the WriteBatch request is exclusive, but batchEndKey
		// is currently the largest key in the batch. Increment it.
		batchEndKey := roachpb.Key(b.batchEndKey).Next()
		if log.V(1) {
			log.Infof(wgCtx, "writebatch [%s,%s)", batchStartKey, batchEndKey)
		}

		repr := b.batch.Finish()
		g.Go(func() error {
			return db.WriteBatch(wgCtx, batchStartKey, batchEndKey, repr)
		})
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
			return err
		}
	}
	// Flush out the last batch.
	if b.batch.Len() > 0 {
		sendWriteBatch()
	}
	return g.Wait()
}
