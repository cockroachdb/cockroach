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
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

var importBatchSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.batch_size",
		"the maximum size of the payload in an AddSSTable request",
		16<<20,
	)
	return s
}()

var importPKAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.pk_buffer_size",
		"the initial size of the BulkAdder buffer handling primary index imports",
		32<<20,
	)
	return s
}()

var importPKAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.max_pk_buffer_size",
		"the maximum size of the BulkAdder buffer handling primary index imports",
		128<<20,
	)
	return s
}()

var importIndexAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.index_buffer_size",
		"the initial size of the BulkAdder buffer handling secondary index imports",
		32<<20,
	)
	return s
}()

var importIndexAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.max_index_buffer_size",
		"the maximum size of the BulkAdder buffer handling secondary index imports",
		512<<20,
	)
	return s
}()

var importBufferIncrementSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.buffer_increment",
		"the size by which the BulkAdder attempts to grow its buffer before flushing",
		32<<20,
	)
	return s
}()

var remoteSSTs = settings.RegisterBoolSetting(
	"kv.bulk_ingest.stream_external_ssts.enabled",
	"if enabled, external SSTables are iterated directly in some cases, rather than being downloaded entirely first",
	true,
)

var remoteSSTSuffixCacheSize = settings.RegisterByteSizeSetting(
	"kv.bulk_ingest.stream_external_ssts.suffix_cache_size",
	"size of suffix of remote SSTs to download and cache before reading from remote stream",
	64<<10,
)

// commandMetadataEstimate is an estimate of how much metadata Raft will add to
// an AddSSTable command. It is intentionally a vast overestimate to avoid
// embedding intricate knowledge of the Raft encoding scheme here.
const commandMetadataEstimate = 1 << 20 // 1 MB

func init() {
	kvserver.SetImportCmd(evalImport)

	// Ensure that the user cannot set the maximum raft command size so low that
	// more than half of an Import or AddSSTable command will be taken up by Raft
	// metadata.
	if commandMetadataEstimate > kvserver.MaxCommandSizeFloor/2 {
		panic(fmt.Sprintf("raft command size floor (%s) is too small for import commands",
			humanizeutil.IBytes(kvserver.MaxCommandSizeFloor)))
	}
}

// MaxImportBatchSize determines the maximum size of the payload in an
// AddSSTable request. It uses the ImportBatchSize setting directly unless the
// specified value would exceed the maximum Raft command size, in which case it
// returns the maximum batch size that will fit within a Raft command.
func MaxImportBatchSize(st *cluster.Settings) int64 {
	desiredSize := importBatchSize.Get(&st.SV)
	maxCommandSize := kvserver.MaxCommandSize.Get(&st.SV)
	if desiredSize+commandMetadataEstimate > maxCommandSize {
		return maxCommandSize - commandMetadataEstimate
	}
	return desiredSize
}

// ImportBufferConfigSizes determines the minimum, maximum and step size for the
// BulkAdder buffer used in import.
func ImportBufferConfigSizes(st *cluster.Settings, isPKAdder bool) (int64, func() int64, int64) {
	if isPKAdder {
		return importPKAdderBufferSize.Get(&st.SV),
			func() int64 { return importPKAdderMaxBufferSize.Get(&st.SV) },
			importBufferIncrementSize.Get(&st.SV)
	}
	return importIndexAdderBufferSize.Get(&st.SV),
		func() int64 { return importIndexAdderMaxBufferSize.Get(&st.SV) },
		importBufferIncrementSize.Get(&st.SV)
}

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs batcheval.CommandArgs) (*roachpb.ImportResponse, error) {
	args := cArgs.Args.(*roachpb.ImportRequest)
	db := cArgs.EvalCtx.DB()
	// args.Rekeys could be using table descriptors from either the old or new
	// foreign key representation on the table descriptor, but this is fine
	// because foreign keys don't matter for the key rewriter.
	kr, err := MakeKeyRewriterFromRekeys(keys.SystemSQLCodec, args.Rekeys)
	if err != nil {
		return nil, errors.Wrap(err, "make key rewriter")
	}

	// The sstables only contain MVCC data and no intents, so using an MVCC
	// iterator is sufficient.
	var iters []storage.SimpleMVCCIterator
	for _, file := range args.Files {
		log.VEventf(ctx, 2, "import file %s %s", file.Path, args.Key)

		dir, err := cArgs.EvalCtx.GetExternalStorage(ctx, file.Dir)
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

		if args.Encryption != nil {
			fileContents, err = DecryptFile(fileContents, args.Encryption.Key)
			if err != nil {
				return nil, err
			}
		}

		if len(file.Sha512) > 0 {
			checksum, err := SHA512ChecksumData(fileContents)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(checksum, file.Sha512) {
				return nil, errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}

		iter, err := storage.NewMemSSTIterator(fileContents, false)
		if err != nil {
			return nil, err
		}

		defer iter.Close()
		iters = append(iters, iter)
	}

	batcher, err := bulk.MakeSSTBatcher(ctx, db, cArgs.EvalCtx.ClusterSettings(), func() int64 { return MaxImportBatchSize(cArgs.EvalCtx.ClusterSettings()) })
	if err != nil {
		return nil, err
	}
	defer batcher.Close()

	startKeyMVCC, endKeyMVCC := storage.MVCCKey{Key: args.DataSpan.Key}, storage.MVCCKey{Key: args.DataSpan.EndKey}
	iter := storage.MakeMultiIterator(iters)
	defer iter.Close()
	var keyScratch, valueScratch []byte

	for iter.SeekGE(startKeyMVCC); ; {
		ok, err := iter.Valid()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		if !args.EndTime.IsEmpty() {
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
		key := storage.MVCCKey{Key: keyScratch, Timestamp: iter.UnsafeKey().Timestamp}
		value := roachpb.Value{RawBytes: valueScratch}
		iter.NextKey()

		key.Key, ok, err = kr.RewriteKey(key.Key, false /* isFromSpan */)
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
		if err := batcher.AddMVCCKey(ctx, key, value.RawBytes); err != nil {
			return nil, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}
	}
	// Flush out the last batch.
	if err := batcher.Flush(ctx); err != nil {
		return nil, err
	}
	log.Event(ctx, "done")
	return &roachpb.ImportResponse{Imported: batcher.GetSummary()}, nil
}

// ExternalSSTReader returns opens an SST in external storage, optionally
// decrypting with the supplied parameters, and returns iterator over it.
func ExternalSSTReader(
	ctx context.Context,
	e cloud.ExternalStorage,
	basename string,
	encryption *roachpb.FileEncryptionOptions,
) (storage.SimpleMVCCIterator, error) {
	// Do an initial read of the file, from the beginning, to get the file size as
	// this is used e.g. to read the trailer.
	var f io.ReadCloser
	var sz int64

	const maxAttempts = 3
	if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		var err error
		f, sz, err = e.ReadFileAt(ctx, basename, 0)
		return err
	}); err != nil {
		return nil, err
	}

	if !remoteSSTs.Get(&e.Settings().SV) {
		content, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			return nil, err
		}
		if encryption != nil {
			content, err = DecryptFile(content, encryption.Key)
			if err != nil {
				return nil, err
			}
		}
		return storage.NewMemSSTIterator(content, false)
	}

	raw := &sstReader{
		sz:   sizeStat(sz),
		body: f,
		openAt: func(offset int64) (io.ReadCloser, error) {
			reader, _, err := e.ReadFileAt(ctx, basename, offset)
			return reader, err
		},
	}

	var reader sstable.ReadableFile = raw

	if encryption != nil {
		r, err := decryptingReader(raw, encryption.Key)
		if err != nil {
			f.Close()
			return nil, err
		}
		reader = r
	} else {
		// We only explicitly buffer the suffix of the file when not decrypting as
		// the decrypting reader has its own internal block buffer.
		if err := raw.readAndCacheSuffix(remoteSSTSuffixCacheSize.Get(&e.Settings().SV)); err != nil {
			f.Close()
			return nil, err
		}
	}

	iter, err := storage.NewSSTIterator(reader)
	if err != nil {
		reader.Close()
		return nil, err
	}

	return iter, nil
}

type sstReader struct {
	sz     sizeStat
	openAt func(int64) (io.ReadCloser, error)
	// body and pos are mutated by calls to ReadAt and Close.
	body io.ReadCloser
	pos  int64

	readPos int64 // readPos is used to transform Read() to ReadAt(readPos).

	// This wrapper's primary purpose is reading SSTs which often perform many
	// tiny reads in a cluster of offsets near the end of the file. If we can read
	// the whole region once and fullfil those from a cache, we can avoid repeated
	// RPCs.
	cache struct {
		pos int64
		buf []byte
	}
}

// Close implements io.Closer.
func (r *sstReader) Close() error {
	r.pos = 0
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

// Stat returns the size of the file.
func (r *sstReader) Stat() (os.FileInfo, error) {
	return r.sz, nil
}

func (r *sstReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.readPos)
	r.readPos += int64(n)
	return n, err
}

// readAndCacheSuffix caches the `size` suffix of the file (which could the
// whole file) for use by later ReadAt calls to avoid making additional RPCs.
func (r *sstReader) readAndCacheSuffix(size int64) error {
	if size == 0 {
		return nil
	}
	r.cache.buf = nil
	r.cache.pos = int64(r.sz) - size
	if r.cache.pos <= 0 {
		r.cache.pos = 0
	}
	reader, err := r.openAt(r.cache.pos)
	if err != nil {
		return err
	}
	defer reader.Close()
	read, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	r.cache.buf = read
	return nil
}

// ReadAt implements io.ReaderAt by opening a Reader at an offset before reading
// from it. Note: contrary to io.ReaderAt, ReadAt does *not* support parallel
// calls.
func (r *sstReader) ReadAt(p []byte, offset int64) (int, error) {
	var read int
	if offset >= r.cache.pos && offset < r.cache.pos+int64(len(r.cache.buf)) {
		read += copy(p, r.cache.buf[offset-r.cache.pos:])
		if read == len(p) {
			return read, nil
		}
		// Advance offset to end of what cache read.
		offset += int64(read)
	}

	if offset == int64(r.sz) {
		return read, io.EOF
	}

	// Position the underlying reader at offset if needed.
	if r.pos != offset {
		if err := r.Close(); err != nil {
			return 0, err
		}
		b, err := r.openAt(offset)
		if err != nil {
			return 0, err
		}
		r.pos = offset
		r.body = b
	}

	var err error
	for n := 0; read < len(p); n, err = r.body.Read(p[read:]) {
		read += n
		if err != nil {
			break
		}
	}
	r.pos += int64(read)

	// If we got an EOF after we had read enough, ignore it.
	if read == len(p) && err == io.EOF {
		return read, nil
	}

	return read, err
}

type sizeStat int64

func (s sizeStat) Size() int64      { return int64(s) }
func (sizeStat) IsDir() bool        { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) ModTime() time.Time { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Mode() os.FileMode  { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Name() string       { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Sys() interface{}   { panic(errors.AssertionFailedf("unimplemented")) }
