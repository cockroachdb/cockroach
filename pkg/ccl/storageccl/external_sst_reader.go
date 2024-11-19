// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageccl

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// RemoteSSTs lets external SSTables get iterated directly in some cases,
// rather than being downloaded entirely first.
var remoteSSTs = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.bulk_ingest.stream_external_ssts.enabled",
	"if enabled, external SSTables are iterated directly in some cases, rather than being downloaded entirely first",
	true,
)

var remoteSSTSuffixCacheSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"kv.bulk_ingest.stream_external_ssts.suffix_cache_size",
	"size of suffix of remote SSTs to download and cache before reading from remote stream",
	64<<10,
)

func getFileWithRetry(
	ctx context.Context, basename string, e cloud.ExternalStorage,
) (ioctx.ReadCloserCtx, int64, error) {
	// Do an initial read of the file, from the beginning, to get the file size as
	// this is used E.g. to read the trailer.
	var f ioctx.ReadCloserCtx
	var sz int64
	const maxAttempts = 3
	if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		var err error
		f, sz, err = e.ReadFile(ctx, basename, cloud.ReadOptions{})
		return err
	}); err != nil {
		return nil, 0, err
	}
	return f, sz, nil
}

// StoreFile groups a file with its corresponding external storage handler.
type StoreFile struct {
	Store    cloud.ExternalStorage
	FilePath string
}

// newMemPebbleSSTReader returns a PebbleSSTIterator for in-memory SSTs from
// external storage, optionally decrypting with the supplied parameters.
//
// ctx is captured and used throughout the life of the returned iterator, until
// the iterator's Close() method is called.
func newMemPebbleSSTReader(
	ctx context.Context,
	storeFiles []StoreFile,
	encryption *kvpb.FileEncryptionOptions,
	iterOps storage.IterOptions,
) (storage.SimpleMVCCIterator, error) {

	inMemorySSTs := make([][]byte, 0, len(storeFiles))
	memAcc := mon.NewStandaloneUnlimitedAccount()

	for _, sf := range storeFiles {
		f, _, err := getFileWithRetry(ctx, sf.FilePath, sf.Store)
		if err != nil {
			return nil, err
		}
		content, err := ioctx.ReadAll(ctx, f)
		f.Close(ctx)
		if err != nil {
			return nil, err
		}
		if encryption != nil {
			content, err = DecryptFile(ctx, content, encryption.Key, memAcc)
			if err != nil {
				return nil, err
			}
		}
		inMemorySSTs = append(inMemorySSTs, content)
	}
	return storage.NewMultiMemSSTIterator(inMemorySSTs, false, iterOps)
}

// ExternalSSTReader returns a PebbleSSTIterator for the SSTs in external storage,
// optionally decrypting with the supplied parameters.
//
// Note: the order of SSTs matters if multiple SSTs contain the exact same
// Pebble key (that is, the same key/timestamp combination). In this case, the
// PebbleIterator will only surface the key in the first SST that contains it.
//
// ctx is captured and used throughout the life of the returned iterator, until
// the iterator's Close() method is called.
func ExternalSSTReader(
	ctx context.Context,
	storeFiles []StoreFile,
	encryption *kvpb.FileEncryptionOptions,
	iterOpts storage.IterOptions,
) (storage.SimpleMVCCIterator, error) {
	// TODO(jackson): Change the interface to accept a two-dimensional
	// [][]StoreFiles slice, and propagate that structure to
	// NewSSTIterator.

	if !remoteSSTs.Get(&storeFiles[0].Store.Settings().SV) {
		return newMemPebbleSSTReader(ctx, storeFiles, encryption, iterOpts)
	}
	remoteCacheSize := remoteSSTSuffixCacheSize.Get(&storeFiles[0].Store.Settings().SV)
	openedReadersByLevel := make([][]sstable.ReadableFile, 0, len(storeFiles))

	// Cleanup any files we've opened if we fail with an error.
	defer func() {
		for _, l := range openedReadersByLevel {
			for _, r := range l {
				_ = r.Close()
			}
		}
	}()

	for _, sf := range storeFiles {
		f, sz, err := getFileWithRetry(ctx, sf.FilePath, sf.Store)
		if err != nil {
			return nil, err
		}

		raw := &sstReader{
			ctx:  ctx,
			sz:   sizeStat(sz),
			body: f,
			openAt: func(offset int64) (ioctx.ReadCloserCtx, error) {
				reader, _, err := sf.Store.ReadFile(ctx, sf.FilePath, cloud.ReadOptions{
					Offset:     offset,
					NoFileSize: true,
				})
				return reader, err
			},
		}

		var reader sstable.ReadableFile

		if encryption != nil {
			r, err := decryptingReader(raw, encryption.Key)
			if err != nil {
				f.Close(ctx)
				return nil, err
			}
			reader = r
		} else {
			// We only explicitly buffer the suffix of the file when not decrypting as
			// the decrypting reader has its own internal block buffer.
			if err := raw.readAndCacheSuffix(remoteCacheSize); err != nil {
				f.Close(ctx)
				return nil, err
			}
			reader = raw
		}
		openedReadersByLevel = append(openedReadersByLevel, []sstable.ReadableFile{reader})
	}

	readerLevels := openedReadersByLevel
	openedReadersByLevel = nil
	return storage.NewSSTIterator(readerLevels, iterOpts)
}

type sstReader struct {
	// ctx is captured at construction time and used for I/O operations.
	ctx    context.Context
	sz     sizeStat
	openAt func(int64) (ioctx.ReadCloserCtx, error)
	// body and pos are mutated by calls to ReadAt and Close.
	body ioctx.ReadCloserCtx
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

func (r *sstReader) reset() error {
	r.pos = 0
	var err error
	if r.body != nil {
		err = r.body.Close(r.ctx)
		r.body = nil
	}
	return err
}

// Close implements io.Closer.
func (r *sstReader) Close() error {
	err := r.reset()
	r.ctx = nil
	return err
}

// Stat returns the size of the file.
func (r *sstReader) Stat() (vfs.FileInfo, error) {
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
	defer reader.Close(r.ctx)
	read, err := ioctx.ReadAll(r.ctx, reader)
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
		if err := r.reset(); err != nil {
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
	for n := 0; read < len(p); n, err = r.body.Read(r.ctx, p[read:]) {
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

func (s sizeStat) Size() int64          { return int64(s) }
func (sizeStat) IsDir() bool            { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) ModTime() time.Time     { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Mode() os.FileMode      { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Name() string           { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Sys() interface{}       { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) DeviceID() vfs.DeviceID { panic(errors.AssertionFailedf("unimplemented")) }
