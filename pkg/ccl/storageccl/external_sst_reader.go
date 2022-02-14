// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

var remoteSSTs = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.bulk_ingest.stream_external_ssts.enabled",
	"if enabled, external SSTables are iterated directly in some cases, rather than being downloaded entirely first",
	true,
)

var remoteSSTSuffixCacheSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"kv.bulk_ingest.stream_external_ssts.suffix_cache_size",
	"size of suffix of remote SSTs to download and cache before reading from remote stream",
	64<<10,
)

// ExternalSSTReader returns opens an SST in external storage, optionally
// decrypting with the supplied parameters, and returns iterator over it.
//
// ctx is captured and used throughout the life of the returned iterator, until
// the iterator's Close() method is called.
func ExternalSSTReader(
	ctx context.Context,
	e cloud.ExternalStorage,
	basename string,
	encryption *roachpb.FileEncryptionOptions,
) (storage.SimpleMVCCIterator, error) {
	// Do an initial read of the file, from the beginning, to get the file size as
	// this is used e.g. to read the trailer.
	var f ioctx.ReadCloserCtx
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
		content, err := ioctx.ReadAll(ctx, f)
		f.Close(ctx)
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
		ctx:  ctx,
		sz:   sizeStat(sz),
		body: f,
		openAt: func(offset int64) (ioctx.ReadCloserCtx, error) {
			reader, _, err := e.ReadFileAt(ctx, basename, offset)
			return reader, err
		},
	}

	var reader sstable.ReadableFile = raw

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
		if err := raw.readAndCacheSuffix(remoteSSTSuffixCacheSize.Get(&e.Settings().SV)); err != nil {
			f.Close(ctx)
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

// Close implements io.Closer.
func (r *sstReader) Close() error {
	r.pos = 0
	r.ctx = nil
	if r.body != nil {
		return r.body.Close(r.ctx)
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

func (s sizeStat) Size() int64      { return int64(s) }
func (sizeStat) IsDir() bool        { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) ModTime() time.Time { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Mode() os.FileMode  { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Name() string       { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Sys() interface{}   { panic(errors.AssertionFailedf("unimplemented")) }
