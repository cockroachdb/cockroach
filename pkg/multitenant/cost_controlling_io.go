// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package multitenant

import (
	"bytes"
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

// DefaultBytesAllowedBeforeAccounting are how maybe bytes we will read.write before trying to wait for
// RUs. The goal here is to avoid waiting in loops for the common case without allowing an
// unbounded number of bytes read/written before accounting for them.
const DefaultBytesAllowedBeforeAccounting = 128 << 20 // 128 MB

// accountingExternalStorage is a wrapper for cloud.ExternalStorage that adds basic RU accounting.
type accountingExternalStorage struct {
	cloud.ExternalStorage

	recorder TenantSideExternalIORecorder
	limit    int64
}

func NewExternalStorageWithAccounting(
	s cloud.ExternalStorage, recorder TenantSideExternalIORecorder, limit int64,
) cloud.ExternalStorage {
	if !s.RequiresExternalIOAccounting() || recorder == nil {
		return s
	}
	return &accountingExternalStorage{
		ExternalStorage: s,
		limit:           limit,
		recorder:        recorder,
	}
}

func (a *accountingExternalStorage) Writer(
	ctx context.Context, basename string,
) (io.WriteCloser, error) {
	inner, err := a.ExternalStorage.Writer(ctx, basename)
	if err != nil {
		return nil, err
	}
	return &accountingWriter{
		ctx:      ctx,
		inner:    inner,
		limit:    a.limit,
		recorder: a.recorder,
	}, nil
}

func (a *accountingExternalStorage) ReadFile(
	ctx context.Context, basename string,
) (ioctx.ReadCloserCtx, error) {
	inner, err := a.ExternalStorage.ReadFile(ctx, basename)
	if err != nil {
		return nil, err
	}
	return &accountingReader{
		inner:    inner,
		limit:    a.limit,
		recorder: a.recorder,
	}, nil
}

func (a *accountingExternalStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	inner, sz, err := a.ExternalStorage.ReadFileAt(ctx, basename, offset)
	if err != nil {
		return nil, 0, err
	}
	return &accountingReader{
		inner:    inner,
		limit:    a.limit,
		recorder: a.recorder,
	}, sz, nil
}

// accountingWriter is an io.WriteCloser that tracks how many total bytes have been written. If limit is > 0, then the
// writer will record the written bytes and wait for the associated RUs in a Write call if more than limit bytes have been
// written. On Close, any previously unaccounted for RUs will be recorded.
//
// If limit <= 0 then we will wait for RUs only on Close().
//
// NB: The implementation allows roughly 2x the limit to be unaccounted if the caller is making write calls with a large
// values just under the limit.
type accountingWriter struct {
	ctx      context.Context
	inner    io.WriteCloser
	recorder TenantSideExternalIORecorder
	limit    int64

	count int64
}

var _ io.WriteCloser = (*accountingWriter)(nil)

func (aw *accountingWriter) Write(d []byte) (int, error) {
	// If past writes have pushed us past the limit, account for them before allowing this write.
	aw.maybeWaitForRUs()

	// If this single write is larger than the limit, immediately account for it.
	if int64(len(d)) > aw.limit {
		return aw.immediatelyAccountedWrite(d)
	}

	n, err := aw.inner.Write(d)
	aw.count += int64(n)
	return n, err
}

func (aw *accountingWriter) immediatelyAccountedWrite(d []byte) (int, error) {
	writeLen := int64(len(d))
	aw.recorder.ExternalIOWriteWait(aw.ctx, writeLen)
	n, err := aw.inner.Write(d)
	if err != nil {
		aw.recorder.ExternalIOWriteFailure(aw.ctx, int64(n), writeLen-int64(n))
		return n, err
	}
	aw.recorder.ExternalIOWriteSuccess(aw.ctx, int64(n))
	return n, err
}

// Close closes the underlying Writer and also waits for any RUs that weren't accounted for on a
// previous call to Write.
func (aw *accountingWriter) Close() error {
	aw.recorder.ExternalIOWriteSuccess(aw.ctx, aw.count)
	aw.count = 0
	return aw.inner.Close()
}

func (aw *accountingWriter) maybeWaitForRUs() {
	if aw.limit > 0 && aw.count >= aw.limit {
		aw.recorder.ExternalIOWriteSuccess(aw.ctx, aw.count)
		aw.count = 0
	}
}

// WriteFileAccounted is like cloud.WriteFile but requests RUs for the written file if a non-nil
// recorder is passed and the given store indicates it should be accounted.
func WriteFileAccounted(
	ctx context.Context,
	recorder TenantSideExternalIORecorder,
	es cloud.ExternalStorage,
	filename string,
	data []byte,
) error {
	if !es.RequiresExternalIOAccounting() || recorder == nil {
		_, err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(data))
		return err
	}

	bytesToWrite := int64(len(data))
	recorder.ExternalIOWriteWait(ctx, int64(bytesToWrite))
	bytesWritten, err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(data))
	if err != nil {
		recorder.ExternalIOWriteFailure(ctx, bytesWritten, bytesToWrite-bytesWritten)
		return nil
	}
	recorder.ExternalIOWriteSuccess(ctx, bytesWritten)
	return nil
}

// accountingReader is an ioctx.ReadCloser that tracks how many total bytes have been read. If limit is > 0, then the
// reader will record the read bytes and wait for the associated RUs in a Read call if more than limit bytes have been
// written. On Close, any previously unaccounted for RUs will be recorded.
//
// If limit <= 0 then we will wait for RUs only on Close().
type accountingReader struct {
	inner    ioctx.ReadCloserCtx
	recorder TenantSideExternalIORecorder
	limit    int64

	count int64
}

var _ ioctx.ReadCloserCtx = (*accountingReader)(nil)

// Read implements ioctx.ReadCloserCtx.
func (ar *accountingReader) Read(ctx context.Context, d []byte) (int, error) {
	// If past writes have pushed us past the limit, account for them before allowing this read.
	if ar.limit > 0 && ar.count >= ar.limit {
		ar.recorder.ExternalIOWriteSuccess(ctx, ar.count)
		ar.count = 0
	}

	n, err := ar.inner.Read(ctx, d)
	ar.count += int64(n)
	return n, err
}

// Close implements ioctx.ReadCloserCtx.
func (ar *accountingReader) Close(ctx context.Context) error {
	ar.recorder.ExternalIOReadWait(ctx, ar.count)
	ar.count = 0
	return ar.inner.Close(ctx)
}
