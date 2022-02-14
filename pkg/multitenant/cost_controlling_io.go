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
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

// DefaultBytesAllowedBeforeAccounting are how maybe bytes we will read.write before trying to wait for
// RUs. The goal here is to avoid waiting in loops for the common case without allowing an
// unbounded number of bytes read/written before accounting for them.
const DefaultBytesAllowedBeforeAccounting = 128 << 20 // 128 MB

// readWriteAccounter is cloud.ReadWriteInterceptor that records ingress and egress bytes.
type readWriteAccounter struct {
	recorder TenantSideExternalIORecorder
	limit    int64
}

// NewReadWriteAccounter returns a cloud.ExternalStorage
// that records ingress and egress bytes iff the storage requires
// external accounting.
//
// Ingress and egress bytes are recorded once at least limit bytes
// have been read or written.
func NewReadWriteAccounter(
	recorder TenantSideExternalIORecorder, limit int64,
) cloud.ReadWriterInterceptor {
	if recorder == nil {
		return nil
	}
	return &readWriteAccounter{
		limit:    limit,
		recorder: recorder,
	}
}

func (a *readWriteAccounter) Writer(
	ctx context.Context, s cloud.ExternalStorage, w io.WriteCloser,
) io.WriteCloser {
	if !s.RequiresExternalIOAccounting() {
		return w
	}
	return &accountingWriter{
		ctx:      ctx,
		inner:    w,
		limit:    a.limit,
		recorder: a.recorder,
	}
}

func (a *readWriteAccounter) Reader(
	ctx context.Context, s cloud.ExternalStorage, r ioctx.ReadCloserCtx,
) ioctx.ReadCloserCtx {
	if !s.RequiresExternalIOAccounting() {
		return r
	}
	fmt.Println("MAKING READER")
	return &accountingReader{
		inner:    r,
		limit:    a.limit,
		recorder: a.recorder,
	}
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
