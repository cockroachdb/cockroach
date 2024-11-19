// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multitenantio

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

// DefaultBytesAllowedBeforeAccounting are how many bytes we will read/written
// before trying to wait for RUs. The goal here is to avoid waiting in loops in
// the common case, but without allowing an unbounded number of bytes to be
// read/written before accounting for them.
var DefaultBytesAllowedBeforeAccounting = settings.RegisterIntSetting(
	settings.SystemVisible,
	"tenant_external_io_default_bytes_allowed_before_accounting",
	"controls how many bytes will be read/written before blocking for RUs when writing to external storage",
	16<<20, // 16 MB
	settings.WithName("tenant_cost_control.external_io.byte_usage_allowance"),
	settings.PositiveInt,
)

// readWriteAccounter is cloud.ReadWriteInterceptor that records ingress and
// egress bytes.
type readWriteAccounter struct {
	recorder multitenant.TenantSideExternalIORecorder
	limit    int64
}

// NewReadWriteAccounter returns a cloud.ExternalStorage
// that records ingress and egress bytes iff the storage requires
// external accounting.
//
// Ingress and egress bytes are recorded once at least limit bytes
// have been read or written.
func NewReadWriteAccounter(
	recorder multitenant.TenantSideExternalIORecorder, limit int64,
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
	_ context.Context, s cloud.ExternalStorage, r ioctx.ReadCloserCtx,
) ioctx.ReadCloserCtx {
	if !s.RequiresExternalIOAccounting() {
		return r
	}
	return &accountingReader{
		inner:    r,
		limit:    a.limit,
		recorder: a.recorder,
	}
}

// accountingWriter is an io.WriteCloser that tracks how many total bytes have
// been written. If limit is > 0, then the writer will record the written bytes
// and wait for the associated RUs in a Write call if more than limit bytes have
// been written. On Close, any previously unaccounted for RUs will be recorded.
//
// If limit <= 0 then we will wait for RUs only on Close().
//
// NB: The implementation optimistically allows Write calls to proceed until the
// rate limiter goes into debt.
type accountingWriter struct {
	ctx      context.Context
	inner    io.WriteCloser
	recorder multitenant.TenantSideExternalIORecorder
	limit    int64

	count int64
}

var _ io.WriteCloser = (*accountingWriter)(nil)

func (aw *accountingWriter) Write(d []byte) (int, error) {
	// If past writes have pushed us past the limit, account for them before
	// allowing this write.
	if err := aw.maybeWaitForRUs(); err != nil {
		return 0, err
	}

	// If this single write is larger than the limit, flush any previously
	// written bytes and block until the rate limiter is not in debt before
	// proceeding.
	if int64(len(d)) > aw.limit {
		if err := aw.waitForRUs(); err != nil {
			return 0, err
		}
	}

	n, err := aw.inner.Write(d)
	aw.count += int64(n)
	return n, err
}

// Close closes the underlying Writer and also waits for any RUs that weren't
// accounted for on a previous call to Write.
func (aw *accountingWriter) Close() error {
	// NB: We only record bytes actually written (according to the underlying
	// writer) in aw.count.
	if err := aw.waitForRUs(); err != nil {
		// We still want to close the underlying writer.
		_ = aw.inner.Close()
		return err
	}
	return aw.inner.Close()
}

// maybeWaitForRUs checks if the count of written bytes exceeds the limit, and
// then blocks until the rate limiter allows that count.
func (aw *accountingWriter) maybeWaitForRUs() error {
	if aw.limit > 0 && aw.count >= aw.limit {
		return aw.waitForRUs()
	}
	return nil
}

// waitForRUs blocks until the rate limiter allows at least the count of already
// written bytes.
func (aw *accountingWriter) waitForRUs() error {
	usage := multitenant.ExternalIOUsage{EgressBytes: aw.count}
	if err := aw.recorder.OnExternalIOWait(aw.ctx, usage); err != nil {
		return err
	}
	aw.count = 0
	return nil
}

// accountingReader is an ioctx.ReadCloser that tracks how many total bytes have
// been read. If limit is > 0, then the reader will record the read bytes and
// wait for the associated RUs in a Read call if more than limit bytes have been
// read. On Close, any previously unaccounted for RUs will be recorded.
//
// If limit <= 0 then we will wait for RUs only on Close().
type accountingReader struct {
	inner    ioctx.ReadCloserCtx
	recorder multitenant.TenantSideExternalIORecorder
	limit    int64
	count    int64
}

var _ ioctx.ReadCloserCtx = (*accountingReader)(nil)

// Read implements ioctx.ReadCloserCtx.
func (ar *accountingReader) Read(ctx context.Context, d []byte) (int, error) {
	// If past reads have pushed us past the limit, account for them before
	// allowing this read.
	if ar.limit > 0 && ar.count >= ar.limit {
		usage := multitenant.ExternalIOUsage{IngressBytes: ar.count}
		if err := ar.recorder.OnExternalIOWait(ctx, usage); err != nil {
			return 0, err
		}
		ar.count = 0
	}

	n, err := ar.inner.Read(ctx, d)
	ar.count += int64(n)
	return n, err
}

// Close implements ioctx.ReadCloserCtx.
func (ar *accountingReader) Close(ctx context.Context) error {
	usage := multitenant.ExternalIOUsage{IngressBytes: ar.count}
	if err := ar.recorder.OnExternalIOWait(ctx, usage); err != nil {
		_ = ar.inner.Close(ctx)
		return err
	}
	ar.count = 0
	return ar.inner.Close(ctx)
}
