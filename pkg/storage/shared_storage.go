// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/pebble/objstorage/shared"
)

// externalStorageReader wraps an ioctx.ReadCloserCtx returned by
// externalStorageWrapper and conforms to the io.ReadCloser interface expected
// by Pebble's shared.Storage.
type externalStorageReader struct {
	// Store a reference to the parent Pebble instance. Metrics around shared
	// storage reads/writes are stored there.
	//
	// TODO(bilal): Refactor the metrics out of Pebble, and store a reference
	// to just the Metrics struct.
	p   *Pebble
	r   ioctx.ReadCloserCtx
	ctx context.Context
}

var _ io.ReadCloser = &externalStorageReader{}

// Read implements the io.ReadCloser interface.
func (e *externalStorageReader) Read(p []byte) (n int, err error) {
	n, err = e.r.Read(e.ctx, p)
	atomic.AddInt64(&e.p.sharedBytesRead, int64(n))
	return n, err
}

// Close implements the io.ReadCloser interface.
func (e *externalStorageReader) Close() error {
	return e.r.Close(e.ctx)
}

// externalStorageWriter wraps an io.WriteCloser returned by
// externalStorageWrapper and tracks metrics on bytes written to shared storage.
type externalStorageWriter struct {
	io.WriteCloser

	// Store a reference to the parent Pebble instance. Metrics around shared
	// storage reads/writes are stored there.
	//
	// TODO(bilal): Refactor the metrics out of Pebble, and store a reference
	// to just the Metrics struct.
	p *Pebble
}

var _ io.WriteCloser = &externalStorageWriter{}

// Write implements the io.Writer interface.
func (e *externalStorageWriter) Write(p []byte) (n int, err error) {
	n, err = e.WriteCloser.Write(p)
	atomic.AddInt64(&e.p.sharedBytesWritten, int64(n))
	return n, err
}

// externalStorageWrapper wraps a cloud.ExternalStorage and implements the
// shared.Storage interface expected by Pebble. Also ensures reads and writes
// to shared cloud storage are tracked in store-specific metrics.
type externalStorageWrapper struct {
	p   *Pebble
	es  cloud.ExternalStorage
	ctx context.Context
}

var _ shared.Storage = &externalStorageWrapper{}

// Close implements the shared.Storage interface.
func (e *externalStorageWrapper) Close() error {
	return e.es.Close()
}

// ReadObjectAt implements the shared.Storage interface.
func (e *externalStorageWrapper) ReadObjectAt(
	basename string, offset int64,
) (io.ReadCloser, int64, error) {
	reader, n, err := e.es.ReadFileAt(e.ctx, basename, offset)
	return &externalStorageReader{p: e.p, r: reader, ctx: e.ctx}, n, err
}

// CreateObject implements the shared.Storage interface.
func (e *externalStorageWrapper) CreateObject(basename string) (io.WriteCloser, error) {
	writer, err := e.es.Writer(e.ctx, basename)
	return &externalStorageWriter{WriteCloser: writer, p: e.p}, err
}

// List implements the shared.Storage interface.
func (e *externalStorageWrapper) List(prefix, delimiter string) ([]string, error) {
	var directoryList []string
	err := e.es.List(e.ctx, prefix, delimiter, func(s string) error {
		directoryList = append(directoryList, s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return directoryList, nil
}

// Delete implements the shared.Storage interface.
func (e *externalStorageWrapper) Delete(basename string) error {
	return e.es.Delete(e.ctx, basename)
}

// Size implements the shared.Storage interface.
func (e *externalStorageWrapper) Size(basename string) (int64, error) {
	return e.es.Size(e.ctx, basename)
}
