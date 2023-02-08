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
)

type externalStorageReader struct {
	p   *Pebble
	r   ioctx.ReadCloserCtx
	ctx context.Context
}

// Read implements the io.ReadCloser interface.
func (e *externalStorageReader) Read(p []byte) (n int, err error) {
	n, err = e.r.Read(e.ctx, p)
	atomic.AddInt64(&e.p.sharedBytesRead, int64(n))
	return n, err
}

// Close implements the io.ReadCloser interface.
func (e *externalStorageReader) Close() error {
	return e.Close()
}

type externalStorageWriter struct {
	io.WriteCloser

	p *Pebble
}

// Write implements the io.Writer interface.
func (e *externalStorageWriter) Write(p []byte) (n int, err error) {
	n, err = e.WriteCloser.Write(p)
	atomic.AddInt64(&e.p.sharedBytesWritten, int64(n))
	return n, err
}

type externalStorageWrapper struct {
	p   *Pebble
	es  cloud.ExternalStorage
	ctx context.Context
}

// Close implements the shared.Storage interface.
func (e *externalStorageWrapper) Close() error {
	return e.es.Close()
}

// ReadObjectAt implements the shared.Storage interface.
func (e *externalStorageWrapper) ReadObjectAt(basename string, offset int64) (io.ReadCloser, int64, error) {
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
