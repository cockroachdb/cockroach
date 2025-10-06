// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// externalStorageReader implements remote.ObjectReader on top of
// cloud.ExternalStorage..
type externalStorageReader struct {
	// Store a reference to the parent Pebble instance. Metrics around remote
	// storage reads/writes are stored there.
	//
	// TODO(bilal): Refactor the metrics out of Pebble, and store a reference
	// to just the Metrics struct.
	p       *Pebble
	es      cloud.ExternalStorage
	objName string
}

var _ remote.ObjectReader = (*externalStorageReader)(nil)

func (r *externalStorageReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	reader, _, err := r.es.ReadFile(ctx, r.objName, cloud.ReadOptions{
		Offset:     offset,
		LengthHint: int64(len(p)),
		NoFileSize: true,
	})
	if err != nil {
		return err
	}
	defer reader.Close(ctx)
	for n := 0; n < len(p); {
		nn, err := reader.Read(ctx, p[n:])
		// The io.Reader interface allows for io.EOF to be returned even if we just
		// successfully filled the buffer p and hit the end of file at the same
		// time. Treat that case as a successful read.
		if err != nil && !(errors.Is(err, io.EOF) && len(p) == nn+n) {
			return err
		}
		n += nn
	}
	atomic.AddInt64(&r.p.sharedBytesRead, int64(len(p)))
	return nil
}

// Close is part of the remote.ObjectReader interface.
func (e *externalStorageReader) Close() error {
	*e = externalStorageReader{}
	return nil
}

// externalStorageWriter wraps an io.WriteCloser returned by
// externalStorageWrapper and tracks metrics on bytes written to remote storage.
type externalStorageWriter struct {
	io.WriteCloser

	// Store a reference to the parent Pebble instance. Metrics around remote
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
// remote.Storage interface expected by Pebble. Also ensures reads and writes
// to remote cloud storage are tracked in store-specific metrics.
type externalStorageWrapper struct {
	p   *Pebble
	es  cloud.ExternalStorage
	ctx context.Context
}

// MakeExternalStorageWrapper returns a remote.Storage implementation that wraps
// cloud.ExternalStorage.
func MakeExternalStorageWrapper(ctx context.Context, es cloud.ExternalStorage) remote.Storage {
	return &externalStorageWrapper{p: &Pebble{}, es: es, ctx: ctx}
}

var _ remote.Storage = &externalStorageWrapper{}

// Close implements the remote.Storage interface.
func (e *externalStorageWrapper) Close() error {
	return e.es.Close()
}

// ReadObject implements the remote.Storage interface.
func (e *externalStorageWrapper) ReadObject(
	ctx context.Context, objName string,
) (_ remote.ObjectReader, objSize int64, _ error) {
	objSize, err := e.es.Size(ctx, objName)
	if err != nil {
		return nil, 0, err
	}
	return &externalStorageReader{
		p:       e.p,
		es:      e.es,
		objName: objName,
	}, objSize, nil
}

// CreateObject implements the remote.Storage interface.
func (e *externalStorageWrapper) CreateObject(objName string) (io.WriteCloser, error) {
	writer, err := e.es.Writer(e.ctx, objName)
	return &externalStorageWriter{WriteCloser: writer, p: e.p}, err
}

// List implements the remote.Storage interface.
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

// Delete implements the remote.Storage interface.
func (e *externalStorageWrapper) Delete(objName string) error {
	return e.es.Delete(e.ctx, objName)
}

// Size implements the remote.Storage interface.
func (e *externalStorageWrapper) Size(objName string) (int64, error) {
	return e.es.Size(e.ctx, objName)
}

func (e *externalStorageWrapper) IsNotExistError(err error) bool {
	return errors.Is(err, cloud.ErrFileDoesNotExist)
}
