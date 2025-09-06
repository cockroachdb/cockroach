// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"io"

	"github.com/cockroachdb/pebble/objstorage"
)

// PebbleWriter is a wrapper around a cloud writer that implements the pebble
// objstorage.Writable API.
type PebbleWriter struct {
	writer io.WriteCloser
	cancel context.CancelFunc
}

var _ objstorage.Writable = &PebbleWriter{}

// OpenPebbleWriter creates a pebble writer instead of an io.WriteCloser.
func OpenPebbleWriter(
	ctx context.Context, storage ExternalStorage, basename string,
) (objstorage.Writable, error) {
	// We create a context here because of how Close functions in the cloud
	// storage implementations. Calling Close() while the context is valid
	// flushes the write. Whereas calling Close() while the context is cancelled
	// aborts the write.
	ctx, cancel := context.WithCancel(ctx)

	fw, err := storage.Writer(ctx, basename)
	if err != nil {
		cancel()
		return nil, err
	}

	return &PebbleWriter{writer: fw, cancel: cancel}, nil
}

func (f *PebbleWriter) Write(p []byte) (err error) {
	n, err := f.writer.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

func (f *PebbleWriter) Finish() error {
	defer f.cancel()
	return f.writer.Close()
}

func (f *PebbleWriter) Abort() {
	f.cancel()
	_ = f.writer.Close()
}
