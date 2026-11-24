// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"io"
)

// AbortableWriter is a wrapper around a cloud writer that implements the
// pebble objstorage.Writable API.
type AbortableWriter struct {
	writer io.WriteCloser
	cancel context.CancelFunc
}

// OpenAbortableWriter creates a writer compatible with objstorage.Writable
// instead of an io.WriteCloser.
func OpenAbortableWriter(
	ctx context.Context, storage ExternalStorage, basename string,
) (*AbortableWriter, error) {
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

	return &AbortableWriter{writer: fw, cancel: cancel}, nil
}

func (f *AbortableWriter) Write(p []byte) (err error) {
	n, err := f.writer.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

func (f *AbortableWriter) StartMetadataPortion() error { return nil }

func (f *AbortableWriter) Finish() error {
	defer f.cancel()
	return f.writer.Close()
}

func (f *AbortableWriter) Abort() {
	f.cancel()
	_ = f.writer.Close()
}
