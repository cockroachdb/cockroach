// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ioextended provides io utilities.
package ioextended

import (
	"bytes"
	"io"
	"sync"

	"go.uber.org/multierr"
)

var (
	// DiscardReader is an io.Reader in which all calls return 0 and io.EOF.
	DiscardReader io.Reader = discardReader{}
	// DiscardReadCloser is an io.ReadCloser in which all calls return 0 and io.EOF.
	DiscardReadCloser io.ReadCloser = io.NopCloser(DiscardReader)
	// DiscardWriteCloser is a discard io.WriteCloser.
	DiscardWriteCloser io.WriteCloser = NopWriteCloser(io.Discard)
	// NopCloser is a no-op closer.
	NopCloser = nopCloser{}
)

// NopWriteCloser returns an io.WriteCloser with a no-op Close method wrapping the provided io.Writer.
func NopWriteCloser(writer io.Writer) io.WriteCloser {
	return nopWriteCloser{Writer: writer}
}

// LockedWriter creates a locked Writer.
func LockedWriter(writer io.Writer) io.Writer {
	return &lockedWriter{writer: writer}
}

// CompositeReadCloser returns a io.ReadCloser that is a composite of the Reader and Closer.
func CompositeReadCloser(reader io.Reader, closer io.Closer) io.ReadCloser {
	return compositeReadCloser{Reader: reader, Closer: closer}
}

// CompositeWriteCloser returns a io.WriteCloser that is a composite of the Writer and Closer.
func CompositeWriteCloser(writer io.Writer, closer io.Closer) io.WriteCloser {
	return compositeWriteCloser{Writer: writer, Closer: closer}
}

// ChainCloser chains the closers by calling them in order.
func ChainCloser(closers ...io.Closer) io.Closer {
	return chainCloser{closers: closers}
}

// ReaderAtForReader converts an io.Reader to an io.ReaderAt.
func ReaderAtForReader(reader io.Reader) (io.ReaderAt, error) {
	if readerAt, ok := reader.(io.ReaderAt); ok {
		return readerAt, nil
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

type discardReader struct{}

func (discardReader) Read([]byte) (int, error) {
	return 0, io.EOF
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}

type nopCloser struct{}

func (nopCloser) Close() error {
	return nil
}

type lockedWriter struct {
	writer io.Writer
	lock   sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.lock.Lock()
	n, err := l.writer.Write(p)
	l.lock.Unlock()
	return n, err
}

type compositeReadCloser struct {
	io.Reader
	io.Closer
}

type compositeWriteCloser struct {
	io.Writer
	io.Closer
}

type chainCloser struct {
	closers []io.Closer
}

func (c chainCloser) Close() error {
	var err error
	for _, closer := range c.closers {
		err = multierr.Append(err, closer.Close())
	}
	return err
}
