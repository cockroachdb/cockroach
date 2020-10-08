// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"io"
	"sync"

	"github.com/golang/snappy"
	"google.golang.org/grpc/encoding"
)

// NB: The encoding.Compressor implementation needs to be goroutine
// safe as multiple goroutines may be using the same compressor for
// different streams on the same connection.
var snappyWriterPool sync.Pool
var snappyReaderPool sync.Pool

type snappyWriter struct {
	*snappy.Writer
}

func (w *snappyWriter) Close() error {
	defer snappyWriterPool.Put(w)
	return w.Writer.Close()
}

type snappyReader struct {
	*snappy.Reader
}

func (r *snappyReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	if err == io.EOF {
		snappyReaderPool.Put(r)
	}
	return n, err
}

type snappyCompressor struct {
}

func (snappyCompressor) Name() string {
	return "snappy"
}

func (snappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	sw, ok := snappyWriterPool.Get().(*snappyWriter)
	if !ok {
		sw = &snappyWriter{snappy.NewBufferedWriter(w)}
	} else {
		sw.Reset(w)
	}
	return sw, nil
}

func (snappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	sr, ok := snappyReaderPool.Get().(*snappyReader)
	if !ok {
		sr = &snappyReader{snappy.NewReader(r)}
	} else {
		sr.Reset(r)
	}
	return sr, nil
}

func init() {
	encoding.RegisterCompressor(snappyCompressor{})
}
