// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
	"google.golang.org/grpc/encoding"
)

const (
	// chunkTypeDecompressedLength is an extension to the Snappy encoding format
	// which is used to annotate Snappy-compressed bytes with their decompressed
	// size. This allows snappyCompressor to implement the DecompressedSize
	// method.
	//
	// The chunk type's value is part of Snappy's reserved skippable chunk types,
	// allowing us to create our own meaning for the chunk. Snappy decoders will
	// ignore the chunk, so it need not be stripped before being passed through a
	// decoder.
	//
	// Recall from the framing format[^1] that
	// > Each chunk consists first a single byte of chunk identifier, then
	// > a three-byte little-endian length of the chunk in bytes (from 0 to
	// > 16777215, inclusive), and then the data if any. The four bytes of
	// > chunk header is not counted in the data length.
	//
	// The encoding format of the chunk type looks like
	//
	//   0xb0 [len(varint(len(buf)))] 0 0 [varint(len(buf))...]
	//
	// [^1]: https://github.com/google/snappy/blob/main/framing_format.txt
	chunkTypeDecompressedLength        = 0xb0
	chunkTypeDecompressedLengthMaxSize = 4 + binary.MaxVarintLen64
)

// NB: The encoding.Compressor implementation needs to be goroutine
// safe as multiple goroutines may be using the same compressor for
// different streams on the same connection.
var snappyWriterPool = sync.Pool{
	New: func() interface{} {
		// We use the deprecated snappy.NewWriter constructor instead of the newer
		// snappy.NewBufferedWriter constructor to avoid internal input buffering.
		// The objects Write method is only called once, so the internal buffering
		// is unnecessary. Meanwhile, removing it avoids a memcpy of the input byte
		// slice and a pooled 64KB buffer (snappy.Writer.ibuf).
		//
		// If the deprecated API is ever removed, it would not be a major loss to
		// switch back to the buffering writer. While it's still around, though, we
		// might as well use it.
		//
		//lint:ignore SA1019 snappy.NewWriter is deprecated
		return &snappyWriter{snappy: snappy.NewWriter(nil)}
	},
}
var snappyReaderPool = sync.Pool{
	New: func() interface{} {
		return &snappyReader{snappy: snappy.NewReader(nil)}
	},
}

type snappyWriter struct {
	snappy *snappy.Writer

	// Fields used to track and write the decompressed length chunk.
	inner    io.Writer
	wroteLen int
	buf      [chunkTypeDecompressedLengthMaxSize]byte
}

func (w *snappyWriter) Write(p []byte) (n int, err error) {
	w.wroteLen += len(p)
	return w.snappy.Write(p)
}

func (w *snappyWriter) Close() error {
	defer w.release()
	if w.wroteLen == 0 {
		// If we never wrote anything, just close the snappy Writer.
		return w.snappy.Close()
	}
	// Flush the snappy Writer into the inner writer.
	if err := w.snappy.Flush(); err != nil {
		return errors.Wrapf(err, "flushing snappy writer")
	}
	// Then write the decompressed length chunk at the end. Make sure to do so in
	// the inner Writer so that we don't snappy compress the custom chunk type.
	n := writeDecompressedLength(&w.buf, w.wroteLen)
	if _, err := w.inner.Write(w.buf[:n]); err != nil {
		return errors.Wrapf(err, "writing decompressed size chunk")
	}
	// Finally, close the snappy Writer.
	return w.snappy.Close()
}

func (w *snappyWriter) release() {
	*w = snappyWriter{snappy: w.snappy}
	w.snappy.Reset(nil) // for GC
	snappyWriterPool.Put(w)
}

type snappyReader struct {
	snappy *snappy.Reader
}

func (r *snappyReader) Read(p []byte) (n int, err error) {
	n, err = r.snappy.Read(p)
	if err == io.EOF {
		r.release()
	}
	return n, err
}

func (r *snappyReader) release() {
	*r = snappyReader{snappy: r.snappy}
	r.snappy.Reset(nil) // for GC
	snappyReaderPool.Put(r)
}

type snappyCompressor struct {
}

func (snappyCompressor) Name() string {
	return "snappy"
}

func (snappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	sw := snappyWriterPool.Get().(*snappyWriter)
	sw.snappy.Reset(w)
	sw.inner = w
	return sw, nil
}

func (snappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	sr := snappyReaderPool.Get().(*snappyReader)
	sr.snappy.Reset(r)
	return sr, nil
}

// DecompressedSize returns the post-decompression size of the provided snappy
// compressed data. The method returns -1 if the decompressed size cannot be
// determined.
//
// The method implements the anonymous interface declared in grpc/rpc_util.go's
// decompress function, which is mentioned in a comment on encoding.Compressor
// but not named. When implemented by an encoding.Compressor, the function can
// pre-allocate a buffer with the exact decompressed size instead of needing to
// dynamically grow the buffer while decompressing (in io.ReadAll), which can
// incur multiple allocations and memory copies.
func (snappyCompressor) DecompressedSize(p []byte) int {
	v, ok := readDecompressedLength(p)
	if !ok {
		// No decompressed size chunk.
		return -1
	}
	return v
}

// writeDecompressedLength writes the decompressed length chunk to the provided
// array. It returns the number of bytes used in the array.
func writeDecompressedLength(dst *[chunkTypeDecompressedLengthMaxSize]byte, v int) int {
	chunkLen := binary.PutUvarint(dst[4:], uint64(v))
	dst[0] = chunkTypeDecompressedLength
	// chunkLen <= MaxVarintLen64 (10), so it always fits in the first uint8.
	dst[1] = uint8(chunkLen)
	dst[2] = 0
	dst[3] = 0
	return 4 + chunkLen
}

// readDecompressedLength reads the decompressed length from the snappy
// compressed data. It does so by stepping through the data chunk by chunk until
// it finds the decompressed length chunk.
//
// If the data contains multiple decompressed length chunks, the value in the
// first will be returned. We could change this if it ever becomes important.
//
// The function returns (0, false) if the decompressed length chunk is missing.
func readDecompressedLength(p []byte) (int, bool) {
	for {
		if len(p) < 4 {
			// Corrupt chunk.
			return 0, false
		}
		chunkType := p[0]
		chunkLen := int(p[1]) | int(p[2])<<8 | int(p[3])<<16
		p = p[4:] // strip chunk header
		if chunkType != chunkTypeDecompressedLength {
			if chunkLen > len(p) {
				// Corrupt chunk.
				return 0, false
			}
			// Jump to next chunk.
			p = p[chunkLen:]
			continue
		}
		// Decode the decompressed length chunk.
		v, n := binary.Uvarint(p)
		if n <= 0 {
			// Uvarint decoding failed.
			return 0, false
		}
		if n != chunkLen {
			// Corrupt decompressed length chunk. We don't strictly need
			// to detect this case, but it acts as a useful sanity check.
			return 0, false
		}
		return int(v), true
	}
}

func init() {
	encoding.RegisterCompressor(snappyCompressor{})
}
