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
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
	"google.golang.org/grpc/encoding"
)

const (
	// github.com/golang/snappy defines:
	// 	chunkTypeCompressedData   = 0x00
	//	chunkTypeUncompressedData = 0x01
	//	chunkTypePadding          = 0xfe
	//	chunkTypeStreamIdentifier = 0xff

	// chunkTypeLengthPrefixIdentifier is an extension to the Snappy encoding
	// format (https://github.com/google/snappy/blob/main/framing_format.txt)
	// which is used to prefix Snappy-compressed bytes with their decompressed
	// size. This allows snappyCompressor to implement the DecompressedSize
	// method.
	//
	// With the optional length prefix, the encoding format looks like
	//
	//   0xfd [varint(len(buf))...] [snappy.encode(buf)...]
	//
	chunkTypeLengthPrefixIdentifier = 0xfd
	lengthPrefixMaxLen              = 1 + binary.MaxVarintLen64
)

var (
	// errCorruptLenPrefix reports that the length prefix of the input is invalid.
	errCorruptLenPrefix = errors.New("snappy: corrupt length prefix")
)

// NB: The encoding.Compressor implementation needs to be goroutine
// safe as multiple goroutines may be using the same compressor for
// different streams on the same connection.
var snappyWriterPool = sync.Pool{
	New: func() interface{} {
		return &snappyWriter{snappy: snappy.NewBufferedWriter(nil)}
	},
}
var snappyReaderPool = sync.Pool{
	New: func() interface{} {
		return &snappyReader{snappy: snappy.NewReader(nil)}
	},
}

type snappyWriter struct {
	snappy *snappy.Writer
	inner  io.Writer

	lenPrefixEnabled bool
	wroteLen         bool
	buf              [lengthPrefixMaxLen]byte
}

func (w *snappyWriter) Write(p []byte) (n int, err error) {
	if w.lenPrefixEnabled {
		if !w.wroteLen {
			w.wroteLen = true
			// Pre-size the inner buffer's capacity (if supported) to a reasonable size
			// to help avoid a reallocation when the compressed bytes are written after
			// the prefix.
			if g, ok := w.inner.(interface{ Grow(n int) }); ok {
				g.Grow(128)
			}
			// Prefix the compressed bytes with their uncompressed length.
			w.buf[0] = chunkTypeLengthPrefixIdentifier
			n = binary.PutUvarint(w.buf[1:], uint64(len(p)))
			n, err = w.inner.Write(w.buf[:n+1])
			if err != nil {
				return n, err
			}
		} else {
			panic("multiple Write calls unexpected")
		}
	}
	return w.snappy.Write(p)
}

func (w *snappyWriter) Close() error {
	defer w.release()
	return w.snappy.Close()
}

func (w *snappyWriter) release() {
	*w = snappyWriter{snappy: w.snappy}
	w.snappy.Reset(nil) // for GC
	snappyWriterPool.Put(w)
}

type snappyReader struct {
	snappy *snappy.Reader
	inner  io.ReadSeeker

	readLen bool
	buf     [lengthPrefixMaxLen]byte
}

func (r *snappyReader) Read(p []byte) (n int, err error) {
	if !r.readLen {
		r.readLen = true
		n, err = r.inner.Read(r.buf[:])
		if err != nil && (err != io.EOF || n == 0) {
			return 0, err
		}
		var seekOffset int64
		if r.buf[0] == chunkTypeLengthPrefixIdentifier {
			// Length prefix was present. Seek past the prefix, ignoring the value,
			// which is only consulted in DecompressedSize.
			_, n = binary.Uvarint(r.buf[1:n])
			if n <= 0 {
				// Uvarint decoding failed.
				return 0, errCorruptLenPrefix
			}
			seekOffset = int64(1 + n)
		} else {
			// Length prefix was not present. Restore the reader's offset so that the
			// snappy.Reader reads from the beginning.
			seekOffset = 0
		}
		_, err = r.inner.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}
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
	lenPrefixEnabled atomic.Bool
}

func (*snappyCompressor) Name() string {
	return "snappy"
}

func (c *snappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	sw := snappyWriterPool.Get().(*snappyWriter)
	sw.snappy.Reset(w)
	sw.inner = w
	sw.lenPrefixEnabled = c.lenPrefixEnabled.Load()
	return sw, nil
}

func (*snappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	rs, ok := r.(io.ReadSeeker)
	if !ok {
		return nil, errors.Errorf("io.Reader given to snappyCompressor must implement io.Seeker, found %T", r)
	}
	sr := snappyReaderPool.Get().(*snappyReader)
	sr.snappy.Reset(r)
	sr.inner = rs
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
func (*snappyCompressor) DecompressedSize(p []byte) int {
	if len(p) == 0 || p[0] != chunkTypeLengthPrefixIdentifier {
		// Not length prefix encoded.
		return -1
	}
	v, n := binary.Uvarint(p[1:])
	if n <= 0 {
		// Uvarint decoding failed.
		return -1
	}
	return int(v)
}

// setLengthPrefixingEnabled configures the snappy compressor to prefix bytes
// with their uncompressed length. Length prefixing should only be enabled if
// all nodes in the cluster understand how to interpret cockroach's length
// prefixing encoding extension.
func (c *snappyCompressor) setLengthPrefixingEnabled(b bool) {
	c.lenPrefixEnabled.Store(b)
}

var globalSnappyCompressor = &snappyCompressor{}

func init() {
	encoding.RegisterCompressor(globalSnappyCompressor)
}
