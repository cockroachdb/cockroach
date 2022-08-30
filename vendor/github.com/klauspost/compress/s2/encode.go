// Copyright 2011 The Snappy-Go Authors. All rights reserved.
// Copyright (c) 2019 Klaus Post. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package s2

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"runtime"
	"sync"
)

// Encode returns the encoded form of src. The returned slice may be a sub-
// slice of dst if dst was large enough to hold the entire encoded block.
// Otherwise, a newly allocated slice will be returned.
//
// The dst and src must not overlap. It is valid to pass a nil dst.
//
// The blocks will require the same amount of memory to decode as encoding,
// and does not make for concurrent decoding.
// Also note that blocks do not contain CRC information, so corruption may be undetected.
//
// If you need to encode larger amounts of data, consider using
// the streaming interface which gives all of these features.
func Encode(dst, src []byte) []byte {
	if n := MaxEncodedLen(len(src)); n < 0 {
		panic(ErrTooLarge)
	} else if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	// The block starts with the varint-encoded length of the decompressed bytes.
	d := binary.PutUvarint(dst, uint64(len(src)))

	if len(src) == 0 {
		return dst[:d]
	}
	if len(src) < minNonLiteralBlockSize {
		d += emitLiteral(dst[d:], src)
		return dst[:d]
	}
	n := encodeBlock(dst[d:], src)
	if n > 0 {
		d += n
		return dst[:d]
	}
	// Not compressible
	d += emitLiteral(dst[d:], src)
	return dst[:d]
}

// EncodeBetter returns the encoded form of src. The returned slice may be a sub-
// slice of dst if dst was large enough to hold the entire encoded block.
// Otherwise, a newly allocated slice will be returned.
//
// EncodeBetter compresses better than Encode but typically with a
// 10-40% speed decrease on both compression and decompression.
//
// The dst and src must not overlap. It is valid to pass a nil dst.
//
// The blocks will require the same amount of memory to decode as encoding,
// and does not make for concurrent decoding.
// Also note that blocks do not contain CRC information, so corruption may be undetected.
//
// If you need to encode larger amounts of data, consider using
// the streaming interface which gives all of these features.
func EncodeBetter(dst, src []byte) []byte {
	if n := MaxEncodedLen(len(src)); n < 0 {
		panic(ErrTooLarge)
	} else if len(dst) < n {
		dst = make([]byte, n)
	}

	// The block starts with the varint-encoded length of the decompressed bytes.
	d := binary.PutUvarint(dst, uint64(len(src)))

	if len(src) == 0 {
		return dst[:d]
	}
	if len(src) < minNonLiteralBlockSize {
		d += emitLiteral(dst[d:], src)
		return dst[:d]
	}
	n := encodeBlockBetter(dst[d:], src)
	if n > 0 {
		d += n
		return dst[:d]
	}
	// Not compressible
	d += emitLiteral(dst[d:], src)
	return dst[:d]
}

// EncodeBest returns the encoded form of src. The returned slice may be a sub-
// slice of dst if dst was large enough to hold the entire encoded block.
// Otherwise, a newly allocated slice will be returned.
//
// EncodeBest compresses as good as reasonably possible but with a
// big speed decrease.
//
// The dst and src must not overlap. It is valid to pass a nil dst.
//
// The blocks will require the same amount of memory to decode as encoding,
// and does not make for concurrent decoding.
// Also note that blocks do not contain CRC information, so corruption may be undetected.
//
// If you need to encode larger amounts of data, consider using
// the streaming interface which gives all of these features.
func EncodeBest(dst, src []byte) []byte {
	if n := MaxEncodedLen(len(src)); n < 0 {
		panic(ErrTooLarge)
	} else if len(dst) < n {
		dst = make([]byte, n)
	}

	// The block starts with the varint-encoded length of the decompressed bytes.
	d := binary.PutUvarint(dst, uint64(len(src)))

	if len(src) == 0 {
		return dst[:d]
	}
	if len(src) < minNonLiteralBlockSize {
		d += emitLiteral(dst[d:], src)
		return dst[:d]
	}
	n := encodeBlockBest(dst[d:], src)
	if n > 0 {
		d += n
		return dst[:d]
	}
	// Not compressible
	d += emitLiteral(dst[d:], src)
	return dst[:d]
}

// EncodeSnappy returns the encoded form of src. The returned slice may be a sub-
// slice of dst if dst was large enough to hold the entire encoded block.
// Otherwise, a newly allocated slice will be returned.
//
// The output is Snappy compatible and will likely decompress faster.
//
// The dst and src must not overlap. It is valid to pass a nil dst.
//
// The blocks will require the same amount of memory to decode as encoding,
// and does not make for concurrent decoding.
// Also note that blocks do not contain CRC information, so corruption may be undetected.
//
// If you need to encode larger amounts of data, consider using
// the streaming interface which gives all of these features.
func EncodeSnappy(dst, src []byte) []byte {
	if n := MaxEncodedLen(len(src)); n < 0 {
		panic(ErrTooLarge)
	} else if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	// The block starts with the varint-encoded length of the decompressed bytes.
	d := binary.PutUvarint(dst, uint64(len(src)))

	if len(src) == 0 {
		return dst[:d]
	}
	if len(src) < minNonLiteralBlockSize {
		d += emitLiteral(dst[d:], src)
		return dst[:d]
	}

	n := encodeBlockSnappy(dst[d:], src)
	if n > 0 {
		d += n
		return dst[:d]
	}
	// Not compressible
	d += emitLiteral(dst[d:], src)
	return dst[:d]
}

// EncodeSnappyBetter returns the encoded form of src. The returned slice may be a sub-
// slice of dst if dst was large enough to hold the entire encoded block.
// Otherwise, a newly allocated slice will be returned.
//
// The output is Snappy compatible and will likely decompress faster.
//
// The dst and src must not overlap. It is valid to pass a nil dst.
//
// The blocks will require the same amount of memory to decode as encoding,
// and does not make for concurrent decoding.
// Also note that blocks do not contain CRC information, so corruption may be undetected.
//
// If you need to encode larger amounts of data, consider using
// the streaming interface which gives all of these features.
func EncodeSnappyBetter(dst, src []byte) []byte {
	if n := MaxEncodedLen(len(src)); n < 0 {
		panic(ErrTooLarge)
	} else if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	// The block starts with the varint-encoded length of the decompressed bytes.
	d := binary.PutUvarint(dst, uint64(len(src)))

	if len(src) == 0 {
		return dst[:d]
	}
	if len(src) < minNonLiteralBlockSize {
		d += emitLiteral(dst[d:], src)
		return dst[:d]
	}

	n := encodeBlockBetterSnappy(dst[d:], src)
	if n > 0 {
		d += n
		return dst[:d]
	}
	// Not compressible
	d += emitLiteral(dst[d:], src)
	return dst[:d]
}

// EncodeSnappyBest returns the encoded form of src. The returned slice may be a sub-
// slice of dst if dst was large enough to hold the entire encoded block.
// Otherwise, a newly allocated slice will be returned.
//
// The output is Snappy compatible and will likely decompress faster.
//
// The dst and src must not overlap. It is valid to pass a nil dst.
//
// The blocks will require the same amount of memory to decode as encoding,
// and does not make for concurrent decoding.
// Also note that blocks do not contain CRC information, so corruption may be undetected.
//
// If you need to encode larger amounts of data, consider using
// the streaming interface which gives all of these features.
func EncodeSnappyBest(dst, src []byte) []byte {
	if n := MaxEncodedLen(len(src)); n < 0 {
		panic(ErrTooLarge)
	} else if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	// The block starts with the varint-encoded length of the decompressed bytes.
	d := binary.PutUvarint(dst, uint64(len(src)))

	if len(src) == 0 {
		return dst[:d]
	}
	if len(src) < minNonLiteralBlockSize {
		d += emitLiteral(dst[d:], src)
		return dst[:d]
	}

	n := encodeBlockBestSnappy(dst[d:], src)
	if n > 0 {
		d += n
		return dst[:d]
	}
	// Not compressible
	d += emitLiteral(dst[d:], src)
	return dst[:d]
}

// ConcatBlocks will concatenate the supplied blocks and append them to the supplied destination.
// If the destination is nil or too small, a new will be allocated.
// The blocks are not validated, so garbage in = garbage out.
// dst may not overlap block data.
// Any data in dst is preserved as is, so it will not be considered a block.
func ConcatBlocks(dst []byte, blocks ...[]byte) ([]byte, error) {
	totalSize := uint64(0)
	compSize := 0
	for _, b := range blocks {
		l, hdr, err := decodedLen(b)
		if err != nil {
			return nil, err
		}
		totalSize += uint64(l)
		compSize += len(b) - hdr
	}
	if totalSize == 0 {
		dst = append(dst, 0)
		return dst, nil
	}
	if totalSize > math.MaxUint32 {
		return nil, ErrTooLarge
	}
	var tmp [binary.MaxVarintLen32]byte
	hdrSize := binary.PutUvarint(tmp[:], totalSize)
	wantSize := hdrSize + compSize

	if cap(dst)-len(dst) < wantSize {
		dst = append(make([]byte, 0, wantSize+len(dst)), dst...)
	}
	dst = append(dst, tmp[:hdrSize]...)
	for _, b := range blocks {
		_, hdr, err := decodedLen(b)
		if err != nil {
			return nil, err
		}
		dst = append(dst, b[hdr:]...)
	}
	return dst, nil
}

// inputMargin is the minimum number of extra input bytes to keep, inside
// encodeBlock's inner loop. On some architectures, this margin lets us
// implement a fast path for emitLiteral, where the copy of short (<= 16 byte)
// literals can be implemented as a single load to and store from a 16-byte
// register. That literal's actual length can be as short as 1 byte, so this
// can copy up to 15 bytes too much, but that's OK as subsequent iterations of
// the encoding loop will fix up the copy overrun, and this inputMargin ensures
// that we don't overrun the dst and src buffers.
const inputMargin = 8

// minNonLiteralBlockSize is the minimum size of the input to encodeBlock that
// will be accepted by the encoder.
const minNonLiteralBlockSize = 32

// MaxBlockSize is the maximum value where MaxEncodedLen will return a valid block size.
// Blocks this big are highly discouraged, though.
const MaxBlockSize = math.MaxUint32 - binary.MaxVarintLen32 - 5

// MaxEncodedLen returns the maximum length of a snappy block, given its
// uncompressed length.
//
// It will return a negative value if srcLen is too large to encode.
// 32 bit platforms will have lower thresholds for rejecting big content.
func MaxEncodedLen(srcLen int) int {
	n := uint64(srcLen)
	if n > 0xffffffff {
		// Also includes negative.
		return -1
	}
	// Size of the varint encoded block size.
	n = n + uint64((bits.Len64(n)+7)/7)

	// Add maximum size of encoding block as literals.
	n += uint64(literalExtraSize(int64(srcLen)))
	if n > 0xffffffff {
		return -1
	}
	return int(n)
}

var errClosed = errors.New("s2: Writer is closed")

// NewWriter returns a new Writer that compresses to w, using the
// framing format described at
// https://github.com/google/snappy/blob/master/framing_format.txt
//
// Users must call Close to guarantee all data has been forwarded to
// the underlying io.Writer and that resources are released.
// They may also call Flush zero or more times before calling Close.
func NewWriter(w io.Writer, opts ...WriterOption) *Writer {
	w2 := Writer{
		blockSize:   defaultBlockSize,
		concurrency: runtime.GOMAXPROCS(0),
		randSrc:     rand.Reader,
		level:       levelFast,
	}
	for _, opt := range opts {
		if err := opt(&w2); err != nil {
			w2.errState = err
			return &w2
		}
	}
	w2.obufLen = obufHeaderLen + MaxEncodedLen(w2.blockSize)
	w2.paramsOK = true
	w2.ibuf = make([]byte, 0, w2.blockSize)
	w2.buffers.New = func() interface{} {
		return make([]byte, w2.obufLen)
	}
	w2.Reset(w)
	return &w2
}

// Writer is an io.Writer that can write Snappy-compressed bytes.
type Writer struct {
	errMu    sync.Mutex
	errState error

	// ibuf is a buffer for the incoming (uncompressed) bytes.
	ibuf []byte

	blockSize     int
	obufLen       int
	concurrency   int
	written       int64
	uncompWritten int64 // Bytes sent to compression
	output        chan chan result
	buffers       sync.Pool
	pad           int

	writer   io.Writer
	randSrc  io.Reader
	writerWg sync.WaitGroup
	index    Index

	// wroteStreamHeader is whether we have written the stream header.
	wroteStreamHeader bool
	paramsOK          bool
	snappy            bool
	flushOnWrite      bool
	appendIndex       bool
	level             uint8
}

const (
	levelUncompressed = iota + 1
	levelFast
	levelBetter
	levelBest
)

type result struct {
	b []byte
	// Uncompressed start offset
	startOffset int64
}

// err returns the previously set error.
// If no error has been set it is set to err if not nil.
func (w *Writer) err(err error) error {
	w.errMu.Lock()
	errSet := w.errState
	if errSet == nil && err != nil {
		w.errState = err
		errSet = err
	}
	w.errMu.Unlock()
	return errSet
}

// Reset discards the writer's state and switches the Snappy writer to write to w.
// This permits reusing a Writer rather than allocating a new one.
func (w *Writer) Reset(writer io.Writer) {
	if !w.paramsOK {
		return
	}
	// Close previous writer, if any.
	if w.output != nil {
		close(w.output)
		w.writerWg.Wait()
		w.output = nil
	}
	w.errState = nil
	w.ibuf = w.ibuf[:0]
	w.wroteStreamHeader = false
	w.written = 0
	w.writer = writer
	w.uncompWritten = 0
	w.index.reset(w.blockSize)

	// If we didn't get a writer, stop here.
	if writer == nil {
		return
	}
	// If no concurrency requested, don't spin up writer goroutine.
	if w.concurrency == 1 {
		return
	}

	toWrite := make(chan chan result, w.concurrency)
	w.output = toWrite
	w.writerWg.Add(1)

	// Start a writer goroutine that will write all output in order.
	go func() {
		defer w.writerWg.Done()

		// Get a queued write.
		for write := range toWrite {
			// Wait for the data to be available.
			input := <-write
			in := input.b
			if len(in) > 0 {
				if w.err(nil) == nil {
					// Don't expose data from previous buffers.
					toWrite := in[:len(in):len(in)]
					// Write to output.
					n, err := writer.Write(toWrite)
					if err == nil && n != len(toWrite) {
						err = io.ErrShortBuffer
					}
					_ = w.err(err)
					w.err(w.index.add(w.written, input.startOffset))
					w.written += int64(n)
				}
			}
			if cap(in) >= w.obufLen {
				w.buffers.Put(in)
			}
			// close the incoming write request.
			// This can be used for synchronizing flushes.
			close(write)
		}
	}()
}

// Write satisfies the io.Writer interface.
func (w *Writer) Write(p []byte) (nRet int, errRet error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if w.flushOnWrite {
		return w.write(p)
	}
	// If we exceed the input buffer size, start writing
	for len(p) > (cap(w.ibuf)-len(w.ibuf)) && w.err(nil) == nil {
		var n int
		if len(w.ibuf) == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, _ = w.write(p)
		} else {
			n = copy(w.ibuf[len(w.ibuf):cap(w.ibuf)], p)
			w.ibuf = w.ibuf[:len(w.ibuf)+n]
			w.write(w.ibuf)
			w.ibuf = w.ibuf[:0]
		}
		nRet += n
		p = p[n:]
	}
	if err := w.err(nil); err != nil {
		return nRet, err
	}
	// p should always be able to fit into w.ibuf now.
	n := copy(w.ibuf[len(w.ibuf):cap(w.ibuf)], p)
	w.ibuf = w.ibuf[:len(w.ibuf)+n]
	nRet += n
	return nRet, nil
}

// ReadFrom implements the io.ReaderFrom interface.
// Using this is typically more efficient since it avoids a memory copy.
// ReadFrom reads data from r until EOF or error.
// The return value n is the number of bytes read.
// Any error except io.EOF encountered during the read is also returned.
func (w *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if len(w.ibuf) > 0 {
		err := w.Flush()
		if err != nil {
			return 0, err
		}
	}
	if br, ok := r.(byter); ok {
		buf := br.Bytes()
		if err := w.EncodeBuffer(buf); err != nil {
			return 0, err
		}
		return int64(len(buf)), w.Flush()
	}
	for {
		inbuf := w.buffers.Get().([]byte)[:w.blockSize+obufHeaderLen]
		n2, err := io.ReadFull(r, inbuf[obufHeaderLen:])
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				err = io.EOF
			}
			if err != io.EOF {
				return n, w.err(err)
			}
		}
		if n2 == 0 {
			break
		}
		n += int64(n2)
		err2 := w.writeFull(inbuf[:n2+obufHeaderLen])
		if w.err(err2) != nil {
			break
		}

		if err != nil {
			// We got EOF and wrote everything
			break
		}
	}

	return n, w.err(nil)
}

// AddSkippableBlock will add a skippable block to the stream.
// The ID must be 0x80-0xfe (inclusive).
// Length of the skippable block must be <= 16777215 bytes.
func (w *Writer) AddSkippableBlock(id uint8, data []byte) (err error) {
	if err := w.err(nil); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	if id < 0x80 || id > chunkTypePadding {
		return fmt.Errorf("invalid skippable block id %x", id)
	}
	if len(data) > maxChunkSize {
		return fmt.Errorf("skippable block excessed maximum size")
	}
	var header [4]byte
	chunkLen := 4 + len(data)
	header[0] = id
	header[1] = uint8(chunkLen >> 0)
	header[2] = uint8(chunkLen >> 8)
	header[3] = uint8(chunkLen >> 16)
	if w.concurrency == 1 {
		write := func(b []byte) error {
			n, err := w.writer.Write(b)
			if err = w.err(err); err != nil {
				return err
			}
			if n != len(data) {
				return w.err(io.ErrShortWrite)
			}
			w.written += int64(n)
			return w.err(nil)
		}
		if !w.wroteStreamHeader {
			w.wroteStreamHeader = true
			if w.snappy {
				if err := write([]byte(magicChunkSnappy)); err != nil {
					return err
				}
			} else {
				if err := write([]byte(magicChunk)); err != nil {
					return err
				}
			}
		}
		if err := write(header[:]); err != nil {
			return err
		}
		if err := write(data); err != nil {
			return err
		}
	}

	// Create output...
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		hWriter := make(chan result)
		w.output <- hWriter
		if w.snappy {
			hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunkSnappy)}
		} else {
			hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunk)}
		}
	}

	// Copy input.
	inbuf := w.buffers.Get().([]byte)[:4]
	copy(inbuf, header[:])
	inbuf = append(inbuf, data...)

	output := make(chan result, 1)
	// Queue output.
	w.output <- output
	output <- result{startOffset: w.uncompWritten, b: inbuf}

	return nil
}

// EncodeBuffer will add a buffer to the stream.
// This is the fastest way to encode a stream,
// but the input buffer cannot be written to by the caller
// until Flush or Close has been called when concurrency != 1.
//
// If you cannot control that, use the regular Write function.
//
// Note that input is not buffered.
// This means that each write will result in discrete blocks being created.
// For buffered writes, use the regular Write function.
func (w *Writer) EncodeBuffer(buf []byte) (err error) {
	if err := w.err(nil); err != nil {
		return err
	}

	if w.flushOnWrite {
		_, err := w.write(buf)
		return err
	}
	// Flush queued data first.
	if len(w.ibuf) > 0 {
		err := w.Flush()
		if err != nil {
			return err
		}
	}
	if w.concurrency == 1 {
		_, err := w.writeSync(buf)
		return err
	}

	// Spawn goroutine and write block to output channel.
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		hWriter := make(chan result)
		w.output <- hWriter
		if w.snappy {
			hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunkSnappy)}
		} else {
			hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunk)}
		}
	}

	for len(buf) > 0 {
		// Cut input.
		uncompressed := buf
		if len(uncompressed) > w.blockSize {
			uncompressed = uncompressed[:w.blockSize]
		}
		buf = buf[len(uncompressed):]
		// Get an output buffer.
		obuf := w.buffers.Get().([]byte)[:len(uncompressed)+obufHeaderLen]
		output := make(chan result)
		// Queue output now, so we keep order.
		w.output <- output
		res := result{
			startOffset: w.uncompWritten,
		}
		w.uncompWritten += int64(len(uncompressed))
		go func() {
			checksum := crc(uncompressed)

			// Set to uncompressed.
			chunkType := uint8(chunkTypeUncompressedData)
			chunkLen := 4 + len(uncompressed)

			// Attempt compressing.
			n := binary.PutUvarint(obuf[obufHeaderLen:], uint64(len(uncompressed)))
			n2 := w.encodeBlock(obuf[obufHeaderLen+n:], uncompressed)

			// Check if we should use this, or store as uncompressed instead.
			if n2 > 0 {
				chunkType = uint8(chunkTypeCompressedData)
				chunkLen = 4 + n + n2
				obuf = obuf[:obufHeaderLen+n+n2]
			} else {
				// copy uncompressed
				copy(obuf[obufHeaderLen:], uncompressed)
			}

			// Fill in the per-chunk header that comes before the body.
			obuf[0] = chunkType
			obuf[1] = uint8(chunkLen >> 0)
			obuf[2] = uint8(chunkLen >> 8)
			obuf[3] = uint8(chunkLen >> 16)
			obuf[4] = uint8(checksum >> 0)
			obuf[5] = uint8(checksum >> 8)
			obuf[6] = uint8(checksum >> 16)
			obuf[7] = uint8(checksum >> 24)

			// Queue final output.
			res.b = obuf
			output <- res
		}()
	}
	return nil
}

func (w *Writer) encodeBlock(obuf, uncompressed []byte) int {
	if w.snappy {
		switch w.level {
		case levelFast:
			return encodeBlockSnappy(obuf, uncompressed)
		case levelBetter:
			return encodeBlockBetterSnappy(obuf, uncompressed)
		case levelBest:
			return encodeBlockBestSnappy(obuf, uncompressed)
		}
		return 0
	}
	switch w.level {
	case levelFast:
		return encodeBlock(obuf, uncompressed)
	case levelBetter:
		return encodeBlockBetter(obuf, uncompressed)
	case levelBest:
		return encodeBlockBest(obuf, uncompressed)
	}
	return 0
}

func (w *Writer) write(p []byte) (nRet int, errRet error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if w.concurrency == 1 {
		return w.writeSync(p)
	}

	// Spawn goroutine and write block to output channel.
	for len(p) > 0 {
		if !w.wroteStreamHeader {
			w.wroteStreamHeader = true
			hWriter := make(chan result)
			w.output <- hWriter
			if w.snappy {
				hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunkSnappy)}
			} else {
				hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunk)}
			}
		}

		var uncompressed []byte
		if len(p) > w.blockSize {
			uncompressed, p = p[:w.blockSize], p[w.blockSize:]
		} else {
			uncompressed, p = p, nil
		}

		// Copy input.
		// If the block is incompressible, this is used for the result.
		inbuf := w.buffers.Get().([]byte)[:len(uncompressed)+obufHeaderLen]
		obuf := w.buffers.Get().([]byte)[:w.obufLen]
		copy(inbuf[obufHeaderLen:], uncompressed)
		uncompressed = inbuf[obufHeaderLen:]

		output := make(chan result)
		// Queue output now, so we keep order.
		w.output <- output
		res := result{
			startOffset: w.uncompWritten,
		}
		w.uncompWritten += int64(len(uncompressed))

		go func() {
			checksum := crc(uncompressed)

			// Set to uncompressed.
			chunkType := uint8(chunkTypeUncompressedData)
			chunkLen := 4 + len(uncompressed)

			// Attempt compressing.
			n := binary.PutUvarint(obuf[obufHeaderLen:], uint64(len(uncompressed)))
			n2 := w.encodeBlock(obuf[obufHeaderLen+n:], uncompressed)

			// Check if we should use this, or store as uncompressed instead.
			if n2 > 0 {
				chunkType = uint8(chunkTypeCompressedData)
				chunkLen = 4 + n + n2
				obuf = obuf[:obufHeaderLen+n+n2]
			} else {
				// Use input as output.
				obuf, inbuf = inbuf, obuf
			}

			// Fill in the per-chunk header that comes before the body.
			obuf[0] = chunkType
			obuf[1] = uint8(chunkLen >> 0)
			obuf[2] = uint8(chunkLen >> 8)
			obuf[3] = uint8(chunkLen >> 16)
			obuf[4] = uint8(checksum >> 0)
			obuf[5] = uint8(checksum >> 8)
			obuf[6] = uint8(checksum >> 16)
			obuf[7] = uint8(checksum >> 24)

			// Queue final output.
			res.b = obuf
			output <- res

			// Put unused buffer back in pool.
			w.buffers.Put(inbuf)
		}()
		nRet += len(uncompressed)
	}
	return nRet, nil
}

// writeFull is a special version of write that will always write the full buffer.
// Data to be compressed should start at offset obufHeaderLen and fill the remainder of the buffer.
// The data will be written as a single block.
// The caller is not allowed to use inbuf after this function has been called.
func (w *Writer) writeFull(inbuf []byte) (errRet error) {
	if err := w.err(nil); err != nil {
		return err
	}

	if w.concurrency == 1 {
		_, err := w.writeSync(inbuf[obufHeaderLen:])
		return err
	}

	// Spawn goroutine and write block to output channel.
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		hWriter := make(chan result)
		w.output <- hWriter
		if w.snappy {
			hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunkSnappy)}
		} else {
			hWriter <- result{startOffset: w.uncompWritten, b: []byte(magicChunk)}
		}
	}

	// Get an output buffer.
	obuf := w.buffers.Get().([]byte)[:w.obufLen]
	uncompressed := inbuf[obufHeaderLen:]

	output := make(chan result)
	// Queue output now, so we keep order.
	w.output <- output
	res := result{
		startOffset: w.uncompWritten,
	}
	w.uncompWritten += int64(len(uncompressed))

	go func() {
		checksum := crc(uncompressed)

		// Set to uncompressed.
		chunkType := uint8(chunkTypeUncompressedData)
		chunkLen := 4 + len(uncompressed)

		// Attempt compressing.
		n := binary.PutUvarint(obuf[obufHeaderLen:], uint64(len(uncompressed)))
		n2 := w.encodeBlock(obuf[obufHeaderLen+n:], uncompressed)

		// Check if we should use this, or store as uncompressed instead.
		if n2 > 0 {
			chunkType = uint8(chunkTypeCompressedData)
			chunkLen = 4 + n + n2
			obuf = obuf[:obufHeaderLen+n+n2]
		} else {
			// Use input as output.
			obuf, inbuf = inbuf, obuf
		}

		// Fill in the per-chunk header that comes before the body.
		obuf[0] = chunkType
		obuf[1] = uint8(chunkLen >> 0)
		obuf[2] = uint8(chunkLen >> 8)
		obuf[3] = uint8(chunkLen >> 16)
		obuf[4] = uint8(checksum >> 0)
		obuf[5] = uint8(checksum >> 8)
		obuf[6] = uint8(checksum >> 16)
		obuf[7] = uint8(checksum >> 24)

		// Queue final output.
		res.b = obuf
		output <- res

		// Put unused buffer back in pool.
		w.buffers.Put(inbuf)
	}()
	return nil
}

func (w *Writer) writeSync(p []byte) (nRet int, errRet error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		var n int
		var err error
		if w.snappy {
			n, err = w.writer.Write([]byte(magicChunkSnappy))
		} else {
			n, err = w.writer.Write([]byte(magicChunk))
		}
		if err != nil {
			return 0, w.err(err)
		}
		if n != len(magicChunk) {
			return 0, w.err(io.ErrShortWrite)
		}
		w.written += int64(n)
	}

	for len(p) > 0 {
		var uncompressed []byte
		if len(p) > w.blockSize {
			uncompressed, p = p[:w.blockSize], p[w.blockSize:]
		} else {
			uncompressed, p = p, nil
		}

		obuf := w.buffers.Get().([]byte)[:w.obufLen]
		checksum := crc(uncompressed)

		// Set to uncompressed.
		chunkType := uint8(chunkTypeUncompressedData)
		chunkLen := 4 + len(uncompressed)

		// Attempt compressing.
		n := binary.PutUvarint(obuf[obufHeaderLen:], uint64(len(uncompressed)))
		n2 := w.encodeBlock(obuf[obufHeaderLen+n:], uncompressed)

		if n2 > 0 {
			chunkType = uint8(chunkTypeCompressedData)
			chunkLen = 4 + n + n2
			obuf = obuf[:obufHeaderLen+n+n2]
		} else {
			obuf = obuf[:8]
		}

		// Fill in the per-chunk header that comes before the body.
		obuf[0] = chunkType
		obuf[1] = uint8(chunkLen >> 0)
		obuf[2] = uint8(chunkLen >> 8)
		obuf[3] = uint8(chunkLen >> 16)
		obuf[4] = uint8(checksum >> 0)
		obuf[5] = uint8(checksum >> 8)
		obuf[6] = uint8(checksum >> 16)
		obuf[7] = uint8(checksum >> 24)

		n, err := w.writer.Write(obuf)
		if err != nil {
			return 0, w.err(err)
		}
		if n != len(obuf) {
			return 0, w.err(io.ErrShortWrite)
		}
		w.err(w.index.add(w.written, w.uncompWritten))
		w.written += int64(n)
		w.uncompWritten += int64(len(uncompressed))

		if chunkType == chunkTypeUncompressedData {
			// Write uncompressed data.
			n, err := w.writer.Write(uncompressed)
			if err != nil {
				return 0, w.err(err)
			}
			if n != len(uncompressed) {
				return 0, w.err(io.ErrShortWrite)
			}
			w.written += int64(n)
		}
		w.buffers.Put(obuf)
		// Queue final output.
		nRet += len(uncompressed)
	}
	return nRet, nil
}

// Flush flushes the Writer to its underlying io.Writer.
// This does not apply padding.
func (w *Writer) Flush() error {
	if err := w.err(nil); err != nil {
		return err
	}

	// Queue any data still in input buffer.
	if len(w.ibuf) != 0 {
		if !w.wroteStreamHeader {
			_, err := w.writeSync(w.ibuf)
			w.ibuf = w.ibuf[:0]
			return w.err(err)
		} else {
			_, err := w.write(w.ibuf)
			w.ibuf = w.ibuf[:0]
			err = w.err(err)
			if err != nil {
				return err
			}
		}
	}
	if w.output == nil {
		return w.err(nil)
	}

	// Send empty buffer
	res := make(chan result)
	w.output <- res
	// Block until this has been picked up.
	res <- result{b: nil, startOffset: w.uncompWritten}
	// When it is closed, we have flushed.
	<-res
	return w.err(nil)
}

// Close calls Flush and then closes the Writer.
// Calling Close multiple times is ok,
// but calling CloseIndex after this will make it not return the index.
func (w *Writer) Close() error {
	_, err := w.closeIndex(w.appendIndex)
	return err
}

// CloseIndex calls Close and returns an index on first call.
// This is not required if you are only adding index to a stream.
func (w *Writer) CloseIndex() ([]byte, error) {
	return w.closeIndex(true)
}

func (w *Writer) closeIndex(idx bool) ([]byte, error) {
	err := w.Flush()
	if w.output != nil {
		close(w.output)
		w.writerWg.Wait()
		w.output = nil
	}

	var index []byte
	if w.err(nil) == nil && w.writer != nil {
		// Create index.
		if idx {
			compSize := int64(-1)
			if w.pad <= 1 {
				compSize = w.written
			}
			index = w.index.appendTo(w.ibuf[:0], w.uncompWritten, compSize)
			// Count as written for padding.
			if w.appendIndex {
				w.written += int64(len(index))
			}
			if true {
				_, err := w.index.Load(index)
				if err != nil {
					panic(err)
				}
			}
		}

		if w.pad > 1 {
			tmp := w.ibuf[:0]
			if len(index) > 0 {
				// Allocate another buffer.
				tmp = w.buffers.Get().([]byte)[:0]
				defer w.buffers.Put(tmp)
			}
			add := calcSkippableFrame(w.written, int64(w.pad))
			frame, err := skippableFrame(tmp, add, w.randSrc)
			if err = w.err(err); err != nil {
				return nil, err
			}
			n, err2 := w.writer.Write(frame)
			if err2 == nil && n != len(frame) {
				err2 = io.ErrShortWrite
			}
			_ = w.err(err2)
		}
		if len(index) > 0 && w.appendIndex {
			n, err2 := w.writer.Write(index)
			if err2 == nil && n != len(index) {
				err2 = io.ErrShortWrite
			}
			_ = w.err(err2)
		}
	}
	err = w.err(errClosed)
	if err == errClosed {
		return index, nil
	}
	return nil, err
}

// calcSkippableFrame will return a total size to be added for written
// to be divisible by multiple.
// The value will always be > skippableFrameHeader.
// The function will panic if written < 0 or wantMultiple <= 0.
func calcSkippableFrame(written, wantMultiple int64) int {
	if wantMultiple <= 0 {
		panic("wantMultiple <= 0")
	}
	if written < 0 {
		panic("written < 0")
	}
	leftOver := written % wantMultiple
	if leftOver == 0 {
		return 0
	}
	toAdd := wantMultiple - leftOver
	for toAdd < skippableFrameHeader {
		toAdd += wantMultiple
	}
	return int(toAdd)
}

// skippableFrame will add a skippable frame with a total size of bytes.
// total should be >= skippableFrameHeader and < maxBlockSize + skippableFrameHeader
func skippableFrame(dst []byte, total int, r io.Reader) ([]byte, error) {
	if total == 0 {
		return dst, nil
	}
	if total < skippableFrameHeader {
		return dst, fmt.Errorf("s2: requested skippable frame (%d) < 4", total)
	}
	if int64(total) >= maxBlockSize+skippableFrameHeader {
		return dst, fmt.Errorf("s2: requested skippable frame (%d) >= max 1<<24", total)
	}
	// Chunk type 0xfe "Section 4.4 Padding (chunk type 0xfe)"
	dst = append(dst, chunkTypePadding)
	f := uint32(total - skippableFrameHeader)
	// Add chunk length.
	dst = append(dst, uint8(f), uint8(f>>8), uint8(f>>16))
	// Add data
	start := len(dst)
	dst = append(dst, make([]byte, f)...)
	_, err := io.ReadFull(r, dst[start:])
	return dst, err
}

// WriterOption is an option for creating a encoder.
type WriterOption func(*Writer) error

// WriterConcurrency will set the concurrency,
// meaning the maximum number of decoders to run concurrently.
// The value supplied must be at least 1.
// By default this will be set to GOMAXPROCS.
func WriterConcurrency(n int) WriterOption {
	return func(w *Writer) error {
		if n <= 0 {
			return errors.New("concurrency must be at least 1")
		}
		w.concurrency = n
		return nil
	}
}

// WriterAddIndex will append an index to the end of a stream
// when it is closed.
func WriterAddIndex() WriterOption {
	return func(w *Writer) error {
		w.appendIndex = true
		return nil
	}
}

// WriterBetterCompression will enable better compression.
// EncodeBetter compresses better than Encode but typically with a
// 10-40% speed decrease on both compression and decompression.
func WriterBetterCompression() WriterOption {
	return func(w *Writer) error {
		w.level = levelBetter
		return nil
	}
}

// WriterBestCompression will enable better compression.
// EncodeBetter compresses better than Encode but typically with a
// big speed decrease on compression.
func WriterBestCompression() WriterOption {
	return func(w *Writer) error {
		w.level = levelBest
		return nil
	}
}

// WriterUncompressed will bypass compression.
// The stream will be written as uncompressed blocks only.
// If concurrency is > 1 CRC and output will still be done async.
func WriterUncompressed() WriterOption {
	return func(w *Writer) error {
		w.level = levelUncompressed
		return nil
	}
}

// WriterBlockSize allows to override the default block size.
// Blocks will be this size or smaller.
// Minimum size is 4KB and and maximum size is 4MB.
//
// Bigger blocks may give bigger throughput on systems with many cores,
// and will increase compression slightly, but it will limit the possible
// concurrency for smaller payloads for both encoding and decoding.
// Default block size is 1MB.
//
// When writing Snappy compatible output using WriterSnappyCompat,
// the maximum block size is 64KB.
func WriterBlockSize(n int) WriterOption {
	return func(w *Writer) error {
		if w.snappy && n > maxSnappyBlockSize || n < minBlockSize {
			return errors.New("s2: block size too large. Must be <= 64K and >=4KB on for snappy compatible output")
		}
		if n > maxBlockSize || n < minBlockSize {
			return errors.New("s2: block size too large. Must be <= 4MB and >=4KB")
		}
		w.blockSize = n
		return nil
	}
}

// WriterPadding will add padding to all output so the size will be a multiple of n.
// This can be used to obfuscate the exact output size or make blocks of a certain size.
// The contents will be a skippable frame, so it will be invisible by the decoder.
// n must be > 0 and <= 4MB.
// The padded area will be filled with data from crypto/rand.Reader.
// The padding will be applied whenever Close is called on the writer.
func WriterPadding(n int) WriterOption {
	return func(w *Writer) error {
		if n <= 0 {
			return fmt.Errorf("s2: padding must be at least 1")
		}
		// No need to waste our time.
		if n == 1 {
			w.pad = 0
		}
		if n > maxBlockSize {
			return fmt.Errorf("s2: padding must less than 4MB")
		}
		w.pad = n
		return nil
	}
}

// WriterPaddingSrc will get random data for padding from the supplied source.
// By default crypto/rand is used.
func WriterPaddingSrc(reader io.Reader) WriterOption {
	return func(w *Writer) error {
		w.randSrc = reader
		return nil
	}
}

// WriterSnappyCompat will write snappy compatible output.
// The output can be decompressed using either snappy or s2.
// If block size is more than 64KB it is set to that.
func WriterSnappyCompat() WriterOption {
	return func(w *Writer) error {
		w.snappy = true
		if w.blockSize > 64<<10 {
			// We choose 8 bytes less than 64K, since that will make literal emits slightly more effective.
			// And allows us to skip some size checks.
			w.blockSize = (64 << 10) - 8
		}
		return nil
	}
}

// WriterFlushOnWrite will compress blocks on each call to the Write function.
//
// This is quite inefficient as blocks size will depend on the write size.
//
// Use WriterConcurrency(1) to also make sure that output is flushed.
// When Write calls return, otherwise they will be written when compression is done.
func WriterFlushOnWrite() WriterOption {
	return func(w *Writer) error {
		w.flushOnWrite = true
		return nil
	}
}
