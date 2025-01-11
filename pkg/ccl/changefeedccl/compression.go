// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"compress/gzip"
	stdgzip "compress/gzip"
	"io"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
)

var (
	useFastGzip = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"changefeed.fast_gzip.enabled",
		"use fast gzip implementation",
		metamorphic.ConstantWithTestBool(
			"changefeed.fast_gzip.enabled", true,
		),
		settings.WithPublic)
	gzipEncoderPool     = sync.Pool{}
	fastGzipEncoderPool = sync.Pool{}
	zstdEncoderPool     = sync.Pool{}
	gzipDecoderPool     = sync.Pool{}
	zstdDecoderPool     = sync.Pool{}
)

type compressionAlgo string

const (
	sinkCompressionGzip compressionAlgo = "gzip"
	sinkCompressionZstd compressionAlgo = "zstd"
)

func (a compressionAlgo) enabled() bool {
	return a != ""
}

type flusher interface {
	Flush() error
}

type resettableEncoder interface {
	Reset(io.Writer)
}

type resettableDecoder interface {
	Reset(io.Reader) error
}

type encoder interface {
	io.WriteCloser
	flusher
	resettableEncoder
}

type decoder interface {
	io.ReadCloser
	resettableDecoder
}

type encWrapper[T encoder] struct {
	encoder T
	pool    *sync.Pool
}

func (e *encWrapper[T]) Close() error {
	if err := e.encoder.Close(); err != nil {
		return errors.Wrap(err, "failed to close writer")
	}
	e.encoder.Reset(nil)
	e.pool.Put(e)
	return nil
}

func (e *encWrapper[T]) Write(p []byte) (int, error) {
	return e.encoder.Write(p)
}

func (e *encWrapper[T]) Reset(w io.Writer) {
	e.encoder.Reset(w)
}

type decWrapper[T decoder] struct {
	decoder T
	pool    *sync.Pool
}

func (d *decWrapper[T]) Close() error {
	if err := d.decoder.Close(); err != nil {
		return errors.Wrap(err, "failed to close reader")
	}

	d.pool.Put(d)
	return nil
}

func (d *decWrapper[T]) Reset(r io.Reader) error {
	return d.decoder.Reset(r)
}

func (d *decWrapper[T]) Read(p []byte) (int, error) {
	return d.decoder.Read(p)
}

type zstdDecoder struct {
	*zstd.Decoder
}

// Close implements io.Closer interface.
func (z *zstdDecoder) Close() error {
	z.Decoder.Close()
	return nil
}

// newCompressionCodec returns compression codec for the specified algorithm,
// which writes compressed data to the destination.
// TODO(yevgeniy): Support compression configuration (level, speed, etc).
// TODO(yevgeniy): Add telemetry.
func newCompressionCodec(
	algo compressionAlgo, sv *settings.Values, dest io.Writer,
) (io.WriteCloser, error) {
	switch algo {
	case sinkCompressionGzip:
		if useFastGzip.Get(sv) {
			pooled := fastGzipEncoderPool.Get()
			if pooled != nil {
				encoder := pooled.(*encWrapper[*pgzip.Writer])
				encoder.Reset(dest)
				return encoder, nil
			}
			encoder, err := pgzip.NewWriterLevel(dest, pgzip.DefaultCompression)
			if err != nil {
				return nil, errors.Wrap(err, "failed to create pgzip encoder")
			}
			return &encWrapper[*pgzip.Writer]{encoder, &fastGzipEncoderPool}, nil
		}

		pooled := gzipEncoderPool.Get()
		if pooled != nil {
			encoder := pooled.(*encWrapper[*gzip.Writer])
			encoder.Reset(dest)
			return encoder, nil
		}
		encoder, err := stdgzip.NewWriterLevel(dest, stdgzip.DefaultCompression)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create gzip encoder")
		}
		return &encWrapper[*gzip.Writer]{encoder, &gzipEncoderPool}, nil
	case sinkCompressionZstd:
		pooled := zstdEncoderPool.Get()
		if pooled != nil {
			encoder := pooled.(*encWrapper[*zstd.Encoder])
			encoder.Reset(dest)
			return encoder, nil
		}
		encoder, err := zstd.NewWriter(dest, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return nil, errors.Wrap(err, "failed to create zstd encoder")
		}
		return &encWrapper[*zstd.Encoder]{encoder, &zstdEncoderPool}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported compression algorithm %q", algo)
	}
}

// newDecompressionReader returns decompression reader for the specified algorithm
func newDecompressionReader(algo compressionAlgo, src io.Reader) (io.ReadCloser, error) {
	switch algo {
	case sinkCompressionGzip:
		// since we are using decompression only for reading error response body, we can use default reader
		pooled := gzipDecoderPool.Get()
		if pooled != nil {
			reader := pooled.(*decWrapper[*pgzip.Reader])
			if err := reader.Reset(src); err != nil {
				return nil, errors.Wrap(err, "failed to reset gzip reader")
			}
			return reader, nil
		}
		reader, err := pgzip.NewReader(src)
		if err != nil {
			return nil, err
		}
		return &decWrapper[*pgzip.Reader]{reader, &gzipDecoderPool}, nil
	case sinkCompressionZstd:
		pooled := zstdDecoderPool.Get()
		if pooled != nil {
			reader := pooled.(*decWrapper[*zstdDecoder])
			if err := reader.Reset(src); err != nil {
				return nil, errors.Wrap(err, "failed to reset zstd reader")
			}
			return reader, nil
		}
		reader, err := zstd.NewReader(src)
		if err != nil {
			return nil, err
		}
		return &decWrapper[*zstdDecoder]{&zstdDecoder{reader}, &zstdDecoderPool}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported compression algorithm %q", algo)
	}
}

// compressionFromString returns compression algorithm type along with file extension.
func compressionFromString(algo string) (_ compressionAlgo, ext string, _ error) {
	if strings.EqualFold(algo, string(sinkCompressionGzip)) {
		return sinkCompressionGzip, ".gz", nil
	}
	if strings.EqualFold(algo, string(sinkCompressionZstd)) {
		return sinkCompressionZstd, ".zst", nil
	}
	return "", "", errors.AssertionFailedf("unsupported compression algorithm %q", algo)
}
