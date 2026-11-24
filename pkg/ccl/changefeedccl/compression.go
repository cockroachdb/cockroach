// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
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

var useFastGzip = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.fast_gzip.enabled",
	"use fast gzip implementation",
	metamorphic.ConstantWithTestBool(
		"changefeed.fast_gzip.enabled", true,
	),
	settings.WithPublic)

// These encoder/decoder pools intentionally omit the `New` field initialization
// for a few reasons:
//  1. The creation of encoders/decoders can fail and return errors, which cannot
//     be handled by sync.Pool's New function (as it doesn't support error returns)
//  2. The first Get() will return nil, allowing the calling code to handle the
//     initialization with proper error handling and parameter configuration
var (
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

type encoder interface {
	io.WriteCloser
	Reset(io.Writer)
}

type decoder interface {
	io.ReadCloser
	Reset(io.Reader) error
}

// encWrapper wraps an encoder and includes a pointer to the pool it's associated with.
type encWrapper struct {
	encoder encoder
	pool    *sync.Pool
}

// Close flushes and closes the underlying encoder, resets, and returns it to the pool.
// This differs from the inner encoder's Close by adding pool management - the encoder
// is recycled after closing rather than being discarded.
func (e *encWrapper) Close() error {
	if e.encoder == nil {
		return nil
	}
	if err := e.encoder.Close(); err != nil {
		return errors.Wrap(err, "failed to close writer")
	}
	e.encoder.Reset(nil)
	e.pool.Put(e.encoder)
	e.encoder = nil
	return nil
}

func (e *encWrapper) Write(p []byte) (int, error) {
	if e.encoder == nil {
		return 0, errors.AssertionFailedf("attempt to write on a closed encoder")
	}
	return e.encoder.Write(p)
}

// decWrapper wraps a decoder with decoders pool.
type decWrapper struct {
	decoder decoder
	pool    *sync.Pool
}

// Close closes the underlying decoder and returns it to the pool.
// Note: The decoder is not reset due to a gzip/pgzip issue
// where nil reader reset causes subsequent reads to hang.
// The gzip/pgzip nil reset issue was confirmed while testing.
func (d *decWrapper) Close() error {
	if d.decoder == nil {
		return nil
	}
	if err := d.decoder.Close(); err != nil {
		return errors.Wrap(err, "failed to close reader")
	}

	d.pool.Put(d.decoder)
	d.decoder = nil
	return nil
}

func (d *decWrapper) Read(p []byte) (int, error) {
	if d.decoder == nil {
		return 0, errors.AssertionFailedf("attempt to read on a closed decoder")
	}
	return d.decoder.Read(p)
}

// zstdDecoder wraps zstd.Decoder to implement io.Closer interface correctly.
// While zstd.Decoder.Close() returns void, io.Closer requires Close() error.
type zstdDecoder struct {
	*zstd.Decoder
}

// Close implements io.Closer interface. It resets the decoder with a nil reader
// instead of closing it, allowing the decoder to be reused. This approach preserves
// the decoder's reusability while satisfying the io.Closer contract.
func (z *zstdDecoder) Close() error {
	if err := z.Decoder.Reset(nil); err != nil {
		return errors.Wrap(err, "failed to close zstd decoder")
	}

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
				encoder := pooled.(*pgzip.Writer)
				encoder.Reset(dest)
				return &encWrapper{encoder: encoder, pool: &fastGzipEncoderPool}, nil
			}
			encoder, err := pgzip.NewWriterLevel(dest, pgzip.DefaultCompression)
			if err != nil {
				return nil, errors.Wrap(err, "failed to create pgzip encoder")
			}
			return &encWrapper{encoder: encoder, pool: &fastGzipEncoderPool}, nil
		}

		pooled := gzipEncoderPool.Get()
		if pooled != nil {
			encoder := pooled.(*stdgzip.Writer)
			encoder.Reset(dest)
			return &encWrapper{encoder: encoder, pool: &gzipEncoderPool}, nil
		}
		encoder, err := stdgzip.NewWriterLevel(dest, stdgzip.DefaultCompression)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create gzip encoder")
		}
		return &encWrapper{encoder: encoder, pool: &gzipEncoderPool}, nil
	case sinkCompressionZstd:
		pooled := zstdEncoderPool.Get()
		if pooled != nil {
			encoder := pooled.(*zstd.Encoder)
			encoder.Reset(dest)
			return &encWrapper{encoder: encoder, pool: &zstdEncoderPool}, nil
		}
		encoder, err := zstd.NewWriter(dest, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return nil, errors.Wrap(err, "failed to create zstd encoder")
		}
		return &encWrapper{encoder: encoder, pool: &zstdEncoderPool}, nil
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
			reader := pooled.(*pgzip.Reader)
			if err := reader.Reset(src); err != nil {
				return nil, errors.Wrap(err, "failed to reset gzip reader")
			}
			return &decWrapper{decoder: reader, pool: &gzipDecoderPool}, nil
		}
		reader, err := pgzip.NewReader(src)
		if err != nil {
			return nil, err
		}
		return &decWrapper{decoder: reader, pool: &gzipDecoderPool}, nil
	case sinkCompressionZstd:
		pooled := zstdDecoderPool.Get()
		if pooled != nil {
			reader := pooled.(*zstdDecoder)
			if err := reader.Reset(src); err != nil {
				return nil, errors.Wrap(err, "failed to reset zstd reader")
			}
			return &decWrapper{decoder: reader, pool: &zstdDecoderPool}, nil
		}
		reader, err := zstd.NewReader(src)
		if err != nil {
			return nil, err
		}
		return &decWrapper{decoder: &zstdDecoder{reader}, pool: &zstdDecoderPool}, nil
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
