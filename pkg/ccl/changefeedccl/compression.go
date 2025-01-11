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

type zstdDecoder struct {
	*zstd.Decoder
}

func (z *zstdDecoder) Close() error {
	z.Decoder.Close()
	return nil
}

func getFromPullOrCreate[T any](
	pool *sync.Pool, create func() (T, error), reset func(v T) error,
) (_ T, done func(), _ error) {
	var zero T
	createDone := func(v T) func() {
		return func() {
			pool.Put(v)
		}
	}
	if p := pool.Get(); p != nil {
		v, ok := p.(T)
		if !ok {
			return zero, nil, errors.AssertionFailedf("failed to cast pool value to %T", zero)
		}
		if err := reset(v); err != nil {
			return zero, nil, errors.Wrapf(err, "failed to reset coder %T", zero)
		}
		return v, createDone(v), nil
	}

	v, err := create()

	if err != nil {
		return zero, nil, err
	}
	return v, createDone(v), nil
}

// newCompressionCodec returns compression codec for the specified algorithm,
// which writes compressed data to the destination.
// TODO(yevgeniy): Support compression configuration (level, speed, etc).
// TODO(yevgeniy): Add telemetry.
func newCompressionCodec(
	algo compressionAlgo, sv *settings.Values, dest io.Writer,
) (_ io.WriteCloser, done func(), _ error) {
	switch algo {
	case sinkCompressionGzip:
		if useFastGzip.Get(sv) {
			create := func() (*pgzip.Writer, error) {
				return pgzip.NewWriterLevel(dest, pgzip.DefaultCompression)
			}
			reset := func(v *pgzip.Writer) error { v.Reset(dest); return nil }
			return getFromPullOrCreate(&fastGzipEncoderPool, create, reset)
		}

		create := func() (*stdgzip.Writer, error) {
			return stdgzip.NewWriterLevel(dest, stdgzip.DefaultCompression)
		}
		reset := func(v *stdgzip.Writer) error { v.Reset(dest); return nil }
		return getFromPullOrCreate(&gzipEncoderPool, create, reset)
	case sinkCompressionZstd:
		create := func() (*zstd.Encoder, error) {
			return zstd.NewWriter(dest, zstd.WithEncoderLevel(zstd.SpeedFastest))
		}
		reset := func(v *zstd.Encoder) error { v.Reset(dest); return nil }
		return getFromPullOrCreate(&zstdEncoderPool, create, reset)
	default:
		return nil, nil, errors.AssertionFailedf("unsupported compression algorithm %q", algo)
	}
}

// newDecompressionReader returns decompression reader for the specified algorithm
func newDecompressionReader(
	algo compressionAlgo, src io.Reader,
) (_ io.ReadCloser, done func(), _ error) {
	switch algo {
	case sinkCompressionGzip:
		// since we are using decompression only for reading error response body, we can use default reader
		create := func() (*pgzip.Reader, error) {
			return pgzip.NewReader(src)
		}
		reset := func(v *pgzip.Reader) error { return v.Reset(src) }
		return getFromPullOrCreate(&gzipDecoderPool, create, reset)
	case sinkCompressionZstd:
		// zstd reader does not implement io.Closer interface, so we need to wrap it
		create := func() (*zstdDecoder, error) {
			decoder, err := zstd.NewReader(src)
			if err != nil {
				return nil, err
			}
			return &zstdDecoder{decoder}, nil
		}
		reset := func(v *zstdDecoder) error { return v.Decoder.Reset(src) }
		return getFromPullOrCreate(&zstdDecoderPool, create, reset)
	default:
		return nil, nil, errors.AssertionFailedf("unsupported compression algorithm %q", algo)
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
