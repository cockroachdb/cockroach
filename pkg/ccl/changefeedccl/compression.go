// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	stdgzip "compress/gzip"
	"io"
	"strings"

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

type compressionAlgo string

const sinkCompressionGzip compressionAlgo = "gzip"
const sinkCompressionZstd compressionAlgo = "zstd"

func (a compressionAlgo) enabled() bool {
	return a != ""
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
			return pgzip.NewWriterLevel(dest, pgzip.DefaultCompression)
		}
		return stdgzip.NewWriterLevel(dest, stdgzip.DefaultCompression)
	case sinkCompressionZstd:
		return zstd.NewWriter(dest, zstd.WithEncoderLevel(zstd.SpeedFastest))
	default:
		return nil, errors.AssertionFailedf("unsupported compression algorithm %q", algo)
	}
}

// newDecompressionReader returns decompression reader for the specified algorithm
func newDecompressionReader(algo compressionAlgo, src io.Reader) (io.ReadCloser, error) {
	switch algo {
	case sinkCompressionGzip:
		// since we are using decompression only for reading error response body, we can use default reader
		return pgzip.NewReader(src)
	case sinkCompressionZstd:
		// zstd reader does not implement io.Closer interface, so we need to wrap it
		decoder, err := zstd.NewReader(src)
		if err != nil {
			return nil, err
		}
		return struct {
			io.Reader
			io.Closer
		}{decoder, io.NopCloser(nil)}, nil
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
