// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	stdgzip "compress/gzip"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
)

var useFastGzip = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"changefeed.fast_gzip.enabled",
	"use fast gzip implementation",
	util.ConstantWithMetamorphicTestBool(
		"changefeed.fast_gzip.enabled", true,
	),
).WithPublic()

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
