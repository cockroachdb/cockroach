// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"bytes"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type codec interface {
	compress(uncompressedData []byte) ([]byte, error)
	decompress(compressedData []byte, maxDecompressedSize int) ([]byte, error)
}

var codecs = map[string]codec{
	"GZIP":   gzipCodec{},
	"ZSTD":   zstdCodec{},
	"LZ4":    lz4Codec{},
	"SNAPPY": snappyCodec{},
}

var invalidCompressionCodecError = pgerror.New(
	pgcode.InvalidParameterValue,
	"only 'gzip', 'lz4', 'snappy', or 'zstd' compression codecs are supported")

func compress(uncompressedData []byte, codecName string) ([]byte, error) {
	c, ok := codecs[strings.ToUpper(codecName)]
	if !ok {
		return nil, invalidCompressionCodecError
	}
	return c.compress(uncompressedData)
}

func decompress(compressedData []byte, codecName string) ([]byte, error) {
	return decompressWithLimit(compressedData, codecName, builtinconstants.MaxAllocatedStringSize)
}

// decompressWithLimit is like decompress but caps the decompressed output at
// maxDecompressedSize bytes, returning errStringTooLarge if it is exceeded. It
// is separated out so tests can exercise the size-cap enforcement with a small
// limit rather than allocating the (very large) production limit.
func decompressWithLimit(
	compressedData []byte, codecName string, maxDecompressedSize int,
) ([]byte, error) {
	c, ok := codecs[strings.ToUpper(codecName)]
	if !ok {
		return nil, invalidCompressionCodecError
	}
	return c.decompress(compressedData, maxDecompressedSize)
}

type gzipCodec struct{}
type zstdCodec struct{}
type lz4Codec struct{}
type snappyCodec struct{}

func (c snappyCodec) compress(uncompressedData []byte) ([]byte, error) {
	return compressUsing(
		uncompressedData,
		func(buf io.Writer) (io.WriteCloser, error) {
			return snappy.NewBufferedWriter(buf), nil
		},
	)
}

func (c snappyCodec) decompress(compressedData []byte, maxDecompressedSize int) ([]byte, error) {
	return decompressUsing(
		compressedData,
		maxDecompressedSize,
		func(buf io.Reader) (io.ReadCloser, error) {
			return io.NopCloser(snappy.NewReader(buf)), nil
		},
	)
}

func (c lz4Codec) compress(uncompressedData []byte) ([]byte, error) {
	return compressUsing(
		uncompressedData,
		func(buf io.Writer) (io.WriteCloser, error) {
			return lz4.NewWriter(buf), nil
		},
	)
}

func (c lz4Codec) decompress(compressedData []byte, maxDecompressedSize int) ([]byte, error) {
	return decompressUsing(
		compressedData,
		maxDecompressedSize,
		func(buf io.Reader) (io.ReadCloser, error) {
			return io.NopCloser(lz4.NewReader(buf)), nil
		},
	)
}

func (c zstdCodec) compress(uncompressedData []byte) ([]byte, error) {
	return compressUsing(
		uncompressedData,
		func(buf io.Writer) (io.WriteCloser, error) {
			return zstd.NewWriter(buf)
		},
	)
}

type readCloserNoError interface {
	io.Reader
	Close()
}

type noErrorCloser struct {
	readCloserNoError
}

func (c noErrorCloser) Close() error {
	c.readCloserNoError.Close()
	return nil
}

func (c zstdCodec) decompress(compressedData []byte, maxDecompressedSize int) ([]byte, error) {
	return decompressUsing(
		compressedData,
		maxDecompressedSize,
		func(buf io.Reader) (io.ReadCloser, error) {
			r, err := zstd.NewReader(buf)
			if err != nil {
				return nil, err
			}
			return noErrorCloser{readCloserNoError: r}, nil
		})
}

func (c gzipCodec) compress(uncompressedData []byte) ([]byte, error) {
	return compressUsing(
		uncompressedData,
		func(buf io.Writer) (io.WriteCloser, error) {
			w := gzip.NewWriter(buf)
			return w, nil
		},
	)
}

func (c gzipCodec) decompress(compressedData []byte, maxDecompressedSize int) ([]byte, error) {
	return decompressUsing(compressedData, maxDecompressedSize, func(buf io.Reader) (io.ReadCloser, error) {
		return gzip.NewReader(buf)
	})
}

// compressUsing compresses uncompressed input using compressor returned
// by the getImpl function.
func compressUsing(
	uncompressed []byte, getImpl func(buf io.Writer) (io.WriteCloser, error),
) ([]byte, error) {
	var buf bytes.Buffer
	w, err := getImpl(&buf)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(uncompressed); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decompressUsing decompresses input data using decompressor returned by
// the getImpl function.
func decompressUsing(
	compressedData []byte,
	maxDecompressedSize int,
	getImpl func(buf io.Reader) (io.ReadCloser, error),
) (_ []byte, err error) {
	r, err := getImpl(bytes.NewBuffer(compressedData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress")
	}

	defer func() {
		err = errors.CombineErrors(err, r.Close())
	}()

	// Read one byte past the limit so we can distinguish output that exactly
	// fills the cap from output that exceeds it.
	decompressedBytes, err := io.ReadAll(io.LimitReader(r, int64(maxDecompressedSize)+1))
	if err != nil {
		return nil, err
	}
	if len(decompressedBytes) > maxDecompressedSize {
		return nil, errStringTooLarge
	}
	return decompressedBytes, nil
}
