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
	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type codec interface {
	compress(uncompressedData []byte) ([]byte, error)
	decompress(compressedData []byte) ([]byte, error)
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
	c, ok := codecs[strings.ToUpper(codecName)]
	if !ok {
		return nil, invalidCompressionCodecError
	}
	return c.decompress(compressedData)
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

func (c snappyCodec) decompress(compressedData []byte) ([]byte, error) {
	return decompressUsing(
		compressedData,
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

func (c lz4Codec) decompress(compressedData []byte) ([]byte, error) {
	return decompressUsing(
		compressedData,
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

func (c zstdCodec) decompress(compressedData []byte) ([]byte, error) {
	return decompressUsing(
		compressedData,
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

func (c gzipCodec) decompress(compressedData []byte) ([]byte, error) {
	return decompressUsing(compressedData, func(buf io.Reader) (io.ReadCloser, error) {
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
	compressedData []byte, getImpl func(buf io.Reader) (io.ReadCloser, error),
) (_ []byte, err error) {
	r, err := getImpl(bytes.NewBuffer(compressedData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress")
	}

	defer func() {
		err = errors.CombineErrors(err, r.Close())
	}()

	decompressedBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return decompressedBytes, nil
}
