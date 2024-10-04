// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcryptocipherccl

import (
	"bytes"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// pkcsPad pads a slice of bytes to a multiple of the given block size
// using the process specified in
// https://datatracker.ietf.org/doc/html/rfc5652#section-6.3.
func pkcsPad(data []byte, blockSize int) ([]byte, error) {
	if blockSize <= 0 || blockSize > math.MaxUint8 {
		return nil, errors.AssertionFailedf("invalid block size for PKCS padding: %d", blockSize)
	}

	paddedData := make([]byte, len(data))
	copy(paddedData, data)

	paddingSize := blockSize - len(data)%blockSize
	padding := bytes.Repeat([]byte{byte(paddingSize)}, paddingSize)
	paddedData = append(paddedData, padding...)

	return paddedData, nil
}

// pkcsUnpad removes the padding added by pkcsPad.
func pkcsUnpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, pgerror.New(pgcode.InvalidParameterValue, "PKCS-padded data is empty")
	}

	paddingLen := data[len(data)-1]
	if paddingLen == 0 || int(paddingLen) > len(data) {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"invalid final byte found in PKCS-padded data: %d", paddingLen)
	}
	for i := 1; i < int(paddingLen); i++ {
		if b := data[len(data)-i-1]; b != paddingLen {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"invalid byte found in PKCS-padded data: expected %d, but found %d", paddingLen, b)
		}
	}

	return data[:len(data)-int(paddingLen)], nil
}

// zeroPadOrTruncate pads a slice of bytes with zeroes if its length is smaller
// than size and truncates the slice to length size otherwise.
func zeroPadOrTruncate(data []byte, size int) ([]byte, error) {
	if size < 0 {
		return nil, errors.AssertionFailedf("cannot zero pad or truncate to negative size")
	}
	if len(data) >= size {
		return data[:size], nil
	}
	paddedData := make([]byte, size)
	copy(paddedData, data)
	return paddedData, nil
}
