// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raw

import (
	"bytes"
	"math"

	"github.com/cockroachdb/errors"
)

// pkcsPad pads a slice of bytes to a multiple of the given block size
// using the process specified in
// https://datatracker.ietf.org/doc/html/rfc5652#section-6.3.
func pkcsPad(data []byte, blockSize int) ([]byte, error) {
	if blockSize <= 0 || blockSize > math.MaxUint8 {
		return nil, errors.Newf("invalid block size for PKCS padding: %d", blockSize)
	}

	paddingLen := blockSize - len(data)%blockSize
	padding := bytes.Repeat([]byte{byte(paddingLen)}, paddingLen)

	return append(data, padding...), nil
}

// pkcsUnpad removes the padding added by pkcsPad.
func pkcsUnpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("PKCS-padded data is empty")
	}

	paddingLen := data[len(data)-1]
	if paddingLen == 0 || int(paddingLen) > len(data) {
		return nil, errors.Newf("invalid final byte found in PKCS-padded data: %d", paddingLen)
	}
	for i := 1; i < int(paddingLen); i++ {
		if b := data[len(data)-i-1]; b != paddingLen {
			return nil, errors.Newf("invalid byte found in PKCS-padded data: expected %d, but found %d", paddingLen, b)
		}
	}

	return data[:len(data)-int(paddingLen)], nil
}
