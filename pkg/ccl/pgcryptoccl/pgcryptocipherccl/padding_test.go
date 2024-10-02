// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcryptocipherccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestPKCSPad(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for name, tc := range map[string]struct {
		data        []byte
		blockSize   int
		expected    []byte
		expectedErr string
	}{
		"invalid block size of 0": {
			data:        []byte{'a'},
			blockSize:   0,
			expectedErr: "invalid block size for PKCS padding: 0",
		},
		"invalid block size of 256": {
			data:        []byte{'a'},
			blockSize:   256,
			expectedErr: "invalid block size for PKCS padding: 256",
		},
		"empty data": {
			data:      []byte{},
			blockSize: 4,
			expected:  []byte{4, 4, 4, 4},
		},
		"data length is not a multiple of block size": {
			data:      []byte{'a'},
			blockSize: 4,
			expected:  []byte{'a', 3, 3, 3},
		},
		"data length is multiple of block size": {
			data:      []byte{'a', 'b', 'c', 'd'},
			blockSize: 4,
			expected:  []byte{'a', 'b', 'c', 'd', 4, 4, 4, 4},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual, err := pkcsPad(tc.data, tc.blockSize)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestPKCSUnpad(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for name, tc := range map[string]struct {
		data        []byte
		expected    []byte
		expectedErr string
	}{
		"empty padded data": {
			data:        []byte{},
			expectedErr: "PKCS-padded data is empty",
		},
		"padded data last byte is 0": {
			data:        []byte{'a', 'b', 'c', 0},
			expectedErr: "invalid final byte found in PKCS-padded data: 0",
		},
		"padded data last byte is greater than data length": {
			data:        []byte{'a', 'a', 20, 20},
			expectedErr: "invalid final byte found in PKCS-padded data: 20",
		},
		"padding has incorrect byte": {
			data:        []byte{'a', 'b', 'c', 'd', 3, 4, 4, 4},
			expectedErr: "invalid byte found in PKCS-padded data: expected 4, but found 3",
		},
		"empty data with full-block padding": {
			data:     []byte{4, 4, 4, 4},
			expected: []byte{},
		},
		"non-empty data with full-block padding": {
			data:     []byte{'a', 'b', 'c', 'd', 4, 4, 4, 4},
			expected: []byte{'a', 'b', 'c', 'd'},
		},
		"non-empty data with partial-block padding": {
			data:     []byte{'a', 'b', 2, 2},
			expected: []byte{'a', 'b'},
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual, err := pkcsUnpad(tc.data)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestZeroPadOrTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for name, tc := range map[string]struct {
		data        []byte
		size        int
		expected    []byte
		expectedErr string
	}{
		"data length less than size": {
			data:     []byte{1, 2},
			size:     3,
			expected: []byte{1, 2, 0},
		},
		"data length equal to size": {
			data:     []byte{1, 2, 3},
			size:     3,
			expected: []byte{1, 2, 3},
		},
		"data length greater than size": {
			data:     []byte{1, 2, 3, 4},
			size:     3,
			expected: []byte{1, 2, 3},
		},
		"empty data": {
			data:     nil,
			size:     3,
			expected: []byte{0, 0, 0},
		},
		"negative size": {
			data:        []byte{1, 2, 3},
			size:        -1,
			expectedErr: "cannot zero pad or truncate to negative size",
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual, err := zeroPadOrTruncate(tc.data, tc.size)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.Equal(t, tc.expected, actual)
		})
	}
}
