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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestParseCipherType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Positive tests
	for input, expected := range map[string]cipherType{
		"aes": {
			algorithm: aesCipher,
			mode:      cbcMode,
			padding:   pkcsPadding,
		},
		"aes/pad:pkcs": {
			algorithm: aesCipher,
			mode:      cbcMode,
			padding:   pkcsPadding,
		},
		"aes/pad:none": {
			algorithm: aesCipher,
			mode:      cbcMode,
			padding:   noPadding,
		},
		"aes-cbc": {
			algorithm: aesCipher,
			mode:      cbcMode,
			padding:   pkcsPadding,
		},
		"aes-cbc/pad:pkcs": {
			algorithm: aesCipher,
			mode:      cbcMode,
			padding:   pkcsPadding,
		},
		"aes-cbc/pad:none": {
			algorithm: aesCipher,
			mode:      cbcMode,
			padding:   noPadding,
		},
		"aes-ecb": {
			algorithm: aesCipher,
			mode:      ecbMode,
			padding:   pkcsPadding,
		},
		"aes-ecb/pad:pkcs": {
			algorithm: aesCipher,
			mode:      ecbMode,
			padding:   pkcsPadding,
		},
		"aes-ecb/pad:none": {
			algorithm: aesCipher,
			mode:      ecbMode,
			padding:   noPadding,
		},
	} {
		t.Run(input, func(t *testing.T) {
			ct, err := parseCipherType(input)
			require.NoError(t, err)
			require.Equal(t, expected, ct)
		})
	}

	// Negative tests
	for input, expectedErr := range map[string]string{
		"aes/pad=pkcs": `cipher type has wrong format: "aes/pad=pkcs"`,
		"bf":           `cipher type has unsupported algorithm: "bf"`,
		"aes-ctr":      `cipher type has unsupported mode: "ctr"`,
		"aes/pad:zero": `cipher type has unsupported padding: "zero"`,
	} {
		t.Run(input, func(t *testing.T) {
			_, err := parseCipherType(input)
			require.EqualError(t, err, expectedErr)
		})
	}
}
