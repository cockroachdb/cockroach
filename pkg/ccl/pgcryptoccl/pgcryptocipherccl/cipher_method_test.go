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

func TestParseCipherMethod(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Positive tests
	for input, expected := range map[string]cipherMethod{
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
	} {
		t.Run(input, func(t *testing.T) {
			ct, err := parseCipherMethod(input)
			require.NoError(t, err)
			require.Equal(t, expected, ct)
		})
	}

	// Negative tests
	for input, expectedErr := range map[string]string{
		// Unsupported algorithms and modes
		"aes-ecb": `unimplemented: ECB mode is insecure and not supported`,
		"bf":      `unimplemented: Blowfish is insecure and not supported`,

		// Invalid values
		"aes/pad=pkcs": `cipher method has wrong format: "aes/pad=pkcs"`,
		"aescbc":       `cipher method has invalid algorithm: "aescbc"`,
		"aes-ctr":      `cipher method has invalid mode: "ctr"`,
		"aes/pad:zero": `cipher method has invalid padding: "zero"`,
	} {
		t.Run(input, func(t *testing.T) {
			_, err := parseCipherMethod(input)
			require.EqualError(t, err, expectedErr)
		})
	}
}
