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
		"aes/pad=pkcs": `cipher method has wrong format: "aes/pad=pkcs"`,
		"bf":           `cipher method has unsupported algorithm: "bf"`,
		"aes-ecb":      `cipher method has unsupported mode: "ecb"`,
		"aes/pad:zero": `cipher method has unsupported padding: "zero"`,
	} {
		t.Run(input, func(t *testing.T) {
			_, err := parseCipherMethod(input)
			require.EqualError(t, err, expectedErr)
		})
	}
}
