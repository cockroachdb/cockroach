// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package pgcryptocipherccl

import (
	"crypto/aes"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzEncryptDecryptAES(f *testing.F) {
	for _, tc := range CipherTestCases {
		f.Add(tc.Plaintext, tc.Key, tc.Iv)
	}
	f.Fuzz(func(t *testing.T, plaintext []byte, key []byte, iv []byte) {
		ciphertext, err := Encrypt(plaintext, key, iv, "aes")
		require.NoError(t, err)
		decryptedCiphertext, err := Decrypt(ciphertext, key, iv, "aes")
		require.NoError(t, err)
		require.Equal(t, plaintext, decryptedCiphertext)
	})
}

func FuzzNoPaddingEncryptDecryptAES(f *testing.F) {
	for _, tc := range CipherTestCases {
		f.Add(tc.Plaintext, tc.Key, tc.Iv)
	}
	f.Fuzz(func(t *testing.T, plaintext []byte, key []byte, iv []byte) {
		ciphertext, err := Encrypt(plaintext, key, iv, "aes/pad:none")
		if plaintextLength := len(plaintext); plaintextLength%aes.BlockSize != 0 {
			require.ErrorIs(t, err, ErrInvalidDataLength)
			return
		}
		require.NoError(t, err)
		decryptedCiphertext, err := Decrypt(ciphertext, key, iv, "aes/pad:none")
		require.NoError(t, err)
		require.Equal(t, plaintext, decryptedCiphertext)
	})
}
