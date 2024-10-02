// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
