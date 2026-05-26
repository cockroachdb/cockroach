// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcryptocipher_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgcrypto/pgcryptocipher"
	"github.com/stretchr/testify/require"
)

func TestEncrypt(t *testing.T) {
	for name, tc := range pgcryptocipher.CipherTestCases {
		t.Run(name, func(t *testing.T) {
			res, err := pgcryptocipher.Encrypt(tc.Plaintext, tc.Key, tc.Iv, tc.CipherType)
			require.NoError(t, err)
			require.Equal(t, tc.Ciphertext, res)
		})
	}
}

func TestDecrypt(t *testing.T) {
	for name, tc := range pgcryptocipher.CipherTestCases {
		t.Run(name, func(t *testing.T) {
			res, err := pgcryptocipher.Decrypt(tc.Ciphertext, tc.Key, tc.Iv, tc.CipherType)
			require.NoError(t, err)
			require.Equal(t, tc.Plaintext, res)
		})
	}
}

func BenchmarkEncrypt(b *testing.B) {
	for name, tc := range pgcryptocipher.CipherTestCases {
		b.Run(name, func(b *testing.B) {
			benchmarkEncrypt(b, tc.Plaintext, tc.Key, tc.Iv, tc.CipherType)
		})
	}
}

func BenchmarkDecrypt(b *testing.B) {
	for name, tc := range pgcryptocipher.CipherTestCases {
		b.Run(name, func(*testing.B) {
			benchmarkDecrypt(b, tc.Ciphertext, tc.Key, tc.Iv, tc.CipherType)
		})
	}
}

func benchmarkEncrypt(b *testing.B, data []byte, key []byte, iv []byte, cipherType string) {
	for n := 0; n < b.N; n++ {
		_, err := pgcryptocipher.Encrypt(data, key, iv, cipherType)
		require.NoError(b, err)
	}
}

func benchmarkDecrypt(b *testing.B, data []byte, key []byte, iv []byte, cipherType string) {
	for n := 0; n < b.N; n++ {
		_, err := pgcryptocipher.Decrypt(data, key, iv, cipherType)
		require.NoError(b, err)
	}
}
