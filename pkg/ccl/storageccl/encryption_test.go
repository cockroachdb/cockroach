// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestEncryptDecrypt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	plaintext := bytes.Repeat([]byte("hello world\n"), 3)
	passphrase := []byte("this is a a key")
	salt, err := GenerateSalt()
	if err != nil {
		t.Fatal(err)
	}
	key := GenerateKey(passphrase, salt)

	t.Run("EncryptFile+DecryptFile", func(t *testing.T) {
		ciphertext, err := EncryptFile(plaintext, key)
		require.NoError(t, err)
		require.True(t, AppearsEncrypted(ciphertext), "cipher text should appear encrypted")

		decrypted, err := DecryptFile(ciphertext, key)
		require.NoError(t, err)
		require.Equal(t, plaintext, decrypted)
	})

	t.Run("helpful error on bad input", func(t *testing.T) {

		_, err := DecryptFile([]byte("a"), key)
		require.EqualError(t, err, "file does not appear to be encrypted")
	})
}

func BenchmarkEncryption(b *testing.B) {
	plaintext1KB := bytes.Repeat([]byte("0123456789abcdef"), 64)
	plaintext100KB := bytes.Repeat(plaintext1KB, 100)
	plaintext1MB := bytes.Repeat(plaintext1KB, 1024)

	passphrase := []byte("this is a a key")
	salt, err := GenerateSalt()
	require.NoError(b, err)
	key := GenerateKey(passphrase, salt)

	ciphertext1KB, err := EncryptFile(plaintext1KB, key)
	require.NoError(b, err)
	ciphertext100KB, err := EncryptFile(plaintext100KB, key)
	require.NoError(b, err)
	ciphertext1MB, err := EncryptFile(plaintext1MB, key)
	require.NoError(b, err)

	b.ResetTimer()

	b.Run("EncryptFile", func(b *testing.B) {
		for _, plaintext := range [][]byte{plaintext1KB, plaintext100KB, plaintext1MB} {
			b.Run(humanizeutil.IBytes(int64(len(plaintext))), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := EncryptFile(plaintext, key)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.SetBytes(int64(len(plaintext)))
			})
		}
	})

	b.Run("DecryptFile", func(b *testing.B) {
		for _, ciphertextOriginal := range [][]byte{ciphertext1KB, ciphertext100KB, ciphertext1MB} {
			// Decrypt reuses/clobbers the original ciphertext slice.
			ciphertext := make([]byte, len(ciphertextOriginal))
			b.Run(humanizeutil.IBytes(int64(len(ciphertext))), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					copy(ciphertext, ciphertextOriginal)
					_, err := DecryptFile(ciphertext, key)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.SetBytes(int64(len(ciphertext)))
			})
		}
	})

	// If each file written or read also requires key derivation it is much more
	// expensive.
	b.Run("DeriveAndEncrypt", func(b *testing.B) {
		for _, plaintext := range [][]byte{plaintext1KB, plaintext100KB, plaintext1MB} {
			b.Run(humanizeutil.IBytes(int64(len(plaintext))), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					derived := GenerateKey(passphrase, salt)
					_, err := EncryptFile(plaintext, derived)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.SetBytes(int64(len(plaintext)))
			})
		}
	})

	b.Run("DeriveAndDecrypt", func(b *testing.B) {
		for _, ciphertextOriginal := range [][]byte{ciphertext1KB, ciphertext100KB, ciphertext1MB} {
			// Decrypt reuses/clobbers the original ciphertext slice.
			ciphertext := make([]byte, len(ciphertextOriginal))
			b.Run(humanizeutil.IBytes(int64(len(ciphertext))), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					copy(ciphertext, ciphertextOriginal)
					derived := GenerateKey(passphrase, salt)
					_, err := DecryptFile(ciphertext, derived)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.SetBytes(int64(len(ciphertext)))
			})
		}
	})
}
