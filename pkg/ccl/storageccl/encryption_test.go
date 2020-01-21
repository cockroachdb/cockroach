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
	key, salt, err := GenerateKey(passphrase)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("EncryptFile+DecryptFile", func(t *testing.T) {
		ciphertext, err := EncryptFile(plaintext, key)
		require.NoError(t, err)
		require.True(t, AppearsEncrypted(ciphertext), "cipher text should appear encrypted")

		decrypted, err := DecryptFile(ciphertext, key)
		require.NoError(t, err)
		require.Equal(t, plaintext, decrypted)
	})

	t.Run("salt-prefixed file using key", func(t *testing.T) {
		ciphertext, err := EncryptSaltedFile(plaintext, key, salt)
		require.NoError(t, err)
		require.True(t, AppearsEncrypted(ciphertext), "cipher text should appear encrypted")

		decrypted, err := DecryptSaltedFile(ciphertext, key, salt)
		require.NoError(t, err)
		require.Equal(t, plaintext, decrypted, "did not roundtrip")

		t.Run("with wrong salt", func(t *testing.T) {
			_, err := DecryptSaltedFile(ciphertext, key, make([]byte, len(salt)))
			require.EqualError(t, err, "salt found in file does not match expected salt")
		})
	})

	t.Run("helpful error on bad input", func(t *testing.T) {

		_, err := DecryptFile([]byte("a"), key)
		require.EqualError(t, err, "file does not appear to be encrypted")

		_, _, err = DeriveKeyFromSaltedFile([]byte("a"), passphrase)
		require.EqualError(t, err, "file does not appear to be encrypted")

		_, err = DecryptSaltedFile([]byte("a"), key, salt)
		require.EqualError(t, err, "file does not appear to be encrypted")
	})

	t.Run("helpful error when using salt-prefix decode on IV-prefix file", func(t *testing.T) {
		encrypted, err := EncryptFile(plaintext, key)
		require.NoError(t, err)
		_, err = DecryptSaltedFile(encrypted, key, salt)
		require.EqualError(t, err, "unexpected encryption scheme/config version 2")
	})
}

func BenchmarkEncryption(b *testing.B) {
	plaintext1KB := bytes.Repeat([]byte("0123456789abcdef"), 64)
	plaintext100KB := bytes.Repeat(plaintext1KB, 100)
	plaintext1MB := bytes.Repeat(plaintext1KB, 1024)

	passphrase := []byte("this is a a key")
	key, salt, err := GenerateKey(passphrase)
	require.NoError(b, err)

	ciphertext1KB, err := EncryptFile(plaintext1KB, key)
	require.NoError(b, err)
	ciphertext100KB, err := EncryptFile(plaintext100KB, key)
	require.NoError(b, err)
	ciphertext1MB, err := EncryptFile(plaintext1MB, key)
	require.NoError(b, err)
	saltedCiphertext1KB, err := EncryptSaltedFile(plaintext1KB, key, salt)
	require.NoError(b, err)
	saltedCiphertext100KB, err := EncryptSaltedFile(plaintext100KB, key, salt)
	require.NoError(b, err)
	saltedCiphertext1MB, err := EncryptSaltedFile(plaintext1MB, key, salt)
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
	b.Run("EncryptSaltedFile", func(b *testing.B) {
		for _, plaintext := range [][]byte{plaintext1KB, plaintext100KB, plaintext1MB} {
			b.Run(humanizeutil.IBytes(int64(len(plaintext))), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := EncryptSaltedFile(plaintext, key, salt)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.SetBytes(int64(len(plaintext)))
			})
		}
	})

	b.Run("DecryptSaltedFileUsingKey", func(b *testing.B) {
		for _, ciphertextOriginal := range [][]byte{saltedCiphertext1KB, saltedCiphertext100KB, saltedCiphertext1MB} {
			// Decrypt reuses/clobbers the original ciphertext slice.
			ciphertext := make([]byte, len(ciphertextOriginal))
			b.Run(humanizeutil.IBytes(int64(len(ciphertext))), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					copy(ciphertext, ciphertextOriginal)
					_, err := DecryptSaltedFile(ciphertext, key, salt)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.SetBytes(int64(len(ciphertext)))
			})
		}
	})
}
