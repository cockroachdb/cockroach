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
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestEncryptDecrypt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	passphrase := []byte("this is a a key")
	salt, err := GenerateSalt()
	if err != nil {
		t.Fatal(err)
	}
	key := GenerateKey(passphrase, salt)

	t.Run("EncryptFile+DecryptFile", func(t *testing.T) {
		for _, textCopies := range []int{0, 1, 3, 10, 100, 10000} {
			plaintext := bytes.Repeat([]byte("hello world\n"), textCopies)
			t.Run(fmt.Sprintf("copies=%d", textCopies), func(t *testing.T) {
				for _, chunkSize := range []int{0, 1, 7, 64, 1 << 10, 1 << 20} {
					encryptionChunkSizeV2 = chunkSize

					t.Run("chunk="+humanizeutil.IBytes(int64(chunkSize)), func(t *testing.T) {
						ciphertext, err := encryptFile(plaintext, key, chunkSize > 0)
						require.NoError(t, err)
						require.True(t, AppearsEncrypted(ciphertext), "cipher text should appear encrypted")

						decrypted, err := DecryptFile(ciphertext, key)
						require.NoError(t, err)
						require.Equal(t, plaintext, decrypted)
					})
				}
			})
		}
	})

	t.Run("helpful error on bad input", func(t *testing.T) {
		_, err := DecryptFile([]byte("a"), key)
		require.EqualError(t, err, "file does not appear to be encrypted")
	})

	t.Run("Random", func(t *testing.T) {
		rng, _ := randutil.NewTestPseudoRand()
		t.Run("DecryptFile", func(t *testing.T) {
			for _, chunked := range []bool{false, true} {
				t.Run(fmt.Sprintf("chunked=%v", chunked), func(t *testing.T) {
					// For some number of randomly chosen chunk-sizes, generate a number
					// of random length plaintexts of random bytes and ensure they each
					// round-trip.
					for i := 0; i < 10; i++ {
						encryptionChunkSizeV2 = rng.Intn(1024*24) + 1
						for j := 0; j < 100; j++ {
							plaintext := randutil.RandBytes(rng, rng.Intn(1024*32))
							ciphertext, err := encryptFile(plaintext, key, chunked)
							require.NoError(t, err)
							decrypted, err := DecryptFile(ciphertext, key)
							require.NoError(t, err)
							if len(plaintext) == 0 {
								require.Equal(t, len(plaintext), len(decrypted))
							} else {
								require.Equal(t, plaintext, decrypted)
							}
						}
					}
				})
			}
		})
	})
	_ = EncryptFileChunked // suppress unused warning.
}

func BenchmarkEncryption(b *testing.B) {
	plaintext1KB := bytes.Repeat([]byte("0123456789abcdef"), 64)
	plaintext100KB := bytes.Repeat(plaintext1KB, 100)
	plaintext1MB := bytes.Repeat(plaintext1KB, 1024)
	plaintext64MB := bytes.Repeat(plaintext1MB, 64)

	passphrase := []byte("this is a a key")
	salt, err := GenerateSalt()
	require.NoError(b, err)
	key := GenerateKey(passphrase, salt)
	chunkSizes := []int{0, 100, 4096, 512 << 10, 1 << 20}
	ciphertext1KB := make([][]byte, len(chunkSizes))
	ciphertext100KB := make([][]byte, len(chunkSizes))
	ciphertext1MB := make([][]byte, len(chunkSizes))
	ciphertext64MB := make([][]byte, len(chunkSizes))
	b.ResetTimer()

	b.Run("EncryptFile", func(b *testing.B) {
		for _, plaintext := range [][]byte{plaintext1KB, plaintext100KB, plaintext1MB, plaintext64MB} {
			b.Run(humanizeutil.IBytes(int64(len(plaintext))), func(b *testing.B) {
				for _, chunkSize := range chunkSizes {
					encryptionChunkSizeV2 = chunkSize
					b.Run("chunk="+humanizeutil.IBytes(int64(chunkSize)), func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							_, err := encryptFile(plaintext, key, chunkSize > 0)
							if err != nil {
								b.Fatal(err)
							}
						}
						b.SetBytes(int64(len(plaintext)))
					})
				}
			})
		}
	})

	for i, chunkSize := range chunkSizes {
		encryptionChunkSizeV2 = chunkSize
		ciphertext1KB[i], err = encryptFile(plaintext1KB, key, chunkSize > 0)
		require.NoError(b, err)
		ciphertext100KB[i], err = encryptFile(plaintext100KB, key, chunkSize > 0)
		require.NoError(b, err)
		ciphertext1MB[i], err = encryptFile(plaintext1MB, key, chunkSize > 0)
		require.NoError(b, err)
		ciphertext64MB[i], err = encryptFile(plaintext64MB, key, chunkSize > 0)
		require.NoError(b, err)
	}
	b.ResetTimer()

	b.Run("DecryptFile", func(b *testing.B) {
		for _, ciphertextOriginal := range [][][]byte{ciphertext1KB, ciphertext100KB, ciphertext1MB, ciphertext64MB} {
			// Decrypt reuses/clobbers the original ciphertext slice.
			b.Run(humanizeutil.IBytes(int64(len(ciphertextOriginal[0]))), func(b *testing.B) {
				for chunkSizeNum, chunkSize := range chunkSizes {
					encryptionChunkSizeV2 = chunkSize

					b.Run("chunk="+humanizeutil.IBytes(int64(chunkSize)), func(b *testing.B) {
						ciphertext := bytes.NewReader(ciphertextOriginal[chunkSizeNum])
						for i := 0; i < b.N; i++ {
							ciphertext.Reset(ciphertextOriginal[chunkSizeNum])
							r, err := DecryptingReader(ciphertext, key)
							if err != nil {
								b.Fatal(err)
							}
							_, err = io.Copy(ioutil.Discard, r)
							if err != nil {
								b.Fatal(err)
							}
						}
						b.SetBytes(int64(len(ciphertextOriginal[chunkSizeNum])))
					})
				}
			})
		}
	})
}
