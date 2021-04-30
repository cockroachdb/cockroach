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
				for _, chunkSize := range []int{1, 7, 64, 1 << 10, 1 << 20} {
					encryptionChunkSizeV2 = chunkSize

					t.Run("chunk="+humanizeutil.IBytes(int64(chunkSize)), func(t *testing.T) {
						ciphertext, err := EncryptFile(plaintext, key)
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

	t.Run("ReadAt", func(t *testing.T) {
		rng, _ := randutil.NewTestPseudoRand()

		encryptionChunkSizeV2 = 32

		plaintext := randutil.RandBytes(rng, 256)
		plainReader := bytes.NewReader(plaintext)

		ciphertext, err := EncryptFile(plaintext, key)
		require.NoError(t, err)

		r, err := decryptingReader(bytes.NewReader(ciphertext), key)
		require.NoError(t, err)

		t.Run("start", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			expectedN, expectedErr := plainReader.ReadAt(expected, 0)
			gotN, gotErr := r.(io.ReaderAt).ReadAt(got, 0)

			require.Equal(t, expectedN, gotN)
			require.Equal(t, expectedErr, gotErr)
			require.Equal(t, expected, got)
		})

		t.Run("spanning", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			expectedN, expectedErr := plainReader.ReadAt(expected, 30)
			gotN, gotErr := r.(io.ReaderAt).ReadAt(got, 30)

			require.Equal(t, expectedN, gotN)
			require.Equal(t, expectedErr, gotErr)
			require.Equal(t, expected, got)

			expectedEmpty := make([]byte, 0)
			gotEmpty := make([]byte, 0)
			expectedEmptyN, expectedEmptyErr := plainReader.ReadAt(expectedEmpty, 30)
			gotEmptyN, gotEmptyErr := r.(io.ReaderAt).ReadAt(gotEmpty, 30)

			require.Equal(t, expectedEmptyN, gotEmptyN)
			require.Equal(t, expectedEmptyErr != nil, gotEmptyErr != nil)
			require.Equal(t, expectedEmpty, gotEmpty)
		})

		t.Run("to-end", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			expectedN, expectedErr := plainReader.ReadAt(expected, 256-24)
			gotN, gotErr := r.(io.ReaderAt).ReadAt(got, 256-24)

			require.Equal(t, expectedN, gotN)
			require.Equal(t, expectedErr, gotErr)
			require.Equal(t, expected, got)
		})

		t.Run("spanning-end", func(t *testing.T) {
			expected := make([]byte, 100)
			got := make([]byte, len(expected))

			expectedN, expectedErr := plainReader.ReadAt(expected, 180)
			gotN, gotErr := r.(io.ReaderAt).ReadAt(got, 180)

			require.Equal(t, expectedN, gotN)
			require.Equal(t, expectedErr, gotErr)
			require.Equal(t, expected, got)
		})

		t.Run("after-end", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			expectedN, _ := plainReader.ReadAt(expected, 300)
			gotN, gotErr := r.(io.ReaderAt).ReadAt(got, 300)

			require.Equal(t, expectedN, gotN)
			require.NotNil(t, gotErr)
			require.Equal(t, expected, got)

			expectedEmpty := make([]byte, 0)
			gotEmpty := make([]byte, 0)
			expectedEmptyN, expectedEmptyErr := plainReader.ReadAt(expectedEmpty, 300)
			gotEmptyN, gotEmptyErr := r.(io.ReaderAt).ReadAt(gotEmpty, 300)

			require.Equal(t, expectedEmptyN, gotEmptyN)
			require.Equal(t, expectedEmptyErr != nil, gotEmptyErr != nil)
			require.Equal(t, expectedEmpty, gotEmpty)
		})
	})

	t.Run("Random", func(t *testing.T) {
		rng, _ := randutil.NewTestPseudoRand()
		t.Run("DecryptFile", func(t *testing.T) {
			// For some number of randomly chosen chunk-sizes, generate a number
			// of random length plaintexts of random bytes and ensure they each
			// round-trip.
			for i := 0; i < 10; i++ {
				encryptionChunkSizeV2 = rng.Intn(1024*24) + 1
				for j := 0; j < 100; j++ {
					plaintext := randutil.RandBytes(rng, rng.Intn(1024*32))
					ciphertext, err := EncryptFile(plaintext, key)
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

		t.Run("ReadAt", func(t *testing.T) {
			// For each random size of chunk and text, verify random reads.
			const chunkSizes, textSizes, reads = 10, 100, 500

			for i := 0; i < chunkSizes; i++ {
				encryptionChunkSizeV2 = rng.Intn(1024*24) + 1
				for j := 0; j < textSizes; j++ {
					plaintext := randutil.RandBytes(rng, rng.Intn(1024*32))
					plainReader := bytes.NewReader(plaintext)
					ciphertext, err := EncryptFile(plaintext, key)
					require.NoError(t, err)
					r, err := decryptingReader(bytes.NewReader(ciphertext), key)
					require.NoError(t, err)
					for k := 0; k < reads; k++ {
						start := rng.Int63n(int64(float64(len(plaintext)+1) * 1.1))
						expected := make([]byte, rng.Int63n(int64(len(plaintext)/2+1)))
						got := make([]byte, len(expected))
						expectedN, expectedErr := plainReader.ReadAt(expected, start)
						gotN, gotErr := r.(io.ReaderAt).ReadAt(got, start)
						require.Equal(t, expectedN, gotN)
						if start < int64(len(plaintext)) {
							require.Equal(t, expectedErr, gotErr)
						} else {
							require.Equal(t, expectedErr != nil, gotErr != nil)
						}
						require.Equal(t, expected[:expectedN], got[:gotN])
					}
				}
			}
		})
	})
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
	chunkSizes := []int{100, 4096, 512 << 10, 1 << 20}
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
							_, err := EncryptFile(plaintext, key)
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
		ciphertext1KB[i], err = EncryptFile(plaintext1KB, key)
		require.NoError(b, err)
		ciphertext100KB[i], err = EncryptFile(plaintext100KB, key)
		require.NoError(b, err)
		ciphertext1MB[i], err = EncryptFile(plaintext1MB, key)
		require.NoError(b, err)
		ciphertext64MB[i], err = EncryptFile(plaintext64MB, key)
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
							r, err := decryptingReader(ciphertext, key)
							if err != nil {
								b.Fatal(err)
							}
							_, err = io.Copy(ioutil.Discard, r.(io.Reader))
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
