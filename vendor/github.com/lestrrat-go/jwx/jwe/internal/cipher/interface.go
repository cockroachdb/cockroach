package cipher

import (
	"crypto/cipher"

	"github.com/lestrrat-go/jwx/jwe/internal/keygen"
)

const (
	TagSize = 16
)

// ContentCipher knows how to encrypt/decrypt the content given a content
// encryption key and other data
type ContentCipher interface {
	KeySize() int
	Encrypt(cek, aad, plaintext []byte) ([]byte, []byte, []byte, error)
	Decrypt(cek, iv, aad, ciphertext, tag []byte) ([]byte, error)
}

type Fetcher interface {
	Fetch([]byte) (cipher.AEAD, error)
}

type gcmFetcher struct{}
type cbcFetcher struct{}

// AesContentCipher represents a cipher based on AES
type AesContentCipher struct {
	NonceGenerator keygen.Generator
	fetch          Fetcher
	keysize        int
	tagsize        int
}
