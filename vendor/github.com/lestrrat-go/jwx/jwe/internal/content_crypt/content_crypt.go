package content_crypt //nolint:golint

import (
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwe/internal/cipher"
	"github.com/pkg/errors"
)

func (c Generic) Algorithm() jwa.ContentEncryptionAlgorithm {
	return c.alg
}

func (c Generic) Encrypt(cek, plaintext, aad []byte) ([]byte, []byte, []byte, error) {
	iv, encrypted, tag, err := c.cipher.Encrypt(cek, plaintext, aad)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, `failed to crypt content`)
	}

	return iv, encrypted, tag, nil
}

func (c Generic) Decrypt(cek, iv, ciphertext, tag, aad []byte) ([]byte, error) {
	return c.cipher.Decrypt(cek, iv, ciphertext, tag, aad)
}

func NewGeneric(alg jwa.ContentEncryptionAlgorithm) (*Generic, error) {
	c, err := cipher.NewAES(alg)
	if err != nil {
		return nil, errors.Wrap(err, `aes crypt: failed to create content cipher`)
	}

	return &Generic{
		alg:     alg,
		cipher:  c,
		keysize: c.KeySize(),
		tagsize: 16,
	}, nil
}

func (c Generic) KeySize() int {
	return c.keysize
}
