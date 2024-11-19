// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcryptocipherccl

import (
	"crypto/aes"
	"crypto/cipher"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

var (
	// ErrInvalidDataLength reports an attempt to either Encrypt or Decrypt data
	// of invalid length.
	ErrInvalidDataLength = pgerror.New(pgcode.InvalidParameterValue, "pgcryptocipherccl: invalid data length")
)

// Encrypt returns the ciphertext obtained by running the encryption
// algorithm for the specified cipher type with the provided key and
// initialization vector over the provided data.
func Encrypt(data []byte, key []byte, iv []byte, cipherType string) ([]byte, error) {
	method, err := parseCipherMethod(cipherType)
	if err != nil {
		return nil, err
	}
	block, err := newCipher(method, key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	data, err = padData(method, data, blockSize)
	if err != nil {
		return nil, err
	}
	err = validateDataLength(data, blockSize)
	if err != nil {
		return nil, err
	}
	return encrypt(method, block, iv, data)
}

// Decrypt returns the plaintext obtained by running the decryption
// algorithm for the specified cipher type with the provided key and
// initialization vector over the provided data.
func Decrypt(data []byte, key []byte, iv []byte, cipherType string) ([]byte, error) {
	method, err := parseCipherMethod(cipherType)
	if err != nil {
		return nil, err
	}
	block, err := newCipher(method, key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	err = validateDataLength(data, blockSize)
	if err != nil {
		return nil, err
	}
	data, err = decrypt(method, block, iv, data)
	if err != nil {
		return nil, err
	}
	return unpadData(method, data)
}

func newCipher(method cipherMethod, key []byte) (cipher.Block, error) {
	switch a := method.algorithm; a {
	case aesCipher:
		var err error
		switch l := len(key); {
		case l >= 32:
			key, err = zeroPadOrTruncate(key, 32)
		case l >= 24:
			key, err = zeroPadOrTruncate(key, 24)
		default:
			key, err = zeroPadOrTruncate(key, 16)
		}
		if err != nil {
			return nil, err
		}
		return aes.NewCipher(key)
	default:
		return nil, errors.AssertionFailedf("cannot create new cipher for unknown algorithm: %d", a)
	}
}

func padData(method cipherMethod, data []byte, blockSize int) ([]byte, error) {
	switch p := method.padding; p {
	case pkcsPadding:
		return pkcsPad(data, blockSize)
	case noPadding:
		return data, nil
	default:
		return nil, errors.AssertionFailedf("cannot pad for unknown padding: %d", p)
	}
}

func unpadData(method cipherMethod, data []byte) ([]byte, error) {
	switch p := method.padding; p {
	case pkcsPadding:
		return pkcsUnpad(data)
	case noPadding:
		return data, nil
	default:
		return nil, errors.AssertionFailedf("cannot unpad for unknown padding: %d", p)
	}
}

func validateDataLength(data []byte, blockSize int) error {
	if dataLength := len(data); dataLength%blockSize != 0 {
		return errors.Wrapf(
			ErrInvalidDataLength,
			`data has length %d, which is not a multiple of block size %d`,
			dataLength, blockSize,
		)
	}
	return nil
}

func encrypt(method cipherMethod, block cipher.Block, iv []byte, data []byte) ([]byte, error) {
	switch m := method.mode; m {
	case cbcMode:
		var err error
		ret := make([]byte, len(data))
		iv, err = zeroPadOrTruncate(iv, block.BlockSize())
		if err != nil {
			return nil, err
		}
		mode := cipher.NewCBCEncrypter(block, iv)
		mode.CryptBlocks(ret, data)
		return ret, nil
	default:
		return nil, errors.AssertionFailedf("cannot encrypt for unknown mode: %d", m)
	}
}

func decrypt(method cipherMethod, block cipher.Block, iv []byte, data []byte) ([]byte, error) {
	switch m := method.mode; m {
	case cbcMode:
		var err error
		ret := make([]byte, len(data))
		iv, err = zeroPadOrTruncate(iv, block.BlockSize())
		if err != nil {
			return nil, err
		}
		mode := cipher.NewCBCDecrypter(block, iv)
		mode.CryptBlocks(ret, data)
		return ret, nil
	default:
		return nil, errors.AssertionFailedf("cannot decrypt for unknown mode: %d", m)
	}
}
