// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcrypto

import "github.com/cockroachdb/cockroach/pkg/sql/pgcrypto/pgcryptocipher"

// Decrypt decrypts data using key and the cipher type, using a zero IV.
func Decrypt(data []byte, key []byte, cipherType string) ([]byte, error) {
	return pgcryptocipher.Decrypt(data, key, nil /* iv */, cipherType)
}

// DecryptIV decrypts data using key and iv with the specified cipher type.
func DecryptIV(data []byte, key []byte, iv []byte, cipherType string) ([]byte, error) {
	return pgcryptocipher.Decrypt(data, key, iv, cipherType)
}

// Encrypt encrypts data using key and the cipher type, using a zero IV.
func Encrypt(data []byte, key []byte, cipherType string) ([]byte, error) {
	return pgcryptocipher.Encrypt(data, key, nil /* iv */, cipherType)
}

// EncryptIV encrypts data using key and iv with the specified cipher type.
func EncryptIV(data []byte, key []byte, iv []byte, cipherType string) ([]byte, error) {
	return pgcryptocipher.Encrypt(data, key, iv, cipherType)
}
