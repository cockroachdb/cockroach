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
	"crypto/aes"
	"crypto/cipher"
	crypto_rand "crypto/rand"
	"crypto/sha256"

	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/pbkdf2"
)

// The following helpers are intended for use in creating and reading encrypted
// files in BACKUPs. Encryption is done using AES-GCM with a key derived from
// the provided passphrase. Individual files are always written with a random IV
// which is prefixed to the ciphertext for retrieval and use by decrypt. Helpers
// are included for deriving a key from a salt and passphrase though the caller
// is responsible for remembering the salt to rederive that key later.

// encryptionPreamble is a constant string prepended in cleartext to ciphertexts
// allowing them to be easily recognized by sight and allowing some basic sanity
// checks when trying to open them (e.g. error if incorrectly using encryption
// on an unencrypted file of vice-versa).
var encryptionPreamble = []byte("encrypt")

const encryptionSaltSize = 16
const encryptionVersionIVPrefix = 1

// GenerateSalt generates a 16 byte random salt.
func GenerateSalt() ([]byte, error) {
	// Pick a unique salt for this file.
	salt := make([]byte, encryptionSaltSize)
	if _, err := crypto_rand.Read(salt); err != nil {
		return nil, err
	}
	return salt, nil
}

// GenerateKey generates a key for the supplied passphrase and salt.
func GenerateKey(passphrase, salt []byte) []byte {
	return pbkdf2.Key(passphrase, salt, 64000, 32, sha256.New)
}

// AppearsEncrypted checks if passed bytes begin with an encryption preamble.
func AppearsEncrypted(text []byte) bool {
	return bytes.HasPrefix(text, encryptionPreamble)
}

// EncryptFile encrypts a file with the supplied key and a randomly chosen IV
// which is prepended in a header on the returned ciphertext. It is intended for
// use on collections of separate data files that are all encrypted/decrypted
// with the same key, such as BACKUP data files, and notably does _not_ include
// information for key derivation as that is _not_ done for individual files in
// such cases. See EncryptFileStoringSalt for a function that produces a
// ciphertext that also includes the salt and thus supports decrypting with only
// the passphrase (at the cost in including key derivation).
func EncryptFile(plaintext, key []byte) ([]byte, error) {
	gcm, err := aesgcm(key)
	if err != nil {
		return nil, err
	}

	// Allocate our output buffer: preamble + 1B version + iv, plus additional
	// pre-allocated capacity for the ciphertext.
	headerSize := len(encryptionPreamble) + 1 + gcm.NonceSize()
	ciphertext := make([]byte, headerSize, headerSize+len(plaintext)+gcm.Overhead())

	// Write our header (preamble+version+IV) to the ciphertext buffer.
	copy(ciphertext, encryptionPreamble)
	ciphertext[len(encryptionPreamble)] = encryptionVersionIVPrefix
	// Pick a unique IV for this file and write it in the header.
	iv := ciphertext[len(encryptionPreamble)+1:]
	if _, err := crypto_rand.Read(iv); err != nil {
		return nil, err
	}

	// Generate and write the actual ciphertext.
	return gcm.Seal(ciphertext, iv, plaintext, nil), nil
}

// DecryptFile decrypts a file encrypted by EncryptFile, using the supplied key
// and reading the IV from a prefix of the file. See comments on EncryptFile
// for intended usage, and see DecryptFile
func DecryptFile(ciphertext, key []byte) ([]byte, error) {
	if !AppearsEncrypted(ciphertext) {
		return nil, errors.New("file does not appear to be encrypted")
	}
	ciphertext = ciphertext[len(encryptionPreamble):]

	if len(ciphertext) < 1 {
		return nil, errors.New("invalid encryption header")
	}
	version := ciphertext[0]
	ciphertext = ciphertext[1:]

	if version != encryptionVersionIVPrefix {
		return nil, errors.Errorf("unexpected encryption scheme/config version %d", version)
	}
	gcm, err := aesgcm(key)
	if err != nil {
		return nil, err
	}

	ivSize := gcm.NonceSize()
	if len(ciphertext) < ivSize {
		return nil, errors.New("invalid encryption header: missing IV")
	}
	iv := ciphertext[:ivSize]
	ciphertext = ciphertext[ivSize:]
	plaintext, err := gcm.Open(ciphertext[:0], iv, ciphertext, nil)
	return plaintext, err
}

func aesgcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
