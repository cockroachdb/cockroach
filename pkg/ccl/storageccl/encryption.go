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

	"github.com/pkg/errors"
	"golang.org/x/crypto/pbkdf2"
)

// The following helpers are intended for use in creating and reading encrypted
// BACKUPs that support being created and restored using a simple passphrase.
// Encryption is done using ASE-GCM with a key derived from the provided
// passphrase. There are helpers here for two types of files:
//  a) unique-IV-prefixed files that are encrypted/decrypted with a known key
//     using EncryptFile and DecryptFile.
//  b) unique-salt-and-IV prefixed files that can be encrypted/decrypted with
//     just a passphrase by (re-)deriving the key from the salt, using various
//     combinations of GenerateKey, EncryptSaltedFile, DecryptSaltedFile and
//     DecryptSaltedFileUsingKey.
//
// The former are relatively cheap to read and write on most modern hardware
// thanks to native support for AES, but require that you manage the key used.
// The advantages of the later type of files is that, with only a passphrase you
// can pick a salt, derive a key and then create a file that can also be read
// with just a passphrase: you can open the file, read the salt to re-derive the
// key and then decrypt the file. However, the key derivation step there is
// (necessarily!) very expensive - using per-file salts and derived keys for
// each individual file in a BACKUP could, assuming we used a nice and secure
// expensive derivation function, add considerable cost to running such a job.
// By offering helpers for both flavors however, a caller can generate a salt
// and key once and use it for the salt-prefixed scheme on a chosen metadata or
// manifest file, then just use that key for writing all accompanying files.

// encryptionPreamble is a constant string prepended to all ciphertexts allowing
// for some basic sanity checks and hopefully more friendly user-facing errors
// when an unencrypted file is incorrectly treated as encrypted or vice-versa.
// The former case is easier to catch -- Decrypt can simply check and should
// *always* find the preamble if its argument was encrypted. The inverse is a
// bit harder -- an unencrypted file could start with any byte string, but if
// a caller a) finds an input file to be un-parsable and b) suspects it might be
// encrypted, then it can use AppearsEncrypted to check if the file has such a
// preamble indicating that that is likely the case.
var encryptionPreamble = []byte("encrypt")

const encryptionSaltSize = 16
const encryptionVersionAESGCMWithSalt = 1
const encryptionVersionAESGCM = 2

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
	ciphertext[len(encryptionPreamble)] = encryptionVersionAESGCM
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

	if version != encryptionVersionAESGCM {
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

// GenerateKey generates a key for the supplied passphrase and returns it along
// with the randomly chosen salt that was used to derive it.
func GenerateKey(passphrase []byte) (key []byte, salt []byte, _ error) {
	// Pick a unique salt for this file.
	salt = make([]byte, encryptionSaltSize)
	if _, err := crypto_rand.Read(salt); err != nil {
		return nil, nil, err
	}
	return kdf(passphrase, salt), salt, nil
}

// EncryptSaltedFile is similar to EncryptFile, except it also includes the
// passed salt – which is assumed to be what was used to derive `key` – in the
// header of the ciphertext. This means decryption of the resulting file is
// possible using only the passphrase, as the key can then be re-derived using
// the salt extracted from the header. Such per-file salt/keys are intended for
// files that need to be read independently of any other files, for example for
// use in manifests -- files that are read/written as part of a large batch job,
// such as the individual data files in a backup -- are expected to instead use
// EncryptFile/DecryptFile which do _not_ include per-file salts, as those jobs
// avoid per-file key derivation which is (intentionally) expensive. In such a
// configuration, a unique salt would be chosen to derive a unique key and then
// all the data files would be encrypted with that key, and then the manifest
// that then recorded all those files would encrypted using the key but with the
// prepended salt, meaning decryption could star with just a passphrase to read
// the manifest, discover the salt and key and then go read the files described
// in that manifest using that key without needing to do any further key
// derivation.
func EncryptSaltedFile(plaintext, key, salt []byte) ([]byte, error) {
	gcm, err := aesgcm(key)
	if err != nil {
		return nil, err
	}

	// Allocate our output buffer: 1B for version + 16B for salt, plus additional
	// pre-allocated capacity for the ciphertext.
	headerSize := len(encryptionPreamble) + 1 + len(salt) + gcm.NonceSize()
	ciphertext := make([]byte, headerSize, headerSize+len(plaintext)+gcm.Overhead())
	copy(ciphertext, encryptionPreamble)

	// Write our header (version + chosen salt) to the ciphertext buffer.
	ciphertext[len(encryptionPreamble)] = encryptionVersionAESGCMWithSalt

	// Store the salt to enable re-deriving the key later.
	_ = copy(ciphertext[len(encryptionPreamble)+1:], salt)

	// Pick a unique IV for this file and store it to allow key/salt reuse.
	iv := ciphertext[len(encryptionPreamble)+1+len(salt) : headerSize]
	if _, err := crypto_rand.Read(iv); err != nil {
		return nil, err
	}

	return gcm.Seal(ciphertext, iv, plaintext, nil), nil
}

// DecryptSaltedFile decrypts a file encrypted by EncryptSaltedFile, using the
// supplied passphrase and deriving the key from the salt stored in the prefix.
// It returns the plaintext along with the salt and derived key, for use in
// decrypting other files encrypted with the same key, for example as described
// on EncryptSaltedFile.
func DecryptSaltedFile(ciphertext, passphrase []byte) (plaintext, key, salt []byte, _ error) {
	if !AppearsEncrypted(ciphertext) {
		return nil, nil, nil, errors.New("file does not appear to be encrypted")
	}
	ciphertext = ciphertext[len(encryptionPreamble):]

	if len(ciphertext) < 1 {
		return nil, nil, nil, errors.New("invalid encryption header")
	}
	version := ciphertext[0]
	ciphertext = ciphertext[1:]

	if version != encryptionVersionAESGCMWithSalt {
		return nil, nil, nil, errors.Errorf("unexpected encryption scheme/config version %d", version)

	}

	if len(ciphertext) < encryptionSaltSize {
		return nil, nil, nil, errors.New("invalid encryption header")
	}

	salt = ciphertext[:encryptionSaltSize]
	ciphertext = ciphertext[encryptionSaltSize:]

	key = kdf(passphrase, salt)

	gcm, err := aesgcm(key)
	if err != nil {
		return nil, nil, nil, err
	}
	ivSize := gcm.NonceSize()
	if len(ciphertext) < ivSize {
		return nil, nil, nil, errors.New("invalid encryption header: missing IV")
	}
	iv := ciphertext[:ivSize]
	ciphertext = ciphertext[ivSize:]

	plaintext, err = gcm.Open(ciphertext[:0], iv, ciphertext, nil)
	return plaintext, key, salt, err
}

// DecryptSaltedFileUsingKey decrypt the same files as DecryptSaltedFile – those
// created by EncryptSaltedFile with a salt in their header prefix – but can be
// used when the key has already been derived, to avoid re-derivation.
func DecryptSaltedFileUsingKey(ciphertext, key, expectedSalt []byte) ([]byte, error) {
	if !AppearsEncrypted(ciphertext) {
		return nil, errors.New("file does not appear to be encrypted")
	}
	ciphertext = ciphertext[len(encryptionPreamble):]

	if len(ciphertext) < 1 {
		return nil, errors.New("invalid encryption header")
	}
	version := ciphertext[0]
	ciphertext = ciphertext[1:]

	if version != encryptionVersionAESGCMWithSalt {
		return nil, errors.Errorf("unexpected encryption scheme/config version %d", version)
	}

	if len(ciphertext) < encryptionSaltSize {
		return nil, errors.New("invalid encryption header")
	}

	salt := ciphertext[:encryptionSaltSize]
	ciphertext = ciphertext[encryptionSaltSize:]

	// We already have a key so we just check that it was indeed dervived from the
	// same salt as expected.
	if !bytes.Equal(salt, expectedSalt) {
		return nil, errors.New("salt found in file does not match expected salt")
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

func kdf(passphrase, salt []byte) []byte {
	// TODO(dt): switch to something way more expensive.
	return pbkdf2.Key(passphrase, salt, 20000, 32, sha256.New)
}

func aesgcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
