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
	"encoding/binary"
	"io"
	"io/ioutil"

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

// v1 is just the IV and then one sealed GCM message.
const encryptionVersionIVPrefix = 1

// v2 is an IV followed by 1 or more sealed GCM messages representing chunks of
// the original input. The last chunk is always less than full size (and may be
// empty) prior to being sealed, which is verified by the reader to
// authticate against truncation at a chunk boundary.
const encryptionVersionChunk = 2

// encryptionChunkSizeV2 is the chunk-size used by v2, i.e. 64kb, which should
// minimize overhead while still while still limiting the size of buffers and
// allowing seeks to mid-file.
var encryptionChunkSizeV2 = 64 << 10 // 64kb
const nonceSize = 12                 // GCM standard nonce
const headerSize = 7 + 1 + nonceSize // preamble + version + iv
const tagSize = 16                   // GCM standard tag

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
// which is prepended in a header on the returned ciphertext.
func EncryptFile(plaintext, key []byte) ([]byte, error) {
	return encryptFile(plaintext, key, false)
}

// EncryptFileChunked is like EncryptFile but chunks the file into fixed-size
// messages encrypted individually, allowing streaming (by-chunk) decryption.
func EncryptFileChunked(plaintext, key []byte) ([]byte, error) {
	return encryptFile(plaintext, key, true)
}

func encryptFile(plaintext, key []byte, chunked bool) ([]byte, error) {
	gcm, err := aesgcm(key)
	if err != nil {
		return nil, err
	}

	// Allocate our output buffer: preamble + 1B version + iv, plus additional
	// pre-allocated capacity for the ciphertext.

	numChunks := 1
	var version byte
	if chunked {
		version = encryptionVersionChunk
		numChunks = (len(plaintext) / encryptionChunkSizeV2) + 1
	} else {
		version = encryptionVersionIVPrefix
	}

	ciphertext := make([]byte, headerSize, headerSize+len(plaintext)+numChunks*gcm.Overhead())

	// Write our header (preamble+version+IV) to the ciphertext buffer.
	copy(ciphertext, encryptionPreamble)
	ciphertext[len(encryptionPreamble)] = version

	// Pick a unique IV for this file and write it in the header.
	ivStart := len(encryptionPreamble) + 1
	if _, err := crypto_rand.Read(ciphertext[ivStart : ivStart+nonceSize]); err != nil {
		return nil, err
	}
	// Make a copy of the IV to increment for each chunk.
	iv := append([]byte{}, ciphertext[ivStart:ivStart+nonceSize]...)

	if !chunked {
		return gcm.Seal(ciphertext, iv, plaintext, nil), nil
	}

	for {
		chunk := plaintext
		if len(chunk) > encryptionChunkSizeV2 {
			chunk = plaintext[:encryptionChunkSizeV2]
		}
		plaintext = plaintext[len(chunk):]
		ciphertext = gcm.Seal(ciphertext, iv, chunk, nil)

		// Unless we sealed less than a full chunk, continue to seal another chunk.
		// Note: there may not be any plaintext left to seal if the chunk we just
		// finished was the end of it, but sealing the (empty) remainder in a final
		// chunk maintains the invariant that a chunked file always ends in a sealed
		// chunk of less than chunk size, thus making tuncation, even along a chunk
		// boundary, detectable.
		if len(chunk) < encryptionChunkSizeV2 {
			break
		}
		binary.BigEndian.PutUint64(iv[4:], binary.BigEndian.Uint64(iv[4:])+1)
	}
	return ciphertext, nil
}

// DecryptFile decrypts a file encrypted by EncryptFile, using the supplied key
// and reading the IV from a prefix of the file. See comments on EncryptFile
// for intended usage, and see DecryptFile
func DecryptFile(ciphertext, key []byte) ([]byte, error) {
	r, err := DecryptingReader(bytes.NewReader(ciphertext), key)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

type decryptReader struct {
	ciphertext io.Reader
	g          cipher.AEAD
	fileIV     []byte

	eof       bool
	ivScratch []byte
	buf       []byte
	pos       int
	chunk     int64
}

// DecryptingReader returns a reader that decrypts on the fly with the given
// key.
func DecryptingReader(ciphertext io.Reader, key []byte) (io.Reader, error) {
	gcm, err := aesgcm(key)
	if err != nil {
		return nil, err
	}

	header := make([]byte, headerSize)
	_, readHeaderErr := io.ReadFull(ciphertext, header)

	// Verify that the read data does indeed look like an encrypted file and has
	// a encoding version we can decode.
	if !AppearsEncrypted(header) {
		return nil, errors.New("file does not appear to be encrypted")
	}
	if readHeaderErr != nil {
		return nil, errors.Wrap(readHeaderErr, "invalid encryption header")
	}

	version := header[len(encryptionPreamble)]
	if version < encryptionVersionIVPrefix || version > encryptionVersionChunk {
		return nil, errors.Errorf("unexpected encryption scheme/config version %d", version)
	}
	iv := header[len(encryptionPreamble)+1:]

	// If this version is not chunked, the entire file is one GCM message so we
	// need to read all of it to open it, and can then just return a simple bytes
	// reader on the decrypted body.
	if version == encryptionVersionIVPrefix {
		buf, err := ioutil.ReadAll(ciphertext)
		if err != nil {
			return nil, err
		}
		buf, err = gcm.Open(buf[:0], iv, buf, nil)
		return bytes.NewReader(buf), errors.Wrap(err, "failed to decrypt — maybe incorrect key")
	}
	buf := make([]byte, nonceSize, encryptionChunkSizeV2+tagSize+nonceSize)
	ivScratch := buf[:nonceSize]
	buf = buf[nonceSize:]
	r := &decryptReader{g: gcm, fileIV: iv, ivScratch: ivScratch, ciphertext: ciphertext, buf: buf, chunk: -1}
	return r, err
}

func (r *decryptReader) fill() error {
	if r.eof {
		return io.EOF
	}
	r.pos = 0
	r.buf = r.buf[:cap(r.buf)]
	r.chunk++
	var read int
	for read < len(r.buf) {
		n, err := r.ciphertext.Read(r.buf[read:])
		read += n

		// If we've reached end of the ciphertext, we still need to need to unseal
		// the current chunk (even if it was empty, to detect truncations).
		if err == io.EOF {
			r.eof = true
			break
		}
		if err != nil {
			return err
		}
	}
	var err error
	r.buf, err = r.g.Open(r.buf[:0], r.chunkIV(r.chunk), r.buf[:read], nil)
	if r.eof && len(r.buf) >= encryptionChunkSizeV2 {
		return errors.Wrap(io.ErrUnexpectedEOF, "encrypted file appears truncated")
	}
	return errors.Wrap(err, "failed to decrypt — maybe incorrect key")
}

func (r *decryptReader) chunkIV(num int64) []byte {
	r.ivScratch = append(r.ivScratch[:0], r.fileIV...)
	binary.BigEndian.PutUint64(r.ivScratch[4:], binary.BigEndian.Uint64(r.ivScratch[4:])+uint64(num))
	return r.ivScratch
}

func (r *decryptReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.buf) {
		if err := r.fill(); err != nil {
			r.chunk = -1
			return 0, err
		}
	}
	read := copy(p, r.buf[r.pos:])
	r.pos += read
	return read, nil
}

func (r *decryptReader) Close() error {
	if closer, ok := r.ciphertext.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func aesgcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
