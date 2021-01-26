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
	"os"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
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
	r, err := decryptingReader(bytes.NewReader(ciphertext), key)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r.(io.Reader))
}

type decryptReader struct {
	ciphertext io.ReaderAt
	g          cipher.AEAD
	fileIV     []byte

	ivScratch []byte
	buf       []byte
	pos       int64 // pos is used to transform Read() to ReadAt(pos).
	chunk     int64
}

type readerAndReaderAt interface {
	io.Reader
	io.ReaderAt
}

func decryptingReader(ciphertext readerAndReaderAt, key []byte) (sstable.ReadableFile, error) {
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
		return vfs.NewMemFile(buf), errors.Wrap(err, "failed to decrypt — maybe incorrect key")
	}
	buf := make([]byte, nonceSize, encryptionChunkSizeV2+tagSize+nonceSize)
	ivScratch := buf[:nonceSize]
	buf = buf[nonceSize:]
	r := &decryptReader{g: gcm, fileIV: iv, ivScratch: ivScratch, ciphertext: ciphertext, buf: buf, chunk: -1}
	return r, err
}

// fill loads the requested chunk into the buffer.
func (r *decryptReader) fill(chunk int64) error {
	if chunk == r.chunk {
		return nil // this chunk is already loaded in buf.
	}

	r.chunk = -1 // invalidate the current buffered chunk while we fill it.
	ciphertextChunkSize := int64(encryptionChunkSizeV2) + tagSize
	// Load the region of ciphertext that corresponds to chunk.
	n, err := r.ciphertext.ReadAt(r.buf[:cap(r.buf)], headerSize+chunk*ciphertextChunkSize)
	if err != nil && err != io.EOF {
		return err
	}
	r.buf = r.buf[:n]

	// Decrypt the ciphertext chunk into buf.
	buf, err := r.g.Open(r.buf[:0], r.chunkIV(chunk), r.buf, nil)
	if err != nil {
		return errors.Wrap(err, "failed to decrypt — maybe incorrect key")
	}
	r.buf = buf
	r.chunk = chunk
	return err
}

func (r *decryptReader) chunkIV(num int64) []byte {
	r.ivScratch = append(r.ivScratch[:0], r.fileIV...)
	binary.BigEndian.PutUint64(r.ivScratch[4:], binary.BigEndian.Uint64(r.ivScratch[4:])+uint64(num))
	return r.ivScratch
}

func (r *decryptReader) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("bad offset")
	}

	var read int
	for {
		chunk := offset / int64(encryptionChunkSizeV2)
		offsetInChunk := offset % int64(encryptionChunkSizeV2)

		if err := r.fill(chunk); err != nil {
			return read, err
		}

		// If the decrypted chunk is too small to contain offset, that implies EOF.
		if offsetInChunk >= int64(len(r.buf)) {
			return read, io.EOF
		}

		// Copy from the chunk.
		n := copy(p[read:], r.buf[offsetInChunk:])
		read += n

		// Return if we've fulfilled the request.
		if read == len(p) {
			return read, nil
		}

		// Return EOF if this was the last chunk (<chunksize).
		if len(r.buf) < encryptionChunkSizeV2 {
			return read, io.EOF
		}

		// Move offset by how much we read and go again.
		offset += int64(n)
	}
}

func (r *decryptReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *decryptReader) Close() error {
	if closer, ok := r.ciphertext.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Size returns the size of the file.
func (r *decryptReader) Stat() (os.FileInfo, error) {
	stater, ok := r.ciphertext.(interface{ Stat() (os.FileInfo, error) })
	if !ok {
		return nil, errors.Newf("%T does not support stat", r.ciphertext)
	}
	stat, err := stater.Stat()
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	size -= headerSize
	size -= tagSize * ((size / (int64(encryptionChunkSizeV2) + tagSize)) + 1)
	return sizeStat(size), nil
}

func aesgcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
