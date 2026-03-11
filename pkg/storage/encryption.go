// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/remote"
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
// checks when trying to open them (E.g. error if incorrectly using encryption
// on an unencrypted file of vice-versa).
var encryptionPreamble = []byte("encrypt")

const encryptionSaltSize = 16

// v2 is an IV followed by 1 or more sealed GCM messages representing chunks of
// the original input. The last chunk is always less than full size (and may be
// empty) prior to being sealed, which is verified by the reader to
// authticate against truncation at a chunk boundary.
// v1 is no longer supported.
const encryptionVersion = 2

// encryptionChunkSizeV2 is the chunk-size used by v2, i.E. 64kb, which should
// minimize overhead while still while still limiting the size of buffers and
// allowing seeks to mid-file.
var encryptionChunkSizeV2 = 64 << 10 // 64kb
const nonceSize = 12                 // GCM standard nonce
const headerSize = 7 + 1 + nonceSize // preamble + version + iv
const encryptionTagSize = 16         // GCM standard tag

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

type encWriter struct {
	gcm        cipher.AEAD
	iv         []byte
	ciphertext objstorage.Writable
	buf        []byte
	bufPos     int
}

// NopCloser wraps an io.Writer to add a no-op Close().
type NopCloser struct {
	io.Writer
}

// Close is a no-op.
func (NopCloser) Close() error {
	return nil
}

// EncryptFile encrypts a file with the supplied key and a randomly chosen IV
// which is prepended in a header on the returned ciphertext.
func EncryptFile(plaintext, key []byte) ([]byte, error) {
	var b objstorage.MemObj
	w, err := EncryptingWriter(&b, key)
	if err != nil {
		return nil, err
	}

	if err := w.Write(plaintext); err != nil {
		w.Abort()
		return nil, err
	}

	if err := w.Finish(); err != nil {
		return nil, err
	}

	return b.Data(), nil
}

// EncryptingWriter returns a writer that wraps an underlying sink writer but
// which encrypts bytes written to it before flushing them to the wrapped sink.
//
// EncryptingWriter takes ownership over the ciphertext writable.
func EncryptingWriter(ciphertext objstorage.Writable, key []byte) (objstorage.Writable, error) {
	gcm, err := aesgcm(key)
	if err != nil {
		ciphertext.Abort()
		return nil, err
	}

	header := make([]byte, len(encryptionPreamble)+1+nonceSize)
	copy(header, encryptionPreamble)
	header[len(encryptionPreamble)] = encryptionVersion

	// Pick a unique IV for this file and write it in the header.
	ivStart := len(encryptionPreamble) + 1
	iv := make([]byte, nonceSize)
	if _, err := crypto_rand.Read(iv); err != nil {
		ciphertext.Abort()
		return nil, err
	}
	copy(header[ivStart:], iv)

	// Write our header (preamble+version+IV) to the ciphertext sink.
	if err := ciphertext.Write(header); err != nil {
		ciphertext.Abort()
		return nil, err
	}

	return &encWriter{gcm: gcm, iv: iv, ciphertext: ciphertext, buf: make([]byte, encryptionChunkSizeV2+encryptionTagSize)}, nil
}

func (e *encWriter) StartMetadataPortion() error {
	if err := e.flush(); err != nil {
		return err
	}
	return e.ciphertext.StartMetadataPortion()
}

func (e *encWriter) Write(p []byte) error {
	var wrote int
	for wrote < len(p) {
		copied := copy(e.buf[e.bufPos:encryptionChunkSizeV2], p[wrote:])
		e.bufPos += copied
		if e.bufPos == encryptionChunkSizeV2 {
			if err := e.flush(); err != nil {
				return err
			}
		}
		wrote += copied
	}
	return nil
}

func (e *encWriter) Finish() error {
	// Note: there may not be any plaintext left to seal if the chunk we just
	// finished was the end of it, but sealing the (empty) remainder in a final
	// chunk maintains the invariant that a chunked file always ends in a sealed
	// chunk of less than chunk size, thus making tuncation, even along a chunk
	// boundary, detectable.
	if err := e.flush(); err != nil {
		e.ciphertext.Abort()
		return err
	}
	return e.ciphertext.Finish()
}

func (e *encWriter) Abort() {
	e.ciphertext.Abort()
}

func (e *encWriter) flush() error {
	e.buf = e.gcm.Seal(e.buf[:0], e.iv, e.buf[:e.bufPos], nil)
	if err := e.ciphertext.Write(e.buf); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(e.iv[4:], binary.BigEndian.Uint64(e.iv[4:])+1)
	e.bufPos = 0
	return nil
}

// DecryptFile decrypts a file encrypted by EncryptFile, using the supplied key and reading the
// IV from a prefix of the file. See comments on EncryptFile for intended usage.
func DecryptFile(
	ctx context.Context, ciphertext, key []byte, mm *mon.BoundAccount,
) ([]byte, error) {
	r, err := newDecryptingReadableFile(bytes.NewReader(ciphertext), key)
	if err != nil {
		return nil, err
	}
	return mon.ReadAll(ctx, ioctx.ReaderAdapter(r.(io.Reader)), mm)
}

// decryptor implements the core decryption logic for files encrypted with
// EncryptFile. It is agnostic to the underlying I/O mechanism: callers provide
// a readCiphertext callback that fetches raw bytes, and decryptor handles
// chunk mapping, IV derivation, and GCM decryption.
type decryptor struct {
	g      cipher.AEAD
	fileIV []byte

	ivScratch []byte
	buf       []byte
	chunk     int64
}

// newDecryptor reads and validates an encryption header, then returns a
// decryptor ready for use. readHeader fills the provided buffer with the
// header bytes from the underlying I/O source.
func newDecryptor(key []byte, readHeader func([]byte) error) (*decryptor, error) {
	header := make([]byte, headerSize)
	readHeaderErr := readHeader(header)

	// Verify that the read data does indeed look like an encrypted file and has
	// a encoding version we can decode.
	if !AppearsEncrypted(header) {
		return nil, errors.New("file does not appear to be encrypted")
	}
	if readHeaderErr != nil {
		return nil, errors.Wrap(readHeaderErr, "invalid encryption header")
	}
	version := header[len(encryptionPreamble)]
	if version != encryptionVersion {
		return nil, errors.Errorf(
			"unexpected encryption scheme/config version %d", version,
		)
	}

	gcm, err := aesgcm(key)
	if err != nil {
		return nil, err
	}

	iv := make([]byte, nonceSize)
	copy(iv, header[len(encryptionPreamble)+1:])

	buf := make(
		[]byte,
		nonceSize,
		encryptionChunkSizeV2+encryptionTagSize+nonceSize,
	)
	ivScratch := buf[:nonceSize]
	buf = buf[nonceSize:]

	return &decryptor{
		g:         gcm,
		fileIV:    iv,
		ivScratch: ivScratch,
		buf:       buf,
		chunk:     -1,
	}, nil
}

func (d *decryptor) chunkIV(num int64) []byte {
	d.ivScratch = append(d.ivScratch[:0], d.fileIV...)
	binary.BigEndian.PutUint64(
		d.ivScratch[4:],
		binary.BigEndian.Uint64(d.ivScratch[4:])+uint64(num),
	)
	return d.ivScratch
}

// fill loads the requested chunk into the buffer.
func (d *decryptor) fill(chunk int64, readCiphertext func(p []byte, off int64) (int, error)) error {
	// This chunk is already loaded in buf.
	if chunk == d.chunk {
		return nil
	}

	// Invalidate the current buffered chunk while we fill it.
	d.chunk = -1
	ciphertextChunkSize := int64(encryptionChunkSizeV2) + encryptionTagSize
	// Load the region of ciphertext that corresponds to chunk.
	n, err := readCiphertext(
		d.buf[:cap(d.buf)], headerSize+chunk*ciphertextChunkSize,
	)
	if err != nil && err != io.EOF {
		return err
	}
	d.buf = d.buf[:n]

	// Decrypt the ciphertext chunk into buf.
	buf, err := d.g.Open(d.buf[:0], d.chunkIV(chunk), d.buf, nil)
	if err != nil {
		return errors.Wrap(err, "failed to decrypt — maybe incorrect key")
	}
	d.buf = buf
	d.chunk = chunk
	return nil
}

// readAt translates a plaintext-offset read into chunk-level decryption calls.
func (d *decryptor) readAt(
	p []byte, offset int64, readCiphertext func(p []byte, off int64) (int, error),
) (int, error) {
	if offset < 0 {
		return 0, errors.New("bad offset")
	}

	var read int
	for {
		chunk := offset / int64(encryptionChunkSizeV2)
		offsetInChunk := offset % int64(encryptionChunkSizeV2)

		if err := d.fill(chunk, readCiphertext); err != nil {
			return read, err
		}

		// If the decrypted chunk is too small to contain offset, that
		// implies EOF.
		if offsetInChunk >= int64(len(d.buf)) {
			return read, io.EOF
		}

		// Copy from the chunk.
		n := copy(p[read:], d.buf[offsetInChunk:])
		read += n

		// Return if we've fulfilled the request.
		if read == len(p) {
			return read, nil
		}

		// Return EOF if this was the last chunk (<chunksize).
		if len(d.buf) < encryptionChunkSizeV2 {
			return read, io.EOF
		}

		// Move offset by how much we read and go again.
		offset += int64(n)
	}
}

func aesgcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func plaintextSize(ciphertextSize int64) int64 {
	size := ciphertextSize - int64(headerSize)
	size -= encryptionTagSize *
		((size / (int64(encryptionChunkSizeV2) + encryptionTagSize)) + 1)
	return size
}

type readerAndReaderAt interface {
	io.Reader
	io.ReaderAt
}

// decryptingReadableFile wraps an io.ReaderAt and transparently decrypts
// reads. It implements objstorage.ReadableFile when the underlying type
// supports Close and Stat. It is not safe for concurrent use.
type decryptingReadableFile struct {
	ciphertext io.ReaderAt
	dec        *decryptor
	pos        int64 // pos is used to transform Read() to ReadAt(pos).
}

var _ objstorage.ReadableFile = (*decryptingReadableFile)(nil)

func newDecryptingReadableFile(
	ciphertext readerAndReaderAt, key []byte,
) (objstorage.ReadableFile, error) {
	dec, err := newDecryptor(key, func(h []byte) error {
		_, err := io.ReadFull(ciphertext, h)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &decryptingReadableFile{
		ciphertext: ciphertext,
		dec:        dec,
	}, nil
}

func (f *decryptingReadableFile) ReadAt(p []byte, off int64) (int, error) {
	return f.dec.readAt(p, off, f.ciphertext.ReadAt)
}

func (f *decryptingReadableFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *decryptingReadableFile) Close() error {
	if closer, ok := f.ciphertext.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Stat returns the plaintext size of the file.
func (f *decryptingReadableFile) Stat() (vfs.FileInfo, error) {
	var size int64
	// The vfs interface uses its own vfs.FileInfo interface. Check for either
	// the vfs Stat() or the os Stat().
	if stater, ok := f.ciphertext.(interface{ Stat() (os.FileInfo, error) }); ok {
		stat, err := stater.Stat()
		if err != nil {
			return nil, err
		}
		size = stat.Size()
	} else if stater, ok := f.ciphertext.(interface{ Stat() (vfs.FileInfo, error) }); ok {
		stat, err := stater.Stat()
		if err != nil {
			return nil, err
		}
		size = stat.Size()
	} else {
		return nil, errors.Newf("%T does not support stat", f.ciphertext)
	}
	return sizeStat(plaintextSize(size)), nil
}

type decryptingReadOnlyStorage struct {
	inner remote.Storage
	key   []byte
}

var _ remote.Storage = (*decryptingReadOnlyStorage)(nil)

func (d *decryptingReadOnlyStorage) ReadObject(
	ctx context.Context, objName string,
) (_ remote.ObjectReader, objSize int64, _ error) {
	reader, ciphertextSize, err := d.inner.ReadObject(ctx, objName)
	if err != nil {
		return nil, 0, err
	}
	dr, err := newDecryptingObjectReader(ctx, reader, ciphertextSize, d.key)
	if err != nil {
		_ = reader.Close()
		return nil, 0, err
	}
	return dr, plaintextSize(ciphertextSize), nil
}

func (d *decryptingReadOnlyStorage) Close() error {
	return d.inner.Close()
}

func (d *decryptingReadOnlyStorage) Size(objName string) (int64, error) {
	ciphertextSize, err := d.inner.Size(objName)
	if err != nil {
		return 0, err
	}
	return plaintextSize(ciphertextSize), nil
}

func (d *decryptingReadOnlyStorage) IsNotExistError(err error) bool {
	return d.inner.IsNotExistError(err)
}

func (d *decryptingReadOnlyStorage) CreateObject(objName string) (io.WriteCloser, error) {
	return nil, errors.New("decryptingReadOnlyStorage: read-only")
}

func (d *decryptingReadOnlyStorage) List(prefix, delimiter string) ([]string, error) {
	return nil, errors.New("decryptingReadOnlyStorage: read-only")
}

func (d *decryptingReadOnlyStorage) Delete(objName string) error {
	return errors.New("decryptingReadOnlyStorage: read-only")
}

// decryptingObjectReader wraps a remote.ObjectReader and transparently decrypts reads.
type decryptingObjectReader struct {
	mu         syncutil.Mutex
	ciphertext remote.ObjectReader
	size       int64
	dec        *decryptor
}

var _ remote.ObjectReader = (*decryptingObjectReader)(nil)

func newDecryptingObjectReader(
	ctx context.Context, ciphertext remote.ObjectReader, ciphertextSize int64, key []byte,
) (remote.ObjectReader, error) {
	dec, err := newDecryptor(key, func(h []byte) error {
		return ciphertext.ReadAt(ctx, h, 0)
	})
	if err != nil {
		return nil, err
	}
	return &decryptingObjectReader{
		ciphertext: ciphertext,
		size:       ciphertextSize,
		dec:        dec,
	}, nil
}

func (d *decryptingObjectReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Adapt remote.ObjectReader.ReadAt (error-only) to io.ReaderAt semantics
	// (n int, error) for the decryptor callback.
	readCiphertext := func(buf []byte, off int64) (int, error) {
		if off >= d.size {
			return 0, io.EOF
		}
		if off+int64(len(buf)) > d.size {
			buf = buf[:d.size-off]
			if err := d.ciphertext.ReadAt(ctx, buf, off); err != nil {
				return 0, err
			}
			return len(buf), io.EOF
		}
		if err := d.ciphertext.ReadAt(ctx, buf, off); err != nil {
			return 0, err
		}
		return len(buf), nil
	}

	n, err := d.dec.readAt(p, offset, readCiphertext)
	if n == len(p) {
		return nil
	}
	if err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

func (d *decryptingObjectReader) Close() error {
	return d.ciphertext.Close()
}
