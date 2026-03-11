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
	"encoding/base64"
	"encoding/binary"
	"io"
	"net/url"

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
const encryptionKeySize = 32

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

// EncryptionKeyQueryParam is the URL query parameter used to embed an
// encryption key in a remote storage locator URI. Used by online restore
// to pass encryption keys through to the storage layer for transparent
// decryption of encrypted backup SSTs.
const EncryptionKeyQueryParam = "COCKROACH_INTERNAL_BACKUP_ENCRYPTION_KEY"

func EncodeDecryptingLocator(uri string, key []byte) (string, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return uri, err
	}

	if len(key) != encryptionKeySize {
		return "", errors.AssertionFailedf("encryption key must be %d bytes", encryptionKeySize)
	}
	encodedKey := base64.URLEncoding.EncodeToString(key)
	vals := url.Query()
	vals.Set(EncryptionKeyQueryParam, encodedKey)
	url.RawQuery = vals.Encode()

	return url.String(), nil
}

// decodeLocator parses a locator URL and extracts the encryption key
// query parameter if present. Returns the cleaned URL (without the param)
// and the decoded key bytes, or the original locator and nil if no key.
func decodeLocator(locator string) (string, []byte, error) {
	uri, err := url.Parse(locator)
	if err != nil {
		return "", nil, errors.Wrap(err, "parsing locator URI")
	}

	keyStr := uri.Query().Get(EncryptionKeyQueryParam)
	if keyStr == "" {
		return locator, nil, nil
	}

	key, err := base64.URLEncoding.DecodeString(keyStr)
	if err != nil {
		return "", nil, errors.Wrap(err, "decoding encryption key")
	}

	vals := uri.Query()
	vals.Del(EncryptionKeyQueryParam)
	uri.RawQuery = vals.Encode()
	return uri.String(), key, nil
}

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

// DecryptFile decrypts a file encrypted by EncryptFile, using the supplied key
// and reading the IV from a prefix of the file. See comments on EncryptFile
// for intended usage, and see DecryptFile.
func DecryptFile(
	ctx context.Context, ciphertext, key []byte, mm *mon.BoundAccount,
) ([]byte, error) {
	r, err := newDecryptingReadableFile(ctx, bytes.NewReader(ciphertext), int64(len(ciphertext)), key)
	if err != nil {
		return nil, err
	}
	return mon.ReadAll(ctx, ioctx.ReaderAdapter(r.(*decryptingReadableFile)), mm)
}

// decryptingReader holds the core decryption logic for chunk-based AES-GCM
// encrypted data. It does not implement any public interface directly;
// instead, decryptReadableFile and decryptObjectReader wrap it to provide
// objstorage.ReadableFile and remote.ObjectReader respectively.
//
// readAt is the core entry point. Callers must hold mu.
type decryptingReader struct {
	ciphertext     remote.ObjectReader
	ciphertextSize int64
	g              cipher.AEAD
	fileIV         []byte

	mu        syncutil.Mutex
	ivScratch []byte
	buf       []byte
	chunk     int64
}

// newDecryptingReader creates a decryptingReader that decrypts chunk-based AES-GCM
// encrypted data from the given ciphertext ObjectReader. ciphertextSize is the
// total size of the encrypted object.
func newDecryptingReader(
	ctx context.Context, ciphertext remote.ObjectReader, ciphertextSize int64, key []byte,
) (*decryptingReader, error) {
	gcm, err := aesgcm(key)
	if err != nil {
		return nil, err
	}

	// Read and validate the encryption header.
	header := make([]byte, headerSize)
	if err := ciphertext.ReadAt(ctx, header, 0); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, errors.New("file does not appear to be encrypted")
		}
		return nil, errors.Wrap(err, "reading encryption header")
	}
	if !AppearsEncrypted(header) {
		return nil, errors.New("file does not appear to be encrypted")
	}
	version := header[len(encryptionPreamble)]
	if version != encryptionVersion {
		return nil, errors.Errorf("unexpected encryption scheme/config version %d", version)
	}
	iv := header[len(encryptionPreamble)+1:]

	buf := make([]byte, nonceSize, encryptionChunkSizeV2+encryptionTagSize+nonceSize)
	ivScratch := buf[:nonceSize]
	buf = buf[nonceSize:]
	return &decryptingReader{
		g: gcm, fileIV: iv, ivScratch: ivScratch,
		ciphertext: ciphertext, ciphertextSize: ciphertextSize,
		buf: buf, chunk: -1,
	}, nil
}

// fill loads the requested chunk into the buffer. Caller must hold r.mu.
func (r *decryptingReader) fill(ctx context.Context, chunk int64) error {
	if chunk == r.chunk {
		return nil // this chunk is already loaded in buf.
	}

	r.chunk = -1 // invalidate the current buffered chunk while we fill it.
	ciphertextChunkSize := int64(encryptionChunkSizeV2) + encryptionTagSize
	offset := int64(headerSize) + chunk*ciphertextChunkSize

	// Compute exact read size, clamping to the ciphertext boundary.
	readSize := ciphertextChunkSize
	if offset+readSize > r.ciphertextSize {
		readSize = r.ciphertextSize - offset
	}
	if readSize <= 0 {
		r.buf = r.buf[:0]
		return nil
	}

	// Load the region of ciphertext that corresponds to chunk.
	r.buf = r.buf[:readSize]
	if err := r.ciphertext.ReadAt(ctx, r.buf, offset); err != nil {
		return err
	}

	// Decrypt the ciphertext chunk into buf.
	buf, err := r.g.Open(r.buf[:0], r.chunkIV(chunk), r.buf, nil)
	if err != nil {
		return errors.Wrap(err, "failed to decrypt — maybe incorrect key")
	}
	r.buf = buf
	r.chunk = chunk
	return nil
}

func (r *decryptingReader) chunkIV(num int64) []byte {
	r.ivScratch = append(r.ivScratch[:0], r.fileIV...)
	binary.BigEndian.PutUint64(r.ivScratch[4:], binary.BigEndian.Uint64(r.ivScratch[4:])+uint64(num))
	return r.ivScratch
}

// readAt is the core read logic. Caller must hold r.mu.
func (r *decryptingReader) readAt(ctx context.Context, p []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("bad offset")
	}

	var read int
	for {
		chunk := offset / int64(encryptionChunkSizeV2)
		offsetInChunk := offset % int64(encryptionChunkSizeV2)

		if err := r.fill(ctx, chunk); err != nil {
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

func (r *decryptingReader) close() error {
	return r.ciphertext.Close()
}

// decryptingReadableFile wraps a *decryptingReader as an objstorage.ReadableFile
// (io.ReaderAt + io.Closer + Stat). Used by ExternalSSTReader and DecryptFile.
type decryptingReadableFile struct {
	dr  *decryptingReader
	pos int64 // sequential read position for Read()
}

var _ objstorage.ReadableFile = (*decryptingReadableFile)(nil)

func (f *decryptingReadableFile) ReadAt(p []byte, offset int64) (int, error) {
	f.dr.mu.Lock()
	defer f.dr.mu.Unlock()
	return f.dr.readAt(context.Background(), p, offset)
}

func (f *decryptingReadableFile) Read(p []byte) (int, error) {
	f.dr.mu.Lock()
	defer f.dr.mu.Unlock()
	n, err := f.dr.readAt(context.Background(), p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *decryptingReadableFile) Close() error {
	return f.dr.close()
}

func (f *decryptingReadableFile) Stat() (vfs.FileInfo, error) {
	return sizeStat(plaintextSize(f.dr.ciphertextSize)), nil
}

// decryptingObjectReader wraps a *decryptingReader as a remote.ObjectReader.
// Used by newDecryptingObjectReader for online restore.
type decryptingObjectReader struct {
	dr *decryptingReader
}

var _ remote.ObjectReader = (*decryptingObjectReader)(nil)

func (d *decryptingObjectReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	d.dr.mu.Lock()
	defer d.dr.mu.Unlock()
	n, err := d.dr.readAt(ctx, p, offset)
	if n == len(p) {
		return nil
	}
	if err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

func (d *decryptingObjectReader) Close() error {
	return d.dr.close()
}

// readerAtObjectReader wraps an io.ReaderAt and a known size as a
// remote.ObjectReader. It translates io.ReaderAt's (n, err) return to
// remote.ObjectReader's fill-or-error contract.
type readerAtObjectReader struct {
	r    io.ReaderAt
	size int64
}

func (a *readerAtObjectReader) ReadAt(_ context.Context, p []byte, offset int64) error {
	if offset >= a.size {
		return io.EOF
	}
	readSize := int64(len(p))
	if offset+readSize > a.size {
		return io.ErrUnexpectedEOF
	}
	_, err := a.r.ReadAt(p, offset)
	return err
}

func (a *readerAtObjectReader) Close() error {
	if closer, ok := a.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// newDecryptingReadableFile creates an objstorage.ReadableFile that decrypts
// ciphertext provided via an io.ReaderAt. Used by ExternalSSTReader and
// DecryptFile.
func newDecryptingReadableFile(
	ctx context.Context, ciphertext io.ReaderAt, ciphertextSize int64, key []byte,
) (objstorage.ReadableFile, error) {
	or := &readerAtObjectReader{r: ciphertext, size: ciphertextSize}
	dr, err := newDecryptingReader(ctx, or, ciphertextSize, key)
	if err != nil {
		return nil, err
	}
	return &decryptingReadableFile{dr: dr}, nil
}

func aesgcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// newDecryptingObjectReader creates a remote.ObjectReader that decrypts
// ciphertext provided via a remote.ObjectReader. Used by online restore.
func newDecryptingObjectReader(
	ctx context.Context, ciphertext remote.ObjectReader, ciphertextSize int64, key []byte,
) (remote.ObjectReader, error) {
	dr, err := newDecryptingReader(ctx, ciphertext, ciphertextSize, key)
	if err != nil {
		return nil, err
	}
	return &decryptingObjectReader{dr: dr}, nil
}

// plaintextSize computes the decrypted plaintext size from the total
// ciphertext size of an encrypted file.
func plaintextSize(ciphertextSize int64) int64 {
	size := ciphertextSize - int64(headerSize)
	size -= encryptionTagSize * ((size / (int64(encryptionChunkSizeV2) + encryptionTagSize)) + 1)
	return size
}

// decryptingReadOnlyStorage wraps a remote.Storage and transparently
// decrypts reads using the provided encryption key. Write operations
// return errors — this wrapper is read-only, intended for online restore
// of encrypted backups.
type decryptingReadOnlyStorage struct {
	inner remote.Storage
	key   []byte
}

var _ remote.Storage = (*decryptingReadOnlyStorage)(nil)

func (d *decryptingReadOnlyStorage) ReadObject(
	ctx context.Context, objName string,
) (_ remote.ObjectReader, objSize int64, _ error) {
	ciphertextReader, ciphertextSize, err := d.inner.ReadObject(ctx, objName)
	if err != nil {
		return nil, 0, err
	}

	dr, err := newDecryptingObjectReader(
		ctx, ciphertextReader, ciphertextSize, d.key,
	)
	if err != nil {
		_ = ciphertextReader.Close()
		return nil, 0, err
	}

	return dr, plaintextSize(ciphertextSize), nil
}

func (d *decryptingReadOnlyStorage) Close() error {
	return d.inner.Close()
}

func (d *decryptingReadOnlyStorage) Size(objName string) (int64, error) {
	return d.inner.Size(objName)
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
