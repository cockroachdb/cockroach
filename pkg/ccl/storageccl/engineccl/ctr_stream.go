// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package engineccl

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// FileCipherStreamCreator wraps the KeyManager interface and provides functions to create a
// FileStream for either a new file (using the active key provided by the KeyManager) or an
// existing file (by looking up the key in the KeyManager).
type FileCipherStreamCreator struct {
	envType    enginepb.EnvType
	keyManager PebbleKeyManager
}

const (
	ctrBlockSize = 16
	// DEPRECATED: The V1 implementation had a distinction between a 12-byte
	// "nonce" and a 4-byte "counter". This was incorrect and we now treat the
	// IV/nonce as a single 128-bit value.
	ctrNonceSize = 12
)

// CreateNew creates a FileStream for a new file using the currently active key. It returns the
// settings used, so that the caller can record these in a file registry.
func (c *FileCipherStreamCreator) CreateNew(
	ctx context.Context,
) (*enginepb.EncryptionSettings, FileStream, error) {
	key, err := c.keyManager.ActiveKeyForWriter(ctx)
	if err != nil {
		return nil, nil, err
	}
	settings := &enginepb.EncryptionSettings{}
	if key == nil || key.Info.EncryptionType == enginepb.EncryptionType_Plaintext {
		settings.EncryptionType = enginepb.EncryptionType_Plaintext
	} else {
		settings.EncryptionType = key.Info.EncryptionType
		settings.KeyId = key.Info.KeyId
		settings.Nonce = make([]byte, ctrNonceSize)
		_, err = rand.Read(settings.Nonce)
		if err != nil {
			return nil, nil, err
		}
		counterBytes := make([]byte, 4)
		if _, err = rand.Read(counterBytes); err != nil {
			return nil, nil, err
		}
		// Does not matter how we convert 4 random bytes into uint32
		settings.Counter = binary.LittleEndian.Uint32(counterBytes)
	}

	fcs, err := createFileCipherStream(settings, key)
	if err != nil {
		return nil, nil, err
	}
	return settings, fcs, nil
}

func createFileCipherStream(
	settings *enginepb.EncryptionSettings, key *enginepb.SecretKey,
) (FileStream, error) {
	switch settings.EncryptionType {
	case enginepb.EncryptionType_Plaintext:
		return &filePlainStream{}, nil

	case enginepb.EncryptionType_AES128_CTR, enginepb.EncryptionType_AES192_CTR, enginepb.EncryptionType_AES256_CTR:
		ctrCS, err := newCTRBlockCipherStream(key, settings.Nonce, settings.Counter)
		if err != nil {
			return nil, err
		}
		return &fileCipherStream{bcs: ctrCS}, nil

	case enginepb.EncryptionType_AES_128_CTR_V2, enginepb.EncryptionType_AES_192_CTR_V2, enginepb.EncryptionType_AES_256_CTR_V2:
		var iv [ctrBlockSize]byte
		copy(iv[:ctrNonceSize], settings.Nonce)
		binary.BigEndian.PutUint32(iv[ctrNonceSize:ctrNonceSize+4], settings.Counter)
		fcs, err := newFileCipherStreamV2(key.Key, iv[:])
		if err != nil {
			return nil, err
		}
		return fcs, nil
	}
	return nil, fmt.Errorf("unknown encryption type %s", settings.EncryptionType)
}

// CreateExisting creates a FileStream for an existing file by looking up the key described by
// settings in the key manager.
func (c *FileCipherStreamCreator) CreateExisting(
	settings *enginepb.EncryptionSettings,
) (FileStream, error) {
	if settings == nil || settings.EncryptionType == enginepb.EncryptionType_Plaintext {
		return &filePlainStream{}, nil
	}
	key, err := c.keyManager.GetKey(settings.KeyId)
	if err != nil {
		return nil, err
	}
	return createFileCipherStream(settings, key)
}

// FileStream encrypts/decrypts byte slices at arbitrary file offsets.
//
// There are two implementations: a noop filePlainStream and a fileCipherStream that wraps
// a ctrBlockCipherStream. The ctrBlockCipherStream does AES in counter mode (CTR). CTR
// allows us to encrypt/decrypt at arbitrary byte offsets in a file (including partial
// blocks) without caring about what preceded the bytes.
type FileStream interface {
	// Encrypt encrypts the data to be written at fileOffset.
	Encrypt(fileOffset int64, data []byte)
	// Decrypt decrypts the data that has been read from fileOffset.
	Decrypt(fileOffset int64, data []byte)
}

// Implements a noop FileStream.
type filePlainStream struct{}

func (s *filePlainStream) Encrypt(fileOffset int64, data []byte) {}
func (s *filePlainStream) Decrypt(fileOffset int64, data []byte) {}

// Implements a FileStream with AES-CTR.
type fileCipherStream struct {
	bcs *cTRBlockCipherStream
}

func (s *fileCipherStream) Encrypt(fileOffset int64, data []byte) {
	if len(data) == 0 {
		return
	}
	blockIndex := uint64(fileOffset / int64(ctrBlockSize))
	blockOffset := int(fileOffset % int64(ctrBlockSize))
	// TODO(sbhola): Use sync.Pool for these temporary buffers.
	var buf struct {
		dataScratch [ctrBlockSize]byte
		ivScratch   [ctrBlockSize]byte
	}
	for len(data) > 0 {
		// The num bytes that must be encrypted in this block.
		byteCount := ctrBlockSize - blockOffset
		if byteCount > len(data) {
			// The data ends before the end of this block.
			byteCount = len(data)
		}
		if byteCount < int(ctrBlockSize) {
			// Need to copy into dataScratch, starting at blockOffset. NB: in CTR mode it does
			// not matter what is contained in the other bytes in the block (the ones we are not
			// initializing using this copy()).
			copy(buf.dataScratch[blockOffset:blockOffset+byteCount], data[:byteCount])
			s.bcs.transform(blockIndex, buf.dataScratch[:], buf.ivScratch[:])
			// Copy the transformed data back into data.
			copy(data[:byteCount], buf.dataScratch[blockOffset:blockOffset+byteCount])
			blockOffset = 0
		} else {
			s.bcs.transform(blockIndex, data[:byteCount], buf.ivScratch[:])
		}
		blockIndex++
		data = data[byteCount:]
	}
}

// For CTR, decryption and encryption are the same
func (s *fileCipherStream) Decrypt(fileOffset int64, data []byte) {
	s.Encrypt(fileOffset, data)
}

// AES in CTR mode.
type cTRBlockCipherStream struct {
	key     *enginepb.SecretKey
	nonce   [ctrNonceSize]byte
	counter uint32

	cBlock cipher.Block
}

func newCTRBlockCipherStream(
	key *enginepb.SecretKey, nonce []byte, counter uint32,
) (*cTRBlockCipherStream, error) {
	switch key.Info.EncryptionType {
	case enginepb.EncryptionType_AES128_CTR:
	case enginepb.EncryptionType_AES192_CTR:
	case enginepb.EncryptionType_AES256_CTR:
	default:
		return nil, fmt.Errorf("unknown EncryptionType: %d", key.Info.EncryptionType)
	}
	stream := &cTRBlockCipherStream{key: key, counter: counter}
	// Copy the nonce since the caller may overwrite it in the future.
	copy(stream.nonce[:], nonce)
	var err error
	if stream.cBlock, err = aes.NewCipher(key.Key); err != nil {
		return nil, err
	}
	if stream.cBlock.BlockSize() != ctrBlockSize {
		return nil, fmt.Errorf("unexpected block size: %d", stream.cBlock.BlockSize())
	}
	return stream, nil
}

// For CTR, decryption and encryption are the same. data must have length equal to
// the block size, and scratch must have length >= block size.
func (s *cTRBlockCipherStream) transform(blockIndex uint64, data []byte, scratch []byte) {
	iv := append(scratch[:0], s.nonce[:]...)
	var blockCounter = uint32(uint64(s.counter) + blockIndex)
	binary.BigEndian.PutUint32(iv[len(iv):len(iv)+4], blockCounter)
	iv = iv[0 : len(iv)+4]
	s.cBlock.Encrypt(iv, iv)
	subtle.XORBytes(data, data, iv)
}

type fileCipherStreamV2 struct {
	aesBlock cipher.Block
	// High and low portions of the 128-bit IV (big-endian).
	ivHi, ivLo uint64
	mu         struct {
		syncutil.Mutex
		// If ctr is non-nil, it is ready to use at fileOffset. This is
		// effectively a single-entry cache; it would be reasonable to change it
		// to a map from fileOffset to CTR objects to track multiple sequential
		// "cursors".
		fileOffset int64
		ctr        cipher.Stream
	}
}

func newFileCipherStreamV2(key, iv []byte) (*fileCipherStreamV2, error) {
	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &fileCipherStreamV2{
		aesBlock: aesBlock,
		ivHi:     binary.BigEndian.Uint64(iv[:8]),
		ivLo:     binary.BigEndian.Uint64(iv[8:]),
	}, nil
}

func (s *fileCipherStreamV2) Encrypt(fileOffset int64, data []byte) {
	var ctr cipher.Stream
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.mu.fileOffset == fileOffset {
			ctr = s.mu.ctr
			s.mu.ctr = nil
		}
	}()
	if ctr == nil {
		// We need to create a new CTR object seeked to the correct position.
		// This means we reimplement some of the IV math that appears inside the
		// CTR implementation.
		blockIndex := uint64(fileOffset / int64(ctrBlockSize))
		blockOffset := int(fileOffset % int64(ctrBlockSize))
		// Add the block index to the 128-bit IV. Overflow in the "hi" portion
		// just wraps around so we can use plain uint64 addition instead of
		// bits.Add64.
		blockIVLo, carry := bits.Add64(s.ivLo, blockIndex, 0)
		blockIVHi := s.ivHi + carry
		var iv [ctrBlockSize]byte
		binary.BigEndian.PutUint64(iv[0:8], blockIVHi)
		binary.BigEndian.PutUint64(iv[8:], blockIVLo)
		ctr = cipher.NewCTR(s.aesBlock, iv[:])
		if blockOffset != 0 {
			// If our read was not block-aligned, consume and discard the partial block.
			var scratch [ctrBlockSize]byte
			ctr.XORKeyStream(scratch[0:blockOffset], scratch[0:blockOffset])
		}
	}
	ctr.XORKeyStream(data, data)
	s.mu.Lock()
	defer s.mu.Unlock()
	// Save our CTR object for reuse in case the next operation follows directly
	// after this one.
	s.mu.ctr = ctr
	s.mu.fileOffset = fileOffset + int64(len(data))
}

func (s *fileCipherStreamV2) Decrypt(fileOffset int64, data []byte) {
	// For CTR mode, encryption and decryption are the same.
	s.Encrypt(fileOffset, data)
}
