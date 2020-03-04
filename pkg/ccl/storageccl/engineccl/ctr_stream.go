// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
)

// FileCipherStreamCreator wraps the KeyManager interface and provides functions to create a
// FileStream for either a new file (using the active key provided by the KeyManager) or an
// existing file (by looking up the key in the KeyManager).
type FileCipherStreamCreator struct {
	envType    enginepb.EnvType
	keyManager PebbleKeyManager
}

const (
	// The difference is 4 bytes, which are supplied by the counter.
	ctrBlockSize = 16
	ctrNonceSize = 12
)

// CreateNew creates a FileStream for a new file using the currently active key. It returns the
// settings used, so that the caller can record these in a file registry.
func (c *FileCipherStreamCreator) CreateNew(
	ctx context.Context,
) (*enginepbccl.EncryptionSettings, FileStream, error) {
	key, err := c.keyManager.ActiveKey(ctx)
	if err != nil {
		return nil, nil, err
	}
	settings := &enginepbccl.EncryptionSettings{}
	if key == nil || key.Info.EncryptionType == enginepbccl.EncryptionType_Plaintext {
		settings.EncryptionType = enginepbccl.EncryptionType_Plaintext
		stream := &filePlainStream{}
		return settings, stream, nil
	}
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
	ctrCS, err := newCTRBlockCipherStream(key, settings.Nonce, settings.Counter)
	if err != nil {
		return nil, nil, err
	}
	return settings, &fileCipherStream{bcs: ctrCS}, nil
}

// CreateExisting creates a FileStream for an existing file by looking up the key described by
// settings in the key manager.
func (c *FileCipherStreamCreator) CreateExisting(
	settings *enginepbccl.EncryptionSettings,
) (FileStream, error) {
	if settings == nil || settings.EncryptionType == enginepbccl.EncryptionType_Plaintext {
		return &filePlainStream{}, nil
	}
	key, err := c.keyManager.GetKey(settings.KeyId)
	if err != nil {
		return nil, err
	}
	ctrCS, err := newCTRBlockCipherStream(key, settings.Nonce, settings.Counter)
	if err != nil {
		return nil, err
	}
	return &fileCipherStream{bcs: ctrCS}, nil
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
	key     *enginepbccl.SecretKey
	nonce   [ctrNonceSize]byte
	counter uint32

	cBlock cipher.Block
}

func newCTRBlockCipherStream(
	key *enginepbccl.SecretKey, nonce []byte, counter uint32,
) (*cTRBlockCipherStream, error) {
	switch key.Info.EncryptionType {
	case enginepbccl.EncryptionType_AES128_CTR:
	case enginepbccl.EncryptionType_AES192_CTR:
	case enginepbccl.EncryptionType_AES256_CTR:
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
	for i := 0; i < ctrBlockSize; i++ {
		data[i] = data[i] ^ iv[i]
	}
}
