package engineccl

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// A FileStreamCreator wraps the KeyManager interface and provides functions to create a
// FileStream for either a new file (using the active key provided by the KeyManager) or an
// existing file (by looking up the key in the KeyManager).
//
// A FileStream interface encrypts/decrypts byte slices at arbitrary file offsets.
// There are two implementations: a noop filePlainStream and a fileCipherStream that wraps
// a ctrBlockCipherStream. The ctrBlockCipherStream does AES in counter mode (CTR).

type FileCipherStreamCreator struct {
	envType enginepb.EnvType
	// Set keyManager when creating, but do not use it.
	keyManager KeyManager
}

const (
	// The difference is 4 bytes, which are supplied by the counter.
	kCTRBlockSize = 16
	kCTRNonceSize = 12
)

func (c *FileCipherStreamCreator) CreateNew() (*enginepbccl.EncryptionSettings, FileStream, error) {
	key, err := c.keyManager.ActiveKey()
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
	settings.Nonce = make([]byte, kCTRNonceSize)
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

func (c *FileCipherStreamCreator) CreateExisting(settings *enginepbccl.EncryptionSettings) (FileStream, error) {
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

type FileStream interface {
	Encrypt(fileOffset int64, data []byte)
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
	blockIndex := uint64(fileOffset / int64(kCTRBlockSize))
	blockOffset := int(fileOffset % int64(kCTRBlockSize))
	dataScratch := make([]byte, kCTRBlockSize)
	ivScratch := make([]byte, kCTRBlockSize)
	for len(data) > 0 {
		// The num bytes that must be encrypted in this block.
		byteCount := int(kCTRBlockSize - blockOffset)
		if byteCount > len(data) {
			// The data ends before the end of this block.
			byteCount = len(data)
		}
		if byteCount < int(kCTRBlockSize) {
			// Need to copy into dataScratch, starting at blockOffset.
			copy(dataScratch[blockOffset:blockOffset+byteCount], data[:byteCount])
			s.bcs.transform(blockIndex, dataScratch, ivScratch)
			// Copy the encrypted data back into data.
			copy(data[:byteCount], dataScratch[blockOffset:blockOffset+byteCount])
			blockOffset = 0
		} else {
			s.bcs.transform(blockIndex, data[:byteCount], ivScratch)
		}
		blockIndex += 1
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
	nonce   []byte
	counter uint32

	cBlock cipher.Block
}

func newCTRBlockCipherStream(key *enginepbccl.SecretKey, nonce []byte, counter uint32) (*cTRBlockCipherStream, error) {
	switch key.Info.EncryptionType {
	case enginepbccl.EncryptionType_AES128_CTR:
	case enginepbccl.EncryptionType_AES192_CTR:
	case enginepbccl.EncryptionType_AES256_CTR:
	default:
		return nil, fmt.Errorf("unknown EncryptionType: %d", key.Info.EncryptionType)
	}
	stream := &cTRBlockCipherStream{key: key, nonce: nonce, counter: counter}
	var err error
	if stream.cBlock, err = aes.NewCipher(key.Key); err != nil {
		return nil, err
	}
	if stream.cBlock.BlockSize() != kCTRBlockSize {
		return nil, fmt.Errorf("unexpected block size: %d", stream.cBlock.BlockSize())
	}
	return stream, nil
}

// For CTR, decryption and encryption are the same. data must have length equal to
// the block size, and scratch must have length >= block size.
func (s *cTRBlockCipherStream) transform(blockIndex uint64, data []byte, scratch []byte) {
	iv := append(scratch[:0], s.nonce...)
	var blockCounter uint32 = uint32(uint64(s.counter) + blockIndex)
	binary.BigEndian.PutUint32(iv[len(iv):len(iv)+4], blockCounter)
	iv = iv[0 : len(iv)+4]
	s.cBlock.Encrypt(iv, iv)
	for i := 0; i < kCTRBlockSize; i++ {
		data[i] = data[i] ^ iv[i]
	}
}
