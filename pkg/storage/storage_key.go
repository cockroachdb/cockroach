package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// TODO: rename to EngineKey.
// TODO: also introduce a LockTableKey which is a specialization of EngineKey
// that separates out the strength and UUID that are in the suffix.
type StorageKey struct {
	Key    []byte
	Suffix []byte
}

// Format implements the fmt.Formatter interface
func (k StorageKey) Format(f fmt.State, c rune) {
	fmt.Fprintf(f, "%s/%x", roachpb.Key(k.Key), k.Suffix)
}

const (
	sentinelLen            = 1
	suffixEncodedLengthLen = 1
)

func (k StorageKey) EncodedLen() int {
	n := len(k.Key) + suffixEncodedLengthLen
	suffixLen := len(k.Suffix)
	if suffixLen > 0 {
		n += sentinelLen + suffixLen
	}
	return n
}

func (k StorageKey) Encode() []byte {
	encodedLen := k.EncodedLen()
	buf := make([]byte, encodedLen)
	k.encodeToSizedBuf(buf)
	return buf
}

func (k StorageKey) EncodeToBuf(buf []byte) []byte {
	encodedLen := k.EncodedLen()
	if cap(buf) < encodedLen {
		buf = make([]byte, encodedLen)
	} else {
		buf = buf[:encodedLen]
	}
	k.encodeToSizedBuf(buf)
	return buf
}

func (k StorageKey) encodeToSizedBuf(buf []byte) {
	copy(buf, k.Key)
	pos := len(k.Key)
	suffixLen := len(k.Suffix)
	if suffixLen > 0 {
		buf[pos] = 0
		pos += sentinelLen
		copy(buf[pos:], k.Suffix)
	}
	buf[len(buf)-1] = byte(suffixLen)
}

func (k StorageKey) IsMVCCKey() bool {
	// TODO: add a comment and const values for the various suffix lengths
	// and use that below.
	return len(k.Suffix) != 16
}

func DecodeStorageKey(key []byte) (skey StorageKey, ok bool) {
	if len(key) == 0 {
		return StorageKey{}, false
	}
	suffixLen := int(key[len(key)-1])
	keyPartEnd := len(key) - 1 - suffixLen
	if keyPartEnd < 0 {
		return StorageKey{}, false
	}

	skey.Key = key[:keyPartEnd]
	if suffixLen > 0 {
		skey.Suffix = key[keyPartEnd+1 : len(key)-1]
	}
	return skey, true
}

func StorageKeyToMVCCKey(skey StorageKey) (MVCCKey, error) {
	k := MVCCKey{Key: skey.Key}
	switch len(skey.Suffix) {
	case 0:
		// No-op.
	case 8:
		k.Timestamp.WallTime = int64(binary.BigEndian.Uint64(skey.Suffix[0:8]))
	case 12:
		k.Timestamp.WallTime = int64(binary.BigEndian.Uint64(skey.Suffix[0:8]))
		k.Timestamp.Logical = int32(binary.BigEndian.Uint32(skey.Suffix[8:12]))
	default:
		return k, errors.Errorf(
			"invalid mvcc key: %x bad timestamp %x", skey.Key, skey.Suffix)
	}
	return k, nil
}

// storageKeyFormatter is an fmt.Formatter for storage keys.
type storageKeyFormatter struct {
	skey StorageKey
}

var _ fmt.Formatter = storageKeyFormatter{}

// Format implements the fmt.Formatter interface.
func (m storageKeyFormatter) Format(f fmt.State, c rune) {
	m.skey.Format(f, c)
}
