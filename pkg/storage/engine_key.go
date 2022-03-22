// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// EngineKey is the general key type that is stored in the engine. It consists
// of a roachpb.Key followed by an optional "version". The term "version" is
// a loose one: often the version is a real version represented as an hlc.Timestamp,
// but it can also be the suffix of a lock table key containing the lock strength
// and txn UUID. These special cases have their own types, MVCCKey and LockTableKey.
// For key kinds that will never have a version, the code has historically used
// MVCCKey, though future code may be better served by using EngineKey (and we
// should consider changing all the legacy code).
//
// The version can have the following lengths in addition to 0 length.
// - Timestamp of MVCC keys: 8 or 12 bytes.
// - Lock table key: 17 bytes.
type EngineKey struct {
	Key     roachpb.Key
	Version []byte
}

// There are multiple decoding functions in the storage package, optimized for
// their particular use case, that demultiplex on the various lengths below.
// If adding another length to this list, remember to search for code
// referencing these lengths and fix it.
const (
	engineKeyNoVersion                             = 0
	engineKeyVersionWallTimeLen                    = 8
	engineKeyVersionWallAndLogicalTimeLen          = 12
	engineKeyVersionWallLogicalAndSyntheticTimeLen = 13
	engineKeyVersionLockTableLen                   = 17
)

// Format implements the fmt.Formatter interface
func (k EngineKey) Format(f fmt.State, c rune) {
	fmt.Fprintf(f, "%s/%x", k.Key, k.Version)
}

// Encoding:
// Key + \x00 (sentinel) [+ Version + <byte representing length of Version + 1>]
//
// The motivation for the sentinel is that we configure the underlying storage
// engine (Pebble) with a Split function that can be used for constructing
// Bloom filters over just the Key field. However, the encoded Key must also
// look like an encoded EngineKey. By splitting, at Key + \x00, the Key looks
// like an EngineKey with no Version.
const (
	sentinel               = '\x00'
	sentinelLen            = 1
	suffixEncodedLengthLen = 1
)

// Copy makes a copy of the key.
func (k EngineKey) Copy() EngineKey {
	buf := make([]byte, len(k.Key)+len(k.Version))
	copy(buf, k.Key)
	k.Key = buf[:len(k.Key)]
	if len(k.Version) > 0 {
		versionCopy := buf[len(k.Key):]
		copy(versionCopy, k.Version)
		k.Version = versionCopy
	}
	return k
}

// EncodedLen returns the encoded length of k.
func (k EngineKey) EncodedLen() int {
	n := len(k.Key) + suffixEncodedLengthLen
	versionLen := len(k.Version)
	if versionLen > 0 {
		n += sentinelLen + versionLen
	}
	return n
}

// Encode encoded the key.
func (k EngineKey) Encode() []byte {
	encodedLen := k.EncodedLen()
	buf := make([]byte, encodedLen)
	k.encodeToSizedBuf(buf)
	return buf
}

// EncodeToBuf attempts to reuse buf for encoding the key, and if undersized,
// allocates a new buffer.
func (k EngineKey) EncodeToBuf(buf []byte) []byte {
	encodedLen := k.EncodedLen()
	if cap(buf) < encodedLen {
		buf = make([]byte, encodedLen)
	} else {
		buf = buf[:encodedLen]
	}
	k.encodeToSizedBuf(buf)
	return buf
}

func (k EngineKey) encodeToSizedBuf(buf []byte) {
	copy(buf, k.Key)
	pos := len(k.Key)
	// The length of the suffix is the full encoded length (len(buf)) minus the
	// length of the key minus the length of the sentinel. Note that the
	// suffixLen is 0 when Version is empty, and when Version is non-empty, it
	// is len(Version)+1. That is, it includes the length byte at the end.
	suffixLen := len(buf) - pos - 1
	if suffixLen > 0 {
		buf[pos] = 0
		pos += sentinelLen
		copy(buf[pos:], k.Version)
	}
	buf[len(buf)-1] = byte(suffixLen)
}

// IsMVCCKey returns true if the key can be decoded as an MVCCKey.
// This includes the case of an empty timestamp.
func (k EngineKey) IsMVCCKey() bool {
	l := len(k.Version)
	return l == engineKeyNoVersion ||
		l == engineKeyVersionWallTimeLen ||
		l == engineKeyVersionWallAndLogicalTimeLen ||
		l == engineKeyVersionWallLogicalAndSyntheticTimeLen
}

// IsLockTableKey returns true if the key can be decoded as a LockTableKey.
func (k EngineKey) IsLockTableKey() bool {
	return len(k.Version) == engineKeyVersionLockTableLen
}

// ToMVCCKey constructs a MVCCKey from the EngineKey.
func (k EngineKey) ToMVCCKey() (MVCCKey, error) {
	key := MVCCKey{Key: k.Key}
	switch len(k.Version) {
	case engineKeyNoVersion:
		// No-op.
	case engineKeyVersionWallTimeLen:
		key.Timestamp.WallTime = int64(binary.BigEndian.Uint64(k.Version[0:8]))
	case engineKeyVersionWallAndLogicalTimeLen:
		key.Timestamp.WallTime = int64(binary.BigEndian.Uint64(k.Version[0:8]))
		key.Timestamp.Logical = int32(binary.BigEndian.Uint32(k.Version[8:12]))
	case engineKeyVersionWallLogicalAndSyntheticTimeLen:
		key.Timestamp.WallTime = int64(binary.BigEndian.Uint64(k.Version[0:8]))
		key.Timestamp.Logical = int32(binary.BigEndian.Uint32(k.Version[8:12]))
		key.Timestamp.Synthetic = k.Version[12] != 0
	default:
		return MVCCKey{}, errors.Errorf("version is not an encoded timestamp %x", k.Version)
	}
	return key, nil
}

// ToLockTableKey constructs a LockTableKey from the EngineKey.
func (k EngineKey) ToLockTableKey() (LockTableKey, error) {
	lockedKey, err := keys.DecodeLockTableSingleKey(k.Key)
	if err != nil {
		return LockTableKey{}, err
	}
	key := LockTableKey{Key: lockedKey}
	switch len(k.Version) {
	case engineKeyVersionLockTableLen:
		key.Strength = lock.Strength(k.Version[0])
		if key.Strength < lock.None || key.Strength > lock.Exclusive {
			return LockTableKey{}, errors.Errorf("unknown strength %d", key.Strength)
		}
		key.TxnUUID = k.Version[1:]
	default:
		return LockTableKey{}, errors.Errorf("version is not valid for a LockTableKey %x", k.Version)
	}
	return key, nil
}

// Validate checks if the EngineKey is a valid MVCCKey or LockTableKey.
func (k EngineKey) Validate() error {
	_, errMVCC := k.ToMVCCKey()
	_, errLock := k.ToLockTableKey()
	if errMVCC != nil && errLock != nil {
		return errors.Newf("key %s is neither an MVCCKey or LockTableKey", k)
	}
	return nil
}

// DecodeEngineKey decodes the given bytes as an EngineKey. This function is
// similar to enginepb.SplitMVCCKey.
// TODO(sumeer): consider removing SplitMVCCKey.
func DecodeEngineKey(b []byte) (key EngineKey, ok bool) {
	if len(b) == 0 {
		return EngineKey{}, false
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(b[len(b)-1])
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(b) - 1 - versionLen
	if keyPartEnd < 0 {
		return EngineKey{}, false
	}
	// Key excludes the sentinel byte.
	key.Key = b[:keyPartEnd]
	if versionLen > 0 {
		// Version consists of the bytes after the sentinel and before the length.
		key.Version = b[keyPartEnd+1 : len(b)-1]
	}
	return key, true
}

// GetKeyPartFromEngineKey is a specialization of DecodeEngineKey which avoids
// constructing a slice for the version part of the key, since the caller does
// not need it.
func GetKeyPartFromEngineKey(engineKey []byte) (key []byte, ok bool) {
	if len(engineKey) == 0 {
		return nil, false
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(engineKey[len(engineKey)-1])
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(engineKey) - 1 - versionLen
	if keyPartEnd < 0 {
		return nil, false
	}
	// Key excludes the sentinel byte.
	return engineKey[:keyPartEnd], true
}

// EngineKeyFormatter is a fmt.Formatter for EngineKeys.
type EngineKeyFormatter struct {
	key EngineKey
}

var _ fmt.Formatter = EngineKeyFormatter{}

// Format implements the fmt.Formatter interface.
func (m EngineKeyFormatter) Format(f fmt.State, c rune) {
	m.key.Format(f, c)
}

// LockTableKey is a key representing a lock in the lock table.
type LockTableKey struct {
	Key      roachpb.Key
	Strength lock.Strength
	// Slice is of length uuid.Size. We use a slice instead of a byte array, to
	// avoid copying a slice when decoding.
	TxnUUID []byte
}

// ToEngineKey converts a lock table key to an EngineKey. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to.
func (lk LockTableKey) ToEngineKey(buf []byte) (EngineKey, []byte) {
	if len(lk.TxnUUID) != uuid.Size {
		panic("invalid TxnUUID")
	}
	if lk.Strength != lock.Exclusive {
		panic("unsupported lock strength")
	}
	// The first term in estimatedLen is for LockTableSingleKey.
	estimatedLen :=
		(len(keys.LocalRangeLockTablePrefix) + len(keys.LockTableSingleKeyInfix) + len(lk.Key) + 3) +
			engineKeyVersionLockTableLen
	if cap(buf) < estimatedLen {
		buf = make([]byte, 0, estimatedLen)
	}
	ltKey, buf := keys.LockTableSingleKey(lk.Key, buf)
	k := EngineKey{Key: ltKey}
	if cap(buf)-len(buf) >= engineKeyVersionLockTableLen {
		k.Version = buf[len(buf) : len(buf)+engineKeyVersionLockTableLen]
	} else {
		// estimatedLen was an underestimate.
		k.Version = make([]byte, engineKeyVersionLockTableLen)
	}
	k.Version[0] = byte(lk.Strength)
	copy(k.Version[1:], lk.TxnUUID)
	return k, buf
}
