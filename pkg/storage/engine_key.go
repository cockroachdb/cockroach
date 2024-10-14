// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
// - Timestamp of MVCC keys: 8, 12, or 13 bytes.
// - Lock table key: 17 bytes.
type EngineKey struct {
	Key     roachpb.Key
	Version []byte
}

// There are multiple decoding functions in the storage package, optimized for
// their particular use case, that demultiplex on the various lengths below.
// If adding another length to this list, remember to search for code
// referencing these lengths and fix it.
// TODO(nvanbenschoten): unify these constants with those in mvcc_key.go.
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
// look like an encoded EngineKey. By splitting at Key + \x00, the Key looks
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
	case engineKeyVersionWallAndLogicalTimeLen, engineKeyVersionWallLogicalAndSyntheticTimeLen:
		key.Timestamp.WallTime = int64(binary.BigEndian.Uint64(k.Version[0:8]))
		key.Timestamp.Logical = int32(binary.BigEndian.Uint32(k.Version[8:12]))
		// NOTE: byte 13 used to store the timestamp's synthetic bit, but this is no
		// longer consulted and can be ignored during decoding.
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
	key.Strength, key.TxnUUID, err = k.decodeLockTableKeyVersion()
	if err != nil {
		return LockTableKey{}, err
	}
	return key, nil
}

// decodeLockTableKeyVersion decodes the strength and transaction ID from the
// version of a LockTableKey, without decoding the key.
func (k EngineKey) decodeLockTableKeyVersion() (lock.Strength, uuid.UUID, error) {
	if len(k.Version) != engineKeyVersionLockTableLen {
		return 0, uuid.UUID{}, errors.Errorf("version is not valid for a LockTableKey %x", k.Version)
	}
	str, err := getReplicatedLockStrengthForByte(k.Version[0])
	if err != nil {
		return 0, uuid.UUID{}, err
	}
	txnID := *(*uuid.UUID)(k.Version[1:])
	return str, txnID, nil
}

// Validate checks if the EngineKey is a valid MVCCKey or LockTableKey.
func (k EngineKey) Validate() error {
	if k.IsLockTableKey() {
		return keys.ValidateLockTableSingleKey(k.Key)
	}
	_, errMVCC := k.ToMVCCKey()
	return errMVCC
}

// DecodeEngineKey decodes the given bytes as an EngineKey. If the caller
// already knows that the key is an MVCCKey, the Version returned is the
// encoded timestamp.
func DecodeEngineKey(b []byte) (key EngineKey, ok bool) {
	if len(b) == 0 {
		return EngineKey{}, false
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(b[len(b)-1])
	if versionLen == 1 {
		// The key encodes an empty version, which is not valid.
		return EngineKey{}, false
	}
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(b) - 1 - versionLen
	if keyPartEnd < 0 || b[keyPartEnd] != 0x00 {
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
	if keyPartEnd < 0 || engineKey[keyPartEnd] != 0x00 {
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
	TxnUUID  uuid.UUID
}

// replicatedLockStrengthToByte is a mapping between lock.Strength and the
// strength byte persisted in a lock table key's encoding. See
// LockTableKey.ToEngineKey().
var replicatedLockStrengthToByte = [...]byte{
	lock.Shared:    1,
	lock.Exclusive: 2,
	lock.Intent:    3,
}

// byteToReplicatedLockStrength is a mapping between the strength byte persisted
// in a lock table key's encoding and the lock.Strength of the lock it
// corresponds to. Also see EngineKey.ToLockTableKey().
var byteToReplicatedLockStrength = func() (arr []lock.Strength) {
	maxByte := byte(0)
	for _, b := range replicatedLockStrengthToByte {
		if b > maxByte {
			maxByte = b
		}
	}
	arr = make([]lock.Strength, maxByte+1)
	for str, b := range replicatedLockStrengthToByte {
		if b != 0 {
			arr[b] = lock.Strength(str)
		}
	}
	return arr
}()

// getByteForReplicatedLockStrength returns a strength byte, suitable for use in
// a lock's key encoding, given its lock strength.
func getByteForReplicatedLockStrength(str lock.Strength) byte {
	if str < 0 || int(str) >= len(replicatedLockStrengthToByte) {
		panic(errors.AssertionFailedf("unexpected lock strength: %s", str))
	}
	b := replicatedLockStrengthToByte[str]
	if b == 0 {
		panic(errors.AssertionFailedf("unexpected lock strength: %s", str))
	}
	return b
}

// getReplicatedLockStrengthForByte returns a replicated lock's strength given
// the strength byte from its key encoding.
func getReplicatedLockStrengthForByte(b byte) (lock.Strength, error) {
	if int(b) >= len(byteToReplicatedLockStrength) { // byte cannot be < 0
		return lock.None, errors.AssertionFailedf("unexpected lock strength byte: %d", b)
	}
	str := byteToReplicatedLockStrength[b]
	if str == 0 {
		return lock.None, errors.AssertionFailedf("unexpected lock strength byte: %d", b)
	}
	return str, nil
}

// mustGetReplicatedLockStrengthForByte is like mustGetReplicatedLockStrength
// except it panics if there is an error.
func mustGetReplicatedLockStrengthForByte(b byte) lock.Strength {
	str, err := getReplicatedLockStrengthForByte(b)
	if err != nil {
		panic(err)
	}
	return str
}

// ToEngineKey converts a lock table key to an EngineKey. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to.
func (lk LockTableKey) ToEngineKey(buf []byte) (EngineKey, []byte) {
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
	k.Version[0] = getByteForReplicatedLockStrength(lk.Strength)
	copy(k.Version[1:], lk.TxnUUID[:])
	return k, buf
}

// EncodedSize returns the size of the LockTableKey when encoded.
func (lk LockTableKey) EncodedSize() int64 {
	return int64(len(lk.Key)) + engineKeyVersionLockTableLen
}

// EngineRangeKeyValue is a raw value for a general range key as stored in the
// engine. It consists of a version (suffix) and corresponding value. The range
// key bounds are not included, but are surfaced via EngineRangeBounds().
type EngineRangeKeyValue struct {
	Version []byte
	Value   []byte
}

// Verify ensures the checksum of the current batch entry matches the data.
// Returns an error on checksum mismatch.
func (key *EngineKey) Verify(value []byte) error {
	if key.IsMVCCKey() {
		mvccKey, err := key.ToMVCCKey()
		if err != nil {
			return err
		}
		if mvccKey.IsValue() {
			return decodeMVCCValueAndVerify(mvccKey.Key, value)
		} else {
			return decodeMVCCMetaAndVerify(mvccKey.Key, value)
		}
	} else if key.IsLockTableKey() {
		lockTableKey, err := key.ToLockTableKey()
		if err != nil {
			return err
		}
		return decodeMVCCMetaAndVerify(lockTableKey.Key, value)
	}
	return decodeMVCCMetaAndVerify(key.Key, value)
}

// decodeMVCCValueAndVerify will try to decode the value as
// MVCCValue and then verify the checksum.
func decodeMVCCValueAndVerify(key roachpb.Key, value []byte) error {
	mvccValue, err := decodeMVCCValueIgnoringHeader(value)
	if err != nil {
		return err
	}
	return mvccValue.Value.Verify(key)
}

// decodeMVCCMetaAndVerify will try to decode the value as
// enginepb.MVCCMetadata and then try to  convert the rawbytes
// as MVCCValue then verify the checksum.
func decodeMVCCMetaAndVerify(key roachpb.Key, value []byte) error {
	// TODO(lyang24): refactor to avoid allocation for MVCCMetadata
	// per each call.
	var meta enginepb.MVCCMetadata
	// Time series data might fail the decoding i.e.
	// key 61
	// value 0262000917bba16e0aea5ca80900
	// N.B. we skip checksum checking in this case.
	// nolint:returnerrcheck
	if err := protoutil.Unmarshal(value, &meta); err != nil {
		return nil
	}
	return decodeMVCCValueAndVerify(key, meta.RawBytes)
}

// EngineKeyRange is a key range composed of EngineKeys.
type EngineKeyRange struct {
	Start, End EngineKey
}
