// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"math"
	"strings"

	"code.google.com/p/biogo.store/interval"
	"code.google.com/p/biogo.store/llrb"
	"code.google.com/p/go-uuid/uuid"
	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// KeyMaxLength is the maximum length of a Key in bytes.
const KeyMaxLength = 4096

var (
	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = Key("")
	// KeyMax is a maximum key value which sorts after all other keys.
	KeyMax = Key(strings.Repeat("\xff", KeyMaxLength))
)

// Key is a custom type for a byte string in proto
// messages which refer to Cockroach keys.
type Key []byte

// EncodedKey is an encoded key, distinguished from Key in that it is
// an encoded version.
type EncodedKey []byte

// MakeKey makes a new key which is the concatenation of the
// given inputs, in order.
func MakeKey(keys ...Key) Key {
	byteSlices := make([][]byte, len(keys))
	for i, k := range keys {
		byteSlices[i] = []byte(k)
	}
	return Key(bytes.Join(byteSlices, nil))
}

func bytesNext(b []byte) []byte {
	return bytes.Join([][]byte{b, []byte{0}}, nil)
}

func bytesPrefixEnd(b []byte) []byte {
	end := append([]byte(nil), b...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end
		}
	}
	// This statement will only be reached if the key is already a
	// maximal byte string (i.e. already \xff...).
	return b
}

// Next returns the next key in lexicographic sort order.
func (k Key) Next() Key {
	return Key(bytesNext(k))
}

// Next returns the next key in lexicographic sort order.
func (k EncodedKey) Next() EncodedKey {
	return EncodedKey(bytes.Join([][]byte{k, Key{0}}, nil))
}

// PrefixEnd determines the end key given key as a prefix, that is the
// key that sorts precisely behind all keys starting with prefix: "1"
// is added to the final byte and the carry propagated. The special
// cases of nil and KeyMin always returns KeyMax.
func (k Key) PrefixEnd() Key {
	if len(k) == 0 {
		return KeyMax
	}
	return Key(bytesPrefixEnd(k))
}

// PrefixEnd determines the key directly after the last key which has
// this key as a prefix. See comments for Key.
func (k EncodedKey) PrefixEnd() EncodedKey {
	if len(k) == 0 {
		return EncodedKey(KeyMax)
	}
	return EncodedKey(bytesPrefixEnd(k))
}

// Less implements the util.Ordered interface.
func (k Key) Less(l Key) bool {
	return bytes.Compare(k, l) < 0
}

// Equal returns whether two keys are identical.
func (k Key) Equal(l Key) bool {
	return bytes.Equal(k, l)
}

// Compare implements the llrb.Comparable interface for tree nodes.
func (k Key) Compare(b interval.Comparable) int {
	return bytes.Compare(k, b.(Key))
}

// String returns a string-formatted version, with a maximum
// key formatted for brevity as "\xff...".
func (k Key) String() string {
	if idx := bytes.Index(k, KeyMax); idx != -1 {
		return string(MakeKey(k[:idx], Key("\xff..."), k[idx+KeyMaxLength:]))
	}
	return string(k)
}

// The following methods implement the custom marshalling and
// unmarshalling necessary to define gogoproto custom types.

// Marshal implements the gogoproto Marshaler interface.
func (k *Key) Marshal() ([]byte, error) {
	return []byte(*k), nil
}

// Marshal implements the gogoproto Marshaler interface.
func (k *EncodedKey) Marshal() ([]byte, error) {
	return []byte(*k), nil
}

// Unmarshal implements the gogoproto Unmarshaler interface.
func (k *Key) Unmarshal(bytes []byte) error {
	*k = Key(append([]byte(nil), bytes...))
	return nil
}

// Unmarshal implements the gogoproto Unmarshaler interface.
func (k *EncodedKey) Unmarshal(bytes []byte) error {
	*k = EncodedKey(append([]byte(nil), bytes...))
	return nil
}

// The following methods implement custom unmarshalling necessary
// for key objects to be converted from JSON.

// UnmarshalJSON implements the json Unmarshaler interface.
func (k *Key) UnmarshalJSON(bytes []byte) error {
	*k = Key(append([]byte(nil), bytes...))
	return nil
}

// UnmarshalJSON implements the json Unmarshaler interface.
func (k *EncodedKey) UnmarshalJSON(bytes []byte) error {
	*k = EncodedKey(append([]byte(nil), bytes...))
	return nil
}

// Timestamp constant values.
var (
	// MaxTimestamp is the max value allowed for Timestamp.
	MaxTimestamp = Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}
	// MinTimestamp is the min value allowed for Timestamp.
	MinTimestamp = Timestamp{WallTime: 0, Logical: 0}
	// emptyMD5 is a zero-filled md5 byte array, used to indicate a nil transaction.
	NoTxnMD5 = [md5.Size]byte{}
)

// Less implements the util.Ordered interface, allowing
// the comparison of timestamps.
func (t Timestamp) Less(s Timestamp) bool {
	return t.WallTime < s.WallTime || (t.WallTime == s.WallTime && t.Logical < s.Logical)
}

// Equal returns whether two timestamps are the same.
func (t Timestamp) Equal(s Timestamp) bool {
	return t.WallTime == s.WallTime && t.Logical == s.Logical
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%09d,%d", t.WallTime/1E9, t.WallTime%1E9, t.Logical)
}

// InitChecksum initializes a checksum based on the provided key and
// the contents of the value. If the value contains a byte slice, the
// checksum includes it directly; if the value contains an integer,
// the checksum includes the integer as 8 bytes in big-endian order.
func (v *Value) InitChecksum(key []byte) {
	if v.Checksum == nil {
		v.Checksum = gogoproto.Uint32(v.computeChecksum(key))
	}
}

// Verify verifies the value's Checksum matches a newly-computed
// checksum of the value's contents. If the value's Checksum is not
// set the verification is a noop. It also ensures that both Bytes
// and Integer are not both set.
func (v *Value) Verify(key []byte) error {
	if v.Checksum != nil {
		if v.GetChecksum() != v.computeChecksum(key) {
			return util.Errorf("invalid checksum for key %q, value %+v", key, v)
		}
	}
	if v.Bytes != nil && v.Integer != nil {
		return util.Errorf("both the value byte slice and integer fields are set for key %q: %+v", key, v)
	}
	return nil
}

// computeChecksum computes a checksum based on the provided key and
// the contents of the value. If the value contains a byte slice, the
// checksum includes it directly; if the value contains an integer,
// the checksum includes the integer as 8 bytes in big-endian order.
func (v *Value) computeChecksum(key []byte) uint32 {
	c := encoding.NewCRC32Checksum(key)
	if v.Bytes != nil {
		c.Write(v.Bytes)
	} else if v.Integer != nil {
		c.Write(encoding.EncodeUint64(nil, uint64(v.GetInteger())))
	}
	return c.Sum32()
}

// KeyGetter is a hack to allow Compare() to work for the batch
// update structs which wrap RawKeyValue.
// TODO(petermattis): Is there somehow a better way to do this?
//   It kept dying at runtime in the previous version of Compare
//   which type cast the llrb.Comparable to a RawKeyValue. Because
//   I'm wrapping a RawKeyValue with BatchDelete/BatchPut/BatchMerge.
type KeyGetter interface {
	KeyGet() []byte
}

// GetKey is an implementation for KeyGetter.
func (kv RawKeyValue) KeyGet() []byte { return kv.Key }

// Compare implements the llrb.Comparable interface for tree nodes.
func (kv RawKeyValue) Compare(b llrb.Comparable) int {
	return bytes.Compare(kv.Key, b.(KeyGetter).KeyGet())
}

// NewTransaction creates a new transaction. The transaction key is
// composed using the specified baseKey (for locality with data
// affected by the transaction) and a random UUID to guarantee
// uniqueness. The specified user-level priority is combined with
// a randomly chosen value to yield a final priority, used to settle
// write conflicts in a way that avoids starvation of long-running
// transactions (see Range.InternalPushTxn).
func NewTransaction(name string, baseKey Key, userPriority int32,
	isolation IsolationType, now Timestamp, maxOffset int64) *Transaction {
	// Compute priority by adjusting based on userPriority factor.
	priority := MakePriority(userPriority)
	// Compute timestamp and max timestamp.
	max := now
	max.WallTime += maxOffset

	return &Transaction{
		Name:         name,
		ID:           append(append([]byte(nil), baseKey...), []byte(uuid.New())...),
		Priority:     priority,
		Isolation:    isolation,
		Timestamp:    now,
		MaxTimestamp: max,
	}
}

// MakePriority generates a random priority value, biased by the
// specified userPriority. If userPriority=100, the resulting
// priority is 100x more likely to be probabilistically greater
// than a similar invocation with userPriority=1.
func MakePriority(userPriority int32) int32 {
	// A currently undocumented feature allows an explicit priority to
	// be set by specifying priority < 1. The explicit priority is
	// simply -userPriority in this case. This is hacky, but currently
	// used for unittesting. Perhaps this should be documented and allowed.
	if userPriority < 1 {
		return -userPriority
	}
	// The idea here is to bias selection of a random priority from the
	// range [1, 2^31-1) such that if userPriority=100, it's 100x more
	// likely to be a higher int32 than if userPriority=1. The formula
	// below chooses random values according to the following table:
	//   userPriority  |  range
	//   1             |  all positive int32s
	//   10            |  top 9/10ths of positive int32s
	//   100           |  top 99/100ths of positive int32s
	//   1000          |  top 999/1000ths of positive int32s
	//   ...etc
	return math.MaxInt32 - util.CachedRand.Int31n(math.MaxInt32/userPriority)
}

// MD5Equal returns whether the specified md5.Size byte arrays are equal.
func MD5Equal(a, b [md5.Size]byte) bool {
	for i := 0; i < md5.Size; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Restart reconfigures a transaction for restart. If the abort flag
// is true, a new transaction ID is allocated; otherwise, the epoch is
// incremented for an in-place retry. The timestamp of the transaction
// on restart is set to the maximum of the transaction's timestamp and
// the specified timestamp.
func (t *Transaction) Restart(abort bool, userPriority, upgradePriority int32, timestamp Timestamp) {
	if abort {
		uu := uuid.New()
		t.ID = append(append([]byte(nil), t.ID[:len(t.ID)-len(uu)]...), []byte(uu)...)
		t.Epoch = 0
	} else {
		t.Epoch++
	}
	if t.Timestamp.Less(timestamp) {
		t.Timestamp = timestamp
	}
	// Potentially upgrade priority both by creating a new random
	// priority using userPriority and considering upgradePriority.
	t.UpgradePriority(MakePriority(userPriority))
	t.UpgradePriority(upgradePriority)
}

// UpgradePriority sets transaction priority to the maximum of current
// priority and the specified minPriority.
func (t *Transaction) UpgradePriority(minPriority int32) {
	if minPriority > t.Priority {
		t.Priority = minPriority
	}
}

// MD5 returns the MD5 digest of the transaction ID as a string.
// This method returns an empty string if the transaction is nil.
func (t *Transaction) MD5() [md5.Size]byte {
	if t == nil {
		return NoTxnMD5
	}
	return md5.Sum(t.ID)
}

// String formats transaction into human readable string.
func (t Transaction) String() string {
	return fmt.Sprintf("%q {id=%s pri=%d, iso=%s, stat=%s, epo=%d, ts=%s}",
		t.Name, t.ID, t.Priority, t.Isolation, t.Status, t.Epoch, t.Timestamp)
}
