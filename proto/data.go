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
	"math/rand"
	"sort"
	"strings"
	"time"

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

// Returns the next possible byte by appending an \x00.
func bytesNext(b []byte) []byte {
	if len(b) == KeyMaxLength && bytes.Equal(b, KeyMax) {
		panic(fmt.Sprint("cannot get the next bytes of KeyMax"))
	}

	return bytes.Join([][]byte{b, []byte{0}}, nil)
}

// Returns the previous byte bounded by the max key length.
// If the last byte is 0, then truncate it. Otherwise decrease the
// last byte by one and add in "\xff"s for the rest of the key length.
// Examples:
// 	 "" -> panic
//   "\x00" -> ""
//   "\xff00" -> "\xff"
//   "\xff03" -> "\xff\x02\xff..."
//   "\xff...\x01" -> "\xff...\x00"
//   "\xff...\x00" -> "\xff..." (same except the \x00 is removed)
//   "\xff..." -> "\xff...\xfe"
func bytesPrev(b []byte) []byte {
	length := len(b)

	// When the byte array is empty.
	if length == 0 {
		panic(fmt.Sprint("cannot get the prev bytes of an empty byte array"))
	}

	// If the last byte is a 0, then drop it.
	if b[length-1] == 0 {
		return append([]byte(nil), b[0:length-1]...)
	}

	// If the last byte isn't 0, subtract one from it and
	// append "\xff"s until the end of the key space.
	prefix := b[0 : length-1]
	reducedValue := []byte{b[length-1] - 1}
	postfix := KeyMax[length:KeyMaxLength]
	return bytes.Join([][]byte{prefix, reducedValue, postfix}, nil)
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

// Prev returns the prev key in lexicographic sort order.
func (k Key) Prev() Key {
	return Key(bytesPrev(k))
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

// Less implements the util.Ordered interface.
func (k EncodedKey) Less(l EncodedKey) bool {
	return bytes.Compare(k, l) < 0
}

// Equal returns whether two keys are identical.
func (k Key) Equal(l Key) bool {
	return bytes.Equal(k, l)
}

// Equal returns whether two keys are identical.
func (k EncodedKey) Equal(l EncodedKey) bool {
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
func (k Key) Marshal() ([]byte, error) {
	return []byte(k), nil
}

// Marshal implements the gogoproto Marshaler interface.
func (k EncodedKey) Marshal() ([]byte, error) {
	return []byte(k), nil
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
	MinTimestamp = Timestamp{WallTime: 0, Logical: 1}
	// ZeroTimestamp is an empty timestamp.
	ZeroTimestamp = Timestamp{WallTime: 0, Logical: 0}
	// NoTxnMD5 is a zero-filled md5 byte array, used to indicate a nil transaction.
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

// Add returns a timestamp with the WallTime and Logical components increased.
func (t Timestamp) Add(wallTime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: t.WallTime + wallTime,
		Logical:  t.Logical + logical,
	}
}

// Forward updates the timestamp from the one given, if that moves it
// forwards in time.
func (t *Timestamp) Forward(s Timestamp) {
	if t.Less(s) {
		*t = s
	}
}

// Backward updates the timestamp from the one given, if that moves it
// backwards in time.
func (t *Timestamp) Backward(s Timestamp) {
	if s.Less(*t) {
		*t = s
	}
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

// KeyGet is an implementation for KeyGetter.
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
		Name:          name,
		Key:           baseKey,
		ID:            []byte(uuid.New()),
		Priority:      priority,
		Isolation:     isolation,
		Timestamp:     now,
		OrigTimestamp: now,
		MaxTimestamp:  max,
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
	if userPriority < 0 {
		return -userPriority
	}
	if userPriority == 0 {
		userPriority = 1
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
	return math.MaxInt32 - rand.Int31n(math.MaxInt32/userPriority)
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

// Restart reconfigures a transaction for restart. The epoch is
// incremented for an in-place restart. The timestamp of the
// transaction on restart is set to the maximum of the transaction's
// timestamp and the specified timestamp.
func (t *Transaction) Restart(userPriority, upgradePriority int32, timestamp Timestamp) {
	t.Epoch++
	if t.Timestamp.Less(timestamp) {
		t.Timestamp = timestamp
	}
	// Set original timestamp to current timestamp on restart.
	t.OrigTimestamp = t.Timestamp
	// Potentially upgrade priority both by creating a new random
	// priority using userPriority and considering upgradePriority.
	t.UpgradePriority(MakePriority(userPriority))
	t.UpgradePriority(upgradePriority)
}

// Update ratchets priority, timestamp and original timestamp values (among
// others) for the transaction. If t.ID is empty, then the transaction is
// copied from o.
func (t *Transaction) Update(o *Transaction) {
	if o == nil {
		return
	}
	if len(t.ID) == 0 {
		*t = *gogoproto.Clone(o).(*Transaction)
		return
	}
	if o.Status != PENDING {
		t.Status = o.Status
	}
	if t.Epoch < o.Epoch {
		t.Epoch = o.Epoch
	}
	if t.Timestamp.Less(o.Timestamp) {
		t.Timestamp = o.Timestamp
	}
	if t.OrigTimestamp.Less(o.OrigTimestamp) {
		t.OrigTimestamp = o.OrigTimestamp
	}
	// Should not actually change at the time of writing.
	t.MaxTimestamp = o.MaxTimestamp
	// Copy the list of nodes without time uncertainty.
	t.CertainNodes = NodeList{Nodes: append(Int32Slice(nil),
		o.CertainNodes.Nodes...)}
	t.UpgradePriority(o.Priority)
}

// UpgradePriority sets transaction priority to the maximum of current
// priority and the specified minPriority.
func (t *Transaction) UpgradePriority(minPriority int32) {
	if minPriority > t.Priority {
		t.Priority = minPriority
	}
}

// MD5 returns the MD5 digest of the transaction ID. This method
// returns an empty string if the transaction is nil.
func (t *Transaction) MD5() [md5.Size]byte {
	if t == nil {
		return NoTxnMD5
	}
	return md5.Sum(t.ID)
}

// String formats transaction into human readable string.
func (t Transaction) String() string {
	return fmt.Sprintf("%q {id=%s pri=%d, iso=%s, stat=%s, epo=%d, ts=%s orig=%s max=%s}",
		t.Name, t.ID, t.Priority, t.Isolation, t.Status, t.Epoch, t.Timestamp, t.OrigTimestamp, t.MaxTimestamp)
}

// IsInline returns true if the value is inlined in the metadata.
func (mvcc *MVCCMetadata) IsInline() bool {
	return mvcc.Value != nil
}

// NewGCMetadata returns a GCMetadata with ByteCounts slice
// initialized to ten byte count values set to zero.
func NewGCMetadata() *GCMetadata {
	return &GCMetadata{
		ByteCounts: make([]int64, 10),
	}
}

// EstimatedBytes computes the estimated count of bytes which a GC run
// is likely to free based on the current time, current non-live
// bytes, and the details of the GCMetadata.
//
// The difference between the non-live bytes as recorded in GCMetadata
// as of the last GC and the current non-live bytes are the newly
// non-live bytes. These are prorated according to (elapsed fraction
// of TTL - 1) to determine which portion, if any, of the newly
// non-live bytes are likely to be GC'able.
//
// If the TTL changed between the last run and the current time, the
// estimate will of course be inaccurate, but that's OK for the
// purposes of deciding the priority of a range for GC.
func (gc *GCMetadata) EstimatedBytes(now time.Time, currentNonLiveBytes int64) int64 {
	elapsed := now.UnixNano() - gc.LastGCNanos
	ttlNanos := int64(gc.TTLSeconds) * 1000000000
	if elapsed < ttlNanos/10 || len(gc.ByteCounts) != 10 || ttlNanos == 0 {
		return 0
	}
	// Fraction of TTL we've advanced since last GC.
	ttlFraction := float64(elapsed) / float64(ttlNanos)
	// Compute index into the byte counts array. Think of this as: which
	// fraction of TTL (e.g. <10%, <20%, ...) that bytes would already
	// need to have been aged in order to be GC'able now.
	index := 10 - int(ttlFraction*10)
	if index < 0 {
		index = 0
	}
	expGCBytes := gc.ByteCounts[index]

	// If the ttlFraction is >= 1, prorate the fraction of current
	// non-live bytes we might expect to GC.
	if ttlFraction >= 1 {
		newNonLiveBytes := currentNonLiveBytes - gc.ByteCounts[0]
		return int64(((ttlFraction-1)/ttlFraction)*float64(newNonLiveBytes)) + expGCBytes
	}
	// Otherwise, just return the expGCBytes.
	return expGCBytes
}

// Add adds the given NodeID to the interface (unless already present)
// and restores ordering.
func (s *NodeList) Add(nodeID int32) {
	if !s.Contains(nodeID) {
		(*s).Nodes = append(s.Nodes, nodeID)
		sort.Sort(Int32Slice(s.Nodes))
	}
}

// Contains returns true if the underlying slice contains the given NodeID.
func (s NodeList) Contains(nodeID int32) bool {
	ns := s.GetNodes()
	i := sort.Search(len(ns), func(i int) bool { return ns[i] >= nodeID })
	return i < len(ns) && ns[i] == nodeID
}

// Int32Slice implements sort.Interface.
type Int32Slice []int32

func (s Int32Slice) Len() int           { return len(s) }
func (s Int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int32Slice) Less(i, j int) bool { return s[i] < s[j] }
