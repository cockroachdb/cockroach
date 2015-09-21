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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/biogo/store/interval"
	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/uuid"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// KeyMaxLength is the maximum length of a Key in bytes.
	KeyMaxLength = 4096
	// MaxPriority is the maximum allowed priority.
	MaxPriority = math.MaxInt32
)

var (
	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = Key("")
	// KeyMax is a maximum key value which sorts after all other keys.
	KeyMax = Key{0xff, 0xff}
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

// KeySlice implements sort.Interface.
type KeySlice []Key

func (s KeySlice) Len() int           { return len(s) }
func (s KeySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s KeySlice) Less(i, j int) bool { return s[i].Less(s[j]) }

// Returns the next possible byte by appending an \x00.
func bytesNext(b []byte) []byte {
	// TODO(spencer): Do we need to enforce KeyMaxLength here?
	return append(append([]byte(nil), b...), 0)
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

// IsPrev is a more efficient version of k.Next().Equal(m).
func (k Key) IsPrev(m Key) bool {
	l := len(m) - 1
	return l == len(k) && m[l] == 0 && k.Equal(m[:l])
}

// Next returns the next key in lexicographic sort order.
// TODO(tschottdorf): duplicate code with (Key).Next().
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

// Less compares two keys.
func (k Key) Less(l Key) bool {
	return bytes.Compare(k, l) < 0
}

// Less compares two keys.
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

// String returns a string-formatted version of the key.
func (k Key) String() string {
	return fmt.Sprintf("%q", []byte(k))
}

// String returns a string-formatted version of the key.
func (k EncodedKey) String() string {
	return fmt.Sprintf("%q", []byte(k))
}

// Format implements the fmt.Formatter interface.
func (k Key) Format(f fmt.State, verb rune) {
	// Note: this implementation doesn't handle the width and precision
	// specifiers such as "%20.10s".
	fmt.Fprint(f, strconv.Quote(string(k)))
}

// Format implements the fmt.Formatter interface.
func (k EncodedKey) Format(f fmt.State, verb rune) {
	// Note: this implementation doesn't handle the width and precision
	// specifiers such as "%20.10s".
	fmt.Fprint(f, strconv.Quote(string(k)))
}

// Timestamp constant values.
var (
	// MaxTimestamp is the max value allowed for Timestamp.
	MaxTimestamp = Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}
	// MinTimestamp is the min value allowed for Timestamp.
	MinTimestamp = Timestamp{WallTime: 0, Logical: 1}
	// ZeroTimestamp is an empty timestamp.
	ZeroTimestamp = Timestamp{WallTime: 0, Logical: 0}
)

// Less compares two timestamps.
func (t Timestamp) Less(s Timestamp) bool {
	return t.WallTime < s.WallTime || (t.WallTime == s.WallTime && t.Logical < s.Logical)
}

// Equal returns whether two timestamps are the same.
func (t Timestamp) Equal(s Timestamp) bool {
	return t.WallTime == s.WallTime && t.Logical == s.Logical
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%.09f,%d", float64(t.WallTime)/1E9, t.Logical)
}

// Add returns a timestamp with the WallTime and Logical components increased.
func (t Timestamp) Add(wallTime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: t.WallTime + wallTime,
		Logical:  t.Logical + logical,
	}
}

// Next returns the timestamp with the next later timestamp.
func (t *Timestamp) Next() Timestamp {
	if t.Logical == math.MaxInt32 {
		if t.WallTime == math.MaxInt32 {
			panic("cannot take the next value to a max timestamp")
		}
		return Timestamp{
			WallTime: t.WallTime + 1,
		}
	}
	return Timestamp{
		WallTime: t.WallTime,
		Logical:  t.Logical + 1,
	}
}

// Prev returns the next earliest timestamp.
func (t *Timestamp) Prev() Timestamp {
	if t.Logical > 0 {
		return Timestamp{
			WallTime: t.WallTime,
			Logical:  t.Logical - 1,
		}
	} else if t.WallTime > 0 {
		return Timestamp{
			WallTime: t.WallTime - 1,
			Logical:  math.MaxInt32,
		}
	}
	panic("cannot take the previous value to a zero timestamp")
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

// GoTime converts the timestamp to a time.Time.
func (t *Timestamp) GoTime() time.Time {
	sec := t.WallTime / 1e9
	nsec := t.WallTime % 1e9
	return time.Unix(sec, nsec)
}

// InitChecksum initializes a checksum based on the provided key and
// the contents of the value. If the value contains a byte slice, the
// checksum includes it directly.
func (v *Value) InitChecksum(key []byte) {
	if v.Checksum == nil {
		v.Checksum = gogoproto.Uint32(v.computeChecksum(key))
	}
}

// Verify verifies the value's Checksum matches a newly-computed
// checksum of the value's contents. If the value's Checksum is not
// set the verification is a noop.
func (v *Value) Verify(key []byte) error {
	if v.Checksum != nil {
		cksum := v.computeChecksum(key)
		if v.GetChecksum() != cksum {
			return fmt.Errorf("invalid checksum (%d) for key %s, value [% x]",
				cksum, Key(key), v)
		}
	}
	return nil
}

// SetInteger encodes the specified int64 value into the bytes field of the
// receiver.
func (v *Value) SetInteger(i int64) {
	v.Bytes = encoding.EncodeUint64(nil, uint64(i))
}

// GetInteger decodes an int64 value from the bytes field of the receiver. If
// the bytes field is not 0 or 8 bytes in length an error will be returned.
func (v *Value) GetInteger() (int64, error) {
	if v == nil || len(v.Bytes) == 0 {
		return 0, nil
	}
	if len(v.Bytes) != 8 {
		return 0, fmt.Errorf("uint64 value should be exactly 8 bytes: %d", len(v.Bytes))
	}
	_, u := encoding.DecodeUint64(v.Bytes)
	return int64(u), nil
}

// SetProto encodes the specified proto message into the bytes field of the receiver.
func (v *Value) SetProto(msg gogoproto.Message) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	v.Bytes = data
	return nil
}

// computeChecksum computes a checksum based on the provided key and
// the contents of the value. If the value contains a byte slice, the
// checksum includes it directly.
func (v *Value) computeChecksum(key []byte) uint32 {
	c := encoding.NewCRC32Checksum(key)
	if v.Bytes != nil {
		if _, err := c.Write(v.Bytes); err != nil {
			panic(err)
		}
	}
	sum := c.Sum32()
	encoding.ReleaseCRC32Checksum(c)
	return sum
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
// affected by the transaction) and a random ID to guarantee
// uniqueness. The specified user-level priority is combined with a
// randomly chosen value to yield a final priority, used to settle
// write conflicts in a way that avoids starvation of long-running
// transactions (see Replica.PushTxn).
func NewTransaction(name string, baseKey Key, userPriority int32,
	isolation IsolationType, now Timestamp, maxOffset int64) *Transaction {
	// Compute priority by adjusting based on userPriority factor.
	priority := MakePriority(nil, userPriority)
	// Compute timestamp and max timestamp.
	max := now
	max.WallTime += maxOffset

	return &Transaction{
		Name:          name,
		Key:           baseKey,
		ID:            uuid.NewUUID4(),
		Priority:      priority,
		Isolation:     isolation,
		Timestamp:     now,
		OrigTimestamp: now,
		MaxTimestamp:  max,
	}
}

// Equal tests two transactions for equality. They are equal if they are
// either simultaneously nil or their IDs match.
func (t *Transaction) Equal(s *Transaction) bool {
	if t == nil && s == nil {
		return true
	}
	if (t == nil && s != nil) || (t != nil && s == nil) {
		return false
	}
	return TxnIDEqual(t.ID, s.ID)
}

// TraceID implements tracer.Traceable. For a nontrivial Transaction, it
// returns 't', followed by the transaction ID. Otherwise, the empty string is
// returned.
func (t *Transaction) TraceID() string {
	if t == nil || len(t.ID) == 0 {
		return ""
	}
	s := uuid.UUID(t.ID).String()
	return "t" + s
}

// TraceName implements tracer.Traceable. It returns TraceID, but using the
// short version of the UUID.
func (t *Transaction) TraceName() string {
	if t == nil || len(t.ID) == 0 {
		return "(none)"
	}
	return "t" + t.Short()
}

// IsInitialized returns true if the transaction has been initialized.
func (t *Transaction) IsInitialized() bool {
	return len(t.ID) > 0
}

// MakePriority generates a random priority value, biased by the
// specified userPriority. If userPriority=100, the resulting
// priority is 100x more likely to be probabilistically greater
// than a similar invocation with userPriority=1.
func MakePriority(r *rand.Rand, userPriority int32) int32 {
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
	if r != nil {
		return math.MaxInt32 - r.Int31n(math.MaxInt32/userPriority)
	}
	return math.MaxInt32 - rand.Int31n(math.MaxInt32/userPriority)
}

// TxnIDEqual returns whether the transaction IDs are equal.
func TxnIDEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
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
	t.UpgradePriority(MakePriority(nil, userPriority))
	t.UpgradePriority(upgradePriority)
}

// Update ratchets priority, timestamp and original timestamp values (among
// others) for the transaction. If t.ID is empty, then the transaction is
// copied from o.
// TODO(tschottdorf): Make sure this updates all required fields. This method
// is prone to code rot.
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
	if t.LastHeartbeat == nil || (o.LastHeartbeat == nil || t.LastHeartbeat.Less(*o.LastHeartbeat)) {
		t.LastHeartbeat = o.LastHeartbeat
	}
	// Should not actually change at the time of writing.
	t.MaxTimestamp = o.MaxTimestamp
	// Copy the list of nodes without time uncertainty.
	t.CertainNodes = NodeList{Nodes: append(Int32Slice(nil),
		o.CertainNodes.Nodes...)}
	t.UpgradePriority(o.Priority)
	if t.Writing && !o.Writing {
		// TODO(tschottdorf): false positives; see #2300.
		// panic("r/w status regression")
	} else {
		t.Writing = o.Writing
	}
}

// UpgradePriority sets transaction priority to the maximum of current
// priority and the specified minPriority.
func (t *Transaction) UpgradePriority(minPriority int32) {
	if minPriority > t.Priority {
		t.Priority = minPriority
	}
}

// String formats transaction into human readable string.
func (t Transaction) String() string {
	var buf bytes.Buffer
	// Compute priority as a floating point number from 0-100 for readability.
	floatPri := 100 * float64(t.Priority) / float64(math.MaxInt32)
	if len(t.Name) > 0 {
		fmt.Fprintf(&buf, "%q ", t.Name)
	}
	fmt.Fprintf(&buf, "id=%s key=%s rw=%t pri=%.8f iso=%s stat=%s epo=%d ts=%s orig=%s max=%s",
		uuid.UUID(t.ID).Short(), t.Key, t.Writing, floatPri, t.Isolation, t.Status, t.Epoch, t.Timestamp, t.OrigTimestamp, t.MaxTimestamp)
	return buf.String()
}

// Short returns the short form of the Transaction's UUID.
func (t *Transaction) Short() string {
	return uuid.UUID(t.GetID()).Short()
}

// NewGCMetadata returns a GCMetadata initialized to have a ByteCounts
// slice with ten byte count values set to zero.  Now is specified as
// nanoseconds since the Unix epoch.
func NewGCMetadata(nowNanos int64) *GCMetadata {
	return &GCMetadata{
		LastScanNanos:     nowNanos,
		OldestIntentNanos: gogoproto.Int64(nowNanos),
	}
}

// Add adds the given NodeID to the interface (unless already present)
// and restores ordering.
func (s *NodeList) Add(nodeID NodeID) {
	if !s.Contains(nodeID) {
		(*s).Nodes = append(s.Nodes, int32(nodeID))
		sort.Sort(Int32Slice(s.Nodes))
	}
}

// Contains returns true if the underlying slice contains the given NodeID.
func (s NodeList) Contains(nodeID NodeID) bool {
	ns := s.Nodes
	i := sort.Search(len(ns), func(i int) bool { return NodeID(ns[i]) >= nodeID })
	return i < len(ns) && NodeID(ns[i]) == nodeID
}

// Int32Slice implements sort.Interface.
type Int32Slice []int32

func (s Int32Slice) Len() int           { return len(s) }
func (s Int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int32Slice) Less(i, j int) bool { return s[i] < s[j] }

var _ fmt.Stringer = &Lease{}

func (l Lease) String() string {
	nodeID, storeID := DecodeRaftNodeID(RaftNodeID(l.RaftNodeID))
	t := time.Unix(l.Start.WallTime/1E9, 0)
	tStr := t.Format("15:04:05.000")
	return fmt.Sprintf("replica %d:%d %s +%.3fs", nodeID, storeID, tStr, float64(l.Expiration.WallTime-l.Start.WallTime)/1E9)
}

// Covers returns true if the given timestamp is strictly less than the
// Lease expiration, which indicates that the lease holder is authorized
// to carry out operations with that timestamp.
func (l Lease) Covers(timestamp Timestamp) bool {
	return timestamp.Less(l.Expiration)
}

// OwnedBy returns whether the lease owner is equal to the given RaftNodeID.
func (l Lease) OwnedBy(id RaftNodeID) bool {
	return l.RaftNodeID == id
}
