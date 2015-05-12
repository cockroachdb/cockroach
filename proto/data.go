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

	"github.com/biogo/store/interval"
	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
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
	KeyMax = Key(encoding.Infinity)
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

// Unmarshal implements the gogoproto Unmarshaler interface.  It should never
// set k to be nil as a nil value should never have unmarshal called for it.
// Futhermore, appending a 0 length byte slice to a 0 length byte slice will
// produce a nil byte slice, which can cause problems when unmarshalling the
// engine.MinKey of "".
func (k *Key) Unmarshal(bytes []byte) error {
	if len(bytes) == 0 {
		*k = []byte{}
	} else {
		*k = Key(append([]byte(nil), bytes...))
	}
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

// Size is required for gogoproto's marshaller.
func (k Key) Size() int {
	return len(k)
}

// Size is required for gogoproto's marshaller.
func (k EncodedKey) Size() int {
	return len(k)
}

// MarshalTo is required for gogoproto's mashaller.
func (k *Key) MarshalTo(data []byte) (int, error) {
	return copy(data, []byte(*k)), nil
}

// MarshalTo is required for gogoproto's mashaller.
func (k *EncodedKey) MarshalTo(data []byte) (int, error) {
	return copy(data, []byte(*k)), nil
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
		cksum := v.computeChecksum(key)
		if v.GetChecksum() != cksum {
			return util.Errorf("invalid checksum (%d) for key %s, value [% x]",
				cksum, Key(key), v)
		}
	}
	if v.Bytes != nil && v.Integer != nil {
		return util.Errorf("both the value byte slice and integer fields are set for key %s: [% x]",
			Key(key), v)
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
// transactions (see Range.InternalPushTxn).
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
		ID:            util.NewUUID4(),
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

// String formats transaction into human readable string.
func (t Transaction) String() string {
	return fmt.Sprintf("%q {id=%s pri=%d, iso=%s, stat=%s, epo=%d, ts=%s orig=%s max=%s}",
		t.Name, util.UUID(t.ID), t.Priority, t.Isolation, t.Status, t.Epoch, t.Timestamp, t.OrigTimestamp, t.MaxTimestamp)
}

// IsInline returns true if the value is inlined in the metadata.
func (mvcc *MVCCMetadata) IsInline() bool {
	return mvcc.Value != nil
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

// ToInternal places the datapoints in a TimeSeriesData message into one or
// more InternalTimeSeriesData messages. The structure and number of messages
// returned depends on two variables: a key duration, and a sample duration.
//
// The key duration is an interval length in nanoseconds used to determine
// how many intervals are grouped into a single InternalTimeSeriesData
// message.
//
// The sample duration is also an interval length in nanoseconds; it must be
// less than or equal to the key duration, and must also evenly divide the key
// duration. Datapoints which fall into the same sample interval will be
// aggregated together into a single Sample.
//
// Example: Assume the desired result is to aggregate individual datapoints into
// the same sample if they occurred within the same second; additionally, all
// samples which occur within the same hour should be stored at the same key
// location within the same InternalTimeSeriesValue. The sample duration should
// be 10^9 nanoseconds (value of time.Second), and the key duration should be
// (3600*10^9) nanoseconds (value of time.Hour).
//
// Note that this method does not accumulate data into individual samples in the
// case where multiple datapoints fall into the same sample period. Internal
// data should be merged into the cockroach data store before being read; the
// engine is responsible for accumulating samples.
//
// The returned slice of InternalTimeSeriesData objects will not be sorted.
//
// For more information on how time series data is stored, see
// InternalTimeSeriesData and its related structures.
func (ts TimeSeriesData) ToInternal(keyDuration int64, sampleDuration int64) (
	[]*InternalTimeSeriesData, error) {
	if keyDuration%sampleDuration != 0 {
		return nil, util.Errorf(
			"sample duration %d does not evenly divide key duration %d.",
			sampleDuration, keyDuration)
	}
	if keyDuration < sampleDuration {
		return nil, util.Errorf(
			"sample duration %d is not less than or equal to key duration %d.",
			sampleDuration, keyDuration)
	}

	result := []*InternalTimeSeriesData{}
	resultByKeyTime := map[int64]*InternalTimeSeriesData{}

	for _, dp := range ts.Datapoints {
		// Determine which InternalTimeSeriesData this datapoint belongs to,
		// creating if it has not already been created for a previous sample.
		keyTime := (dp.TimestampNanos / keyDuration) * keyDuration
		itsd, ok := resultByKeyTime[keyTime]
		if !ok {
			itsd = &InternalTimeSeriesData{
				StartTimestampNanos: keyTime,
				SampleDurationNanos: sampleDuration,
			}
			result = append(result, itsd)
			resultByKeyTime[keyTime] = itsd
		}

		// Create a new sample for this datapoint and place it into the
		// InternalTimeSeriesData.
		sampleOffset := int32((dp.TimestampNanos - keyTime) / sampleDuration)
		sample := &InternalTimeSeriesSample{
			Offset: sampleOffset,
			Count:  1,
			Sum:    dp.Value,
		}
		itsd.Samples = append(itsd.Samples, sample)
	}

	return result, nil
}

// Delta returns the difference between two MVCCStats structures.
func (ms *MVCCStats) Delta(oms *MVCCStats) MVCCStats {
	result := *ms
	result.Subtract(oms)
	return result
}

// Add adds values from oms to ms.
func (ms *MVCCStats) Add(oms *MVCCStats) {
	ms.LiveBytes += oms.LiveBytes
	ms.KeyBytes += oms.KeyBytes
	ms.ValBytes += oms.ValBytes
	ms.IntentBytes += oms.IntentBytes
	ms.LiveCount += oms.LiveCount
	ms.KeyCount += oms.KeyCount
	ms.ValCount += oms.ValCount
	ms.IntentCount += oms.IntentCount
	ms.IntentAge += oms.IntentAge
	ms.GCBytesAge += oms.GCBytesAge
	ms.SysBytes += oms.SysBytes
	ms.SysCount += oms.SysCount
	ms.LastUpdateNanos += oms.LastUpdateNanos
}

// Subtract subtracts the values of oms from ms.
func (ms *MVCCStats) Subtract(oms *MVCCStats) {
	ms.LiveBytes -= oms.LiveBytes
	ms.KeyBytes -= oms.KeyBytes
	ms.ValBytes -= oms.ValBytes
	ms.IntentBytes -= oms.IntentBytes
	ms.LiveCount -= oms.LiveCount
	ms.KeyCount -= oms.KeyCount
	ms.ValCount -= oms.ValCount
	ms.IntentCount -= oms.IntentCount
	ms.IntentAge -= oms.IntentAge
	ms.GCBytesAge -= oms.GCBytesAge
	ms.SysBytes -= oms.SysBytes
	ms.SysCount -= oms.SysCount
	ms.LastUpdateNanos -= oms.LastUpdateNanos
}

var _ fmt.Stringer = &Lease{}

func (l Lease) String() string {
	return fmt.Sprintf("%s-%s@%d", l.Start, l.Expiration, l.RaftNodeID)
}
