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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package roachpb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode"

	"github.com/biogo/store/interval"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// RKeyMin is a minimum key value which sorts before all other keys.
	RKeyMin = RKey("")
	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = Key(RKeyMin)
	// RKeyMax is a maximum key value which sorts after all other keys.
	RKeyMax = RKey{0xff, 0xff}
	// KeyMax is a maximum key value which sorts after all other keys.
	KeyMax = Key(RKeyMax)

	// PrettyPrintKey is a function to print key with human readable format
	// it's implement at package git.com/cockroachdb/cockroach/keys to avoid package circle import
	PrettyPrintKey func(key Key) string
)

// RKey denotes a Key whose local addressing has been accounted for.
// A key can be transformed to an RKey by keys.Addr().
type RKey Key

// AsRawKey returns the RKey as a Key. This is to be used only in select
// situations in which an RKey is known to not contain a wrapped locally-
// addressed Key. Whenever the Key which created the RKey is still available,
// it should be used instead.
func (rk RKey) AsRawKey() Key {
	return Key(rk)
}

// Less compares two RKeys.
func (rk RKey) Less(otherRK RKey) bool {
	return bytes.Compare(rk, otherRK) < 0
}

// Equal checks for byte-wise equality.
func (rk RKey) Equal(other []byte) bool {
	return bytes.Equal(rk, other)
}

// Next returns the RKey that sorts immediately after the given one.
// The method may only take a shallow copy of the RKey, so both the
// receiver and the return value should be treated as immutable after.
func (rk RKey) Next() RKey {
	return RKey(BytesNext(rk))
}

// PrefixEnd determines the end key given key as a prefix, that is the
// key that sorts precisely behind all keys starting with prefix: "1"
// is added to the final byte and the carry propagated. The special
// cases of nil and KeyMin always returns KeyMax.
func (rk RKey) PrefixEnd() RKey {
	if len(rk) == 0 {
		return RKeyMax
	}
	return RKey(bytesPrefixEnd(rk))
}

func (rk RKey) String() string {
	return Key(rk).String()
}

// Key is a custom type for a byte string in proto
// messages which refer to Cockroach keys.
type Key []byte

// BytesNext returns the next possible byte slice, using the extra capacity
// of the provided slice if possible, and if not, appending an \x00.
func BytesNext(b []byte) []byte {
	if cap(b) > len(b) {
		bNext := b[:len(b)+1]
		if bNext[len(bNext)-1] == 0 {
			return bNext
		}
	}
	// TODO(spencer): Do we need to enforce KeyMaxLength here?
	// Switched to "make and copy" pattern in #4963 for performance.
	bn := make([]byte, len(b)+1)
	copy(bn, b)
	bn[len(bn)-1] = 0
	return bn
}

func bytesPrefixEnd(b []byte) []byte {
	// Switched to "make and copy" pattern in #4963 for performance.
	end := make([]byte, len(b))
	copy(end, b)
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

// Next returns the next key in lexicographic sort order. The method may only
// take a shallow copy of the Key, so both the receiver and the return
// value should be treated as immutable after.
func (k Key) Next() Key {
	return Key(BytesNext(k))
}

// IsPrev is a more efficient version of k.Next().Equal(m).
func (k Key) IsPrev(m Key) bool {
	l := len(m) - 1
	return l == len(k) && m[l] == 0 && k.Equal(m[:l])
}

// PrefixEnd determines the end key given key as a prefix, that is the
// key that sorts precisely behind all keys starting with prefix: "1"
// is added to the final byte and the carry propagated. The special
// cases of nil and KeyMin always returns KeyMax.
func (k Key) PrefixEnd() Key {
	if len(k) == 0 {
		return Key(RKeyMax)
	}
	return Key(bytesPrefixEnd(k))
}

// Equal returns whether two keys are identical.
func (k Key) Equal(l Key) bool {
	return bytes.Equal(k, l)
}

// Compare implements the interval.Comparable interface for tree nodes.
func (k Key) Compare(b interval.Comparable) int {
	return bytes.Compare(k, b.(Key))
}

// String returns a string-formatted version of the key.
func (k Key) String() string {
	if PrettyPrintKey != nil {
		return PrettyPrintKey(k)
	}

	return fmt.Sprintf("%q", []byte(k))
}

// Format implements the fmt.Formatter interface.
func (k Key) Format(f fmt.State, verb rune) {
	// Note: this implementation doesn't handle the width and precision
	// specifiers such as "%20.10s".
	if PrettyPrintKey != nil {
		fmt.Fprint(f, PrettyPrintKey(k))
	} else {
		fmt.Fprintf(f, strconv.Quote(string(k)))
	}
}

const (
	checksumUninitialized = 0
	checksumSize          = 4
	tagPos                = checksumSize
	headerSize            = tagPos + 1
)

func (v Value) checksum() uint32 {
	if len(v.RawBytes) < checksumSize {
		return 0
	}
	_, u, err := encoding.DecodeUint32Ascending(v.RawBytes[:checksumSize])
	if err != nil {
		panic(err)
	}
	return u
}

func (v *Value) setChecksum(cksum uint32) {
	if len(v.RawBytes) >= checksumSize {
		encoding.EncodeUint32Ascending(v.RawBytes[:0], cksum)
	}
}

// InitChecksum initializes a checksum based on the provided key and
// the contents of the value. If the value contains a byte slice, the
// checksum includes it directly.
//
// TODO(peter): This method should return an error if the Value is corrupted
// (e.g. the RawBytes field is > 0 but smaller than the header size).
func (v *Value) InitChecksum(key []byte) {
	if v.RawBytes == nil {
		return
	}
	// Should be uninitialized.
	if v.checksum() != checksumUninitialized {
		panic(fmt.Sprintf("initialized checksum = %x", v.checksum()))
	}
	v.setChecksum(v.computeChecksum(key))
}

// ClearChecksum clears the checksum value.
func (v *Value) ClearChecksum() {
	v.setChecksum(0)
}

// Verify verifies the value's Checksum matches a newly-computed
// checksum of the value's contents. If the value's Checksum is not
// set the verification is a noop.
func (v Value) Verify(key []byte) error {
	if n := len(v.RawBytes); n > 0 && n < headerSize {
		return fmt.Errorf("%s: invalid header size: %d", Key(key), n)
	}
	if sum := v.checksum(); sum != 0 {
		if computedSum := v.computeChecksum(key); computedSum != sum {
			return fmt.Errorf("%s: invalid checksum (%x) value [% x]",
				Key(key), computedSum, v.RawBytes)
		}
	}
	return nil
}

// ShallowClone returns a shallow clone of the receiver.
func (v *Value) ShallowClone() *Value {
	if v == nil {
		return nil
	}
	t := *v
	return &t
}

// MakeValueFromString returns a value with bytes and tag set.
func MakeValueFromString(s string) Value {
	v := Value{}
	v.SetString(s)
	return v
}

// MakeValueFromBytes returns a value with bytes and tag set.
func MakeValueFromBytes(bs []byte) Value {
	v := Value{}
	v.SetBytes(bs)
	return v
}

// MakeValueFromBytesAndTimestamp returns a value with bytes, timestamp and
// tag set.
func MakeValueFromBytesAndTimestamp(bs []byte, t hlc.Timestamp) Value {
	v := Value{Timestamp: t}
	v.SetBytes(bs)
	return v
}

// GetTag retrieves the value type.
func (v Value) GetTag() ValueType {
	if len(v.RawBytes) <= tagPos {
		return ValueType_UNKNOWN
	}
	return ValueType(v.RawBytes[tagPos])
}

func (v *Value) setTag(t ValueType) {
	v.RawBytes[tagPos] = byte(t)
}

func (v Value) dataBytes() []byte {
	return v.RawBytes[headerSize:]
}

// SetBytes sets the bytes and tag field of the receiver and clears the checksum.
func (v *Value) SetBytes(b []byte) {
	v.RawBytes = make([]byte, headerSize+len(b))
	copy(v.dataBytes(), b)
	v.setTag(ValueType_BYTES)
}

// SetString sets the bytes and tag field of the receiver and clears the
// checksum. This is identical to SetBytes, but specialized for a string
// argument.
func (v *Value) SetString(s string) {
	v.RawBytes = make([]byte, headerSize+len(s))
	copy(v.dataBytes(), s)
	v.setTag(ValueType_BYTES)
}

// SetFloat encodes the specified float64 value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetFloat(f float64) {
	v.RawBytes = make([]byte, headerSize+8)
	encoding.EncodeUint64Ascending(v.RawBytes[headerSize:headerSize], math.Float64bits(f))
	v.setTag(ValueType_FLOAT)
}

// SetBool encodes the specified bool value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetBool(b bool) {
	// 0 or 1 will always encode to a 1-byte long varint.
	v.RawBytes = make([]byte, headerSize+1)
	i := int64(0)
	if b {
		i = 1
	}
	_ = binary.PutVarint(v.RawBytes[headerSize:], i)
	v.setTag(ValueType_INT)
}

// SetInt encodes the specified int64 value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetInt(i int64) {
	v.RawBytes = make([]byte, headerSize+binary.MaxVarintLen64)
	n := binary.PutVarint(v.RawBytes[headerSize:], i)
	v.RawBytes = v.RawBytes[:headerSize+n]
	v.setTag(ValueType_INT)
}

// SetProto encodes the specified proto message into the bytes field of the
// receiver and clears the checksum. If the proto message is an
// InternalTimeSeriesData, the tag will be set to TIMESERIES rather than BYTES.
func (v *Value) SetProto(msg proto.Message) error {
	// Fast-path for when the proto implements MarshalTo and Size (i.e. all of
	// our protos). The fast-path marshals directly into the Value.RawBytes field
	// instead of allocating a separate []byte and copying.
	type marshalTo interface {
		MarshalTo(data []byte) (int, error)
		Size() int
	}
	if m, ok := msg.(marshalTo); ok {
		size := m.Size()
		v.RawBytes = make([]byte, headerSize+size)
		// NB: This call to protoutil.Interceptor would be more natural
		// encapsulated in protoutil.MarshalTo, yet that approach imposes a
		// significant (~30%) slowdown. It is unclear why. See
		// BenchmarkValueSetProto.
		protoutil.Interceptor(msg)
		if _, err := m.MarshalTo(v.RawBytes[headerSize:]); err != nil {
			return err
		}
		// Special handling for timeseries data.
		if _, ok := msg.(*InternalTimeSeriesData); ok {
			v.setTag(ValueType_TIMESERIES)
		} else {
			v.setTag(ValueType_BYTES)
		}
		return nil
	}

	data, err := protoutil.Marshal(msg)
	if err != nil {
		return err
	}
	v.SetBytes(data)
	return nil
}

// SetTime encodes the specified time value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetTime(t time.Time) {
	const encodingSizeOverestimate = 11
	v.RawBytes = make([]byte, headerSize, headerSize+encodingSizeOverestimate)
	v.RawBytes = encoding.EncodeTimeAscending(v.RawBytes, t)
	v.setTag(ValueType_TIME)
}

// SetDuration encodes the specified duration value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetDuration(t duration.Duration) error {
	var err error
	v.RawBytes = make([]byte, headerSize, headerSize+encoding.EncodedDurationMaxLen)
	v.RawBytes, err = encoding.EncodeDurationAscending(v.RawBytes, t)
	if err != nil {
		return err
	}
	v.setTag(ValueType_DURATION)
	return nil
}

// SetDecimal encodes the specified decimal value into the bytes field of
// the receiver using Gob encoding, sets the tag and clears the checksum.
func (v *Value) SetDecimal(dec *inf.Dec) error {
	decSize := encoding.UpperBoundNonsortingDecimalSize(dec)
	v.RawBytes = make([]byte, headerSize, headerSize+decSize)
	v.RawBytes = encoding.EncodeNonsortingDecimal(v.RawBytes, dec)
	v.setTag(ValueType_DECIMAL)
	return nil
}

// SetTuple sets the tuple bytes and tag field of the receiver and clears the
// checksum.
func (v *Value) SetTuple(data []byte) {
	// TODO(dan): Reuse this and stop allocating on every SetTuple call. Same for
	// the other SetFoos.
	v.RawBytes = make([]byte, headerSize+len(data))
	copy(v.dataBytes(), data)
	v.setTag(ValueType_TUPLE)
}

// GetBytes returns the bytes field of the receiver. If the tag is not
// BYTES an error will be returned.
func (v Value) GetBytes() ([]byte, error) {
	if tag := v.GetTag(); tag != ValueType_BYTES {
		return nil, fmt.Errorf("value type is not %s: %s", ValueType_BYTES, tag)
	}
	return v.dataBytes(), nil
}

// GetFloat decodes a float64 value from the bytes field of the receiver. If
// the bytes field is not 8 bytes in length or the tag is not FLOAT an error
// will be returned.
func (v Value) GetFloat() (float64, error) {
	if tag := v.GetTag(); tag != ValueType_FLOAT {
		return 0, fmt.Errorf("value type is not %s: %s", ValueType_FLOAT, tag)
	}
	dataBytes := v.dataBytes()
	if len(dataBytes) != 8 {
		return 0, fmt.Errorf("float64 value should be exactly 8 bytes: %d", len(dataBytes))
	}
	_, u, err := encoding.DecodeUint64Ascending(dataBytes)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(u), nil
}

// GetBool decodes a bool value from the bytes field of the receiver. If the
// tag is not INT (the tag used for bool values) or the value cannot be decoded
// an error will be returned.
func (v Value) GetBool() (bool, error) {
	if tag := v.GetTag(); tag != ValueType_INT {
		return false, fmt.Errorf("value type is not %s: %s", ValueType_INT, tag)
	}
	i, n := binary.Varint(v.dataBytes())
	if n <= 0 {
		return false, fmt.Errorf("int64 varint decoding failed: %d", n)
	}
	if i > 1 || i < 0 {
		return false, fmt.Errorf("invalid bool: %d", i)
	}
	return i != 0, nil
}

// GetInt decodes an int64 value from the bytes field of the receiver. If the
// tag is not INT or the value cannot be decoded an error will be returned.
func (v Value) GetInt() (int64, error) {
	if tag := v.GetTag(); tag != ValueType_INT {
		return 0, fmt.Errorf("value type is not %s: %s", ValueType_INT, tag)
	}
	i, n := binary.Varint(v.dataBytes())
	if n <= 0 {
		return 0, fmt.Errorf("int64 varint decoding failed: %d", n)
	}
	return i, nil
}

// GetProto unmarshals the bytes field of the receiver into msg. If
// unmarshalling fails or the tag is not BYTES, an error will be
// returned.
func (v Value) GetProto(msg proto.Message) error {
	expectedTag := ValueType_BYTES

	// Special handling for ts data.
	if _, ok := msg.(*InternalTimeSeriesData); ok {
		expectedTag = ValueType_TIMESERIES
	}

	if tag := v.GetTag(); tag != expectedTag {
		return fmt.Errorf("value type is not %s: %s", expectedTag, tag)
	}
	return proto.Unmarshal(v.dataBytes(), msg)
}

// GetTime decodes a time value from the bytes field of the receiver. If the
// tag is not TIME an error will be returned.
func (v Value) GetTime() (time.Time, error) {
	if tag := v.GetTag(); tag != ValueType_TIME {
		return time.Time{}, fmt.Errorf("value type is not %s: %s", ValueType_TIME, tag)
	}
	_, t, err := encoding.DecodeTimeAscending(v.dataBytes())
	return t, err
}

// GetDuration decodes a duration value from the bytes field of the receiver. If
// the tag is not DURATION an error will be returned.
func (v Value) GetDuration() (duration.Duration, error) {
	if tag := v.GetTag(); tag != ValueType_DURATION {
		return duration.Duration{}, fmt.Errorf("value type is not %s: %s", ValueType_DURATION, tag)
	}
	_, t, err := encoding.DecodeDurationAscending(v.dataBytes())
	return t, err
}

// GetDecimal decodes a decimal value from the bytes of the receiver. If the
// tag is not DECIMAL an error will be returned.
func (v Value) GetDecimal() (*inf.Dec, error) {
	if tag := v.GetTag(); tag != ValueType_DECIMAL {
		return nil, fmt.Errorf("value type is not %s: %s", ValueType_DECIMAL, tag)
	}
	return encoding.DecodeNonsortingDecimal(v.dataBytes(), nil)
}

// GetTimeseries decodes an InternalTimeSeriesData value from the bytes
// field of the receiver. An error will be returned if the tag is not
// TIMESERIES or if decoding fails.
func (v Value) GetTimeseries() (InternalTimeSeriesData, error) {
	ts := InternalTimeSeriesData{}
	return ts, v.GetProto(&ts)
}

// GetTuple returns the tuple bytes of the receiver. If the tag is not TUPLE an
// error will be returned.
func (v Value) GetTuple() ([]byte, error) {
	if tag := v.GetTag(); tag != ValueType_TUPLE {
		return nil, fmt.Errorf("value type is not %s: %s", ValueType_TUPLE, tag)
	}
	return v.dataBytes(), nil
}

var crc32Pool = sync.Pool{
	New: func() interface{} {
		return crc32.NewIEEE()
	},
}

// computeChecksum computes a checksum based on the provided key and
// the contents of the value.
func (v Value) computeChecksum(key []byte) uint32 {
	if len(v.RawBytes) < headerSize {
		return 0
	}
	crc := crc32Pool.Get().(hash.Hash32)
	if _, err := crc.Write(key); err != nil {
		panic(err)
	}
	if _, err := crc.Write(v.RawBytes[checksumSize:]); err != nil {
		panic(err)
	}
	sum := crc.Sum32()
	crc.Reset()
	crc32Pool.Put(crc)
	// We reserved the value 0 (checksumUninitialized) to indicate that a checksum
	// has not been initialized. This reservation is accomplished by folding a
	// computed checksum of 0 to the value 1.
	if sum == checksumUninitialized {
		return 1
	}
	return sum
}

// PrettyPrint returns the value in a human readable format.
// e.g. `Put /Table/51/1/1/0 -> /TUPLE/2:2:Int/7/1:3:Float/6.28`
// In `1:3:Float/6.28`, the `1` is the column id diff as stored, `3` is the
// computed (i.e. not stored) actual column id, `Float` is the type, and `6.28`
// is the encoded value.
func (v Value) PrettyPrint() string {
	var buf bytes.Buffer
	t := v.GetTag()
	buf.WriteRune('/')
	buf.WriteString(t.String())
	buf.WriteRune('/')

	var err error
	switch t {
	case ValueType_TUPLE:
		b := v.dataBytes()
		var colID uint32
		for i := 0; len(b) > 0; i++ {
			if i != 0 {
				buf.WriteRune('/')
			}
			_, _, colIDDiff, typ, err := encoding.DecodeValueTag(b)
			if err != nil {
				break
			}
			colID += colIDDiff
			var s string
			b, s, err = encoding.PrettyPrintValueEncoded(b)
			if err != nil {
				break
			}
			fmt.Fprintf(&buf, "%d:%d:%s/%s", colIDDiff, colID, typ, s)
		}
	case ValueType_INT:
		var i int64
		i, err = v.GetInt()
		buf.WriteString(strconv.FormatInt(i, 10))
	case ValueType_FLOAT:
		var f float64
		f, err = v.GetFloat()
		buf.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
	case ValueType_BYTES:
		var data []byte
		data, err = v.GetBytes()
		printable := len(bytes.TrimLeftFunc(data, unicode.IsPrint)) == 0
		if printable {
			buf.WriteString(string(data))
		} else {
			buf.WriteString(hex.EncodeToString(data))
		}
	case ValueType_TIME:
		var t time.Time
		t, err = v.GetTime()
		buf.WriteString(t.UTC().Format(time.RFC3339Nano))
	case ValueType_DECIMAL:
		var d *inf.Dec
		d, err = v.GetDecimal()
		buf.WriteString(d.String())
	case ValueType_DURATION:
		var d duration.Duration
		d, err = v.GetDuration()
		buf.WriteString(d.String())
	default:
		err = errors.Errorf("unknown tag: %s", t)
	}
	if err != nil {
		// Ignore the contents of buf and return directly.
		return fmt.Sprintf("/<err: %s>", err)
	}
	return buf.String()
}

const (
	// MinTxnPriority is the minimum allowed txn priority.
	MinTxnPriority = 0
	// MaxTxnPriority is the maximum allowed txn priority.
	MaxTxnPriority = math.MaxInt32
)

// NewTransaction creates a new transaction. The transaction key is
// composed using the specified baseKey (for locality with data
// affected by the transaction) and a random ID to guarantee
// uniqueness. The specified user-level priority is combined with a
// randomly chosen value to yield a final priority, used to settle
// write conflicts in a way that avoids starvation of long-running
// transactions (see Replica.PushTxn).
func NewTransaction(
	name string,
	baseKey Key,
	userPriority UserPriority,
	isolation enginepb.IsolationType,
	now hlc.Timestamp,
	maxOffset int64,
) *Transaction {
	u := uuid.MakeV4()

	return &Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:       baseKey,
			ID:        &u,
			Isolation: isolation,
			Timestamp: now,
			Priority:  MakePriority(userPriority),
			Sequence:  1,
		},
		Name:          name,
		OrigTimestamp: now,
		MaxTimestamp:  now.Add(maxOffset, 0),
	}
}

// LastActive returns the last timestamp at which client activity definitely
// occurred, i.e. the maximum of OrigTimestamp and LastHeartbeat.
func (t Transaction) LastActive() hlc.Timestamp {
	candidate := t.OrigTimestamp
	if t.LastHeartbeat != nil && candidate.Less(*t.LastHeartbeat) {
		candidate = *t.LastHeartbeat
	}
	return candidate
}

// Clone creates a copy of the given transaction. The copy is "mostly" deep,
// but does share pieces of memory with the original such as Key, ID and the
// keys with the intent spans.
func (t Transaction) Clone() Transaction {
	if t.LastHeartbeat != nil {
		h := *t.LastHeartbeat
		t.LastHeartbeat = &h
	}
	mt := t.ObservedTimestamps
	if len(mt) != 0 {
		t.ObservedTimestamps = make([]ObservedTimestamp, len(mt))
		copy(t.ObservedTimestamps, mt)
	}
	// Note that we're not cloning the span keys under the assumption that the
	// keys themselves are not mutable.
	t.Intents = append([]Span(nil), t.Intents...)
	return t
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

// IsInitialized returns true if the transaction has been initialized.
func (t *Transaction) IsInitialized() bool {
	return t.ID != nil
}

// MakePriority generates a random priority value, biased by the
// specified userPriority. If userPriority=100, the random priority
// will be 100x more likely to be greater than if userPriority=1. If
// userPriority = 0.1, the random priority will be 1/10th as likely to
// be greater than if userPriority=1. Balance is achieved when
// userPriority=1, in which case the priority chosen is unbiased.
//
// If userPriority is less than or equal to MinUserPriority, returns
// MinTxnPriority; if greater than or equal to MaxUserPriority,
// returns MaxTxnPriority.
func MakePriority(userPriority UserPriority) int32 {
	// A currently undocumented feature allows an explicit priority to
	// be set by specifying priority < 1. The explicit priority is
	// simply -userPriority in this case. This is hacky, but currently
	// used for unittesting. Perhaps this should be documented and allowed.
	if userPriority < 0 {
		if -userPriority > UserPriority(math.MaxInt32) {
			panic(fmt.Sprintf("cannot set explicit priority to a value less than -%d", math.MaxInt32))
		}
		return int32(-userPriority)
	} else if userPriority == 0 {
		userPriority = 1
	} else if userPriority >= MaxUserPriority {
		return MaxTxnPriority
	} else if userPriority <= MinUserPriority {
		return MinTxnPriority
	}

	// We generate random values which are biased according to priorities. If v1 is a value
	// generated for priority p1 and v2 is a value of priority v2, we want the ratio of wins vs
	// losses to be the same with the ratio of priorities:
	//
	//    P[ v1 > v2 ]     p1                                           p1
	//    ------------  =  --     or, equivalently:    P[ v1 > v2 ] = -------
	//    P[ v2 < v1 ]     p2                                         p1 + p2
	//
	//
	// For example, priority 10 wins 10 out of 11 times over priority 1, and it wins 100 out of 101
	// times over priority 0.1.
	//
	//
	// We use the exponential distribution. This distribution has the probability density function
	//   PDF_lambda(x) = lambda * exp(-lambda * x)
	// and the cumulative distribution function (i.e. probability that a random value is smaller
	// than x):
	//   CDF_lambda(x) = Integral_0^x PDF_lambda(x) dx
	//                 = 1 - exp(-lambda * x)
	//
	// Let's assume we generate x from the exponential distribution with the lambda rate set to
	// l1 and we generate y from the distribution with the rate set to l2. The probability that x
	// wins is:
	//    P[ x > y ] = Integral_0^inf Integral_0^x PDF_l1(x) PDF_l2(y) dy dx
	//               = Integral_0^inf PDF_l1(x) Integral_0^x PDF_l2(y) dy dx
	//               = Integral_0^inf PDF_l1(x) CDF_l2(x) dx
	//               = Integral_0^inf PDF_l1(x) (1 - exp(-l2 * x)) dx
	//               = 1 - Integral_0^inf l1 * exp(-(l1+l2) * x) dx
	//               = 1 - l1 / (l1 + l2) * Integral_0^inf PDF_(l1+l2)(x) dx
	//               = 1 - l1 / (l1 + l2)
	//               = l2 / (l1 + l2)
	//
	// We want this probability to be p1 / (p1 + p2) which we can get by setting
	//    l1 = 1 / p1
	//    l2 = 1 / p2
	// It's easy to verify that (1/p2) / (1/p1 + 1/p2) = p1 / (p2 + p1).
	//
	// We can generate an exponentially distributed value using (rand.ExpFloat64() / lambda).
	// In our case this works out to simply rand.ExpFloat64() * userPriority.
	val := rand.ExpFloat64() * float64(userPriority)

	// To convert to an integer, we scale things to accommodate a few (5) standard deviations for
	// the maximum priority. The choice of the value is a trade-off between loss of resolution for
	// low priorities and overflow (capping the value to MaxInt32) for high priorities.
	//
	// For userPriority=MaxUserPriority, the probability of overflow is 0.7%.
	// For userPriority=(MaxUserPriority/2), the probability of overflow is 0.005%.
	val = (val / (5 * MaxUserPriority)) * math.MaxInt32
	if val >= MaxTxnPriority {
		return MaxTxnPriority
	}
	return int32(val)
}

// TxnIDEqual returns whether the transaction IDs are equal.
func TxnIDEqual(a, b *uuid.UUID) bool {
	if a == nil && b == nil {
		return true
	} else if a != nil && b != nil {
		return *a == *b
	}
	return false
}

// Restart reconfigures a transaction for restart. The epoch is
// incremented for an in-place restart. The timestamp of the
// transaction on restart is set to the maximum of the transaction's
// timestamp and the specified timestamp.
func (t *Transaction) Restart(
	userPriority UserPriority, upgradePriority int32, timestamp hlc.Timestamp,
) {
	t.Epoch++
	if t.Timestamp.Less(timestamp) {
		t.Timestamp = timestamp
	}
	// Set original timestamp to current timestamp on restart.
	t.OrigTimestamp = t.Timestamp
	// Upgrade priority to the maximum of:
	// - the current transaction priority
	// - a random priority created from userPriority
	// - the conflicting transaction's upgradePriority
	t.UpgradePriority(MakePriority(userPriority))
	t.UpgradePriority(upgradePriority)
	t.WriteTooOld = false
	t.RetryOnPush = false
}

// Update ratchets priority, timestamp and original timestamp values (among
// others) for the transaction. If t.ID is empty, then the transaction is
// copied from o.
func (t *Transaction) Update(o *Transaction) {
	if o == nil {
		return
	}
	if t.ID == nil {
		*t = o.Clone()
		return
	}
	if len(t.Key) == 0 {
		t.Key = o.Key
	}
	if o.Status != PENDING {
		t.Status = o.Status
	}
	if t.Epoch < o.Epoch {
		t.Epoch = o.Epoch
	}
	t.Timestamp.Forward(o.Timestamp)
	t.OrigTimestamp.Forward(o.OrigTimestamp)
	t.MaxTimestamp.Forward(o.MaxTimestamp)
	if o.LastHeartbeat != nil {
		if t.LastHeartbeat == nil {
			t.LastHeartbeat = &hlc.Timestamp{}
		}
		t.LastHeartbeat.Forward(*o.LastHeartbeat)
	}

	// Absorb the collected clock uncertainty information.
	for _, v := range o.ObservedTimestamps {
		t.UpdateObservedTimestamp(v.NodeID, v.Timestamp)
	}
	t.UpgradePriority(o.Priority)
	// We can't assert against regression here since it can actually happen
	// that we update from a transaction which isn't Writing.
	t.Writing = t.Writing || o.Writing
	// This isn't or'd (similar to Writing) because we want WriteTooOld
	// and RetryOnPush to be set each time according to "o". This allows
	// a persisted txn to have its WriteTooOld flag reset on update.
	// TODO(tschottdorf): reset in a central location when it's certifiably
	//   a new request. Update is called in many situations and shouldn't
	//   reset anything.
	t.WriteTooOld = o.WriteTooOld
	t.RetryOnPush = o.RetryOnPush
	if t.Sequence < o.Sequence {
		t.Sequence = o.Sequence
	}
	if len(o.Intents) > 0 {
		t.Intents = o.Intents
	}
}

// UpgradePriority sets transaction priority to the maximum of current
// priority and the specified minPriority. The exception is if the
// current priority is set to the minimum, in which case the minimum
// is preserved.
func (t *Transaction) UpgradePriority(minPriority int32) {
	if minPriority > t.Priority && t.Priority != MinTxnPriority {
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
	fmt.Fprintf(&buf, "id=%s key=%s rw=%t pri=%.8f iso=%s stat=%s epo=%d ts=%s orig=%s max=%s wto=%t rop=%t",
		t.Short(), Key(t.Key), t.Writing, floatPri, t.Isolation, t.Status, t.Epoch, t.Timestamp,
		t.OrigTimestamp, t.MaxTimestamp, t.WriteTooOld, t.RetryOnPush)
	return buf.String()
}

// ResetObservedTimestamps clears out all timestamps recorded from individual
// nodes.
func (t *Transaction) ResetObservedTimestamps() {
	t.ObservedTimestamps = nil
}

// UpdateObservedTimestamp stores a timestamp off a node's clock for future
// operations in the transaction. When multiple calls are made for a single
// nodeID, the lowest timestamp prevails.
func (t *Transaction) UpdateObservedTimestamp(nodeID NodeID, maxTS hlc.Timestamp) {
	s := observedTimestampSlice(t.ObservedTimestamps)
	t.ObservedTimestamps = s.update(nodeID, maxTS)
}

// GetObservedTimestamp returns the lowest HLC timestamp recorded from the
// given node's clock during the transaction. The returned boolean is false if
// no observation about the requested node was found. Otherwise, MaxTimestamp
// can be lowered to the returned timestamp when reading from nodeID.
func (t Transaction) GetObservedTimestamp(nodeID NodeID) (hlc.Timestamp, bool) {
	s := observedTimestampSlice(t.ObservedTimestamps)
	return s.get(nodeID)
}

var _ fmt.Stringer = &Lease{}

func (l Lease) String() string {
	var proposedSuffix string
	if l.ProposedTS != nil {
		proposedSuffix = fmt.Sprintf(" pro=%s", l.ProposedTS)
	}
	if l.Type() == LeaseExpiration {
		return fmt.Sprintf("repl=%s start=%s exp=%s%s", l.Replica, l.Start, l.Expiration, proposedSuffix)
	}
	return fmt.Sprintf("repl=%s start=%s epo=%d%s", l.Replica, l.Start, *l.Epoch, proposedSuffix)
}

// OwnedBy returns whether the given store is the lease owner.
func (l Lease) OwnedBy(storeID StoreID) bool {
	return l.Replica.StoreID == storeID
}

// LeaseType describes the type of lease.
type LeaseType int

const (
	// LeaseNone specifies no lease, to be used as a default value.
	LeaseNone LeaseType = iota
	// LeaseExpiration allows range operations while the wall clock is
	// within the expiration timestamp.
	LeaseExpiration
	// LeaseEpoch allows range operations while the node liveness epoch
	// is equal to the lease epoch.
	LeaseEpoch
)

// Type returns the lease type.
func (l Lease) Type() LeaseType {
	if l.Epoch == nil {
		return LeaseExpiration
	}
	return LeaseEpoch
}

// Equivalent determines whether ol is considered the same lease
// for the purposes of matching leases when executing a command.
// For expiration-based leases, extensions are allowed.
// Ignore proposed timestamps for lease verification; for epoch-
// based leases, the start time of the lease is sufficient to
// avoid using an older lease with same epoch.
func (l Lease) Equivalent(ol Lease) error {
	// Ignore proposed timestamp & deprecated start stasis.
	l.ProposedTS, ol.ProposedTS = nil, nil
	l.DeprecatedStartStasis = ol.DeprecatedStartStasis
	// If both leases are epoch-based, we must dereference the epochs
	// and then set to nil.
	if l.Type() == LeaseEpoch && ol.Type() == LeaseEpoch && *l.Epoch == *ol.Epoch {
		l.Epoch, ol.Epoch = nil, nil
	}
	// For expiration-based leases, extensions are considered equivalent.
	if l.Type() == LeaseExpiration && ol.Type() == LeaseExpiration &&
		l.Expiration.Less(ol.Expiration) {
		l.Expiration = ol.Expiration
	}
	if l == ol {
		return nil
	}
	return errors.Errorf("leases %+v and %+v are not equivalent", l, ol)
}

// AsIntents takes a slice of spans and returns it as a slice of intents for
// the given transaction.
func AsIntents(spans []Span, txn *Transaction) []Intent {
	ret := make([]Intent, len(spans))
	for i := range spans {
		ret[i] = Intent{
			Span:   spans[i],
			Txn:    txn.TxnMeta,
			Status: txn.Status,
		}
	}
	return ret
}

// Equal compares for equality.
func (s Span) Equal(o Span) bool {
	return s.Key.Equal(o.Key) && s.EndKey.Equal(o.EndKey)
}

// Overlaps returns whether the two spans overlap.
func (s Span) Overlaps(o Span) bool {
	if len(s.EndKey) == 0 && len(o.EndKey) == 0 {
		return s.Key.Equal(o.Key)
	} else if len(s.EndKey) == 0 {
		return bytes.Compare(s.Key, o.Key) >= 0 && bytes.Compare(s.Key, o.EndKey) < 0
	} else if len(o.EndKey) == 0 {
		return bytes.Compare(o.Key, s.Key) >= 0 && bytes.Compare(o.Key, s.EndKey) < 0
	}
	return bytes.Compare(s.EndKey, o.Key) > 0 && bytes.Compare(s.Key, o.EndKey) < 0
}

// Spans is a slice of spans.
type Spans []Span

// implement Sort.Interface
func (a Spans) Len() int           { return len(a) }
func (a Spans) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Spans) Less(i, j int) bool { return a[i].Key.Compare(a[j].Key) < 0 }

// RSpan is a key range with an inclusive start RKey and an exclusive end RKey.
type RSpan struct {
	Key, EndKey RKey
}

// Equal compares for equality.
func (rs RSpan) Equal(o RSpan) bool {
	return rs.Key.Equal(o.Key) && rs.EndKey.Equal(o.EndKey)
}

// ContainsKey returns whether this span contains the specified key.
func (rs RSpan) ContainsKey(key RKey) bool {
	return bytes.Compare(key, rs.Key) >= 0 && bytes.Compare(key, rs.EndKey) < 0
}

// ContainsExclusiveEndKey returns whether this span contains the specified key.
// A span is considered to include its EndKey (e.g., span ["a", b") contains
// "b" according to this function, but does not contain "a").
func (rs RSpan) ContainsExclusiveEndKey(key RKey) bool {
	return bytes.Compare(key, rs.Key) > 0 && bytes.Compare(key, rs.EndKey) <= 0
}

// ContainsKeyRange returns whether this span contains the specified
// key range from start (inclusive) to end (exclusive).
// If end is empty or start is equal to end, returns ContainsKey(start).
func (rs RSpan) ContainsKeyRange(start, end RKey) bool {
	if len(end) == 0 {
		return rs.ContainsKey(start)
	}
	if comp := bytes.Compare(end, start); comp < 0 {
		return false
	} else if comp == 0 {
		return rs.ContainsKey(start)
	}
	return bytes.Compare(start, rs.Key) >= 0 && bytes.Compare(rs.EndKey, end) >= 0
}

// Intersect returns the intersection of the current span and the
// descriptor's range. Returns an error if the span and the
// descriptor's range do not overlap.
func (rs RSpan) Intersect(desc *RangeDescriptor) (RSpan, error) {
	if !rs.Key.Less(desc.EndKey) || !desc.StartKey.Less(rs.EndKey) {
		return rs, errors.Errorf("span and descriptor's range do not overlap: %s vs %s", rs, desc)
	}

	key := rs.Key
	if key.Less(desc.StartKey) {
		key = desc.StartKey
	}
	endKey := rs.EndKey
	if !desc.ContainsKeyRange(desc.StartKey, endKey) {
		endKey = desc.EndKey
	}
	return RSpan{key, endKey}, nil
}

// KeyValueByKey implements sorting of a slice of KeyValues by key.
type KeyValueByKey []KeyValue

// Len implements sort.Interface.
func (kv KeyValueByKey) Len() int {
	return len(kv)
}

// Less implements sort.Interface.
func (kv KeyValueByKey) Less(i, j int) bool {
	return bytes.Compare(kv[i].Key, kv[j].Key) < 0
}

// Swap implements sort.Interface.
func (kv KeyValueByKey) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

var _ sort.Interface = KeyValueByKey{}

// observedTimestampSlice maintains a sorted list of observed timestamps.
type observedTimestampSlice []ObservedTimestamp

func (s observedTimestampSlice) index(nodeID NodeID) int {
	return sort.Search(len(s),
		func(i int) bool {
			return s[i].NodeID >= nodeID
		},
	)
}

// get the observed timestamp for the specified node, returning false if no
// timestamp exists.
func (s observedTimestampSlice) get(nodeID NodeID) (hlc.Timestamp, bool) {
	i := s.index(nodeID)
	if i < len(s) && s[i].NodeID == nodeID {
		return s[i].Timestamp, true
	}
	return hlc.Timestamp{}, false
}

// update the timestamp for the specified node, or add a new entry in the
// correct (sorted) location.
func (s observedTimestampSlice) update(
	nodeID NodeID, timestamp hlc.Timestamp,
) observedTimestampSlice {
	i := s.index(nodeID)
	if i < len(s) && s[i].NodeID == nodeID {
		if timestamp.Less(s[i].Timestamp) {
			s[i].Timestamp = timestamp
		}
		return s
	}
	s = append(s, ObservedTimestamp{})
	copy(s[i+1:], s[i:])
	s[i] = ObservedTimestamp{NodeID: nodeID, Timestamp: timestamp}
	return s
}
