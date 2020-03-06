// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
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

	// PrettyPrintKey prints a key in human readable format. It's
	// implemented in package git.com/cockroachdb/cockroach/keys to avoid
	// package circle import.
	// valDirs correspond to the encoding direction of each encoded value
	// in the key (if known). If left unspecified, the default encoding
	// direction for each value type is used (see
	// encoding.go:prettyPrintFirstValue).
	PrettyPrintKey func(valDirs []encoding.Direction, key Key) string

	// PrettyPrintRange prints a key range in human readable format. It's
	// implemented in package git.com/cockroachdb/cockroach/keys to avoid
	// package circle import.
	PrettyPrintRange func(start, end Key, maxChars int) string
)

// RKey denotes a Key whose local addressing has been accounted for.
// A key can be transformed to an RKey by keys.Addr().
//
// RKey stands for "resolved key," as in a key whose address has been resolved.
type RKey Key

// AsRawKey returns the RKey as a Key. This is to be used only in select
// situations in which an RKey is known to not contain a wrapped locally-
// addressed Key. That is, it must only be used when the original Key was not a
// local key. Whenever the Key which created the RKey is still available, it
// should be used instead.
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

// StringWithDirs - see Key.String.WithDirs.
func (rk RKey) StringWithDirs(valDirs []encoding.Direction, maxLen int) string {
	return Key(rk).StringWithDirs(valDirs, maxLen)
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
			return end[:i+1]
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

// Compare compares the two Keys.
func (k Key) Compare(b Key) int {
	return bytes.Compare(k, b)
}

// String returns a string-formatted version of the key.
func (k Key) String() string {
	return k.StringWithDirs(nil /* valDirs */, 0 /* maxLen */)
}

// StringWithDirs is the value encoding direction-aware version of String.
//
// Args:
// valDirs: The direction for the key's components, generally needed for correct
// 	decoding. If nil, the values are pretty-printed with default encoding
// 	direction.
// maxLen: If not 0, only the first maxLen chars from the decoded key are
//   returned, plus a "..." suffix.
func (k Key) StringWithDirs(valDirs []encoding.Direction, maxLen int) string {
	var s string
	if PrettyPrintKey != nil {
		s = PrettyPrintKey(valDirs, k)
	} else {
		s = fmt.Sprintf("%q", []byte(k))
	}
	if maxLen != 0 && len(s) > maxLen {
		return s[0:maxLen] + "..."
	}
	return s
}

// Format implements the fmt.Formatter interface.
func (k Key) Format(f fmt.State, verb rune) {
	// Note: this implementation doesn't handle the width and precision
	// specifiers such as "%20.10s".
	if verb == 'x' {
		fmt.Fprintf(f, "%x", []byte(k))
	} else if PrettyPrintKey != nil {
		fmt.Fprint(f, PrettyPrintKey(nil /* valDirs */, k))
	} else {
		fmt.Fprint(f, strconv.Quote(string(k)))
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

// IsPresent returns true if the value is present (existent and not a tombstone).
func (v *Value) IsPresent() bool {
	return v != nil && len(v.RawBytes) != 0
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

func (v *Value) ensureRawBytes(size int) {
	if cap(v.RawBytes) < size {
		v.RawBytes = make([]byte, size)
		return
	}
	v.RawBytes = v.RawBytes[:size]
	v.setChecksum(checksumUninitialized)
}

// EqualData returns a boolean reporting whether the receiver and the parameter
// have equivalent byte values. This check ignores the optional checksum field
// in the Values' byte slices, returning only whether the Values have the same
// tag and encoded data.
//
// This method should be used whenever the raw bytes of two Values are being
// compared instead of comparing the RawBytes slices directly because it ignores
// the checksum header, which is optional.
func (v Value) EqualData(o Value) bool {
	return bytes.Equal(v.RawBytes[checksumSize:], o.RawBytes[checksumSize:])
}

// SetBytes sets the bytes and tag field of the receiver and clears the checksum.
func (v *Value) SetBytes(b []byte) {
	v.ensureRawBytes(headerSize + len(b))
	copy(v.dataBytes(), b)
	v.setTag(ValueType_BYTES)
}

// SetString sets the bytes and tag field of the receiver and clears the
// checksum. This is identical to SetBytes, but specialized for a string
// argument.
func (v *Value) SetString(s string) {
	v.ensureRawBytes(headerSize + len(s))
	copy(v.dataBytes(), s)
	v.setTag(ValueType_BYTES)
}

// SetFloat encodes the specified float64 value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetFloat(f float64) {
	v.ensureRawBytes(headerSize + 8)
	encoding.EncodeUint64Ascending(v.RawBytes[headerSize:headerSize], math.Float64bits(f))
	v.setTag(ValueType_FLOAT)
}

// SetBool encodes the specified bool value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetBool(b bool) {
	// 0 or 1 will always encode to a 1-byte long varint.
	v.ensureRawBytes(headerSize + 1)
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
	v.ensureRawBytes(headerSize + binary.MaxVarintLen64)
	n := binary.PutVarint(v.RawBytes[headerSize:], i)
	v.RawBytes = v.RawBytes[:headerSize+n]
	v.setTag(ValueType_INT)
}

// SetProto encodes the specified proto message into the bytes field of the
// receiver and clears the checksum. If the proto message is an
// InternalTimeSeriesData, the tag will be set to TIMESERIES rather than BYTES.
func (v *Value) SetProto(msg protoutil.Message) error {
	msg = protoutil.MaybeFuzz(msg)
	// All of the Cockroach protos implement MarshalTo and Size. So we marshal
	// directly into the Value.RawBytes field instead of allocating a separate
	// []byte and copying.
	v.ensureRawBytes(headerSize + msg.Size())
	if _, err := protoutil.MarshalToWithoutFuzzing(msg, v.RawBytes[headerSize:]); err != nil {
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

// SetTime encodes the specified time value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetTime(t time.Time) {
	const encodingSizeOverestimate = 11
	v.ensureRawBytes(headerSize + encodingSizeOverestimate)
	v.RawBytes = encoding.EncodeTimeAscending(v.RawBytes[:headerSize], t)
	v.setTag(ValueType_TIME)
}

// SetTimeTZ encodes the specified time value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetTimeTZ(t timetz.TimeTZ) {
	v.ensureRawBytes(headerSize + encoding.EncodedTimeTZMaxLen)
	v.RawBytes = encoding.EncodeTimeTZAscending(v.RawBytes[:headerSize], t)
	v.setTag(ValueType_TIMETZ)
}

// SetDuration encodes the specified duration value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetDuration(t duration.Duration) error {
	var err error
	v.ensureRawBytes(headerSize + encoding.EncodedDurationMaxLen)
	v.RawBytes, err = encoding.EncodeDurationAscending(v.RawBytes[:headerSize], t)
	if err != nil {
		return err
	}
	v.setTag(ValueType_DURATION)
	return nil
}

// SetBitArray encodes the specified bit array value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetBitArray(t bitarray.BitArray) {
	words, _ := t.EncodingParts()
	v.ensureRawBytes(headerSize + encoding.NonsortingUvarintMaxLen + 8*len(words))
	v.RawBytes = encoding.EncodeUntaggedBitArrayValue(v.RawBytes[:headerSize], t)
	v.setTag(ValueType_BITARRAY)
}

// SetDecimal encodes the specified decimal value into the bytes field of
// the receiver using Gob encoding, sets the tag and clears the checksum.
func (v *Value) SetDecimal(dec *apd.Decimal) error {
	decSize := encoding.UpperBoundNonsortingDecimalSize(dec)
	v.ensureRawBytes(headerSize + decSize)
	v.RawBytes = encoding.EncodeNonsortingDecimal(v.RawBytes[:headerSize], dec)
	v.setTag(ValueType_DECIMAL)
	return nil
}

// SetTuple sets the tuple bytes and tag field of the receiver and clears the
// checksum.
func (v *Value) SetTuple(data []byte) {
	v.ensureRawBytes(headerSize + len(data))
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
func (v Value) GetProto(msg protoutil.Message) error {
	expectedTag := ValueType_BYTES

	// Special handling for ts data.
	if _, ok := msg.(*InternalTimeSeriesData); ok {
		expectedTag = ValueType_TIMESERIES
	}

	if tag := v.GetTag(); tag != expectedTag {
		return fmt.Errorf("value type is not %s: %s", expectedTag, tag)
	}
	return protoutil.Unmarshal(v.dataBytes(), msg)
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

// GetTimeTZ decodes a time value from the bytes field of the receiver. If the
// tag is not TIMETZ an error will be returned.
func (v Value) GetTimeTZ() (timetz.TimeTZ, error) {
	if tag := v.GetTag(); tag != ValueType_TIMETZ {
		return timetz.TimeTZ{}, fmt.Errorf("value type is not %s: %s", ValueType_TIMETZ, tag)
	}
	_, t, err := encoding.DecodeTimeTZAscending(v.dataBytes())
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

// GetBitArray decodes a bit array value from the bytes field of the receiver. If
// the tag is not BITARRAY an error will be returned.
func (v Value) GetBitArray() (bitarray.BitArray, error) {
	if tag := v.GetTag(); tag != ValueType_BITARRAY {
		return bitarray.BitArray{}, fmt.Errorf("value type is not %s: %s", ValueType_BITARRAY, tag)
	}
	_, t, err := encoding.DecodeUntaggedBitArrayValue(v.dataBytes())
	return t, err
}

// GetDecimal decodes a decimal value from the bytes of the receiver. If the
// tag is not DECIMAL an error will be returned.
func (v Value) GetDecimal() (apd.Decimal, error) {
	if tag := v.GetTag(); tag != ValueType_DECIMAL {
		return apd.Decimal{}, fmt.Errorf("value type is not %s: %s", ValueType_DECIMAL, tag)
	}
	return encoding.DecodeNonsortingDecimal(v.dataBytes(), nil)
}

// GetDecimalInto decodes a decimal value from the bytes of the receiver,
// writing it directly into the provided non-null apd.Decimal. If the
// tag is not DECIMAL an error will be returned.
func (v Value) GetDecimalInto(d *apd.Decimal) error {
	if tag := v.GetTag(); tag != ValueType_DECIMAL {
		return fmt.Errorf("value type is not %s: %s", ValueType_DECIMAL, tag)
	}
	return encoding.DecodeIntoNonsortingDecimal(d, v.dataBytes(), nil)
}

// GetTimeseries decodes an InternalTimeSeriesData value from the bytes
// field of the receiver. An error will be returned if the tag is not
// TIMESERIES or if decoding fails.
func (v Value) GetTimeseries() (InternalTimeSeriesData, error) {
	ts := InternalTimeSeriesData{}
	// GetProto mutates its argument. `return ts, v.GetProto(&ts)`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := v.GetProto(&ts)
	return ts, err
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

func computeChecksum(key, rawBytes []byte, crc hash.Hash32) uint32 {
	if len(rawBytes) < headerSize {
		return 0
	}
	if _, err := crc.Write(key); err != nil {
		panic(err)
	}
	if _, err := crc.Write(rawBytes[checksumSize:]); err != nil {
		panic(err)
	}
	sum := crc.Sum32()
	crc.Reset()
	// We reserved the value 0 (checksumUninitialized) to indicate that a checksum
	// has not been initialized. This reservation is accomplished by folding a
	// computed checksum of 0 to the value 1.
	if sum == checksumUninitialized {
		return 1
	}
	return sum
}

// computeChecksum computes a checksum based on the provided key and
// the contents of the value.
func (v Value) computeChecksum(key []byte) uint32 {
	crc := crc32Pool.Get().(hash.Hash32)
	sum := computeChecksum(key, v.RawBytes, crc)
	crc32Pool.Put(crc)
	return sum
}

// PrettyPrint returns the value in a human readable format.
// e.g. `Put /Table/51/1/1/0 -> /TUPLE/2:2:Int/7/1:3:Float/6.28`
// In `1:3:Float/6.28`, the `1` is the column id diff as stored, `3` is the
// computed (i.e. not stored) actual column id, `Float` is the type, and `6.28`
// is the encoded value.
func (v Value) PrettyPrint() string {
	if len(v.RawBytes) == 0 {
		return "/<empty>"
	}
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
		if encoding.PrintableBytes(data) {
			buf.WriteString(string(data))
		} else {
			buf.WriteString("0x")
			buf.WriteString(hex.EncodeToString(data))
		}
	case ValueType_BITARRAY:
		var data bitarray.BitArray
		data, err = v.GetBitArray()
		buf.WriteByte('B')
		data.Format(&buf)
	case ValueType_TIME:
		var t time.Time
		t, err = v.GetTime()
		buf.WriteString(t.UTC().Format(time.RFC3339Nano))
	case ValueType_DECIMAL:
		var d apd.Decimal
		d, err = v.GetDecimal()
		buf.WriteString(d.String())
	case ValueType_DURATION:
		var d duration.Duration
		d, err = v.GetDuration()
		buf.WriteString(d.StringNanos())
	default:
		err = errors.Errorf("unknown tag: %s", t)
	}
	if err != nil {
		// Ignore the contents of buf and return directly.
		return fmt.Sprintf("/<err: %s>", err)
	}
	return buf.String()
}

// IsFinalized determines whether the transaction status is in a finalized
// state. A finalized state is terminal, meaning that once a transaction
// enters one of these states, it will never leave it.
func (ts TransactionStatus) IsFinalized() bool {
	return ts == COMMITTED || ts == ABORTED
}

// IsCommittedOrStaging determines if the transaction is morally committed (i.e.
// in the COMMITTED or STAGING state).
func (ts TransactionStatus) IsCommittedOrStaging() bool {
	return ts == COMMITTED || ts == STAGING
}

var _ log.SafeMessager = Transaction{}

// MakeTransaction creates a new transaction. The transaction key is
// composed using the specified baseKey (for locality with data
// affected by the transaction) and a random ID to guarantee
// uniqueness. The specified user-level priority is combined with a
// randomly chosen value to yield a final priority, used to settle
// write conflicts in a way that avoids starvation of long-running
// transactions (see Replica.PushTxn).
//
// baseKey can be nil, in which case it will be set when sending the first
// write.
func MakeTransaction(
	name string, baseKey Key, userPriority UserPriority, now hlc.Timestamp, maxOffsetNs int64,
) Transaction {
	u := uuid.FastMakeV4()
	maxTS := now.Add(maxOffsetNs, 0)

	return Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            baseKey,
			ID:             u,
			WriteTimestamp: now,
			MinTimestamp:   now,
			Priority:       MakePriority(userPriority),
			Sequence:       0, // 1-indexed, incremented before each Request
		},
		Name:                    name,
		LastHeartbeat:           now,
		ReadTimestamp:           now,
		MaxTimestamp:            maxTS,
		DeprecatedOrigTimestamp: now, // For compatibility with 19.2.
	}
}

// LastActive returns the last timestamp at which client activity definitely
// occurred, i.e. the maximum of ReadTimestamp and LastHeartbeat.
func (t Transaction) LastActive() hlc.Timestamp {
	ts := t.LastHeartbeat
	ts.Forward(t.ReadTimestamp)

	// For compatibility with 19.2, handle the case where ReadTimestamp isn't
	// set.
	ts.Forward(t.DeprecatedOrigTimestamp)
	return ts
}

// Clone creates a copy of the given transaction. The copy is shallow because
// none of the references held by a transaction allow interior mutability.
func (t Transaction) Clone() *Transaction {
	return &t
}

// AssertInitialized crashes if the transaction is not initialized.
func (t *Transaction) AssertInitialized(ctx context.Context) {
	if t.ID == (uuid.UUID{}) || t.WriteTimestamp == (hlc.Timestamp{}) {
		log.Fatalf(ctx, "uninitialized txn: %s", *t)
	}
}

// MakePriority generates a random priority value, biased by the specified
// userPriority. If userPriority=100, the random priority will be 100x more
// likely to be greater than if userPriority=1. If userPriority = 0.1, the
// random priority will be 1/10th as likely to be greater than if
// userPriority=NormalUserPriority ( = 1). Balance is achieved when
// userPriority=NormalUserPriority, in which case the priority chosen is
// unbiased.
//
// If userPriority is less than or equal to MinUserPriority, returns
// MinTxnPriority; if greater than or equal to MaxUserPriority, returns
// MaxTxnPriority. If userPriority is 0, returns NormalUserPriority.
func MakePriority(userPriority UserPriority) enginepb.TxnPriority {
	// A currently undocumented feature allows an explicit priority to
	// be set by specifying priority < 1. The explicit priority is
	// simply -userPriority in this case. This is hacky, but currently
	// used for unittesting. Perhaps this should be documented and allowed.
	if userPriority < 0 {
		if -userPriority > UserPriority(math.MaxInt32) {
			panic(fmt.Sprintf("cannot set explicit priority to a value less than -%d", math.MaxInt32))
		}
		return enginepb.TxnPriority(-userPriority)
	} else if userPriority == 0 {
		userPriority = NormalUserPriority
	} else if userPriority >= MaxUserPriority {
		return enginepb.MaxTxnPriority
	} else if userPriority <= MinUserPriority {
		return enginepb.MinTxnPriority
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
	val = (val / (5 * float64(MaxUserPriority))) * math.MaxInt32
	if val < float64(enginepb.MinTxnPriority+1) {
		return enginepb.MinTxnPriority + 1
	} else if val > float64(enginepb.MaxTxnPriority-1) {
		return enginepb.MaxTxnPriority - 1
	}
	return enginepb.TxnPriority(val)
}

// Restart reconfigures a transaction for restart. The epoch is
// incremented for an in-place restart. The timestamp of the
// transaction on restart is set to the maximum of the transaction's
// timestamp and the specified timestamp.
func (t *Transaction) Restart(
	userPriority UserPriority, upgradePriority enginepb.TxnPriority, timestamp hlc.Timestamp,
) {
	t.BumpEpoch()
	if t.WriteTimestamp.Less(timestamp) {
		t.WriteTimestamp = timestamp
	}
	t.ReadTimestamp = t.WriteTimestamp
	t.DeprecatedOrigTimestamp = t.WriteTimestamp // For 19.2 compatibility.
	// Upgrade priority to the maximum of:
	// - the current transaction priority
	// - a random priority created from userPriority
	// - the conflicting transaction's upgradePriority
	t.UpgradePriority(MakePriority(userPriority))
	t.UpgradePriority(upgradePriority)
	// Reset all epoch-scoped state.
	t.Sequence = 0
	t.WriteTooOld = false
	t.CommitTimestampFixed = false
	t.LockSpans = nil
	t.InFlightWrites = nil
	t.IgnoredSeqNums = nil
}

// BumpEpoch increments the transaction's epoch, allowing for an in-place
// restart. This invalidates all write intents previously written at lower
// epochs.
func (t *Transaction) BumpEpoch() {
	t.Epoch++
}

// Update ratchets priority, timestamp and original timestamp values (among
// others) for the transaction. If t.ID is empty, then the transaction is
// copied from o.
func (t *Transaction) Update(o *Transaction) {
	if o == nil {
		return
	}
	o.AssertInitialized(context.TODO())
	if t.ID == (uuid.UUID{}) {
		*t = *o
		return
	} else if t.ID != o.ID {
		log.Fatalf(context.Background(), "updating txn %s with different txn %s", t.String(), o.String())
		return
	}
	if len(t.Key) == 0 {
		t.Key = o.Key
	}

	// Update epoch-scoped state, depending on the two transactions' epochs.
	if t.Epoch < o.Epoch {
		// Replace all epoch-scoped state.
		t.Epoch = o.Epoch
		t.Status = o.Status
		t.WriteTooOld = o.WriteTooOld
		t.CommitTimestampFixed = o.CommitTimestampFixed
		t.Sequence = o.Sequence
		t.LockSpans = o.LockSpans
		t.InFlightWrites = o.InFlightWrites
		t.IgnoredSeqNums = o.IgnoredSeqNums
	} else if t.Epoch == o.Epoch {
		// Forward all epoch-scoped state.
		switch t.Status {
		case PENDING:
			t.Status = o.Status
		case STAGING:
			if o.Status != PENDING {
				t.Status = o.Status
			}
		case ABORTED:
			if o.Status == COMMITTED {
				log.Warningf(context.Background(), "updating ABORTED txn %s with COMMITTED txn %s", t.String(), o.String())
			}
		case COMMITTED:
			// Nothing to do.
		}

		if t.ReadTimestamp.Equal(o.ReadTimestamp) {
			// If neither of the transactions has a bumped ReadTimestamp, then the
			// WriteTooOld flag is cumulative.
			t.WriteTooOld = t.WriteTooOld || o.WriteTooOld
			t.CommitTimestampFixed = t.CommitTimestampFixed || o.CommitTimestampFixed
		} else if t.ReadTimestamp.Less(o.ReadTimestamp) {
			// If `o` has a higher ReadTimestamp (i.e. it's the result of a refresh,
			// which refresh generally clears the WriteTooOld field), then it dictates
			// the WriteTooOld field. This relies on refreshes not being performed
			// concurrently with any requests whose response's WriteTooOld field
			// matters.
			t.WriteTooOld = o.WriteTooOld
			t.CommitTimestampFixed = o.CommitTimestampFixed
		}
		// If t has a higher ReadTimestamp, than it gets to dictate the
		// WriteTooOld field - so there's nothing to update.

		if t.Sequence < o.Sequence {
			t.Sequence = o.Sequence
		}
		if len(o.LockSpans) > 0 {
			t.LockSpans = o.LockSpans
		}
		if len(o.InFlightWrites) > 0 {
			t.InFlightWrites = o.InFlightWrites
		}
		if len(o.IgnoredSeqNums) > 0 {
			t.IgnoredSeqNums = o.IgnoredSeqNums
		}
	} else /* t.Epoch > o.Epoch */ {
		// Ignore epoch-specific state from previous epoch. However, ensure that
		// the transaction status still makes sense.
		switch o.Status {
		case ABORTED:
			// Once aborted, always aborted. The transaction coordinator might
			// have incremented the txn's epoch without realizing that it was
			// aborted.
			t.Status = ABORTED
		case COMMITTED:
			log.Warningf(context.Background(), "updating txn %s with COMMITTED txn at earlier epoch %s", t.String(), o.String())
		}
	}

	// Forward each of the transaction timestamps.
	t.WriteTimestamp.Forward(o.WriteTimestamp)
	t.LastHeartbeat.Forward(o.LastHeartbeat)
	t.DeprecatedOrigTimestamp.Forward(o.DeprecatedOrigTimestamp)
	t.MaxTimestamp.Forward(o.MaxTimestamp)
	t.ReadTimestamp.Forward(o.ReadTimestamp)

	// On update, set lower bound timestamps to the minimum seen by either txn.
	// These shouldn't differ unless one of them is empty, but we're careful
	// anyway.
	if t.MinTimestamp == (hlc.Timestamp{}) {
		t.MinTimestamp = o.MinTimestamp
	} else if o.MinTimestamp != (hlc.Timestamp{}) {
		t.MinTimestamp.Backward(o.MinTimestamp)
	}

	// Absorb the collected clock uncertainty information.
	for _, v := range o.ObservedTimestamps {
		t.UpdateObservedTimestamp(v.NodeID, v.Timestamp)
	}

	// Ratchet the transaction priority.
	t.UpgradePriority(o.Priority)
}

// UpgradePriority sets transaction priority to the maximum of current
// priority and the specified minPriority. The exception is if the
// current priority is set to the minimum, in which case the minimum
// is preserved.
func (t *Transaction) UpgradePriority(minPriority enginepb.TxnPriority) {
	if minPriority > t.Priority && t.Priority != enginepb.MinTxnPriority {
		t.Priority = minPriority
	}
}

// IsLocking returns whether the transaction has begun acquiring locks.
// This method will never return false for a writing transaction.
func (t *Transaction) IsLocking() bool {
	return t.Key != nil
}

// String formats transaction into human readable string.
//
// NOTE: When updating String(), you probably want to also update SafeMessage().
func (t Transaction) String() string {
	var buf strings.Builder
	if len(t.Name) > 0 {
		fmt.Fprintf(&buf, "%q ", t.Name)
	}
	fmt.Fprintf(&buf, "meta={%s} lock=%t stat=%s rts=%s wto=%t max=%s",
		t.TxnMeta, t.IsLocking(), t.Status, t.ReadTimestamp, t.WriteTooOld, t.MaxTimestamp)
	if ni := len(t.LockSpans); t.Status != PENDING && ni > 0 {
		fmt.Fprintf(&buf, " int=%d", ni)
	}
	if nw := len(t.InFlightWrites); t.Status != PENDING && nw > 0 {
		fmt.Fprintf(&buf, " ifw=%d", nw)
	}
	if ni := len(t.IgnoredSeqNums); ni > 0 {
		fmt.Fprintf(&buf, " isn=%d", ni)
	}
	return buf.String()
}

// SafeMessage implements the SafeMessager interface.
//
// This method should be kept largely synchronized with String(), except that it
// can't include sensitive info (e.g. the transaction key).
func (t Transaction) SafeMessage() string {
	var buf strings.Builder
	if len(t.Name) > 0 {
		fmt.Fprintf(&buf, "%q ", t.Name)
	}
	fmt.Fprintf(&buf, "meta={%s} lock=%t stat=%s rts=%s wto=%t max=%s",
		t.TxnMeta.SafeMessage(), t.IsLocking(), t.Status, t.ReadTimestamp, t.WriteTooOld, t.MaxTimestamp)
	if ni := len(t.LockSpans); t.Status != PENDING && ni > 0 {
		fmt.Fprintf(&buf, " int=%d", ni)
	}
	if nw := len(t.InFlightWrites); t.Status != PENDING && nw > 0 {
		fmt.Fprintf(&buf, " ifw=%d", nw)
	}
	if ni := len(t.IgnoredSeqNums); ni > 0 {
		fmt.Fprintf(&buf, " isn=%d", ni)
	}
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
	// Fast path optimization for either no observed timestamps or
	// exactly one, for the same nodeID as we're updating.
	if l := len(t.ObservedTimestamps); l == 0 {
		t.ObservedTimestamps = []ObservedTimestamp{{NodeID: nodeID, Timestamp: maxTS}}
		return
	} else if l == 1 && t.ObservedTimestamps[0].NodeID == nodeID {
		if maxTS.Less(t.ObservedTimestamps[0].Timestamp) {
			t.ObservedTimestamps = []ObservedTimestamp{{NodeID: nodeID, Timestamp: maxTS}}
		}
		return
	}
	s := observedTimestampSlice(t.ObservedTimestamps)
	t.ObservedTimestamps = s.update(nodeID, maxTS)
}

// GetObservedTimestamp returns the lowest HLC timestamp recorded from the
// given node's clock during the transaction. The returned boolean is false if
// no observation about the requested node was found. Otherwise, MaxTimestamp
// can be lowered to the returned timestamp when reading from nodeID.
func (t *Transaction) GetObservedTimestamp(nodeID NodeID) (hlc.Timestamp, bool) {
	s := observedTimestampSlice(t.ObservedTimestamps)
	return s.get(nodeID)
}

// AddIgnoredSeqNumRange adds the given range to the given list of
// ignored seqnum ranges. Since none of the references held by a Transaction
// allow interior mutations, the existing list is copied instead of being
// mutated in place.
//
// The following invariants are assumed to hold and are preserved:
// - the list contains no overlapping ranges
// - the list contains no contiguous ranges
// - the list is sorted, with larger seqnums at the end
//
// Additionally, the caller must ensure:
//
// 1) if the new range overlaps with some range in the list, then it
//    also overlaps with every subsequent range in the list.
//
// 2) the new range's "end" seqnum is larger or equal to the "end"
//    seqnum of the last element in the list.
//
// For example:
//     current list [3 5] [10 20] [22 24]
//     new item:    [8 26]
//     final list:  [3 5] [8 26]
//
//     current list [3 5] [10 20] [22 24]
//     new item:    [28 32]
//     final list:  [3 5] [10 20] [22 24] [28 32]
//
// This corresponds to savepoints semantics:
//
// - Property 1 says that a rollback to an earlier savepoint
//   rolls back over all writes following that savepoint.
// - Property 2 comes from that the new range's 'end' seqnum is the
//   current write seqnum and thus larger than or equal to every
//   previously seen value.
func (t *Transaction) AddIgnoredSeqNumRange(newRange enginepb.IgnoredSeqNumRange) {
	// Truncate the list at the last element not included in the new range.

	list := t.IgnoredSeqNums
	i := sort.Search(len(list), func(i int) bool {
		return list[i].End >= newRange.Start
	})

	cpy := make([]enginepb.IgnoredSeqNumRange, i+1)
	copy(cpy[:i], list[:i])
	cpy[i] = newRange
	t.IgnoredSeqNums = cpy
}

// AsRecord returns a TransactionRecord object containing only the subset of
// fields from the receiver that must be persisted in the transaction record.
func (t *Transaction) AsRecord() TransactionRecord {
	var tr TransactionRecord
	tr.TxnMeta = t.TxnMeta
	tr.Status = t.Status
	tr.LastHeartbeat = t.LastHeartbeat
	tr.LockSpans = t.LockSpans
	tr.InFlightWrites = t.InFlightWrites
	tr.IgnoredSeqNums = t.IgnoredSeqNums
	return tr
}

// AsTransaction returns a Transaction object containing populated fields for
// state in the transaction record and empty fields for state omitted from the
// transaction record.
func (tr *TransactionRecord) AsTransaction() Transaction {
	var t Transaction
	t.TxnMeta = tr.TxnMeta
	t.Status = tr.Status
	t.LastHeartbeat = tr.LastHeartbeat
	t.LockSpans = tr.LockSpans
	t.InFlightWrites = tr.InFlightWrites
	t.IgnoredSeqNums = tr.IgnoredSeqNums
	return t
}

// PrepareTransactionForRetry returns a new Transaction to be used for retrying
// the original Transaction. Depending on the error, this might return an
// already-existing Transaction with an incremented epoch, or a completely new
// Transaction.
//
// The caller should generally check that the error was
// meant for this Transaction before calling this.
//
// pri is the priority that should be used when giving the restarted transaction
// the chance to get a higher priority. Not used when the transaction is being
// aborted.
//
// In case retryErr tells us that a new Transaction needs to be created,
// isolation and name help initialize this new transaction.
func PrepareTransactionForRetry(
	ctx context.Context, pErr *Error, pri UserPriority, clock *hlc.Clock,
) Transaction {
	if pErr.TransactionRestart == TransactionRestart_NONE {
		log.Fatalf(ctx, "invalid retryable err (%T): %s", pErr.GetDetail(), pErr)
	}

	if pErr.GetTxn() == nil {
		log.Fatalf(ctx, "missing txn for retryable error: %s", pErr)
	}

	txn := *pErr.GetTxn()
	aborted := false
	switch tErr := pErr.GetDetail().(type) {
	case *TransactionAbortedError:
		// The txn coming with a TransactionAbortedError is not supposed to be used
		// for the restart. Instead, a brand new transaction is created.
		aborted = true
		// TODO(andrei): Should we preserve the ObservedTimestamps across the
		// restart?
		errTxnPri := txn.Priority
		// Start the new transaction at the current time from the local clock.
		// The local hlc should have been advanced to at least the error's
		// timestamp already.
		now := clock.Now()
		txn = MakeTransaction(
			txn.Name,
			nil, // baseKey
			// We have errTxnPri, but this wants a UserPriority. So we're going to
			// overwrite the priority below.
			NormalUserPriority,
			now,
			clock.MaxOffset().Nanoseconds(),
		)
		// Use the priority communicated back by the server.
		txn.Priority = errTxnPri
	case *ReadWithinUncertaintyIntervalError:
		txn.WriteTimestamp.Forward(
			readWithinUncertaintyIntervalRetryTimestamp(ctx, &txn, tErr, pErr.OriginNode))
	case *TransactionPushError:
		// Increase timestamp if applicable, ensuring that we're just ahead of
		// the pushee.
		txn.WriteTimestamp.Forward(tErr.PusheeTxn.WriteTimestamp)
		txn.UpgradePriority(tErr.PusheeTxn.Priority - 1)
	case *TransactionRetryError:
		// Nothing to do. Transaction.Timestamp has already been forwarded to be
		// ahead of any timestamp cache entries or newer versions which caused
		// the restart.
	case *WriteTooOldError:
		// Increase the timestamp to the ts at which we've actually written.
		txn.WriteTimestamp.Forward(writeTooOldRetryTimestamp(&txn, tErr))
	default:
		log.Fatalf(ctx, "invalid retryable err (%T): %s", pErr.GetDetail(), pErr)
	}
	if !aborted {
		if txn.Status.IsFinalized() {
			log.Fatalf(ctx, "transaction unexpectedly finalized in (%T): %s", pErr.GetDetail(), pErr)
		}
		txn.Restart(pri, txn.Priority, txn.WriteTimestamp)
	}
	return txn
}

// CanTransactionRetryAtRefreshedTimestamp returns whether the transaction
// specified in the supplied error can be retried at a refreshed timestamp to
// avoid a client-side transaction restart. If true, returns a cloned, updated
// Transaction object with the provisional commit timestamp and refreshed
// timestamp set appropriately.
func CanTransactionRetryAtRefreshedTimestamp(
	ctx context.Context, pErr *Error,
) (bool, *Transaction) {
	txn := pErr.GetTxn()
	if txn == nil || txn.CommitTimestampFixed {
		return false, nil
	}
	timestamp := txn.WriteTimestamp
	switch err := pErr.GetDetail().(type) {
	case *TransactionRetryError:
		if err.Reason != RETRY_SERIALIZABLE && err.Reason != RETRY_WRITE_TOO_OLD {
			return false, nil
		}
	case *WriteTooOldError:
		// TODO(andrei): Chances of success for on write-too-old conditions might be
		// usually small: if our txn previously read the key that generated this
		// error, obviously the refresh will fail. It might be worth trying to
		// detect these cases and save the futile attempt; we'd need to have access
		// to the key that generated the error.
		timestamp.Forward(writeTooOldRetryTimestamp(txn, err))
	case *ReadWithinUncertaintyIntervalError:
		timestamp.Forward(
			readWithinUncertaintyIntervalRetryTimestamp(ctx, txn, err, pErr.OriginNode))
	default:
		return false, nil
	}

	newTxn := txn.Clone()
	newTxn.WriteTimestamp.Forward(timestamp)
	newTxn.ReadTimestamp.Forward(newTxn.WriteTimestamp)
	newTxn.WriteTooOld = false

	return true, newTxn
}

func readWithinUncertaintyIntervalRetryTimestamp(
	ctx context.Context, txn *Transaction, err *ReadWithinUncertaintyIntervalError, origin NodeID,
) hlc.Timestamp {
	// If the reader encountered a newer write within the uncertainty
	// interval, we advance the txn's timestamp just past the last observed
	// timestamp from the node.
	ts, ok := txn.GetObservedTimestamp(origin)
	if !ok {
		log.Fatalf(ctx,
			"missing observed timestamp for node %d found on uncertainty restart. "+
				"err: %s. txn: %s. Observed timestamps: %v",
			origin, err, txn, txn.ObservedTimestamps)
	}
	// Also forward by the existing timestamp.
	ts.Forward(err.ExistingTimestamp.Next())
	return ts
}

func writeTooOldRetryTimestamp(txn *Transaction, err *WriteTooOldError) hlc.Timestamp {
	return err.ActualTimestamp
}

// Replicas returns all of the replicas present in the descriptor after this
// trigger applies.
func (crt ChangeReplicasTrigger) Replicas() []ReplicaDescriptor {
	if crt.Desc != nil {
		return crt.Desc.Replicas().All()
	}
	return crt.DeprecatedUpdatedReplicas
}

// NextReplicaID returns the next replica id to use after this trigger applies.
func (crt ChangeReplicasTrigger) NextReplicaID() ReplicaID {
	if crt.Desc != nil {
		return crt.Desc.NextReplicaID
	}
	return crt.DeprecatedNextReplicaID
}

// ConfChange returns the configuration change described by the trigger.
func (crt ChangeReplicasTrigger) ConfChange(encodedCtx []byte) (raftpb.ConfChangeI, error) {
	return confChangeImpl(crt, encodedCtx)
}

func (crt ChangeReplicasTrigger) alwaysV2() bool {
	// NB: we can return true in 20.1, but we don't win anything unless
	// we are actively trying to migrate out of V1 membership changes, which
	// could modestly simplify small areas of our codebase.
	return false
}

// confChangeImpl is the implementation of (ChangeReplicasTrigger).ConfChange
// narrowed down to the inputs it actually needs for better testability.
func confChangeImpl(
	crt interface {
		Added() []ReplicaDescriptor
		Removed() []ReplicaDescriptor
		Replicas() []ReplicaDescriptor
		alwaysV2() bool
	},
	encodedCtx []byte,
) (raftpb.ConfChangeI, error) {
	added, removed, replicas := crt.Added(), crt.Removed(), crt.Replicas()

	var sl []raftpb.ConfChangeSingle

	checkExists := func(in ReplicaDescriptor) error {
		for _, rDesc := range replicas {
			if rDesc.ReplicaID == in.ReplicaID {
				if a, b := in.GetType(), rDesc.GetType(); a != b {
					return errors.Errorf("have %s, but descriptor has %s", in, rDesc)
				}
				return nil
			}
		}
		return errors.Errorf("%s missing from descriptors %v", in, replicas)
	}
	checkNotExists := func(in ReplicaDescriptor) error {
		for _, rDesc := range replicas {
			if rDesc.ReplicaID == in.ReplicaID {
				return errors.Errorf("%s must no longer be present in descriptor", in)
			}
		}
		return nil
	}

	for _, rDesc := range removed {
		sl = append(sl, raftpb.ConfChangeSingle{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: uint64(rDesc.ReplicaID),
		})

		switch rDesc.GetType() {
		case VOTER_OUTGOING:
			// If a voter is removed through joint consensus, it will
			// be turned into an outgoing voter first.
			if err := checkExists(rDesc); err != nil {
				return nil, err
			}
		case VOTER_DEMOTING:
			// If a voter is demoted through joint consensus, it will
			// be turned into a demoting voter first.
			if err := checkExists(rDesc); err != nil {
				return nil, err
			}
			// It's being re-added as a learner, not only removed.
			sl = append(sl, raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddLearnerNode,
				NodeID: uint64(rDesc.ReplicaID),
			})
		case LEARNER:
			// A learner could in theory show up in the descriptor if the
			// removal was really a demotion and no joint consensus is used.
			// But etcd/raft currently forces us to go through joint consensus
			// when demoting, so demotions will always have a VOTER_DEMOTING
			// instead. We must be straight-up removing a voter or learner, so
			// the target should be gone from the descriptor at this point.
			if err := checkNotExists(rDesc); err != nil {
				return nil, err
			}
		case VOTER_FULL:
			// A voter can't be in the descriptor if it's being removed.
			if err := checkNotExists(rDesc); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("can't remove replica in state %v", rDesc.GetType())
		}
	}

	for _, rDesc := range added {
		// The incoming descriptor must also be present in the set of all
		// replicas, which is ultimately the authoritative one because that's
		// what's written to the KV store.
		if err := checkExists(rDesc); err != nil {
			return nil, err
		}

		var changeType raftpb.ConfChangeType
		switch rDesc.GetType() {
		case VOTER_FULL:
			// We're adding a new voter.
			changeType = raftpb.ConfChangeAddNode
		case VOTER_INCOMING:
			// We're adding a voter, but will transition into a joint config
			// first.
			changeType = raftpb.ConfChangeAddNode
		case LEARNER:
			// We're adding a learner.
			// Note that we're guaranteed by virtue of the upstream
			// ChangeReplicas txn that this learner is not currently a voter.
			// Demotions (i.e. transitioning from voter to learner) are not
			// represented in `added`; they're handled in `removed` above.
			changeType = raftpb.ConfChangeAddLearnerNode
		default:
			// A voter that is demoting was just removed and re-added in the
			// `removals` handler. We should not see it again here.
			// A voter that's outgoing similarly has no reason to show up here.
			return nil, errors.Errorf("can't add replica in state %v", rDesc.GetType())
		}
		sl = append(sl, raftpb.ConfChangeSingle{
			Type:   changeType,
			NodeID: uint64(rDesc.ReplicaID),
		})
	}

	// Check whether we're entering a joint state. This is the case precisely when
	// the resulting descriptors tells us that this is the case. Note that we've
	// made sure above that all of the additions/removals are in tune with that
	// descriptor already.
	var enteringJoint bool
	for _, rDesc := range replicas {
		switch rDesc.GetType() {
		case VOTER_INCOMING, VOTER_OUTGOING, VOTER_DEMOTING:
			enteringJoint = true
		default:
		}
	}
	wantLeaveJoint := len(added)+len(removed) == 0
	if !enteringJoint {
		if len(added)+len(removed) > 1 {
			return nil, errors.Errorf("change requires joint consensus")
		}
	} else if wantLeaveJoint {
		return nil, errors.Errorf("descriptor enters joint state, but trigger is requesting to leave one")
	}

	var cc raftpb.ConfChangeI

	if enteringJoint || crt.alwaysV2() {
		// V2 membership changes, which allow atomic replication changes. We
		// track the joint state in the range descriptor and thus we need to be
		// in charge of when to leave the joint state.
		transition := raftpb.ConfChangeTransitionJointExplicit
		if !enteringJoint {
			// If we're using V2 just to avoid V1 (and not because we actually
			// have a change that requires V2), then use an auto transition
			// which skips the joint state. This is necessary: our descriptor
			// says we're not supposed to go through one.
			transition = raftpb.ConfChangeTransitionAuto
		}
		cc = raftpb.ConfChangeV2{
			Transition: transition,
			Changes:    sl,
			Context:    encodedCtx,
		}
	} else if wantLeaveJoint {
		// Transitioning out of a joint config.
		cc = raftpb.ConfChangeV2{
			Context: encodedCtx,
		}
	} else {
		// Legacy path with exactly one change.
		cc = raftpb.ConfChange{
			Type:    sl[0].Type,
			NodeID:  sl[0].NodeID,
			Context: encodedCtx,
		}
	}
	return cc, nil
}

var _ fmt.Stringer = &ChangeReplicasTrigger{}

func (crt ChangeReplicasTrigger) String() string {
	var nextReplicaID ReplicaID
	var afterReplicas []ReplicaDescriptor
	added, removed := crt.Added(), crt.Removed()
	if crt.Desc != nil {
		nextReplicaID = crt.Desc.NextReplicaID
		// NB: we don't want to mutate InternalReplicas, so we don't call
		// .Replicas()
		//
		// TODO(tbg): revisit after #39489 is merged.
		afterReplicas = crt.Desc.InternalReplicas
	} else {
		nextReplicaID = crt.DeprecatedNextReplicaID
		afterReplicas = crt.DeprecatedUpdatedReplicas
	}
	var chgS strings.Builder
	cc, err := crt.ConfChange(nil)
	if err != nil {
		fmt.Fprintf(&chgS, "<malformed ChangeReplicasTrigger: %s>", err)
	} else {
		ccv2 := cc.AsV2()
		if ccv2.LeaveJoint() {
			// NB: this isn't missing a trailing space.
			//
			// TODO(tbg): could list the replicas that will actually leave the
			// voter set.
			fmt.Fprintf(&chgS, "LEAVE_JOINT")
		} else if _, ok := ccv2.EnterJoint(); ok {
			fmt.Fprintf(&chgS, "ENTER_JOINT(%s) ", raftpb.ConfChangesToString(ccv2.Changes))
		} else {
			fmt.Fprintf(&chgS, "SIMPLE(%s) ", raftpb.ConfChangesToString(ccv2.Changes))
		}
	}
	if len(added) > 0 {
		fmt.Fprintf(&chgS, "%s%s", ADD_REPLICA, added)
	}
	if len(removed) > 0 {
		if len(added) > 0 {
			chgS.WriteString(", ")
		}
		fmt.Fprintf(&chgS, "%s%s", REMOVE_REPLICA, removed)
	}
	fmt.Fprintf(&chgS, ": after=%s next=%d", afterReplicas, nextReplicaID)
	return chgS.String()
}

func (crt ChangeReplicasTrigger) legacy() (ReplicaDescriptor, bool) {
	if len(crt.InternalAddedReplicas)+len(crt.InternalRemovedReplicas) == 0 && crt.DeprecatedReplica.ReplicaID != 0 {
		return crt.DeprecatedReplica, true
	}
	return ReplicaDescriptor{}, false
}

// Added returns the replicas added by this change (if there are any).
func (crt ChangeReplicasTrigger) Added() []ReplicaDescriptor {
	if rDesc, ok := crt.legacy(); ok && crt.DeprecatedChangeType == ADD_REPLICA {
		return []ReplicaDescriptor{rDesc}
	}
	return crt.InternalAddedReplicas
}

// Removed returns the replicas whose removal is initiated by this change (if there are any).
// Note that in an atomic replication change, Removed() contains the replicas when they are
// transitioning to VOTER_{OUTGOING,DEMOTING} (from VOTER_FULL). The subsequent trigger
// leaving the joint configuration has an empty Removed().
func (crt ChangeReplicasTrigger) Removed() []ReplicaDescriptor {
	if rDesc, ok := crt.legacy(); ok && crt.DeprecatedChangeType == REMOVE_REPLICA {
		return []ReplicaDescriptor{rDesc}
	}
	return crt.InternalRemovedReplicas
}

// LeaseSequence is a custom type for a lease sequence number.
type LeaseSequence int64

// String implements the fmt.Stringer interface.
func (s LeaseSequence) String() string {
	return strconv.FormatInt(int64(s), 10)
}

var _ fmt.Stringer = &Lease{}

func (l Lease) String() string {
	var proposedSuffix string
	if l.ProposedTS != nil {
		proposedSuffix = fmt.Sprintf(" pro=%s", l.ProposedTS)
	}
	if l.Type() == LeaseExpiration {
		return fmt.Sprintf("repl=%s seq=%s start=%s exp=%s%s", l.Replica, l.Sequence, l.Start, l.Expiration, proposedSuffix)
	}
	return fmt.Sprintf("repl=%s seq=%s start=%s epo=%d%s", l.Replica, l.Sequence, l.Start, l.Epoch, proposedSuffix)
}

// BootstrapLease returns the lease to persist for the range of a freshly bootstrapped store. The
// returned lease is morally "empty" but has a few fields set to non-nil zero values because some
// used to be non-nullable and we now fuzz their nullability in tests. As a consequence, it's better
// to always use zero fields here so that the initial stats are constant.
func BootstrapLease() Lease {
	return Lease{
		Expiration:            &hlc.Timestamp{},
		DeprecatedStartStasis: &hlc.Timestamp{},
	}
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
	if l.Epoch == 0 {
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
//
// NB: Lease.Equivalent is NOT symmetric. For expiration-based
// leases, a lease is equivalent to another with an equal or
// later expiration, but not an earlier expiration.
func (l Lease) Equivalent(newL Lease) bool {
	// Ignore proposed timestamp & deprecated start stasis.
	l.ProposedTS, newL.ProposedTS = nil, nil
	l.DeprecatedStartStasis, newL.DeprecatedStartStasis = nil, nil
	// Ignore sequence numbers, they are simply a reflection of
	// the equivalency of other fields.
	l.Sequence, newL.Sequence = 0, 0
	// Ignore the ReplicaDescriptor's type. This shouldn't affect lease
	// equivalency because Raft state shouldn't be factored into the state of a
	// Replica's lease. We don't expect a leaseholder to ever become a LEARNER
	// replica, but that also shouldn't prevent it from extending its lease. The
	// code also avoids a potential bug where an unset ReplicaType and a set
	// VOTER ReplicaType are considered distinct and non-equivalent.
	//
	// Change this line to the following when ReplicaType becomes non-nullable:
	//  l.Replica.Type, newL.Replica.Type = 0, 0
	l.Replica.Type, newL.Replica.Type = nil, nil
	// If both leases are epoch-based, we must dereference the epochs
	// and then set to nil.
	switch l.Type() {
	case LeaseEpoch:
		// Ignore expirations. This seems benign but since we changed the
		// nullability of this field in the 1.2 cycle, it's crucial and
		// tested in TestLeaseEquivalence.
		l.Expiration, newL.Expiration = nil, nil

		if l.Epoch == newL.Epoch {
			l.Epoch, newL.Epoch = 0, 0
		}
	case LeaseExpiration:
		// See the comment above, though this field's nullability wasn't
		// changed. We nil it out for completeness only.
		l.Epoch, newL.Epoch = 0, 0

		// For expiration-based leases, extensions are considered equivalent.
		// This is the one case where Equivalent is not commutative and, as
		// such, requires special handling beneath Raft (see checkForcedErrLocked).
		if l.GetExpiration().LessEq(newL.GetExpiration()) {
			l.Expiration, newL.Expiration = nil, nil
		}
	}
	return l == newL
}

// GetExpiration returns the lease expiration or the zero timestamp if the
// receiver is not an expiration-based lease.
func (l Lease) GetExpiration() hlc.Timestamp {
	if l.Expiration == nil {
		return hlc.Timestamp{}
	}
	return *l.Expiration
}

// equivalentTimestamps compares two timestamps for equality and also considers
// the nil timestamp equal to the zero timestamp.
func equivalentTimestamps(a, b *hlc.Timestamp) bool {
	if a == nil {
		if b == nil {
			return true
		}
		if (*b == hlc.Timestamp{}) {
			return true
		}
	} else if b == nil {
		if (*a == hlc.Timestamp{}) {
			return true
		}
	}
	return a.Equal(b)
}

// Equal implements the gogoproto Equal interface. This implementation is
// forked from the gogoproto generated code to allow l.Expiration == nil and
// l.Expiration == &hlc.Timestamp{} to compare equal. Ditto for
// DeprecatedStartStasis.
func (l *Lease) Equal(that interface{}) bool {
	if that == nil {
		return l == nil
	}

	that1, ok := that.(*Lease)
	if !ok {
		that2, ok := that.(Lease)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return l == nil
	} else if l == nil {
		return false
	}

	if !l.Start.Equal(&that1.Start) {
		return false
	}
	if !equivalentTimestamps(l.Expiration, that1.Expiration) {
		return false
	}
	if !l.Replica.Equal(&that1.Replica) {
		return false
	}
	if !equivalentTimestamps(l.DeprecatedStartStasis, that1.DeprecatedStartStasis) {
		return false
	}
	if !l.ProposedTS.Equal(that1.ProposedTS) {
		return false
	}
	if l.Epoch != that1.Epoch {
		return false
	}
	if l.Sequence != that1.Sequence {
		return false
	}
	return true
}

// MakeIntent makes an intent with the given txn and key.
// This is suitable for use when constructing WriteIntentError.
func MakeIntent(txn *enginepb.TxnMeta, key Key) Intent {
	var i Intent
	i.Key = key
	i.Txn = *txn
	return i
}

// AsIntents takes a transaction and a slice of keys and
// returns it as a slice of intents.
func AsIntents(txn *enginepb.TxnMeta, keys []Key) []Intent {
	ret := make([]Intent, len(keys))
	for i := range keys {
		ret[i] = MakeIntent(txn, keys[i])
	}
	return ret
}

// MakeLockUpdate makes a lock update from the given span and txn.
// The function assumes that the lock has a replicated durability.
func MakeLockUpdate(txn *Transaction, span Span) LockUpdate {
	return MakeLockUpdateWithDur(txn, span, lock.Replicated)
}

// MakeLockUpdateWithDur makes a lock update from the given span,
// txn, and lock durability.
func MakeLockUpdateWithDur(txn *Transaction, span Span, dur lock.Durability) LockUpdate {
	update := LockUpdate{Span: span}
	update.SetTxn(txn)
	update.Durability = dur
	return update
}

// AsLockUpdates takes a slice of spans and returns it as a slice
// of lock updates for the given transaction and lock durability.
func AsLockUpdates(txn *Transaction, spans []Span, dur lock.Durability) []LockUpdate {
	ret := make([]LockUpdate, len(spans))
	for i := range spans {
		ret[i] = MakeLockUpdateWithDur(txn, spans[i], dur)
	}
	return ret
}

// SetTxn updates the transaction details in the lock update.
func (u *LockUpdate) SetTxn(txn *Transaction) {
	u.Txn = txn.TxnMeta
	u.Status = txn.Status
	u.IgnoredSeqNums = txn.IgnoredSeqNums
}

// EqualValue compares for equality.
func (s Span) EqualValue(o Span) bool {
	return s.Key.Equal(o.Key) && s.EndKey.Equal(o.EndKey)
}

// Overlaps returns true WLOG for span A and B iff:
// 1. Both spans contain one key (just the start key) and they are equal; or
// 2. The span with only one key is contained inside the other span; or
// 3. The end key of span A is strictly greater than the start key of span B
//    and the end key of span B is strictly greater than the start key of span
//    A.
func (s Span) Overlaps(o Span) bool {
	if !s.Valid() || !o.Valid() {
		return false
	}

	if len(s.EndKey) == 0 && len(o.EndKey) == 0 {
		return s.Key.Equal(o.Key)
	} else if len(s.EndKey) == 0 {
		return bytes.Compare(s.Key, o.Key) >= 0 && bytes.Compare(s.Key, o.EndKey) < 0
	} else if len(o.EndKey) == 0 {
		return bytes.Compare(o.Key, s.Key) >= 0 && bytes.Compare(o.Key, s.EndKey) < 0
	}
	return bytes.Compare(s.EndKey, o.Key) > 0 && bytes.Compare(s.Key, o.EndKey) < 0
}

// Combine creates a new span containing the full union of the key
// space covered by the two spans. This includes any key space not
// covered by either span, but between them if the spans are disjoint.
// Warning: using this method to combine local and non-local spans is
// not recommended and will result in potentially database-wide
// spans being returned. Use with caution.
func (s Span) Combine(o Span) Span {
	if !s.Valid() || !o.Valid() {
		return Span{}
	}

	min := s.Key
	max := s.Key
	if len(s.EndKey) > 0 {
		max = s.EndKey
	}
	if o.Key.Compare(min) < 0 {
		min = o.Key
	} else if o.Key.Compare(max) > 0 {
		max = o.Key
	}
	if len(o.EndKey) > 0 && o.EndKey.Compare(max) > 0 {
		max = o.EndKey
	}
	if min.Equal(max) {
		return Span{Key: min}
	} else if s.Key.Equal(max) || o.Key.Equal(max) {
		return Span{Key: min, EndKey: max.Next()}
	}
	return Span{Key: min, EndKey: max}
}

// Contains returns whether the receiver contains the given span.
func (s Span) Contains(o Span) bool {
	if !s.Valid() || !o.Valid() {
		return false
	}

	if len(s.EndKey) == 0 && len(o.EndKey) == 0 {
		return s.Key.Equal(o.Key)
	} else if len(s.EndKey) == 0 {
		return false
	} else if len(o.EndKey) == 0 {
		return bytes.Compare(o.Key, s.Key) >= 0 && bytes.Compare(o.Key, s.EndKey) < 0
	}
	return bytes.Compare(s.Key, o.Key) <= 0 && bytes.Compare(s.EndKey, o.EndKey) >= 0
}

// ContainsKey returns whether the span contains the given key.
func (s Span) ContainsKey(key Key) bool {
	return bytes.Compare(key, s.Key) >= 0 && bytes.Compare(key, s.EndKey) < 0
}

// ProperlyContainsKey returns whether the span properly contains the given key.
func (s Span) ProperlyContainsKey(key Key) bool {
	return bytes.Compare(key, s.Key) > 0 && bytes.Compare(key, s.EndKey) < 0
}

// AsRange returns the Span as an interval.Range.
func (s Span) AsRange() interval.Range {
	startKey := s.Key
	endKey := s.EndKey
	if len(endKey) == 0 {
		endKey = s.Key.Next()
		startKey = endKey[:len(startKey)]
	}
	return interval.Range{
		Start: interval.Comparable(startKey),
		End:   interval.Comparable(endKey),
	}
}

func (s Span) String() string {
	const maxChars = math.MaxInt32
	return PrettyPrintRange(s.Key, s.EndKey, maxChars)
}

// SplitOnKey returns two spans where the left span has EndKey and right span
// has start Key of the split key, respectively.
// If the split key lies outside the span, the original span is returned on the
// left (and right is an invalid span with empty keys).
func (s Span) SplitOnKey(key Key) (left Span, right Span) {
	// Cannot split on or before start key or on or after end key.
	if bytes.Compare(key, s.Key) <= 0 || bytes.Compare(key, s.EndKey) >= 0 {
		return s, Span{}
	}

	return Span{Key: s.Key, EndKey: key}, Span{Key: key, EndKey: s.EndKey}
}

// Valid returns whether or not the span is a "valid span".
// A valid span cannot have an empty start and end key and must satisfy either:
// 1. The end key is empty.
// 2. The start key is lexicographically-ordered before the end key.
func (s Span) Valid() bool {
	// s.Key can be empty if it is KeyMin.
	// Can't have both KeyMin start and end keys.
	if len(s.Key) == 0 && len(s.EndKey) == 0 {
		return false
	}

	if len(s.EndKey) == 0 {
		return true
	}

	if bytes.Compare(s.Key, s.EndKey) >= 0 {
		return false
	}

	return true
}

// Spans is a slice of spans.
type Spans []Span

// implement Sort.Interface
func (a Spans) Len() int           { return len(a) }
func (a Spans) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Spans) Less(i, j int) bool { return a[i].Key.Compare(a[j].Key) < 0 }

// ContainsKey returns whether any of the spans in the set of spans contains
// the given key.
func (a Spans) ContainsKey(key Key) bool {
	for _, span := range a {
		if span.ContainsKey(key) {
			return true
		}
	}

	return false
}

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

// ContainsKeyInverted returns whether this span contains the specified key. The
// receiver span is considered inverted, meaning that instead of containing the
// range ["key","endKey"), it contains the range ("key","endKey"].
func (rs RSpan) ContainsKeyInverted(key RKey) bool {
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

func (rs RSpan) String() string {
	const maxChars = math.MaxInt32
	return PrettyPrintRange(Key(rs.Key), Key(rs.EndKey), maxChars)
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

// AsRawSpanWithNoLocals returns the RSpan as a Span. This is to be used only
// in select situations in which an RSpan is known to not contain a wrapped
// locally-addressed Span.
func (rs RSpan) AsRawSpanWithNoLocals() Span {
	return Span{
		Key:    Key(rs.Key),
		EndKey: Key(rs.EndKey),
	}
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

// observedTimestampSlice maintains an immutable sorted list of observed
// timestamps.
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
// correct (sorted) location. The receiver is not mutated.
func (s observedTimestampSlice) update(
	nodeID NodeID, timestamp hlc.Timestamp,
) observedTimestampSlice {
	i := s.index(nodeID)
	if i < len(s) && s[i].NodeID == nodeID {
		if timestamp.Less(s[i].Timestamp) {
			// The input slice is immutable, so copy and update.
			cpy := make(observedTimestampSlice, len(s))
			copy(cpy, s)
			cpy[i].Timestamp = timestamp
			return cpy
		}
		return s
	}
	// The input slice is immutable, so copy and update. Don't append to
	// avoid an allocation. Doing so could invalidate a previous update
	// to this receiver.
	cpy := make(observedTimestampSlice, len(s)+1)
	copy(cpy[:i], s[:i])
	cpy[i] = ObservedTimestamp{NodeID: nodeID, Timestamp: timestamp}
	copy(cpy[i+1:], s[i:])
	return cpy
}

// SequencedWriteBySeq implements sorting of a slice of SequencedWrites
// by sequence number.
type SequencedWriteBySeq []SequencedWrite

// Len implements sort.Interface.
func (s SequencedWriteBySeq) Len() int { return len(s) }

// Less implements sort.Interface.
func (s SequencedWriteBySeq) Less(i, j int) bool { return s[i].Sequence < s[j].Sequence }

// Swap implements sort.Interface.
func (s SequencedWriteBySeq) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

var _ sort.Interface = SequencedWriteBySeq{}

// Find searches for the index of the SequencedWrite with the provided
// sequence number. Returns -1 if no corresponding write is found.
func (s SequencedWriteBySeq) Find(seq enginepb.TxnSeq) int {
	if util.RaceEnabled {
		if !sort.IsSorted(s) {
			panic("SequencedWriteBySeq must be sorted")
		}
	}
	if i := sort.Search(len(s), func(i int) bool {
		return s[i].Sequence >= seq
	}); i < len(s) && s[i].Sequence == seq {
		return i
	}
	return -1
}

// Silence unused warning.
var _ = (SequencedWriteBySeq{}).Find

func init() {
	// Inject the format dependency into the enginepb package.
	enginepb.FormatBytesAsKey = func(k []byte) string { return Key(k).String() }
	enginepb.FormatBytesAsValue = func(v []byte) string { return Value{RawBytes: v}.PrettyPrint() }
}
