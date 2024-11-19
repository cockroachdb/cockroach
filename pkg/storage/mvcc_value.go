// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	extendedLenSize     = 4 // also checksumSize for roachpb.Value
	tagPos              = extendedLenSize
	tagSize             = 1
	extendedPreludeSize = extendedLenSize + tagSize

	extendedEncodingSentinel = byte(roachpb.ValueType_MVCC_EXTENDED_ENCODING_SENTINEL)
)

// MVCCValue is a versioned value, stored at an associated MVCCKey with a
// non-zero version timestamp.
//
// MVCCValue wraps a roachpb.Value and extends it with MVCC-level metadata which
// is stored in an enginepb.MVCCValueHeader struct.
//
// The mvcc value has a "simple" and an "extended" encoding scheme, depending on
// whether the value's header is empty or not. If the value's header is empty,
// it is omitted in the encoding and the mvcc value's encoding is identical to
// that of roachpb.Value. This provided backwards compatibility and ensures that
// the MVCCValue optimizes away in the common case. If the value's header is not
// empty, it is prepended to the roachpb.Value encoding. The encoding scheme's
// variants are:
//
// Simple (identical to the roachpb.Value encoding):
//
//	<4-byte-checksum><1-byte-tag><encoded-data>
//
// Extended (header prepended to roachpb.Value encoding):
//
//	<4-byte-header-len><1-byte-sentinel><mvcc-header><4-byte-checksum><1-byte-tag><encoded-data>
//
// The two encoding scheme variants are distinguished using the 5th byte, which
// is either the roachpb.Value tag (which has many values) or a sentinel tag not
// used by the roachpb.Value encoding which indicates the extended encoding
// scheme.
//
// For a deletion tombstone, the encoding of roachpb.Value is special cased to
// be empty, i.e., no checksum, tag, or encoded-data. In that case the extended
// encoding above is simply:
//
//	<4-byte-header-len><1-byte-sentinel><mvcc-header>
//
// To identify a deletion tombstone from an encoded MVCCValue, callers should
// decode the value using DecodeMVCCValue and then use the IsTombstone method.
// For example:
//
//	valRaw := iter.UnsafeValue()
//	val, err := DecodeMVCCValue(valRaw)
//	if err != nil { ... }
//	isTombstone := val.IsTombstone()
type MVCCValue struct {
	enginepb.MVCCValueHeader
	Value roachpb.Value
}

// IsTombstone returns whether the MVCCValue represents a deletion tombstone.
func (v MVCCValue) IsTombstone() bool {
	return len(v.Value.RawBytes) == 0
}

// LocalTimestampNeeded returns whether the MVCCValue's local timestamp is
// needed, or whether it can be implied by (i.e. set to the same value as)
// its key's version timestamp.
//
// TODO(erikgrinaker): Consider making this and GetLocalTimestamp() generic over
// MVCCKey and MVCCRangeKey once generics have matured a bit.
func (v MVCCValue) LocalTimestampNeeded(keyTS hlc.Timestamp) bool {
	// If the local timestamp is empty, it is assumed to be equal to the key's
	// version timestamp and so the local timestamp is not needed.
	return !v.LocalTimestamp.IsEmpty() &&
		// If the local timestamp is not empty, it is safe for the local clock
		// timestamp to be rounded down, as this will simply lead to additional
		// uncertainty restarts. In such cases, the local timestamp is not needed.
		// However, it is not safe for the local clock timestamp to be rounded up,
		// as this could lead to stale reads. As a result, in such cases, the local
		// timestamp is needed and cannot be implied by the version timestamp.
		v.LocalTimestamp.ToTimestamp().Less(keyTS)
}

// GetLocalTimestamp returns the MVCCValue's local timestamp. If the local
// timestamp is not set explicitly, its implicit value is taken from the
// provided key version timestamp and returned.
func (v MVCCValue) GetLocalTimestamp(keyTS hlc.Timestamp) hlc.ClockTimestamp {
	if v.LocalTimestamp.IsEmpty() {
		return hlc.ClockTimestamp(keyTS)
	}
	return v.LocalTimestamp
}

// String implements the fmt.Stringer interface.
func (v MVCCValue) String() string {
	return redact.StringWithoutMarkers(v)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (v MVCCValue) SafeFormat(w redact.SafePrinter, _ rune) {
	if v.MVCCValueHeader != (enginepb.MVCCValueHeader{}) {
		fields := make([]string, 0)
		w.Printf("{")
		if !v.LocalTimestamp.IsEmpty() {
			fields = append(fields, fmt.Sprintf("localTs=%s", v.LocalTimestamp))
		}
		if v.ImportEpoch != 0 {
			fields = append(fields, fmt.Sprintf("importEpoch=%v", v.ImportEpoch))
		}
		if v.OriginID != 0 {
			fields = append(fields, fmt.Sprintf("originID=%v", v.OriginID))
		}
		if v.OriginTimestamp.IsSet() {
			fields = append(fields, fmt.Sprintf("originTs=%s", v.OriginTimestamp))
		}
		w.Print(strings.Join(fields, ", "))
		w.Printf("}")
	}
	w.Print(v.Value.PrettyPrint())
}

// EncodeMVCCValueForExport encodes fields from the MVCCValueHeader
// that are appropriate for export out of the cluster.
//
// The returned bool is true if the provided buffer was used or
// reallocated and false if the MVCCValue.Value.RawBytes were returned
// directly.
func EncodeMVCCValueForExport(mvccValue MVCCValue, b []byte) ([]byte, bool, error) {
	mvccValue.MVCCValueHeader.LocalTimestamp = hlc.ClockTimestamp{}
	if mvccValue.MVCCValueHeader.IsEmpty() {
		return mvccValue.Value.RawBytes, false, nil
	}
	return EncodeMVCCValueToBuf(mvccValue, b)
}

// When running a metamorphic build, disable the simple MVCC value encoding to
// prevent code from assuming that the MVCCValue encoding is identical to the
// roachpb.Value encoding.
var disableSimpleValueEncoding = metamorphic.ConstantWithTestBool(
	"mvcc-value-disable-simple-encoding", false)

// DisableMetamorphicSimpleValueEncoding disables the disableSimpleValueEncoding
// metamorphic bool for the duration of a test, resetting it at the end.
func DisableMetamorphicSimpleValueEncoding(t interface {
	Helper()
	Cleanup(func())
}) {
	t.Helper()
	if disableSimpleValueEncoding {
		disableSimpleValueEncoding = false
		t.Cleanup(func() {
			disableSimpleValueEncoding = true
		})
	}
}

// encodedMVCCValueSize returns the size of the MVCCValue when encoded.
func encodedMVCCValueSize(v MVCCValue) int {
	if v.MVCCValueHeader.IsEmpty() && !disableSimpleValueEncoding {
		return len(v.Value.RawBytes)
	}
	return extendedPreludeSize + v.MVCCValueHeader.Size() + len(v.Value.RawBytes)
}

// EncodeMVCCValue encodes an MVCCValue into its Pebble representation. See the
// comment on MVCCValue for a description of the encoding scheme.
func EncodeMVCCValue(v MVCCValue) ([]byte, error) {
	b, _, err := EncodeMVCCValueToBuf(v, nil)
	return b, err
}

// EncodeMVCCValueToBuf encodes an MVCCValue into its Pebble
// representation. See the comment on MVCCValue for a description of
// the encoding scheme.
//
// If extended encoding is required, the given buffer will be used if
// it is large enough. If the provided buffer is not large enough a
// new buffer is allocated.
//
// The returned bool is true if the provided buffer was used or
// reallocated and false if the MVCCValue.Value.RawBytes were returned
// directly.
//
// TODO(erikgrinaker): This could mid-stack inline when we compared
// v.MVCCValueHeader == enginepb.MVCCValueHeader{} instead of IsEmpty(), but
// struct comparisons have a significant performance regression in Go 1.19 which
// negates the inlining gain. Reconsider this with Go 1.20. See:
// https://github.com/cockroachdb/cockroach/issues/88818
func EncodeMVCCValueToBuf(v MVCCValue, buf []byte) ([]byte, bool, error) {
	if v.MVCCValueHeader.IsEmpty() && !disableSimpleValueEncoding {
		// Simple encoding. Use the roachpb.Value encoding directly with no
		// modification. No need to re-allocate or copy.
		return v.Value.RawBytes, false, nil
	}

	// NB: This code is duplicated in encodeExtendedMVCCValueToSizedBuf and
	// edits should be replicated there.

	// Extended encoding. Wrap the roachpb.Value encoding with a header containing
	// MVCC-level metadata. Requires a re-allocation and copy.
	headerLen := v.MVCCValueHeader.Size()
	headerSize := extendedPreludeSize + headerLen
	valueSize := headerSize + len(v.Value.RawBytes)

	if valueSize > cap(buf) {
		buf = make([]byte, valueSize)
	} else {
		buf = buf[:valueSize]
	}
	// Extended encoding. Wrap the roachpb.Value encoding with a header containing
	// MVCC-level metadata. Requires a copy.
	// 4-byte-header-len
	binary.BigEndian.PutUint32(buf, uint32(headerLen))
	// 1-byte-sentinel
	buf[tagPos] = extendedEncodingSentinel
	// mvcc-header
	//
	// NOTE: we don't use protoutil to avoid passing v.MVCCValueHeader through
	// an interface, which would cause a heap allocation and incur the cost of
	// dynamic dispatch.
	if _, err := v.MVCCValueHeader.MarshalToSizedBuffer(buf[extendedPreludeSize:headerSize]); err != nil {
		return nil, false, errors.Wrap(err, "marshaling MVCCValueHeader")
	}
	// <4-byte-checksum><1-byte-tag><encoded-data> or empty for tombstone
	copy(buf[headerSize:], v.Value.RawBytes)
	return buf, true, nil
}

func mvccValueSize(v MVCCValue) (size int, extendedEncoding bool) {
	if v.MVCCValueHeader.IsEmpty() && !disableSimpleValueEncoding {
		return len(v.Value.RawBytes), false
	}
	return extendedPreludeSize + v.MVCCValueHeader.Size() + len(v.Value.RawBytes), true
}

// encodeExtendedMVCCValueToSizedBuf encodes an MVCCValue into its encoded form
// in the provided buffer. The provided buf must be exactly sized, matching the
// value returned by MVCCValue.encodedMVCCValueSize.
//
// See EncodeMVCCValueToBuf for detailed comments on the encoding scheme.
func encodeExtendedMVCCValueToSizedBuf(v MVCCValue, buf []byte) error {
	if buildutil.CrdbTestBuild {
		if sz := encodedMVCCValueSize(v); sz != len(buf) {
			panic(errors.AssertionFailedf("provided buf (len=%d) is not sized correctly; expected %d", len(buf), sz))
		}
	}
	headerSize := len(buf) - len(v.Value.RawBytes)
	headerLen := headerSize - extendedPreludeSize
	binary.BigEndian.PutUint32(buf, uint32(headerLen))
	buf[tagPos] = extendedEncodingSentinel
	if _, err := v.MVCCValueHeader.MarshalToSizedBuffer(buf[extendedPreludeSize:headerSize]); err != nil {
		return errors.Wrap(err, "marshaling MVCCValueHeader")
	}
	if buildutil.CrdbTestBuild && len(buf[headerSize:]) != len(v.Value.RawBytes) {
		panic(errors.AssertionFailedf("insufficient space for raw value; expected %d, got %d", len(v.Value.RawBytes), len(buf[headerSize:])))
	}
	copy(buf[headerSize:], v.Value.RawBytes)
	return nil
}

// DecodeMVCCValue decodes an MVCCKey from its Pebble representation.
//
// NOTE: this function does not inline, so it is not suitable for performance
// critical code paths. Instead, callers that care about performance and would
// like to avoid function calls should manually call the two decoding functions.
// tryDecodeSimpleMVCCValue does inline, so callers can use it to avoid making
// any function calls when decoding an MVCCValue that is encoded with the simple
// encoding.
func DecodeMVCCValue(buf []byte) (MVCCValue, error) {
	v, ok, err := tryDecodeSimpleMVCCValue(buf)
	if ok || err != nil {
		return v, err
	}
	return decodeExtendedMVCCValue(buf, true)
}

// DecodeValueFromMVCCValue decodes and MVCCValue and returns the
// roachpb.Value portion without parsing the MVCCValueHeader.
//
// NB: Caller assumes that this function does not copy or re-allocate
// the underlying byte slice.
//
//gcassert:inline
func DecodeValueFromMVCCValue(buf []byte) (roachpb.Value, error) {
	if len(buf) == 0 {
		// Tombstone with no header.
		return roachpb.Value{}, nil
	}
	if len(buf) <= tagPos {
		return roachpb.Value{}, errMVCCValueMissingTag
	}
	if buf[tagPos] != extendedEncodingSentinel {
		return roachpb.Value{RawBytes: buf}, nil
	}

	// Extended encoding
	headerLen := binary.BigEndian.Uint32(buf)
	headerSize := extendedPreludeSize + headerLen
	if len(buf) < int(headerSize) {
		return roachpb.Value{}, errMVCCValueMissingHeader
	}
	return roachpb.Value{RawBytes: buf[headerSize:]}, nil
}

// DecodeMVCCValueAndErr is a helper that can be called using the ([]byte,
// error) pair returned from the iterator UnsafeValue(), Value() methods.
func DecodeMVCCValueAndErr(buf []byte, err error) (MVCCValue, error) {
	if err != nil {
		return MVCCValue{}, err
	}
	return DecodeMVCCValue(buf)
}

// Static error definitions, to permit inlining.
var errMVCCValueMissingTag = errors.Errorf("invalid encoded mvcc value, missing tag")
var errMVCCValueMissingHeader = errors.Errorf("invalid encoded mvcc value, missing header")

// tryDecodeSimpleMVCCValue attempts to decode an MVCCValue that is using the
// simple encoding. If successful, returns the decoded value and true. If the
// value was using the extended encoding, returns false, in which case the
// caller should call decodeExtendedMVCCValue.
//
//gcassert:inline
func tryDecodeSimpleMVCCValue(buf []byte) (MVCCValue, bool, error) {
	if len(buf) == 0 {
		// Tombstone with no header.
		return MVCCValue{}, true, nil
	}
	if len(buf) <= tagPos {
		return MVCCValue{}, false, errMVCCValueMissingTag
	}
	if buf[tagPos] != extendedEncodingSentinel {
		// Simple encoding. The encoding is equivalent to the roachpb.Value
		// encoding, so inflate it directly. No need to copy or slice.
		return MVCCValue{Value: roachpb.Value{RawBytes: buf}}, true, nil
	}
	// Extended encoding. The caller should call decodeExtendedMVCCValue.
	return MVCCValue{}, false, nil
}

//gcassert:inline
func decodeMVCCValueIgnoringHeader(buf []byte) (MVCCValue, error) {
	if len(buf) == 0 {
		return MVCCValue{}, nil
	}
	if len(buf) <= tagPos {
		return MVCCValue{}, errMVCCValueMissingTag
	}
	if buf[tagPos] != extendedEncodingSentinel {
		return MVCCValue{Value: roachpb.Value{RawBytes: buf}}, nil
	}

	// Extended encoding
	headerLen := binary.BigEndian.Uint32(buf)
	headerSize := extendedPreludeSize + headerLen
	if len(buf) < int(headerSize) {
		return MVCCValue{}, errMVCCValueMissingHeader
	}
	return MVCCValue{Value: roachpb.Value{RawBytes: buf[headerSize:]}}, nil
}

func decodeExtendedMVCCValue(buf []byte, unmarshalHeader bool) (MVCCValue, error) {
	headerLen := binary.BigEndian.Uint32(buf)
	headerSize := extendedPreludeSize + headerLen
	if len(buf) < int(headerSize) {
		return MVCCValue{}, errMVCCValueMissingHeader
	}
	var v MVCCValue
	if unmarshalHeader {
		// NOTE: we don't use protoutil to avoid passing header through an interface,
		// which would cause a heap allocation and incur the cost of dynamic dispatch.
		if err := v.MVCCValueHeader.Unmarshal(buf[extendedPreludeSize:headerSize]); err != nil {
			return MVCCValue{}, errors.Wrapf(err, "unmarshaling MVCCValueHeader")
		}
	}
	v.Value.RawBytes = buf[headerSize:]
	return v, nil
}

// EncodedMVCCValueIsTombstone is faster than decoding a MVCCValue and then
// calling MVCCValue.IsTombstone. It should be used when the caller does not
// need a decoded value.
//
//gcassert:inline
func EncodedMVCCValueIsTombstone(buf []byte) (bool, error) {
	if len(buf) == 0 {
		return true, nil
	}
	if len(buf) <= tagPos {
		return false, errMVCCValueMissingTag
	}
	if buf[tagPos] != extendedEncodingSentinel {
		return false, nil
	}
	headerSize := extendedPreludeSize + binary.BigEndian.Uint32(buf)
	if len(buf) < int(headerSize) {
		return false, errMVCCValueMissingHeader
	}
	return len(buf) == int(headerSize), nil
}

func init() {
	// Inject the format dependency into the enginepb package.
	enginepb.FormatBytesAsValue = func(v []byte) redact.RedactableString {
		val, err := DecodeMVCCValue(v)
		if err != nil {
			return redact.Sprintf("err=%v", err)
		}
		return redact.Sprint(val)
	}
}
