// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecodeFramed_RoundTrip checks that DecodeFramed
// recovers the original payload across a range of sizes.
// Nil/empty inputs round-trip via len-equality (DecodeFramed
// returns a non-nil zero-length slice for the tombstone shape).
func TestEncodeDecodeFramed_RoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, payload := range [][]byte{
		nil,
		{},
		[]byte("hi"),
		[]byte("a longer payload with mixed bytes \x00\xff\x7f"),
		make([]byte, 4096),
	} {
		got, err := DecodeFramed(EncodeFramed(payload))
		require.NoError(t, err)
		require.Equal(t, len(payload), len(got))
		require.True(t, bytes.Equal(payload, got))
	}
}

// TestEncodeFramed_TombstoneShape locks down the §3-tombstone
// invariant: a nil/empty payload encodes to exactly 4 bytes whose
// little-endian uint32 equals FramingMagic.
func TestEncodeFramed_TombstoneShape(t *testing.T) {
	defer leaktest.AfterTest(t)()
	enc := EncodeFramed(nil)
	require.Len(t, enc, FramingPrefixLen)
	require.Equal(t, FramingMagic, binary.LittleEndian.Uint32(enc))
}

// TestDecodeFramed_AllZerosRejected is the headline reason for
// the XOR'd magic: a 4-byte all-zero file is unambiguously
// corruption / null upload, never a valid framing — its prefix
// would have to equal FramingMagic to decode.
func TestDecodeFramed_AllZerosRejected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, err := DecodeFramed(make([]byte, FramingPrefixLen))
	require.Error(t, err)
	require.Contains(t, err.Error(), "CRC32C mismatch")
}

// TestDecodeFramed_TooShort: anything shorter than the prefix
// can't be a valid framed object.
func TestDecodeFramed_TooShort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, err := DecodeFramed([]byte{0x01, 0x02, 0x03})
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")
}

// TestDecodeFramed_CorruptedPayload catches a bit-flip in the
// payload after framing.
func TestDecodeFramed_CorruptedPayload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	enc := EncodeFramed([]byte("hello"))
	enc[FramingPrefixLen+1] ^= 0x01 // flip a bit in the payload
	_, err := DecodeFramed(enc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CRC32C mismatch")
}
