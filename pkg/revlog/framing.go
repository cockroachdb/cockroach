// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/cockroachdb/errors"
)

// crc32cTable is the Castagnoli polynomial table used by the
// framing checksum.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// FramingMagic is the constant XOR'd into the on-disk framing
// prefix. Its bytes spell ASCII 'c','r','d','b' (0x63, 0x72, 0x64,
// 0x62) read as a little-endian uint32. Two purposes (see
// revlog-format.md §3):
//
//   - A 4-byte all-zero file is not a valid framing — the prefix
//     would have to equal MAGIC — so accidental zero / null
//     uploads are unambiguously corruption rather than a
//     legitimately empty (tombstone) payload, which has prefix ==
//     MAGIC.
//   - The constant doubles as a format-version sentinel: bumping
//     it breaks old readers loudly.
const FramingMagic uint32 = 0x62647263

// FramingPrefixLen is the byte length of the framing prefix that
// every framed object on disk carries.
const FramingPrefixLen = 4

// EncodeFramed wraps payload in the on-disk framing layout: a
// 4-byte little-endian uint32 of CRC32C(payload) XOR FramingMagic,
// followed by payload itself. Callers PUT the returned bytes
// verbatim to external storage.
//
// A nil/empty payload is valid — it produces a 4-byte object whose
// content equals FramingMagic, the format's tombstone shape (see
// §3 of revlog-format.md).
func EncodeFramed(payload []byte) []byte {
	out := make([]byte, FramingPrefixLen+len(payload))
	binary.LittleEndian.PutUint32(out[:FramingPrefixLen],
		crc32.Checksum(payload, crc32cTable)^FramingMagic)
	copy(out[FramingPrefixLen:], payload)
	return out
}

// DecodeFramed inverts EncodeFramed: it verifies the prefix
// (XORing back FramingMagic and comparing CRC32C against the
// payload), and returns the payload slice. The returned slice
// aliases buf — callers that want to retain it past buf's
// lifetime should copy.
//
// Returns an error if buf is shorter than the prefix or if the
// recovered CRC doesn't match — both of which indicate a
// corrupted, truncated, or wrongly-formatted (e.g. all-zero) file.
func DecodeFramed(buf []byte) ([]byte, error) {
	if len(buf) < FramingPrefixLen {
		return nil, errors.Errorf(
			"revlog: framed object too short: %d bytes (need at least %d)",
			len(buf), FramingPrefixLen)
	}
	stored := binary.LittleEndian.Uint32(buf[:FramingPrefixLen])
	want := stored ^ FramingMagic
	payload := buf[FramingPrefixLen:]
	if got := crc32.Checksum(payload, crc32cTable); got != want {
		return nil, errors.Errorf(
			"revlog: framed object CRC32C mismatch: want %x, got %x", want, got)
	}
	return payload, nil
}
