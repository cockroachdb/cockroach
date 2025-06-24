// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package system

import "encoding/binary"

// BigEndian is true if we're running on a system with big endian architecture
// like s390x (most architectures we're running on are little endian).
var BigEndian = binary.NativeEndian.Uint16([]byte{0x12, 0x34}) == uint16(0x1234)
