// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uint128

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/cockroachdb/errors"
)

// Uint128 is a big-endian 128 bit unsigned integer which wraps two uint64s.
type Uint128 struct {
	Hi, Lo uint64
}

// GetBytes returns a big-endian byte representation.
func (u Uint128) GetBytes() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], u.Hi)
	binary.BigEndian.PutUint64(buf[8:], u.Lo)
	return buf
}

// AppendBytes appends big-endian byte representation to the
// buffer and returns the buffer.
func (u Uint128) AppendBytes(buf []byte) []byte {
	buf = binary.BigEndian.AppendUint64(buf, u.Hi)
	return binary.BigEndian.AppendUint64(buf, u.Lo)
}

// String returns a hexadecimal string representation.
func (u Uint128) String() string {
	return hex.EncodeToString(u.GetBytes())
}

// Equal returns whether or not the Uint128 are equivalent.
func (u Uint128) Equal(o Uint128) bool {
	return u.Hi == o.Hi && u.Lo == o.Lo
}

// Compare compares the two Uint128.
func (u Uint128) Compare(o Uint128) int {
	if u.Hi > o.Hi {
		return 1
	} else if u.Hi < o.Hi {
		return -1
	} else if u.Lo > o.Lo {
		return 1
	} else if u.Lo < o.Lo {
		return -1
	}
	return 0
}

// Add returns a new Uint128 incremented by n.
func (u Uint128) Add(n uint64) Uint128 {
	lo := u.Lo + n
	hi := u.Hi
	if u.Lo > lo {
		hi++
	}
	return Uint128{hi, lo}
}

// Sub returns a new Uint128 decremented by n.
func (u Uint128) Sub(n uint64) Uint128 {
	lo := u.Lo - n
	hi := u.Hi
	if u.Lo < lo {
		hi--
	}
	return Uint128{hi, lo}
}

// And returns a new Uint128 that is the bitwise AND of two Uint128 values.
func (u Uint128) And(o Uint128) Uint128 {
	return Uint128{u.Hi & o.Hi, u.Lo & o.Lo}
}

// Or returns a new Uint128 that is the bitwise OR of two Uint128 values.
func (u Uint128) Or(o Uint128) Uint128 {
	return Uint128{u.Hi | o.Hi, u.Lo | o.Lo}
}

// Xor returns a new Uint128 that is the bitwise XOR of two Uint128 values.
func (u Uint128) Xor(o Uint128) Uint128 {
	return Uint128{u.Hi ^ o.Hi, u.Lo ^ o.Lo}
}

// FromBytes parses the byte slice as a 128 bit big-endian unsigned integer.
// The caller is responsible for ensuring the byte slice contains 16 bytes.
func FromBytes(b []byte) Uint128 {
	hi := binary.BigEndian.Uint64(b[:8])
	lo := binary.BigEndian.Uint64(b[8:])
	return Uint128{hi, lo}
}

// Size returns the marshalled byte length, satisfying the gogoproto
// custom type interface so Uint128 can be used as the Go type for a
// proto bytes field.
func (u Uint128) Size() int { return 16 }

// MarshalTo writes u as 16 big-endian bytes into data, satisfying the
// gogoproto custom type interface.
func (u Uint128) MarshalTo(data []byte) (int, error) {
	binary.BigEndian.PutUint64(data[:8], u.Hi)
	binary.BigEndian.PutUint64(data[8:16], u.Lo)
	return 16, nil
}

// Unmarshal reads u from a 16-byte big-endian buffer, satisfying the
// gogoproto custom type interface.
func (u *Uint128) Unmarshal(data []byte) error {
	if len(data) != 16 {
		return errors.Errorf("input data length %d for uint128 must be 16 bytes", len(data))
	}
	u.Hi = binary.BigEndian.Uint64(data[:8])
	u.Lo = binary.BigEndian.Uint64(data[8:16])
	return nil
}

// FromString parses a hexadecimal string as a 128-bit big-endian unsigned integer.
func FromString(s string) (Uint128, error) {
	if len(s) > 32 {
		return Uint128{}, errors.Errorf("input string %s too large for uint128", s)
	}
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return Uint128{}, errors.Wrapf(err, "could not decode %s as hex", s)
	}

	// Grow the byte slice if it's smaller than 16 bytes, by prepending 0s
	if len(bytes) < 16 {
		bytesCopy := make([]byte, 16)
		copy(bytesCopy[(16-len(bytes)):], bytes)
		bytes = bytesCopy
	}

	return FromBytes(bytes), nil
}

// FromInts takes in two unsigned 64-bit integers and constructs a Uint128.
func FromInts(hi uint64, lo uint64) Uint128 {
	return Uint128{hi, lo}
}
