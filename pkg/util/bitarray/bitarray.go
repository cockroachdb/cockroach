// Copyright 2018 The Cockroach Authors.
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

package bitarray

import (
	"bytes"
	"math/big"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// BitArray implements a bit string of arbitrary length.
type BitArray struct {
	// Bits is the backing array of bits.
	// We really would prefer an unsigned bigint type here
	// but go only provides a signed one. Oh well.
	Bits big.Int
	// BitLen is the specified number of bits.
	// This is the specified number of bits when input from a string,
	// including high-order zero bits. It can be zero to indicate
	// an empty array.
	BitLen uint
}

// String implements the fmt.Stringer interface.
func (d *BitArray) String() string {
	var buf bytes.Buffer
	d.Format(&buf)
	return buf.String()
}

// Clone makes a copy of the bit array.
func (d *BitArray) Clone() BitArray {
	a := BitArray{BitLen: d.BitLen}
	a.Bits.SetBits(append([]big.Word(nil), d.Bits.Bits()...))
	return a
}

// Format prints out the bit array to the buffer.
func (d *BitArray) Format(buf *bytes.Buffer) {
	if d.BitLen > 0 {
		s := d.Bits.Text(2)
		for i := 0; i < int(d.BitLen)-len(s); i++ {
			// Left pad with zeros.
			buf.WriteByte('0')
		}
		buf.WriteString(s)
	}
}

var bigIntZero = big.NewInt(0)
var bigIntOne = big.NewInt(1)

// Inc increments the bit array by one, and bumps
// its bit length if necessary.
func (d *BitArray) Inc() {
	d.Bits.Add(&d.Bits, bigIntOne)
	if uint(d.Bits.BitLen()) > d.BitLen {
		d.BitLen++
	}
}

// LeftShift sets d to the result of v << n if n is positive,
// or v >> n if n is negative.
// As per PostgreSQL arithmetic, the result is truncated
// to the BitLen.
func (d *BitArray) LeftShift(v *BitArray, n int64) {
	if n == 0 {
		// Nothing to do.
		return
	}
	if n < 0 {
		// Right shift.
		n = -n
		if n >= int64(d.BitLen) {
			// Underflow to zero. Nothing to do further.
			d.Bits = *bigIntZero
			return
		}
		d.Bits.Rsh(&v.Bits, uint(n))
		return
	}
	// Left shift. We can't just shift blindly; PostgreSQL requires
	// us to truncate to the width.
	if n >= int64(d.BitLen) {
		// Overflow to zero. Fill with zeros and take the shortcut.
		d.Bits = *bigIntZero
		return
	}
	d.Bits.Lsh(&v.Bits, uint(n))
	d.SetWidth(d.BitLen)
}

// Compare compares the two bit arrays.
func (d *BitArray) Compare(v *BitArray) int {
	cmp := d.Bits.CmpAbs(&v.Bits)
	if cmp != 0 {
		return cmp
	}
	// Same bits, but perhaps different lengths: use the BitLen to
	// compare.
	if d.BitLen < v.BitLen {
		return -1
	}
	if d.BitLen > v.BitLen {
		return 1
	}
	return 0
}

// SetWidth enforces the specified width. The result is truncated to
// that number of bits.
func (d *BitArray) SetWidth(n uint) {
	// Truncate the overflow bits, if any.
	// Unfortunately the big package does not offer us this
	// functionality.
	// So we use the formula x & ((1 << n) - 1)
	// to truncate to n bits instead.
	if uint(d.Bits.BitLen()) > n {
		var tmp big.Int
		tmp.Lsh(bigIntOne, n)
		tmp.Sub(&tmp, bigIntOne)
		d.Bits.And(&d.Bits, &tmp)
	}
	d.BitLen = n
}

var errInvalidBinaryDigit = pgerror.NewErrorf(pgerror.CodeSyntaxError,
	"could not parse string as bit array: invalid binary digit")

// Parse parses a string representation of binary digits.
func (d *BitArray) Parse(s string) error {
	d.BitLen = uint(len(s))
	if len(s) == 0 {
		d.Bits = *bigIntZero
		return nil
	}
	_, ok := d.Bits.SetString(s, 2)
	if !ok {
		return errInvalidBinaryDigit
	}
	return nil
}
