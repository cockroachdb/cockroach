// Copyright 2022 The Cockroach Authors.
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

package apd

import (
	"fmt"
	"math/big"
	"math/bits"
	"math/rand"
	"strconv"
	"unsafe"
)

// The inlineWords capacity is set to accommodate any value that would fit in a
// 128-bit integer (i.e. values with an absolute value up to 2^128 - 1).
const inlineWords = 128 / bits.UintSize

// BigInt is a wrapper around big.Int. It minimizes memory allocation by using
// an inline array to back the big.Int's variable-length "nat" slice when the
// integer's value is sufficiently small.
// The zero value is ready to use.
type BigInt struct {
	// A wrapped big.Int. Only set to the BigInt's value when the value exceeds
	// what is representable in the _inline array.
	//
	// When the BigInt's value is still small enough to use the _inline array,
	// this field doubles as integer's negative flag. See negSentinel.
	//
	// Methods should access this field through inner.
	_inner *big.Int

	// The inlined backing array use for short-lived, stack-allocated big.Int
	// structs during arithmetic when the value is small.
	//
	// Each BigInt maintains (through big.Int) an internal reference to a
	// variable-length integer value, which is represented by a []big.Word. The
	// _inline field and the inner and updateInner methods combine to allow
	// BigInt to inline this variable-length integer array within the BigInt
	// struct when its value is sufficiently small. In the inner method, we
	// point a temporary big.Int's nat slice at this _inline array. big.Int will
	// avoid re-allocating this array until it is provided with a value that
	// exceeds the initial capacity. Later in updateInner, we detect whether the
	// array has been re-allocated. If so, we switch to using the _inner. If
	// not, we continue to use this array.
	_inline [inlineWords]big.Word
}

// NewBigInt allocates and returns a new BigInt set to x.
//
// NOTE: BigInt jumps through hoops to avoid escaping to the heap. As such, most
// users of BigInt should not need this function. They should instead declare a
// zero-valued BigInt directly on the stack and interact with references to this
// stack-allocated value. Recall that the zero-valued BigInt is ready to use.
func NewBigInt(x int64) *BigInt {
	return new(BigInt).SetInt64(x)
}

// Set as the value of BigInt._inner as a "sentinel" flag to indicate that a
// BigInt is negative ((big.Int).Sign() < 0) but the absolute value is still
// small enough to represent in the _inline array.
var negSentinel = new(big.Int)

// isInline returns whether the BigInt stores its value in its _inline array.
//gcassert:inline
func (z *BigInt) isInline() bool {
	return z._inner == nil || z._inner == negSentinel
}

// The memory representation of big.Int. Used for unsafe modification below.
type intStruct struct {
	neg bool
	abs []big.Word
}

// noescape hides a pointer from escape analysis. noescape is the identity
// function but escape analysis doesn't think the output depends on the input.
// noescape is inlined and currently compiles down to zero instructions.
//
// USE CAREFULLY!
//
// This was copied from strings.Builder, which has identical code which was
// itself copied from the runtime.
// For more, see issues #23382 and #7921 in github.com/golang/go.
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	//lint:ignore SA4016 intentional no-op to hide pointer from escape analysis.
	return unsafe.Pointer(x ^ 0)
}

// inner returns the BigInt's current value as a *big.Int.
//
// NOTE: this was carefully written to permit function inlining. Modify with
// care.
//gcassert:inline
func (z *BigInt) inner(tmp *big.Int) *big.Int {
	// Point the big.Int at the inline array. When doing so, use noescape to
	// avoid forcing the BigInt to escape to the heap. Go's escape analysis
	// struggles with self-referential pointers, and it can't prove that we
	// only assign _inner to a heap-allocated object (which must not contain
	// pointers that reference the stack or the GC explodes) if the big.Int's
	// backing array has been re-allocated onto the heap first.
	//
	// NOTE: SetBits sets the neg field to false, so this must come before the
	// negSentinel handling.
	tmp.SetBits((*[inlineWords]big.Word)(noescape(unsafe.Pointer(&z._inline[0])))[:])

	if z._inner != nil {
		if z._inner != negSentinel {
			// The variable-length big.Int reference is set.
			return z._inner
		}

		// This is the negative sentinel, which indicates that the integer is
		// negative but still stored inline. Update the big.Int accordingly. We
		// use unsafe because (*big.Int).Neg is too complex and prevents this
		// method from being inlined.
		(*intStruct)(unsafe.Pointer(tmp)).neg = true
	}
	return tmp
}

// innerOrNil is like inner, but returns a nil *big.Int if the receiver is nil.
// NOTE: this is not inlined.
func (z *BigInt) innerOrNil(tmp *big.Int) *big.Int {
	if z == nil {
		return nil
	}
	return z.inner(tmp)
}

// innerOrAlias is like inner, but returns the provided *big.Int if the receiver
// and the other *BigInt argument reference the same object.
// NOTE: this is not inlined.
func (z *BigInt) innerOrAlias(tmp *big.Int, a *BigInt, ai *big.Int) *big.Int {
	if a == z {
		return ai
	}
	return z.inner(tmp)
}

// innerOrNilOrAlias is like inner, but with the added semantics specified for
// both innerOrNil and innerOrAlias.
// NOTE: this is not inlined.
func (z *BigInt) innerOrNilOrAlias(tmp *big.Int, a *BigInt, ai *big.Int) *big.Int {
	if z == nil {
		return nil
	} else if z == a {
		return ai
	}
	return z.inner(tmp)
}

// updateInner updates the BigInt's current value with the provided *big.Int.
//
// NOTE: this was carefully written to permit function inlining. Modify with
// care.
//gcassert:inline
func (z *BigInt) updateInner(src *big.Int) {
	if z._inner == src {
		return
	}

	bits := src.Bits()
	bitsLen := len(bits)
	if bitsLen > 0 && &z._inline[0] != &bits[0] {
		// The big.Int re-allocated its backing array during arithmetic because
		// the value grew beyond what could fit in the _inline array. Switch to
		// a heap-allocated, variable-length big.Int and store that in _inner.
		// From now on, all arithmetic will use this big.Int directly.
		//
		// Allocate a new big.Int and perform a shallow-copy of the argument to
		// prevent it from escaping off the stack.
		z._inner = new(big.Int)
		*z._inner = *src
	} else {
		// Zero out all words beyond the end of the big.Int's current Word
		// slice. big.Int arithmetic can sometimes leave these words "dirty".
		// They would cause issues when the _inline array is injected into the
		// next big.Int if not cleared.
		for bitsLen < len(z._inline) {
			z._inline[bitsLen] = 0
			bitsLen++
		}

		// Set or unset the negative sentinel, according to the argument's sign.
		// We use unsafe because (*big.Int).Sign is too complex and prevents
		// this method from being inlined.
		if (*intStruct)(unsafe.Pointer(src)).neg {
			z._inner = negSentinel
		} else {
			z._inner = nil
		}
	}
}

const wordsInUint64 = 64 / bits.UintSize

func init() {
	if inlineWords < wordsInUint64 {
		panic("inline array must be at least 64 bits large")
	}
}

// innerAsUint64 returns the BigInt's current absolute value as a uint64 and a
// flag indicating whether the value is negative. If the value is not stored
// inline or if it can not fit in a uint64, false is returned.
//
// NOTE: this was carefully written to permit function inlining. Modify with
// care.
//gcassert:inline
func (z *BigInt) innerAsUint64() (val uint64, neg bool, ok bool) {
	if !z.isInline() {
		// The value is not stored inline.
		return 0, false, false
	}
	if wordsInUint64 == 1 && inlineWords == 2 {
		// Manually unrolled loop for current inlineWords setting.
		if z._inline[1] != 0 {
			// The value can not fit in a uint64.
			return 0, false, false
		}
	} else {
		// Fallback for other values of inlineWords.
		for i := wordsInUint64; i < len(z._inline); i++ {
			if z._inline[i] != 0 {
				// The value can not fit in a uint64.
				return 0, false, false
			}
		}
	}

	val = uint64(z._inline[0])
	if wordsInUint64 == 2 {
		// From big.low64.
		val = uint64(z._inline[1])<<32 | val
	}
	neg = z._inner == negSentinel
	return val, neg, true
}

// updateInnerFromUint64 updates the BigInt's current value with the provided
// absolute value and sign.
//
// NOTE: this was carefully written to permit function inlining. Modify with
// care.
//gcassert:inline
func (z *BigInt) updateInnerFromUint64(val uint64, neg bool) {
	// Set the inline value.
	z._inline[0] = big.Word(val)
	if wordsInUint64 == 2 {
		// From (big.nat).setUint64.
		z._inline[1] = big.Word(val >> 32)
	}

	// Clear out all other words in the inline array.
	if wordsInUint64 == 1 && inlineWords == 2 {
		// Manually unrolled loop for current inlineWords setting.
		z._inline[1] = 0
	} else {
		// Fallback for other values of inlineWords.
		for i := wordsInUint64; i < len(z._inline); i++ {
			z._inline[i] = 0
		}
	}

	// Set or unset the negative sentinel.
	if neg {
		z._inner = negSentinel
	} else {
		z._inner = nil
	}
}

const (
	bigIntSize     = unsafe.Sizeof(BigInt{})
	mathBigIntSize = unsafe.Sizeof(big.Int{})
	mathWordSize   = unsafe.Sizeof(big.Word(0))
)

// Size returns the total memory footprint of z in bytes.
func (z *BigInt) Size() uintptr {
	if z.isInline() {
		return bigIntSize
	}
	return bigIntSize + mathBigIntSize + uintptr(cap(z._inner.Bits()))*mathWordSize
}

///////////////////////////////////////////////////////////////////////////////
//                    inline arithmetic for small values                     //
///////////////////////////////////////////////////////////////////////////////

//gcassert:inline
func addInline(xVal, yVal uint64, xNeg, yNeg bool) (zVal uint64, zNeg, ok bool) {
	if xNeg == yNeg {
		sum, carry := bits.Add64(xVal, yVal, 0)
		overflow := carry != 0
		return sum, xNeg, !overflow
	}

	diff, borrow := bits.Sub64(xVal, yVal, 0)
	if borrow != 0 { // underflow
		xNeg = !xNeg
		diff = yVal - xVal
	}
	if diff == 0 {
		xNeg = false
	}
	return diff, xNeg, true
}

//gcassert:inline
func mulInline(xVal, yVal uint64, xNeg, yNeg bool) (zVal uint64, zNeg, ok bool) {
	hi, lo := bits.Mul64(xVal, yVal)
	neg := xNeg != yNeg
	overflow := hi != 0
	return lo, neg, !overflow
}

//gcassert:inline
func quoInline(xVal, yVal uint64, xNeg, yNeg bool) (quoVal uint64, quoNeg, ok bool) {
	if yVal == 0 { // divide by 0
		return 0, false, false
	}
	quo := xVal / yVal
	neg := xNeg != yNeg
	return quo, neg, true
}

//gcassert:inline
func remInline(xVal, yVal uint64, xNeg, yNeg bool) (remVal uint64, remNeg, ok bool) {
	if yVal == 0 { // divide by 0
		return 0, false, false
	}
	rem := xVal % yVal
	return rem, xNeg, true
}

///////////////////////////////////////////////////////////////////////////////
//                        big.Int API wrapper methods                        //
///////////////////////////////////////////////////////////////////////////////

// Abs calls (big.Int).Abs.
func (z *BigInt) Abs(x *BigInt) *BigInt {
	if x.isInline() {
		z._inline = x._inline
		z._inner = nil // !negSentinel
		return z
	}
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Abs(x.inner(&tmp2))
	z.updateInner(zi)
	return z
}

// Add calls (big.Int).Add.
func (z *BigInt) Add(x, y *BigInt) *BigInt {
	if xVal, xNeg, ok := x.innerAsUint64(); ok {
		if yVal, yNeg, ok := y.innerAsUint64(); ok {
			if zVal, zNeg, ok := addInline(xVal, yVal, xNeg, yNeg); ok {
				z.updateInnerFromUint64(zVal, zNeg)
				return z
			}
		}
	}
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Add(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// And calls (big.Int).And.
func (z *BigInt) And(x, y *BigInt) *BigInt {
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.And(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// AndNot calls (big.Int).AndNot.
func (z *BigInt) AndNot(x, y *BigInt) *BigInt {
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.AndNot(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// Append calls (big.Int).Append.
func (z *BigInt) Append(buf []byte, base int) []byte {
	if z == nil {
		// Fast-path that avoids innerOrNil, allowing inner to be inlined.
		return append(buf, "<nil>"...)
	}
	if zVal, zNeg, ok := z.innerAsUint64(); ok {
		// Check if the base is supported by strconv.AppendUint.
		if base >= 2 && base <= 36 {
			if zNeg {
				buf = append(buf, '-')
			}
			return strconv.AppendUint(buf, zVal, base)
		}
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).Append(buf, base)
}

// Binomial calls (big.Int).Binomial.
func (z *BigInt) Binomial(n, k int64) *BigInt {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Binomial(n, k)
	z.updateInner(zi)
	return z
}

// Bit calls (big.Int).Bit.
func (z *BigInt) Bit(i int) uint {
	if i == 0 && z.isInline() {
		// Optimization for common case: odd/even test of z.
		return uint(z._inline[0] & 1)
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).Bit(i)
}

// BitLen calls (big.Int).BitLen.
func (z *BigInt) BitLen() int {
	if z.isInline() {
		// Find largest non-zero inline word.
		for i := len(z._inline) - 1; i >= 0; i-- {
			if z._inline[i] != 0 {
				return i*bits.UintSize + bits.Len(uint(z._inline[i]))
			}
		}
		return 0
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).BitLen()
}

// Bits calls (big.Int).Bits.
func (z *BigInt) Bits() []big.Word {
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).Bits()
}

// Bytes calls (big.Int).Bytes.
func (z *BigInt) Bytes() []byte {
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).Bytes()
}

// Cmp calls (big.Int).Cmp.
func (z *BigInt) Cmp(y *BigInt) (r int) {
	if zVal, zNeg, ok := z.innerAsUint64(); ok {
		if yVal, yNeg, ok := y.innerAsUint64(); ok {
			switch {
			case zNeg == yNeg:
				switch {
				case zVal < yVal:
					r = -1
				case zVal > yVal:
					r = 1
				}
				if zNeg {
					r = -r
				}
			case zNeg:
				r = -1
			default:
				r = 1
			}
			return r
		}
	}
	var tmp1, tmp2 big.Int //gcassert:noescape
	return z.inner(&tmp1).Cmp(y.inner(&tmp2))
}

// CmpAbs calls (big.Int).CmpAbs.
func (z *BigInt) CmpAbs(y *BigInt) (r int) {
	if zVal, _, ok := z.innerAsUint64(); ok {
		if yVal, _, ok := y.innerAsUint64(); ok {
			switch {
			case zVal < yVal:
				r = -1
			case zVal > yVal:
				r = 1
			}
			return r
		}
	}
	var tmp1, tmp2 big.Int //gcassert:noescape
	return z.inner(&tmp1).CmpAbs(y.inner(&tmp2))
}

// Div calls (big.Int).Div.
func (z *BigInt) Div(x, y *BigInt) *BigInt {
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Div(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// DivMod calls (big.Int).DivMod.
func (z *BigInt) DivMod(x, y, m *BigInt) (*BigInt, *BigInt) {
	var tmp1, tmp2, tmp3, tmp4 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	mi := m.inner(&tmp2)
	// NOTE: innerOrAlias for the y param because (big.Int).DivMod needs to
	// detect when y is aliased to the receiver.
	zi.DivMod(x.inner(&tmp3), y.innerOrAlias(&tmp4, z, zi), mi)
	z.updateInner(zi)
	m.updateInner(mi)
	return z, m
}

// Exp calls (big.Int).Exp.
func (z *BigInt) Exp(x, y, m *BigInt) *BigInt {
	var tmp1, tmp2, tmp3, tmp4 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	if zi.Exp(x.inner(&tmp2), y.inner(&tmp3), m.innerOrNil(&tmp4)) == nil {
		return nil
	}
	z.updateInner(zi)
	return z
}

// Format calls (big.Int).Format.
func (z *BigInt) Format(s fmt.State, ch rune) {
	var tmp1 big.Int //gcassert:noescape
	z.innerOrNil(&tmp1).Format(s, ch)
}

// GCD calls (big.Int).GCD.
func (z *BigInt) GCD(x, y, a, b *BigInt) *BigInt {
	var tmp1, tmp2, tmp3, tmp4, tmp5 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	ai := a.inner(&tmp2)
	bi := b.inner(&tmp3)
	xi := x.innerOrNil(&tmp4)
	// NOTE: innerOrNilOrAlias for the y param because (big.Int).GCD needs to
	// detect when y is aliased to b. See "avoid aliasing b" in lehmerGCD.
	yi := y.innerOrNilOrAlias(&tmp5, b, bi)
	zi.GCD(xi, yi, ai, bi)
	z.updateInner(zi)
	if xi != nil {
		x.updateInner(xi)
	}
	if yi != nil {
		y.updateInner(yi)
	}
	return z
}

// GobEncode calls (big.Int).GobEncode.
func (z *BigInt) GobEncode() ([]byte, error) {
	var tmp1 big.Int //gcassert:noescape
	return z.innerOrNil(&tmp1).GobEncode()
}

// GobDecode calls (big.Int).GobDecode.
func (z *BigInt) GobDecode(buf []byte) error {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	if err := zi.GobDecode(buf); err != nil {
		return err
	}
	z.updateInner(zi)
	return nil
}

// Int64 calls (big.Int).Int64.
func (z *BigInt) Int64() int64 {
	if zVal, zNeg, ok := z.innerAsUint64(); ok {
		// The unchecked cast from uint64 to int64 looks unsafe, but it is
		// allowed and is identical to the logic in (big.Int).Int64. Per the
		// method's contract:
		// > If z cannot be represented in an int64, the result is undefined.
		zi := int64(zVal)
		if zNeg {
			zi = -zi
		}
		return zi
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).Int64()
}

// IsInt64 calls (big.Int).IsInt64.
func (z *BigInt) IsInt64() bool {
	if zVal, zNeg, ok := z.innerAsUint64(); ok {
		// From (big.Int).IsInt64.
		zi := int64(zVal)
		return zi >= 0 || zNeg && zi == -zi
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).IsInt64()
}

// IsUint64 calls (big.Int).IsUint64.
func (z *BigInt) IsUint64() bool {
	if _, zNeg, ok := z.innerAsUint64(); ok {
		return !zNeg
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).IsUint64()
}

// Lsh calls (big.Int).Lsh.
func (z *BigInt) Lsh(x *BigInt, n uint) *BigInt {
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Lsh(x.inner(&tmp2), n)
	z.updateInner(zi)
	return z
}

// MarshalJSON calls (big.Int).MarshalJSON.
func (z *BigInt) MarshalJSON() ([]byte, error) {
	var tmp1 big.Int //gcassert:noescape
	return z.innerOrNil(&tmp1).MarshalJSON()
}

// MarshalText calls (big.Int).MarshalText.
func (z *BigInt) MarshalText() (text []byte, err error) {
	var tmp1 big.Int //gcassert:noescape
	return z.innerOrNil(&tmp1).MarshalText()
}

// Mod calls (big.Int).Mod.
func (z *BigInt) Mod(x, y *BigInt) *BigInt {
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	// NOTE: innerOrAlias for the y param because (big.Int).Mod needs to detect
	// when y is aliased to the receiver.
	zi.Mod(x.inner(&tmp2), y.innerOrAlias(&tmp3, z, zi))
	z.updateInner(zi)
	return z
}

// ModInverse calls (big.Int).ModInverse.
func (z *BigInt) ModInverse(g, n *BigInt) *BigInt {
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	if zi.ModInverse(g.inner(&tmp2), n.inner(&tmp3)) == nil {
		return nil
	}
	z.updateInner(zi)
	return z
}

// ModSqrt calls (big.Int).ModSqrt.
func (z *BigInt) ModSqrt(x, p *BigInt) *BigInt {
	var tmp1, tmp2 big.Int //gcassert:noescape
	var tmp3 big.Int       // escapes because of https://github.com/golang/go/pull/50527.
	zi := z.inner(&tmp1)
	if zi.ModSqrt(x.inner(&tmp2), p.inner(&tmp3)) == nil {
		return nil
	}
	z.updateInner(zi)
	return z
}

// Mul calls (big.Int).Mul.
func (z *BigInt) Mul(x, y *BigInt) *BigInt {
	if xVal, xNeg, ok := x.innerAsUint64(); ok {
		if yVal, yNeg, ok := y.innerAsUint64(); ok {
			if zVal, zNeg, ok := mulInline(xVal, yVal, xNeg, yNeg); ok {
				z.updateInnerFromUint64(zVal, zNeg)
				return z
			}
		}
	}
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Mul(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// MulRange calls (big.Int).MulRange.
func (z *BigInt) MulRange(x, y int64) *BigInt {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.MulRange(x, y)
	z.updateInner(zi)
	return z
}

// Neg calls (big.Int).Neg.
func (z *BigInt) Neg(x *BigInt) *BigInt {
	if x.isInline() {
		z._inline = x._inline
		if x._inner == negSentinel {
			z._inner = nil
		} else {
			z._inner = negSentinel
		}
		return z
	}
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Neg(x.inner(&tmp2))
	z.updateInner(zi)
	return z
}

// Not calls (big.Int).Not.
func (z *BigInt) Not(x *BigInt) *BigInt {
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Not(x.inner(&tmp2))
	z.updateInner(zi)
	return z
}

// Or calls (big.Int).Or.
func (z *BigInt) Or(x, y *BigInt) *BigInt {
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Or(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// ProbablyPrime calls (big.Int).ProbablyPrime.
func (z *BigInt) ProbablyPrime(n int) bool {
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).ProbablyPrime(n)
}

// Quo calls (big.Int).Quo.
func (z *BigInt) Quo(x, y *BigInt) *BigInt {
	if xVal, xNeg, ok := x.innerAsUint64(); ok {
		if yVal, yNeg, ok := y.innerAsUint64(); ok {
			if quoVal, quoNeg, ok := quoInline(xVal, yVal, xNeg, yNeg); ok {
				z.updateInnerFromUint64(quoVal, quoNeg)
				return z
			}
		}
	}
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Quo(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// QuoRem calls (big.Int).QuoRem.
func (z *BigInt) QuoRem(x, y, r *BigInt) (*BigInt, *BigInt) {
	if xVal, xNeg, ok := x.innerAsUint64(); ok {
		if yVal, yNeg, ok := y.innerAsUint64(); ok {
			if quoVal, quoNeg, ok := quoInline(xVal, yVal, xNeg, yNeg); ok {
				if remVal, remNeg, ok := remInline(xVal, yVal, xNeg, yNeg); ok {
					z.updateInnerFromUint64(quoVal, quoNeg)
					r.updateInnerFromUint64(remVal, remNeg)
					return z, r
				}
			}
		}
	}
	var tmp1, tmp2, tmp3, tmp4 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	ri := r.inner(&tmp2)
	zi.QuoRem(x.inner(&tmp3), y.inner(&tmp4), ri)
	z.updateInner(zi)
	r.updateInner(ri)
	return z, r
}

// Rand calls (big.Int).Rand.
func (z *BigInt) Rand(rnd *rand.Rand, n *BigInt) *BigInt {
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Rand(rnd, n.inner(&tmp2))
	z.updateInner(zi)
	return z
}

// Rem calls (big.Int).Rem.
func (z *BigInt) Rem(x, y *BigInt) *BigInt {
	if xVal, xNeg, ok := x.innerAsUint64(); ok {
		if yVal, yNeg, ok := y.innerAsUint64(); ok {
			if remVal, remNeg, ok := remInline(xVal, yVal, xNeg, yNeg); ok {
				z.updateInnerFromUint64(remVal, remNeg)
				return z
			}
		}
	}
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Rem(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// Rsh calls (big.Int).Rsh.
func (z *BigInt) Rsh(x *BigInt, n uint) *BigInt {
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Rsh(x.inner(&tmp2), n)
	z.updateInner(zi)
	return z
}

// Scan calls (big.Int).Scan.
func (z *BigInt) Scan(s fmt.ScanState, ch rune) error {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	if err := zi.Scan(s, ch); err != nil {
		return err
	}
	z.updateInner(zi)
	return nil
}

// Set calls (big.Int).Set.
func (z *BigInt) Set(x *BigInt) *BigInt {
	if x.isInline() {
		*z = *x
		return z
	}
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Set(x.inner(&tmp2))
	z.updateInner(zi)
	return z
}

// SetBit calls (big.Int).SetBit.
func (z *BigInt) SetBit(x *BigInt, i int, b uint) *BigInt {
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.SetBit(x.inner(&tmp2), i, b)
	z.updateInner(zi)
	return z
}

// SetBits calls (big.Int).SetBits.
func (z *BigInt) SetBits(abs []big.Word) *BigInt {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.SetBits(abs)
	z.updateInner(zi)
	return z
}

// SetBytes calls (big.Int).SetBytes.
func (z *BigInt) SetBytes(buf []byte) *BigInt {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.SetBytes(buf)
	z.updateInner(zi)
	return z
}

// SetInt64 calls (big.Int).SetInt64.
func (z *BigInt) SetInt64(x int64) *BigInt {
	neg := false
	if x < 0 {
		neg = true
		x = -x
	}
	z.updateInnerFromUint64(uint64(x), neg)
	return z
}

// SetString calls (big.Int).SetString.
func (z *BigInt) SetString(s string, base int) (*BigInt, bool) {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	if _, ok := zi.SetString(s, base); !ok {
		return nil, false
	}
	z.updateInner(zi)
	return z, true
}

// SetUint64 calls (big.Int).SetUint64.
func (z *BigInt) SetUint64(x uint64) *BigInt {
	z.updateInnerFromUint64(x, false)
	return z
}

// Sign calls (big.Int).Sign.
func (z *BigInt) Sign() int {
	if z._inner == nil {
		if z._inline == [inlineWords]big.Word{} {
			return 0
		}
		return 1
	} else if z._inner == negSentinel {
		return -1
	}
	return z._inner.Sign()
}

// Sqrt calls (big.Int).Sqrt.
func (z *BigInt) Sqrt(x *BigInt) *BigInt {
	var tmp1, tmp2 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Sqrt(x.inner(&tmp2))
	z.updateInner(zi)
	return z
}

// String calls (big.Int).String.
func (z *BigInt) String() string {
	if z == nil {
		// Fast-path that avoids innerOrNil, allowing inner to be inlined.
		return "<nil>"
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).String()
}

// Sub calls (big.Int).Sub.
func (z *BigInt) Sub(x, y *BigInt) *BigInt {
	if xVal, xNeg, ok := x.innerAsUint64(); ok {
		if yVal, yNeg, ok := y.innerAsUint64(); ok {
			if zVal, zNeg, ok := addInline(xVal, yVal, xNeg, !yNeg); ok {
				z.updateInnerFromUint64(zVal, zNeg)
				return z
			}
		}
	}
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Sub(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

// Text calls (big.Int).Text.
func (z *BigInt) Text(base int) string {
	if z == nil {
		// Fast-path that avoids innerOrNil, allowing inner to be inlined.
		return "<nil>"
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).Text(base)
}

// TrailingZeroBits calls (big.Int).TrailingZeroBits.
func (z *BigInt) TrailingZeroBits() uint {
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).TrailingZeroBits()
}

// Uint64 calls (big.Int).Uint64.
func (z *BigInt) Uint64() uint64 {
	if zVal, _, ok := z.innerAsUint64(); ok {
		return zVal
	}
	var tmp1 big.Int //gcassert:noescape
	return z.inner(&tmp1).Uint64()
}

// UnmarshalJSON calls (big.Int).UnmarshalJSON.
func (z *BigInt) UnmarshalJSON(text []byte) error {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	if err := zi.UnmarshalJSON(text); err != nil {
		return err
	}
	z.updateInner(zi)
	return nil
}

// UnmarshalText calls (big.Int).UnmarshalText.
func (z *BigInt) UnmarshalText(text []byte) error {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	if err := zi.UnmarshalText(text); err != nil {
		return err
	}
	z.updateInner(zi)
	return nil
}

// Xor calls (big.Int).Xor.
func (z *BigInt) Xor(x, y *BigInt) *BigInt {
	var tmp1, tmp2, tmp3 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Xor(x.inner(&tmp2), y.inner(&tmp3))
	z.updateInner(zi)
	return z
}

///////////////////////////////////////////////////////////////////////////////
//                     apd.BigInt / math/big.Int interop                     //
///////////////////////////////////////////////////////////////////////////////

// MathBigInt returns the math/big.Int representation of z.
func (z *BigInt) MathBigInt() *big.Int {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	// NOTE: We can't return zi directly, because it may be pointing into z's
	// _inline array. We have disabled escape analysis for such aliasing, so
	// this would be unsafe as it would not force the receiver to escape and
	// could leave the return value pointing into stack memory.
	return new(big.Int).Set(zi)
}

// SetMathBigInt sets z to x and returns z.
func (z *BigInt) SetMathBigInt(x *big.Int) *BigInt {
	var tmp1 big.Int //gcassert:noescape
	zi := z.inner(&tmp1)
	zi.Set(x)
	z.updateInner(zi)
	return z
}
