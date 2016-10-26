// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package decimal

import (
	"fmt"
	"math/big"

	"gopkg.in/inf.v0"
)

// tableSize is the magnitude of the maximum power of 10 exponent that is stored
// in the pow10LookupTable. The table will store both negative and positive powers
// 10 up to this magnitude. For instance, if the tableSize if 3, then the lookup
// table will store power of 10 values from 10^-3 to 10^3.
const tableSize = 32

var pow10LookupTable [2*tableSize + 1]inf.Dec

func init() {
	tmpInt := new(big.Int)
	for i := -tableSize; i <= tableSize; i++ {
		setDecWithPow(&pow10LookupTable[i+tableSize], tmpInt, i)
	}
}

// setDecWithPow sets the decimal value to the power of 10 provided. For non-negative values,
// it guarantees to produce a decimal value with a scale of 0, which means that it's underlying
// UnscaledBig value will also have a value of 10 to the provided power. A nullable *big.Int can
// be provided to avoid an allocation during computation.
func setDecWithPow(dec *inf.Dec, tmpInt *big.Int, pow int) {
	if pow >= 0 {
		bi := dec.SetScale(0).UnscaledBig()
		bi.SetInt64(10)
		if tmpInt == nil {
			tmpInt = new(big.Int)
		}
		bi.Exp(bi, tmpInt.SetInt64(int64(pow)), nil)
	} else {
		dec.SetUnscaled(1).SetScale(inf.Scale(-pow))
	}
}

// PowerOfTenDec returns an *inf.Dec with the value 10^pow. It should
// be treated as immutable.
//
// Non-negative powers of 10 will have their value represented in their
// underlying big.Int with a scale of 0, while negative powers of 10 will
// have their value represented in their scale with a big.Int value of 1.
func PowerOfTenDec(pow int) *inf.Dec {
	if pow >= -tableSize && pow <= tableSize {
		return &pow10LookupTable[pow+tableSize]
	}
	dec := new(inf.Dec)
	setDecWithPow(dec, nil, pow)
	return dec
}

// PowerOfTenInt returns a *big.Int with the value 10^pow. It should
// be treated as immutable.
func PowerOfTenInt(pow int) *big.Int {
	if pow < 0 {
		panic(fmt.Sprintf("PowerOfTenInt called with negative power: %d", pow))
	}
	dec := PowerOfTenDec(pow)
	if s := dec.Scale(); s != 0 {
		panic(fmt.Sprintf("PowerOfTenDec(%v) returned decimal with non-zero scale: %d", dec, s))
	}
	return dec.UnscaledBig()
}

// digitsLookupTable is used to map binary digit counts to their corresponding
// decimal border values. The map relies on the proof that (without leading zeros)
// for any given number of binary digits r, such that the number represented is
// between 2^r and 2^(r+1)-1, there are only two possible decimal digit counts
// k and k+1 that the binary r digits could be representing.
//
// Using this proof, for a given digit count, the map will return the lower number
// of decimal digits (k) the binary digit count could represent, along with the
// value of the border between the two decimal digit counts (10^k).
const digitsTableSize = 128

var digitsLookupTable [digitsTableSize + 1]tableVal

type tableVal struct {
	digits int
	border big.Int
}

func init() {
	curVal := big.NewInt(1)
	curExp := new(big.Int)
	for i := 1; i <= digitsTableSize; i++ {
		if i > 1 {
			curVal.Lsh(curVal, 1)
		}

		elem := &digitsLookupTable[i]
		elem.digits = len(curVal.String())

		elem.border.SetInt64(10)
		curExp.SetInt64(int64(elem.digits))
		elem.border.Exp(&elem.border, curExp, nil)
	}
}

func lookupBits(bitLen int) (tableVal, bool) {
	if bitLen > 0 && bitLen < len(digitsLookupTable) {
		return digitsLookupTable[bitLen], true
	}
	return tableVal{}, false
}

// NumDigits returns the number of decimal digits that make up
// big.Int value. The function first attempts to look this digit
// count up in the digitsLookupTable. If the value is not there,
// it defaults to constructing a string value for the big.Int and
// using this to determine the number of digits. If a string value
// is constructed, it will be returned so it can be used again.
func NumDigits(bi *big.Int, tmp []byte) (int, []byte) {
	if val, ok := lookupBits(bi.BitLen()); ok {
		if bi.Cmp(&val.border) < 0 {
			return val.digits, nil
		}
		return val.digits + 1, nil
	}
	bs := bi.Append(tmp, 10)
	return len(bs), bs
}
