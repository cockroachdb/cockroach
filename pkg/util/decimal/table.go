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
