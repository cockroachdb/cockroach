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
	"math/big"

	"gopkg.in/inf.v0"
)

const tableSize = 32

var pow10LookupTable [2*tableSize + 1]inf.Dec

func init() {
	tmpInt := new(big.Int)
	for i := -tableSize; i <= tableSize; i++ {
		bi := pow10LookupTable[i+tableSize].UnscaledBig()
		bi.SetInt64(10)
		bi.Exp(bi, tmpInt.SetInt64(int64(i)), nil)
	}
}

// PowerOfTenDec returns an *inf.Dec with the value 10^pow. It should
// be treated as immutable.
func PowerOfTenDec(pow int) *inf.Dec {
	if pow >= -tableSize && pow <= tableSize {
		return &pow10LookupTable[pow+tableSize]
	}
	dec := inf.NewDec(10, 0)
	bi := dec.UnscaledBig()
	bi.Exp(bi, big.NewInt(int64(pow)), nil)
	return dec
}

// PowerOfTenInt returns a *big.Int with the value 10^pow. It should
// be treated as immutable.
func PowerOfTenInt(pow int) *big.Int {
	return PowerOfTenDec(pow).UnscaledBig()
}
