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

import "testing"

func TestPowerOfTenDec(t *testing.T) {
	tests := []struct {
		pow int
		str string
	}{
		{
			pow: -tableSize - 1,
			str: "0.000000000000000000000000000000001",
		},
		{
			pow: -5,
			str: "0.00001",
		},
		{
			pow: -1,
			str: "0.1",
		},
		{
			pow: 0,
			str: "1",
		},
		{
			pow: 1,
			str: "10",
		},
		{
			pow: 5,
			str: "100000",
		},
		{
			pow: tableSize + 1,
			str: "1000000000000000000000000000000000",
		},
	}

	for i, test := range tests {
		d := PowerOfTenDec(test.pow)
		if s := d.String(); s != test.str {
			t.Errorf("%d: expected PowerOfTenDec(%d) to give %s, got %s", i, test.pow, test.str, s)
		}
	}
}

func TestPowerOfTenInt(t *testing.T) {
	tests := []struct {
		pow int
		str string
	}{
		{
			pow: 0,
			str: "1",
		},
		{
			pow: 1,
			str: "10",
		},
		{
			pow: 5,
			str: "100000",
		},
		{
			pow: tableSize + 1,
			str: "1000000000000000000000000000000000",
		},
	}

	for i, test := range tests {
		bi := PowerOfTenInt(test.pow)
		if s := bi.String(); s != test.str {
			t.Errorf("%d: expected PowerOfTenInt(%d) to give %s, got %s", i, test.pow, test.str, s)
		}
	}
}
