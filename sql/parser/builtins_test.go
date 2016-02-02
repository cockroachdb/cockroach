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
// Author: Matt Jibson

package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func TestReverseBits(t *testing.T) {
	tests := []int64{
		1,
		-1,
		0,
		100,
		-100,
		math.MaxInt64,
		math.MinInt64,
	}
	for _, i := range tests {
		r := reverseBitsString(i)
		v := toBits(reverseBits(i))
		if r != v {
			t.Fatalf("%v: \ngot\t%v\nexp\t%v", i, v, r)
		}
	}
}

func reverseBitsString(i int64) string {
	return reverseString(toBits(i))
}

func toBits(i int64) string {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i)
	s := new(bytes.Buffer)
	for _, v := range buf.Bytes() {
		fmt.Fprintf(s, "%08b", v)
	}
	return s.String()
}

func reverseString(s string) string {
	n := len(s)
	runes := make([]rune, n)
	for _, rune := range s {
		n--
		runes[n] = rune
	}
	return string(runes[n:])
}
