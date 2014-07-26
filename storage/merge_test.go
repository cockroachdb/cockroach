// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/util"
)

func gibberishBytes(n int) []byte {
	b := make([]byte, n, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(math.MaxUint8 + 1))
	}
	return b
}

// TestGoMerge tests the function goMerge but not the integration with
// the storage engines. For that, see the engine tests.
func TestGoMerge(t *testing.T) {
	// Let's start with stuff that should go wrong.
	badCombinations := []struct {
		old, update interface{}
	}{
		{Counter(0), Appender("")},
		{[]byte("apple"), nil},
		{"", ""},
		{0, 0},
		{Appender(""), Counter(0)},
		{0, "asd"},
		{float64(1.3), Counter(0)},
		{Counter(0), nil},
	}
	for i, c := range badCombinations {
		_, err := goMerge(util.GobEncodeOrDie(c.old), util.GobEncodeOrDie(c.update))
		if err == nil {
			t.Errorf("goMerge: %d: expected error", i)
		}
	}

	testCasesCounter := []struct {
		old, update, expected Counter
		wantError             bool
	}{
		{0, 10, 10, false},
		{10, 20, 30, false},
		{595, -600, -5, false},
		// Close to overflow, but not quite there.
		{math.MinInt64 + 3, -3, math.MinInt64, false},
		{math.MaxInt64, 0, math.MaxInt64, false},
		// Overflows.
		{math.MaxInt64, 1, 0, true},
		{-1, math.MinInt64, 0, true},
	}

	gibber1, gibber2 := gibberishBytes(100), gibberishBytes(200)

	testCasesAppender := []struct {
		old, update, expected Appender
	}{
		{Appender(""), Appender(""), Appender("")},
		{nil, Appender(""), Appender("")},
		{nil, nil, nil},
		{Appender("\n "), Appender(" \t "), Appender("\n  \t ")},
		{Appender("ქართული"), Appender("\nKhartuli"), Appender("ქართული\nKhartuli")},
		{gibber1, gibber2, append(append([]byte(nil), gibber1...), gibber2...)},
	}

	for i, c := range testCasesCounter {
		oEncoded := util.GobEncodeOrDie(c.old)
		uEncoded := util.GobEncodeOrDie(c.update)

		result, err := goMerge(oEncoded, uEncoded)
		if c.wantError {
			if err == nil {
				t.Errorf("goMerge: %d: wanted error but got success", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("goMerge error: %d: %v", i, err)
			continue
		}
		resultDecoded := util.GobDecodeOrDie(result)
		if resultDecoded != c.expected {
			t.Errorf("goMerge error: %d: want %v, get %v", i, c.expected, result)
		}
	}
	for i, c := range testCasesAppender {
		oEncoded := util.GobEncodeOrDie(c.old)
		uEncoded := util.GobEncodeOrDie(c.update)

		result, err := goMerge(oEncoded, uEncoded)
		if err != nil {
			t.Errorf("goMerge error: %d: %v", i, err)
			continue
		}
		resultDecoded := util.GobDecodeOrDie(result)
		if !bytes.Equal(resultDecoded.(Appender), c.expected) {
			t.Errorf("goMerge error: %d: want %v, get %v", i, c.expected, result)
		}
	}

}
