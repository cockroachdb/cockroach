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

package parser

import (
	"reflect"
	"testing"
)

func TestLexer(t *testing.T) {
	// Test the lookahead logic in Lex.
	testData := []struct {
		sql      string
		expected []int
	}{
		{`WITH TIME`, []int{WITH_LA, TIME}},
		{`WITH ORDINALITY`, []int{WITH_LA, ORDINALITY}},
		{`NOT BETWEEN`, []int{NOT_LA, BETWEEN}},
		{`NOT IN`, []int{NOT_LA, IN}},
		{`NOT SIMILAR`, []int{NOT_LA, SIMILAR}},
		{`AS OF SYSTEM TIME`, []int{AS_LA, OF, SYSTEM, TIME}},
	}
	for i, d := range testData {
		s := makeScanner(d.sql)
		var scanTokens []sqlSymType
		for {
			var lval sqlSymType
			s.scan(&lval)
			if lval.id == 0 {
				break
			}
			scanTokens = append(scanTokens, lval)
		}
		var l lexer
		l.init(d.sql, scanTokens, defaultNakedIntType, defaultNakedSerialType)
		var lexTokens []int
		for {
			var lval sqlSymType
			id := l.Lex(&lval)
			if id == 0 {
				break
			}
			lexTokens = append(lexTokens, id)
		}

		if !reflect.DeepEqual(d.expected, lexTokens) {
			t.Errorf("%d: %q: expected %d, but found %d", i, d.sql, d.expected, lexTokens)
		}
	}

}
