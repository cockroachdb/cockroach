// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		l.init(d.sql, scanTokens, defaultNakedIntType)
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
