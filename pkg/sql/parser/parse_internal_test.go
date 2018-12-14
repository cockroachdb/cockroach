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

func TestScanOneStmt(t *testing.T) {
	type stmt struct {
		sql string
		tok []int
	}
	testData := []struct {
		sql string
		exp []stmt
	}{
		{sql: ``, exp: nil},
		{sql: `;`, exp: nil},
		{sql: `  ;  ; `, exp: nil},
		{
			sql: `SELECT 1`,
			exp: []stmt{{
				sql: `SELECT 1`,
				tok: []int{SELECT, ICONST},
			}},
		},
		{
			sql: `SELECT 1;`,
			exp: []stmt{{
				sql: `SELECT 1`,
				tok: []int{SELECT, ICONST},
			}},
		},
		{
			sql: `SELECT 1 /* comment */  ; /* comment */  ; /* comment */ `,
			exp: []stmt{{
				sql: `SELECT 1`,
				tok: []int{SELECT, ICONST},
			}},
		},
		{
			sql: `;SELECT 1`,
			exp: []stmt{{
				sql: `SELECT 1`,
				tok: []int{SELECT, ICONST},
			}},
		},
		{
			sql: `  ; /* comment */ ;  SELECT 1`,
			exp: []stmt{{
				sql: `SELECT 1`,
				tok: []int{SELECT, ICONST},
			}},
		},
		{
			sql: `;;SELECT 1;;`,
			exp: []stmt{{
				sql: `SELECT 1`,
				tok: []int{SELECT, ICONST},
			}},
		},
		{
			sql: ` ; /* x */ SELECT 1  ; SET /* y */ ; ;  INSERT INTO table;  ;`,
			exp: []stmt{
				{
					sql: `SELECT 1`,
					tok: []int{SELECT, ICONST},
				},
				{
					sql: `SET`,
					tok: []int{SET},
				},
				{
					sql: `INSERT INTO table`,
					tok: []int{INSERT, INTO, TABLE},
				},
			},
		},
		{
			sql: `SELECT ';'`,
			exp: []stmt{{
				sql: `SELECT ';'`,
				tok: []int{SELECT, SCONST},
			}},
		},
		{
			sql: `SELECT ';';`,
			exp: []stmt{{
				sql: `SELECT ';'`,
				tok: []int{SELECT, SCONST},
			}},
		},
		{
			// An error should stop the scanning and return the rest of the string.
			sql: `SELECT 1; SELECT 0x FROM t; SELECT 2`,
			exp: []stmt{
				{
					sql: `SELECT 1`,
					tok: []int{SELECT, ICONST},
				},
				{
					sql: `SELECT 0x FROM t; SELECT 2`,
					tok: []int{SELECT, ERROR},
				},
			},
		},
	}

	for _, tc := range testData {
		var p Parser
		p.scanner.init(tc.sql)

		var result []stmt
		for {
			sql, tokens, done := p.scanOneStmt()
			if sql == "" {
				break
			}
			s := stmt{sql: sql}
			for _, t := range tokens {
				s.tok = append(s.tok, int(t.id))
			}
			result = append(result, s)
			if done {
				break
			}
		}
		if !reflect.DeepEqual(result, tc.exp) {
			t.Errorf("expected\n  %+v, but found\n  %+v", tc.exp, result)
		}
	}
}
