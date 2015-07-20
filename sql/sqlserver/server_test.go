// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf

package sqlserver

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
)

func TestProcessInsertRows(t *testing.T) {
	s := (*Server)(nil) // enough for now

	vInt := int64(5)
	vFloat := float64(3.14159)
	vStr := "five"
	vBool := true

	dInt := sqlwire.Datum{IntVal: &vInt}
	dFloat := sqlwire.Datum{FloatVal: &vFloat}
	dStr := sqlwire.Datum{StringVal: &vStr}
	dBool := sqlwire.Datum{BoolVal: &vBool}

	// Floats aren't generated from processInsertRows. A parser.NumVal currently
	// turns into a StringVal datum.
	_, _ = vFloat, dFloat

	asRow := func(datums ...sqlwire.Datum) []sqlwire.Result_Row {
		return []sqlwire.Result_Row{
			{Values: datums},
		}
	}

	testCases := []struct {
		stmt parser.SelectStatement
		rows []sqlwire.Result_Row
		ok   bool
	}{
		{
			parser.Values{{parser.IntVal(vInt)}},
			asRow(dInt),
			true,
		},
		{
			// This one will likely have to change.
			parser.Values{{parser.NumVal("five")}},
			asRow(dStr),
			true,
		},
		{
			parser.Values{{parser.StrVal(vStr)}},
			asRow(dStr),
			true,
		},
		{
			parser.Values{{parser.BytesVal(vStr)}},
			asRow(dStr), // string, not bytes!
			true,
		},
		{
			parser.Values{{parser.BoolVal(vBool)}},
			asRow(dBool),
			true,
		},
	}

	for i, tc := range testCases {
		rows, err := s.processInsertRows(tc.stmt)
		if err == nil != tc.ok {
			t.Errorf("%d: error_expected=%t, but got error %v", i, tc.ok, err)
		}
		if !reflect.DeepEqual(rows, tc.rows) {
			t.Errorf("%d: expected rows:\n%+v\nactual rows:\n%+v", i, tc.rows, rows)
		}
	}
}
