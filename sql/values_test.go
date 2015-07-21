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

package sql

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
)

func TestValues(t *testing.T) {
	p := planner{}

	vInt := int64(5)
	vNum := 3.14159
	vStr := "two furs one cub"
	vBool := true

	unsupp := &parser.RangeCond{}

	asRow := func(datums ...parser.Datum) []parser.DTuple {
		return []parser.DTuple{datums}
	}

	testCases := []struct {
		stmt parser.Values
		rows []parser.DTuple
		ok   bool
	}{
		{
			parser.Values{{parser.IntVal(vInt)}},
			asRow(parser.DInt(vInt)),
			true,
		},
		{
			parser.Values{{parser.IntVal(vInt), parser.IntVal(vInt)}},
			asRow(parser.DInt(vInt), parser.DInt(vInt)),
			true,
		},
		{
			parser.Values{{parser.NumVal(fmt.Sprintf("%0.5f", vNum))}},
			asRow(parser.DFloat(vNum)),
			true,
		},
		{
			parser.Values{{parser.StrVal(vStr)}},
			asRow(parser.DString(vStr)),
			true,
		},
		{
			parser.Values{{parser.BytesVal(vStr)}},
			asRow(parser.DString(vStr)),
			true,
		},
		{
			parser.Values{{parser.BoolVal(vBool)}},
			asRow(parser.DBool(vBool)),
			true,
		},
		{
			parser.Values{{unsupp}},
			nil,
			false,
		},
	}

	for i, tc := range testCases {
		plan, err := p.Values(tc.stmt)
		if err == nil != tc.ok {
			t.Errorf("%d: error_expected=%t, but got error %v", i, tc.ok, err)
		}
		if plan != nil {
			var rows []parser.DTuple
			for plan.Next() {
				rows = append(rows, plan.Values())
			}
			if !reflect.DeepEqual(rows, tc.rows) {
				t.Errorf("%d: expected rows:\n%+v\nactual rows:\n%+v", i, tc.rows, rows)
			}
		}
	}
}
