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
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
)

func TestProcessSelect(t *testing.T) {
	s := (*Server)(nil) // enough for now

	vInt := int64(5)
	vNum := "3.14159"
	vStr := "two furs one cub"
	vBool := true

	dInt := driver.Datum{IntVal: &vInt}
	dFloat := func() driver.Datum {
		tmp, err := strconv.ParseFloat(vNum, 64)
		if err != nil {
			panic(err)
		}
		return driver.Datum{FloatVal: &tmp}
	}()
	dStr := driver.Datum{StringVal: &vStr}
	dBool := driver.Datum{BoolVal: &vBool}

	unsupp := &parser.RangeCond{}

	asRow := func(datums ...driver.Datum) []driver.Result_Row {
		return []driver.Result_Row{
			{Values: datums},
		}
	}

	testCases := []struct {
		stmt parser.SelectStatement
		rows []driver.Result_Row
		ok   bool
	}{
		{
			parser.Values{{parser.IntVal(vInt)}},
			asRow(dInt),
			true,
		},
		{
			parser.Values{{parser.IntVal(vInt), parser.IntVal(vInt)}},
			asRow(dInt, dInt),
			true,
		},
		{
			parser.Values{{parser.NumVal(vNum)}},
			asRow(dFloat),
			true,
		},
		{
			parser.Values{{parser.StrVal(vStr)}},
			asRow(dStr),
			true,
		},
		{
			parser.Values{{parser.BytesVal(vStr)}},
			asRow(dStr), // string, not bytes, returned!
			true,
		},
		{
			parser.Values{{parser.BoolVal(vBool)}},
			asRow(dBool),
			true,
		},
		{
			parser.Values{{unsupp}},
			nil,
			false,
		},
	}

	for i, tc := range testCases {
		rows, err := s.processSelect(tc.stmt)
		if err == nil != tc.ok {
			t.Errorf("%d: error_expected=%t, but got error %v", i, tc.ok, err)
		}
		if !reflect.DeepEqual(rows, tc.rows) {
			t.Errorf("%d: expected rows:\n%+v\nactual rows:\n%+v", i, tc.rows, rows)
		}
	}
}
