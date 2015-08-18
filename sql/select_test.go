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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestQValueInfo(t *testing.T) {
	defer leaktest.AfterTest(t)

	const intersect = "intersect"
	const union = "union"

	type inputData struct {
		a     qvalueInfo
		b     qvalueInfo
		mode  string
		start bool
	}

	intersectStart := func(a qvalueInfo, b qvalueInfo) inputData {
		return inputData{a: a, b: b, mode: intersect, start: true}
	}
	intersectEnd := func(a qvalueInfo, b qvalueInfo) inputData {
		return inputData{a: a, b: b, mode: intersect, start: false}
	}
	unionStart := func(a qvalueInfo, b qvalueInfo) inputData {
		return inputData{a: a, b: b, mode: union, start: true}
	}
	unionEnd := func(a qvalueInfo, b qvalueInfo) inputData {
		return inputData{a: a, b: b, mode: union, start: false}
	}

	null := qvalueInfo{}
	gt := func(i int) qvalueInfo {
		return qvalueInfo{datum: parser.DInt(i), op: parser.GT}
	}
	ge := func(i int) qvalueInfo {
		return qvalueInfo{datum: parser.DInt(i), op: parser.GE}
	}
	lt := func(i int) qvalueInfo {
		return qvalueInfo{datum: parser.DInt(i), op: parser.LT}
	}
	le := func(i int) qvalueInfo {
		return qvalueInfo{datum: parser.DInt(i), op: parser.LE}
	}

	testCases := []struct {
		inputData
		expected qvalueInfo
	}{
		{intersectStart(null, gt(1)), gt(1)},
		{intersectStart(null, ge(1)), ge(1)},
		{intersectStart(ge(1), ge(1)), ge(1)},
		{intersectStart(gt(1), gt(1)), gt(1)},
		{intersectStart(ge(1), gt(1)), gt(1)},
		{intersectStart(gt(1), ge(1)), gt(1)},
		{intersectStart(gt(1), gt(2)), gt(2)},
		{intersectStart(ge(1), gt(2)), gt(2)},
		{intersectStart(gt(2), gt(1)), gt(2)},
		{intersectStart(gt(2), ge(1)), gt(2)},
		{intersectStart(ge(2), ge(1)), ge(2)},
		{intersectStart(ge(1), ge(2)), ge(2)},

		{intersectEnd(null, lt(1)), lt(1)},
		{intersectEnd(null, le(1)), le(1)},
		{intersectEnd(le(1), le(1)), le(1)},
		{intersectEnd(lt(1), lt(1)), lt(1)},
		{intersectEnd(le(1), lt(1)), lt(1)},
		{intersectEnd(lt(1), le(1)), lt(1)},
		{intersectEnd(lt(1), lt(2)), lt(1)},
		{intersectEnd(le(1), lt(2)), le(1)},
		{intersectEnd(lt(2), lt(1)), lt(1)},
		{intersectEnd(lt(2), le(1)), le(1)},
		{intersectEnd(le(2), le(1)), le(1)},
		{intersectEnd(le(1), le(2)), le(1)},

		{unionStart(null, gt(1)), gt(1)},
		{unionStart(null, ge(1)), ge(1)},
		{unionStart(ge(1), ge(1)), ge(1)},
		{unionStart(gt(1), gt(1)), gt(1)},
		{unionStart(ge(1), gt(1)), ge(1)},
		{unionStart(gt(1), ge(1)), ge(1)},
		{unionStart(gt(1), gt(2)), gt(1)},
		{unionStart(ge(1), gt(2)), ge(1)},
		{unionStart(gt(2), gt(1)), gt(1)},
		{unionStart(gt(2), ge(1)), ge(1)},
		{unionStart(ge(2), ge(1)), ge(1)},
		{unionStart(ge(1), ge(2)), ge(1)},

		{unionEnd(null, lt(1)), lt(1)},
		{unionEnd(null, le(1)), le(1)},
		{unionEnd(le(1), le(1)), le(1)},
		{unionEnd(lt(1), lt(1)), lt(1)},
		{unionEnd(le(1), lt(1)), le(1)},
		{unionEnd(lt(1), le(1)), le(1)},
		{unionEnd(lt(1), lt(2)), lt(2)},
		{unionEnd(le(1), lt(2)), lt(2)},
		{unionEnd(lt(2), lt(1)), lt(2)},
		{unionEnd(lt(2), le(1)), lt(2)},
		{unionEnd(le(2), le(1)), le(2)},
		{unionEnd(le(1), le(2)), le(2)},
	}
	for i, test := range testCases {
		q := test.a
		switch test.mode {
		case intersect:
			q.intersect(test.b, test.start)
		case union:
			q.union(test.b, test.start)
		}
		if q.datum != test.expected.datum || q.op != test.expected.op {
			t.Fatalf("%d: expected %s%v, but found %s%v: %s(%s%v, %s%v)",
				i, test.expected.op, test.expected.datum, q.op, q.datum,
				test.mode, test.a.op, test.a.datum, test.b.op, test.b.datum)
		}
	}
}
