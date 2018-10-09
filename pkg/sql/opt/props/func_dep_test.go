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

package props_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Other tests also exercise the ColsAreKey methods.
func TestFuncDeps_ColsAreKey(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT * FROM abcde LEFT OUTER JOIN (SELECT *, p+q FROM mnpq) ON c=1 AND m=1 WHERE a=m
	nullExtendedCols := util.MakeFastIntSet(10, 11, 12, 13, 14)
	loj := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	mnpq.AddSynthesizedCol(util.MakeFastIntSet(12, 13), 14)
	loj.MakeProduct(mnpq)
	loj.AddConstants(util.MakeFastIntSet(3))
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 10, 11))
	loj.AddEquivalency(1, 10)
	verifyFD(t, loj, "(10,11): ()-->(3), (1)-->(2,4,5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12,13)~~>(14), (1,10,11)-->(14), (1)==(10), (10)==(1)")

	testcases := []struct {
		cols   opt.ColSet
		strict bool
		lax    bool
	}{
		{cols: util.MakeFastIntSet(1, 2, 3, 4, 5, 10, 11, 12, 13, 14), strict: true, lax: true},
		{cols: util.MakeFastIntSet(1, 2, 3, 4, 5, 10, 12, 13, 14), strict: false, lax: false},
		{cols: util.MakeFastIntSet(1, 11), strict: true, lax: true},
		{cols: util.MakeFastIntSet(10, 11), strict: true, lax: true},
		{cols: util.MakeFastIntSet(1), strict: false, lax: false},
		{cols: util.MakeFastIntSet(10), strict: false, lax: false},
		{cols: util.MakeFastIntSet(11), strict: false, lax: false},
		{cols: util.MakeFastIntSet(), strict: false, lax: false},
		{cols: util.MakeFastIntSet(2, 11), strict: false, lax: true},
	}

	for _, tc := range testcases {
		testColsAreStrictKey(t, loj, tc.cols, tc.strict)
		testColsAreLaxKey(t, loj, tc.cols, tc.lax)
	}
}

func TestFuncDeps_ComputeClosure(t *testing.T) {
	// (a)-->(b,c,d)
	// (b,c,e)-->(f)
	// (d)==(e)
	// (e)==(d)
	fd1 := &props.FuncDepSet{}
	fd1.AddSynthesizedCol(util.MakeFastIntSet(1), 2)
	fd1.AddSynthesizedCol(util.MakeFastIntSet(1), 3)
	fd1.AddSynthesizedCol(util.MakeFastIntSet(1), 4)
	fd1.AddSynthesizedCol(util.MakeFastIntSet(2, 3, 5), 6)
	fd1.AddEquivalency(4, 5)
	verifyFD(t, fd1, "(1)-->(2-4), (2,3,5)-->(6), (4)==(5), (5)==(4)")

	// ()~~>(a)
	// (a)~~>(d)
	// ()-->(b)
	// (b)==(c)
	// (c)==(b)
	// (d)-->(e)
	fd2 := &props.FuncDepSet{}
	fd2.AddConstants(util.MakeFastIntSet(1, 2))
	fd2.AddSynthesizedCol(util.MakeFastIntSet(1), 4)
	fd2.MakeOuter(util.MakeFastIntSet(1, 4), util.MakeFastIntSet())
	fd2.AddEquivalency(2, 3)
	fd2.AddSynthesizedCol(util.MakeFastIntSet(4), 5)
	verifyFD(t, fd2, "()-->(2,3), ()~~>(1), (1)~~>(4), (2)==(3), (3)==(2), (4)-->(5)")

	testcases := []struct {
		fd       *props.FuncDepSet
		in       opt.ColSet
		expected opt.ColSet
	}{
		{fd: fd1, in: util.MakeFastIntSet(), expected: util.MakeFastIntSet()},
		{fd: fd1, in: util.MakeFastIntSet(1), expected: util.MakeFastIntSet(1, 2, 3, 4, 5, 6)},
		{fd: fd1, in: util.MakeFastIntSet(2), expected: util.MakeFastIntSet(2)},
		{fd: fd1, in: util.MakeFastIntSet(2, 3, 4), expected: util.MakeFastIntSet(2, 3, 4, 5, 6)},
		{fd: fd1, in: util.MakeFastIntSet(4), expected: util.MakeFastIntSet(4, 5)},

		{fd: fd2, in: util.MakeFastIntSet(), expected: util.MakeFastIntSet(2, 3)},
		{fd: fd2, in: util.MakeFastIntSet(1), expected: util.MakeFastIntSet(1, 2, 3)},
		{fd: fd2, in: util.MakeFastIntSet(1, 4), expected: util.MakeFastIntSet(1, 2, 3, 4, 5)},
	}

	for _, tc := range testcases {
		closure := tc.fd.ComputeClosure(tc.in)
		if !closure.Equals(tc.expected) {
			t.Errorf("in: %s, expected: %s, actual: %s", tc.in, tc.expected, closure)
		}
	}
}

func TestFuncDeps_InClosureOf(t *testing.T) {
	// ()~~>(a)
	// (a)~~>(d)
	// ()-->(b)
	// (b)==(c)
	// (c)==(b)
	// (d)-->(e)
	fd := &props.FuncDepSet{}
	fd.AddConstants(util.MakeFastIntSet(1, 2))
	fd.AddSynthesizedCol(util.MakeFastIntSet(1), 4)
	fd.MakeOuter(util.MakeFastIntSet(1, 4), util.MakeFastIntSet())
	fd.AddEquivalency(2, 3)
	fd.AddSynthesizedCol(util.MakeFastIntSet(4), 5)
	verifyFD(t, fd, "()-->(2,3), ()~~>(1), (1)~~>(4), (2)==(3), (3)==(2), (4)-->(5)")

	testcases := []struct {
		cols     []int
		in       []int
		expected bool
	}{
		{cols: []int{}, in: []int{}, expected: true},
		{cols: []int{}, in: []int{1}, expected: true},
		{cols: []int{2, 3}, in: []int{}, expected: true},
		{cols: []int{2}, in: []int{3}, expected: true},
		{cols: []int{3}, in: []int{2}, expected: true},
		{cols: []int{3, 5}, in: []int{2, 4}, expected: true},

		{cols: []int{1}, in: []int{}, expected: false},
		{cols: []int{4}, in: []int{5}, expected: false},
		{cols: []int{2, 3, 4}, in: []int{1, 2, 3}, expected: false},
	}

	for _, tc := range testcases {
		cols := util.MakeFastIntSet(tc.cols...)
		in := util.MakeFastIntSet(tc.in...)
		actual := fd.InClosureOf(cols, in)
		if actual != tc.expected {
			if tc.expected {
				t.Errorf("expected %s to be in closure of %s", cols, in)
			} else {
				t.Errorf("expected %s to not be in closure of %s", cols, in)
			}
		}
	}
}

func TestFuncDeps_ComputeEquivClosure(t *testing.T) {
	// (a)==(b,d)
	// (b)==(a,c)
	// (c)==(b)
	// (d)==(a)
	// (a)~~>(e)
	// (a)-->(f)
	fd1 := &props.FuncDepSet{}
	fd1.AddSynthesizedCol(util.MakeFastIntSet(1), 5)
	fd1.MakeOuter(util.MakeFastIntSet(1, 5), util.MakeFastIntSet())
	fd1.AddSynthesizedCol(util.MakeFastIntSet(1), 6)
	fd1.AddEquivalency(1, 2)
	fd1.AddEquivalency(2, 3)
	fd1.AddEquivalency(1, 4)
	verifyFD(t, fd1, "(1)~~>(5), (1)-->(6), (1)==(2-4), (2)==(1,3,4), (3)==(1,2,4), (4)==(1-3)")

	testcases := []struct {
		fd       *props.FuncDepSet
		in       opt.ColSet
		expected opt.ColSet
	}{
		{fd: fd1, in: util.MakeFastIntSet(), expected: util.MakeFastIntSet()},
		{fd: fd1, in: util.MakeFastIntSet(1), expected: util.MakeFastIntSet(1, 2, 3, 4)},
		{fd: fd1, in: util.MakeFastIntSet(2), expected: util.MakeFastIntSet(1, 2, 3, 4)},
		{fd: fd1, in: util.MakeFastIntSet(3), expected: util.MakeFastIntSet(1, 2, 3, 4)},
		{fd: fd1, in: util.MakeFastIntSet(4), expected: util.MakeFastIntSet(1, 2, 3, 4)},
		{fd: fd1, in: util.MakeFastIntSet(5, 6), expected: util.MakeFastIntSet(5, 6)},
	}

	for _, tc := range testcases {
		closure := tc.fd.ComputeEquivClosure(tc.in)
		if !closure.Equals(tc.expected) {
			t.Errorf("in: %s, expected: %s, actual: %s", tc.in, tc.expected, closure)
		}
	}
}

func TestFuncDeps_EquivReps(t *testing.T) {
	// (a)==(b,d)
	// (b)==(a,c)
	// (c)==(b)
	// (a)~~>(e)
	// (a)-->(f)
	fd1 := &props.FuncDepSet{}
	fd1.AddSynthesizedCol(util.MakeFastIntSet(1), 5)
	fd1.MakeOuter(util.MakeFastIntSet(1, 5), util.MakeFastIntSet())
	fd1.AddSynthesizedCol(util.MakeFastIntSet(1), 6)
	fd1.AddEquivalency(1, 2)
	fd1.AddEquivalency(2, 3)
	verifyFD(t, fd1, "(1)~~>(5), (1)-->(6), (1)==(2,3), (2)==(1,3), (3)==(1,2)")

	// (a)==(b,d)
	// (b)==(a,c)
	// (c)==(b)
	// (d)==(a)
	// (a)~~>(e)
	// (a)-->(f)
	fd2 := &props.FuncDepSet{}
	fd2.CopyFrom(fd1)
	fd2.AddEquivalency(1, 4)
	verifyFD(t, fd2, "(1)~~>(5), (1)-->(6), (1)==(2-4), (2)==(1,3,4), (3)==(1,2,4), (4)==(1-3)")

	// (a)==(b,d)
	// (b)==(a,c)
	// (c)==(b)
	// (d)==(e)
	// (a)~~>(e)
	// (a)-->(f)
	fd3 := &props.FuncDepSet{}
	fd3.CopyFrom(fd1)
	fd3.AddEquivalency(4, 5)
	verifyFD(t, fd3, "(1)~~>(5), (1)-->(6), (1)==(2,3), (2)==(1,3), (3)==(1,2), (4)==(5), (5)==(4)")

	testcases := []struct {
		fd       *props.FuncDepSet
		expected opt.ColSet
	}{
		{fd: fd1, expected: util.MakeFastIntSet(1)},
		{fd: fd2, expected: util.MakeFastIntSet(1)},
		{fd: fd3, expected: util.MakeFastIntSet(1, 4)},
	}

	for _, tc := range testcases {
		closure := tc.fd.EquivReps()
		if !closure.Equals(tc.expected) {
			t.Errorf("fd: %s, expected: %s, actual: %s", tc.fd, tc.expected, closure)
		}
	}
}

func TestFuncDeps_AddStrictKey(t *testing.T) {
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT DISTINCT ON (p) m, n, p, q FROM mnpq
	mnpq := makeMnpqFD(t)
	allCols := util.MakeFastIntSet(10, 11, 12, 13)
	mnpq.AddStrictKey(util.MakeFastIntSet(12), allCols)
	verifyFD(t, mnpq, "(12): (10,11)-->(12,13), (12)-->(10,11,13)")
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(12), true)
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(13), false)
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(10, 11), true)

	// SELECT DISTINCT ON (m, n, p) m, n, p, q FROM mnpq
	mnpq = makeMnpqFD(t)
	mnpq.AddStrictKey(util.MakeFastIntSet(10, 11, 12), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13)")
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(10, 11), true)
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(11, 12), false)

	// SELECT DISTINCT ON (n, p, q) m, n, p, q FROM mnpq
	mnpq = makeMnpqFD(t)
	mnpq.AddStrictKey(util.MakeFastIntSet(11, 12, 13), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13), (11-13)-->(10)")
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(11, 12, 13), true)
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(11, 12), false)
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(10, 11), true)

	// All columns together form a key.
	//   CREATE TABLE ab (a INT, b INT, PRIMARY KEY (a, b))
	allCols = util.MakeFastIntSet(1, 2)
	ab := &props.FuncDepSet{}
	ab.AddStrictKey(allCols, allCols)
	verifyFD(t, ab, "(1,2): ")
	testColsAreStrictKey(t, ab, util.MakeFastIntSet(1, 2), true)
	testColsAreStrictKey(t, ab, util.MakeFastIntSet(1), false)

	// Empty key.
	empty := &props.FuncDepSet{}
	empty.AddStrictKey(opt.ColSet{}, util.MakeFastIntSet(1))
	verifyFD(t, empty, "(): ()-->(1)")
	testColsAreStrictKey(t, empty, util.MakeFastIntSet(), true)
	testColsAreStrictKey(t, empty, util.MakeFastIntSet(1), true)
}

func TestFuncDeps_AddLaxKey(t *testing.T) {
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// CREATE UNIQUE INDEX idx ON mnpq (p)
	mnpq := makeMnpqFD(t)
	allCols := util.MakeFastIntSet(10, 11, 12, 13)
	mnpq.AddLaxKey(util.MakeFastIntSet(12), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13), (12)~~>(10,11,13)")
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(12), false)
	testColsAreLaxKey(t, mnpq, util.MakeFastIntSet(12), true)
	testColsAreLaxKey(t, mnpq, util.MakeFastIntSet(10, 11), true)

	// CREATE UNIQUE INDEX idx ON mnpq (m, n, p)
	mnpq = makeMnpqFD(t)
	mnpq.AddLaxKey(util.MakeFastIntSet(10, 11, 12), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13)")
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(10, 11), true)
	testColsAreLaxKey(t, mnpq, util.MakeFastIntSet(10, 11), true)
	testColsAreLaxKey(t, mnpq, util.MakeFastIntSet(10, 11, 12), true)

	// No key.
	empty := &props.FuncDepSet{}
	empty.AddLaxKey(opt.ColSet{}, util.MakeFastIntSet(1))
	verifyFD(t, empty, "()~~>(1)")
	testColsAreStrictKey(t, empty, util.MakeFastIntSet(), false)
	testColsAreLaxKey(t, empty, util.MakeFastIntSet(), false)
}

func TestFuncDeps_MakeMax1Row(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde LIMIT 1
	abcde := makeAbcdeFD(t)
	abcde.MakeMax1Row(util.MakeFastIntSet(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "(): ()-->(1-5)")
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(), true)

	// No columns.
	abcde = makeAbcdeFD(t)
	abcde.MakeMax1Row(opt.ColSet{})
	verifyFD(t, abcde, "(): ")
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(), true)
}

func TestFuncDeps_MakeNotNull(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde WHERE b IS NOT NULL
	abcde := makeAbcdeFD(t)
	abcde.MakeNotNull(util.MakeFastIntSet(2))
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)~~>(1,4,5)")

	// SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)-->(1,4,5)")

	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT * FROM abcde LEFT OUTER JOIN mnpq ON a=1 AND b=1 AND m=1 AND p=1 WHERE p IS NOT NULL
	nullExtendedCols := util.MakeFastIntSet(10, 11, 12, 13)
	loj := makeProductFD(t)
	loj.AddConstants(util.MakeFastIntSet(1, 2, 10, 12))
	verifyFD(t, loj, "(11): ()-->(1-5,10,12), (11)-->(13)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 10, 11, 12))
	verifyFD(t, loj, "(11): ()-->(1-5), (11)-->(10,12,13), ()~~>(10,12)")
	loj.MakeNotNull(util.MakeFastIntSet(1, 2, 12))
	verifyFD(t, loj, "(11): ()-->(1-5,12), (11)-->(10,13), ()~~>(10)")

	// Test MakeNotNull triggering key reduction.
	//   SELECT * FROM (SELECT DISTINCT b, c, d, e FROM abcde) WHERE b IS NOT NULL AND c IS NOT NULL
	allCols := util.MakeFastIntSet(2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	abcde.ProjectCols(allCols)
	abcde.AddStrictKey(allCols, allCols)
	verifyFD(t, abcde, "(2-5): (2,3)~~>(4,5)")
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(2,3): (2,3)-->(4,5)")
}

func TestFuncDeps_AddEquivalency(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT * FROM abcde, mnpq
	product := makeProductFD(t)

	// Multiple equivalencies.
	//   SELECT * FROM abcde, mnpq WHERE b=m AND c=n AND d=d
	var bmcn props.FuncDepSet
	bmcn.CopyFrom(product)
	bmcn.AddEquivalency(2, 10)
	bmcn.AddEquivalency(3, 11)
	bmcn.AddEquivalency(4, 4)
	verifyFD(t, &bmcn, "(1): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (2)==(10), (10)==(2), (3)==(11), (11)==(3)")
	testColsAreStrictKey(t, &bmcn, util.MakeFastIntSet(2, 3, 4, 5, 10, 11, 12, 13), false)

	// SELECT * FROM abcde, mnpq WHERE a=m AND a=n
	var amn props.FuncDepSet
	amn.CopyFrom(product)
	amn.AddEquivalency(1, 10)
	amn.AddEquivalency(1, 11)
	verifyFD(t, &amn, "(11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10,11), (10)==(1,11), (11)==(1,10)")
	testColsAreStrictKey(t, &amn, util.MakeFastIntSet(1), true)
	testColsAreStrictKey(t, &amn, util.MakeFastIntSet(10), true)
	testColsAreStrictKey(t, &amn, util.MakeFastIntSet(11), true)

	// Override weaker dependencies with equivalency.
	//   CREATE TABLE ab (a INT PRIMARY KEY, b INT, UNIQUE(b))
	//   SELECT * FROM ab WHERE a=b
	allCols := util.MakeFastIntSet(1, 2)
	ab := &props.FuncDepSet{}
	ab.AddStrictKey(util.MakeFastIntSet(1), allCols)
	ab.AddLaxKey(util.MakeFastIntSet(2), allCols)
	verifyFD(t, ab, "(1): (1)-->(2), (2)~~>(1)")
	ab.AddEquivalency(1, 2)
	verifyFD(t, ab, "(1): (1)==(2), (2)==(1)")
	testColsAreStrictKey(t, ab, util.MakeFastIntSet(2), true)

	// Multiple equivalencies + constant.
	//   SELECT * FROM abcde, mnpq ON a=m WHERE m=n AND n=1
	cnst := makeJoinFD(t)
	cnst.AddEquivalency(10, 11)
	cnst.AddConstants(util.MakeFastIntSet(11))
	verifyFD(t, cnst, "(): ()-->(1-5,10-13), (1)==(10,11), (10)==(1,11), (11)==(1,10)")
}

func TestFuncDeps_AddConstants(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde WHERE c>2
	abcde := makeAbcdeFD(t)
	abcde.AddConstants(util.MakeFastIntSet(2))
	verifyFD(t, abcde, "(1): ()-->(2), (1)-->(3-5), (2,3)~~>(1,4,5)")
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(1): ()-->(2), (1)-->(3-5), (2,3)-->(1,4,5)")
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(3), true)

	// CREATE TABLE wxyz (w INT, x INT, y INT, z INT, PRIMARY KEY(w, x, y, z))
	// SELECT * FROM wxyz WHERE x IS NULL AND y IS NULL
	allCols := util.MakeFastIntSet(1, 2, 3, 4)
	xyz := &props.FuncDepSet{}
	xyz.AddStrictKey(allCols, allCols)
	xyz.AddConstants(util.MakeFastIntSet(2, 3))
	verifyFD(t, xyz, "(1,4): ()-->(2,3)")
	testColsAreStrictKey(t, xyz, util.MakeFastIntSet(2, 3), false)

	// SELECT * FROM (SELECT * FROM wxyz WHERE x=1) WHERE y=2
	allCols = util.MakeFastIntSet(1, 2, 3, 4)
	xyz = &props.FuncDepSet{}
	xyz.AddStrictKey(allCols, allCols)
	xyz.AddConstants(util.MakeFastIntSet(2))
	xyz.MakeNotNull(util.MakeFastIntSet(2))
	xyz.AddConstants(util.MakeFastIntSet(3))
	xyz.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, xyz, "(1,4): ()-->(2,3)")

	// SELECT * FROM (SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL) WHERE b=1
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)-->(1,4,5)")
	abcde.AddConstants(util.MakeFastIntSet(2))
	verifyFD(t, abcde, "(1): ()-->(2), (1)-->(3-5), (3)-->(1,4,5)")

	// SELECT * FROM (SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL) WHERE b=1 AND c=2
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	abcde.AddConstants(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(): ()-->(1-5)")

	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT a, m, n FROM abcde, mnpq WHERE a=m AND n IS NULL
	var am props.FuncDepSet
	am.CopyFrom(makeJoinFD(t))
	am.AddConstants(util.MakeFastIntSet(11))
	verifyFD(t, &am, "(10): ()-->(11), (1)-->(2-5), (2,3)~~>(1,4,5), (10)-->(12,13), (1)==(10), (10)==(1)")
	am.ProjectCols(util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, &am, "(10): ()-->(11), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, &am, util.MakeFastIntSet(1), true)
	testColsAreStrictKey(t, &am, util.MakeFastIntSet(1, 10), true)

	// Equivalency, with one of equivalent columns set to constant.
	//   SELECT * FROM abcde, mnpq WHERE a=m AND m=5
	var eqConst props.FuncDepSet
	eqConst.CopyFrom(makeJoinFD(t))
	eqConst.AddConstants(util.MakeFastIntSet(10))
	eqConst.MakeNotNull(util.MakeFastIntSet(10))
	verifyFD(t, &eqConst, "(11): ()-->(1-5,10), (11)-->(12,13), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, &eqConst, util.MakeFastIntSet(1, 2, 3, 10, 12), false)
}

// Figure, page references are from this paper:
// Norman Paulley, Glenn. (2000).
// Exploiting Functional Dependence in Query Optimization.
// https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf
func TestFuncDeps_AddSynthesizedCol(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	abcde := makeAbcdeFD(t)

	// Construct FD from figure 3.4, page 119:
	//   SELECT a, b, d, e, func(b, c) AS f FROM abcde
	var abdef props.FuncDepSet
	abdef.CopyFrom(abcde)
	abdef.AddSynthesizedCol(util.MakeFastIntSet(2, 3), 6)
	verifyFD(t, &abdef, "(1): (1)-->(2-5), (2,3)~~>(1,4,5), (2,3)-->(6)")
	abdef.ProjectCols(util.MakeFastIntSet(1, 2, 4, 5, 6))
	verifyFD(t, &abdef, "(1): (1)-->(2,4-6)")

	// Add another synthesized column, based on the first synthesized column.
	abdef.AddSynthesizedCol(util.MakeFastIntSet(6), 7)
	verifyFD(t, &abdef, "(1): (1)-->(2,4-6), (6)-->(7)")
	testColsAreStrictKey(t, &abdef, util.MakeFastIntSet(2, 3), false)

	// Add a constant synthesized column, not based on any other column.
	abdef.AddSynthesizedCol(opt.ColSet{}, 8)
	verifyFD(t, &abdef, "(1): ()-->(8), (1)-->(2,4-6), (6)-->(7)")
	testColsAreStrictKey(t, &abdef, util.MakeFastIntSet(2, 3, 4, 5, 6, 7, 8), false)

	// Remove columns and add computed column.
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde, mnpq WHERE a=m
	//   SELECT a, n, b+1 FROM abcde, mnpq WHERE a=m
	var anb1 props.FuncDepSet
	anb1.CopyFrom(makeJoinFD(t))
	anb1.AddSynthesizedCol(util.MakeFastIntSet(2), 100)
	verifyFD(t, &anb1, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1), (2)-->(100)")
	anb1.ProjectCols(util.MakeFastIntSet(1, 11, 100))
	verifyFD(t, &anb1, "(1,11): (1)-->(100)")
	testColsAreStrictKey(t, &anb1, util.MakeFastIntSet(1, 11, 100), true)
}

func TestFuncDeps_ProjectCols(t *testing.T) {
	// Remove column from lax dependency.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   SELECT a, c, d, e FROM abcde
	abde := makeAbcdeFD(t)
	abde.ProjectCols(util.MakeFastIntSet(1, 3, 4, 5))
	verifyFD(t, abde, "(1): (1)-->(3-5)")

	// Try removing columns that are only dependants (i.e. never determinants).
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde, mnpq WHERE a=m
	//   SELECT a, b, c, m, n FROM abcde, mnpq WHERE a=m
	var abcmn props.FuncDepSet
	abcmn.CopyFrom(makeJoinFD(t))
	abcmn.ProjectCols(util.MakeFastIntSet(1, 2, 3, 10, 11))
	verifyFD(t, &abcmn, "(10,11): (1)-->(2,3), (2,3)~~>(1,10), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, &abcmn, util.MakeFastIntSet(1, 11), true)
	testColsAreStrictKey(t, &abcmn, util.MakeFastIntSet(2, 3), false)

	// Remove column that is constant and part of multi-column determinant.
	//   SELECT a, c, d, e FROM abcde WHERE b=1
	abcde := makeAbcdeFD(t)
	abcde.AddConstants(util.MakeFastIntSet(2))
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(1): ()-->(2), (1)-->(3-5), (2,3)-->(1,4,5)")
	abcde.ProjectCols(util.MakeFastIntSet(1, 3, 4, 5))
	verifyFD(t, abcde, "(1): (1)-->(3-5), (3)-->(1,4,5)")

	// Remove key columns, but expect another key to be found.
	//   SELECT b, c, n FROM abcde, mnpq WHERE a=m AND b IS NOT NULL AND c IS NOT NULL
	switchKey := makeJoinFD(t)
	switchKey.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, switchKey, "(10,11): (1)-->(2-5), (2,3)-->(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	switchKey.ProjectCols(util.MakeFastIntSet(2, 3, 11))
	verifyFD(t, switchKey, "(2,3,11): ")

	// Remove column from every determinant and ensure that all FDs go away.
	//   SELECT d FROM abcde, mnpq WHERE a=m AND 1=1 AND n=2
	noKey := makeJoinFD(t)
	verifyFD(t, noKey, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	noKey.ProjectCols(util.MakeFastIntSet(2, 11))
	verifyFD(t, noKey, "")
	testColsAreStrictKey(t, noKey, util.MakeFastIntSet(), false)

	// Remove columns so that there is no longer a key.
	//   SELECT b, c, d, e, n, p, q FROM abcde, mnpq WHERE a=m
	var bcden props.FuncDepSet
	bcden.CopyFrom(makeJoinFD(t))
	bcden.ProjectCols(util.MakeFastIntSet(2, 3, 4, 5, 11, 12, 13))
	verifyFD(t, &bcden, "(2,3)~~>(4,5)")
	testColsAreStrictKey(t, &bcden, util.MakeFastIntSet(2, 3, 4, 5, 11, 12, 13), false)

	// Remove remainder of columns (N rows, 0 cols projected).
	bcden.ProjectCols(opt.ColSet{})
	verifyFD(t, &bcden, "")

	// Project single column.
	//   SELECT d FROM abcde, mnpq WHERE a=m AND a=1 AND n=1
	oneRow := makeJoinFD(t)
	oneRow.AddConstants(util.MakeFastIntSet(1, 11))
	verifyFD(t, oneRow, "(): ()-->(1-5,10-13), (1)==(10), (10)==(1)")
	oneRow.ProjectCols(util.MakeFastIntSet(4))
	verifyFD(t, oneRow, "(): ()-->(4)")

	// Remove column that has equivalent substitute.
	//   SELECT e, one FROM (SELECT *, d+1 AS one FROM abcde) WHERE d=e
	abcde = makeAbcdeFD(t)
	abcde.AddSynthesizedCol(util.MakeFastIntSet(4), 6)
	abcde.AddEquivalency(4, 5)
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)~~>(1,4,5), (4)-->(6), (4)==(5), (5)==(4)")
	abcde.ProjectCols(util.MakeFastIntSet(5, 6))
	verifyFD(t, abcde, "(5)-->(6)")

	// Remove column that has equivalent substitute and is part of composite
	// determinant.
	//   SELECT d, e FROM abcde WHERE b=d AND c=e
	abcde = makeAbcdeFD(t)
	abcde.AddEquivalency(2, 4)
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)~~>(1,4,5), (2)==(4), (4)==(2)")
	abcde.ProjectCols(util.MakeFastIntSet(3, 4, 5))
	verifyFD(t, abcde, "(3,4)~~>(5)")

	// Equivalent substitution results in (4,5)~~>(4,5), which is eliminated.
	//   SELECT d, e FROM abcde WHERE b=d AND c=e
	abcde = makeAbcdeFD(t)
	abcde.AddEquivalency(2, 4)
	abcde.AddEquivalency(3, 5)
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)~~>(1,4,5), (2)==(4), (4)==(2), (3)==(5), (5)==(3)")
	abcde.ProjectCols(util.MakeFastIntSet(4, 5))
	verifyFD(t, abcde, "")

	// Use ProjectCols to add columns (make sure key is extended).
	//   SELECT d, e FROM abcde WHERE b=d AND c=e
	abcde = makeAbcdeFD(t)
	abcde.ProjectCols(util.MakeFastIntSet(1, 2, 3, 4, 5, 6, 7))
	verifyFD(t, abcde, "(1): (1)-->(2-7), (2,3)~~>(1,4,5)")
}

func TestFuncDeps_AddFrom(t *testing.T) {
	// Remove lax dependency, then add it back.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	abcde := makeAbcdeFD(t)
	abcde.ProjectCols(util.MakeFastIntSet(1, 2, 4))
	verifyFD(t, abcde, "(1): (1)-->(2,4)")
	abcde.AddFrom(makeAbcdeFD(t))
	abcde.AddStrictKey(util.MakeFastIntSet(1), util.MakeFastIntSet(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(1), true)

	// Remove strict dependency, then add it back.
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	abcde.ProjectCols(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(2,3): ")
	abcde.AddFrom(makeAbcdeFD(t))
	abcde.AddStrictKey(util.MakeFastIntSet(2, 3), util.MakeFastIntSet(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "(2,3): (1)-->(2-5), (2,3)-->(1,4,5)")
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(1), true)
}

func TestFuncDeps_AddEquivFrom(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT * FROM abcde, mnpq WHERE a=m AND b=n
	product := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	product.MakeProduct(mnpq)
	product.AddEquivalency(1, 10)
	verifyFD(t, product, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")

	var equiv props.FuncDepSet
	equiv.AddEquivFrom(product)
	verifyFD(t, &equiv, "(1)==(10), (10)==(1)")

	product.AddEquivalency(2, 11)
	equiv.ProjectCols(opt.ColSet{})
	equiv.AddEquivFrom(product)
	verifyFD(t, &equiv, "(1)==(10), (10)==(1), (2)==(11), (11)==(2)")
}

func TestFuncDeps_MakeProduct(t *testing.T) {
	// Union dependencies and removed columns and keys:
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM (SELECT a, b, c FROM abcde WHERE d=e), (SELECT m, n FROM mnpq WHERE p=q)
	product := makeAbcdeFD(t)
	product.AddEquivalency(4, 5)
	product.ProjectCols(util.MakeFastIntSet(1, 2, 3))
	mnpq := makeMnpqFD(t)
	mnpq.AddEquivalency(12, 13)
	mnpq.ProjectCols(util.MakeFastIntSet(10, 11))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(1,10,11): (1)-->(2,3), (2,3)~~>(1)")

	// Constants on both sides.
	//   SELECT * FROM (SELECT * FROM abcde b=1), (SELECT * FROM mnpq WHERE p=1)
	product = makeAbcdeFD(t)
	product.AddConstants(util.MakeFastIntSet(2))
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(util.MakeFastIntSet(12))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(1,10,11): ()-->(2,12), (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(13)")

	// Key only on left side:
	//   SELECT * FROM abcde, (SELECT p, q FROM mnpq)
	product = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.ProjectCols(util.MakeFastIntSet(12, 13))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreStrictKey(t, product, util.MakeFastIntSet(1, 2, 3, 4, 5, 12, 13), false)

	// Key only on right side:
	//   SELECT * FROM (SELECT d, e FROM abcde), mnpq
	product = makeAbcdeFD(t)
	product.ProjectCols(util.MakeFastIntSet(4, 5))
	product.MakeProduct(makeMnpqFD(t))
	verifyFD(t, product, "(10,11)-->(12,13)")
	testColsAreStrictKey(t, product, util.MakeFastIntSet(4, 5, 10, 11, 12, 13), false)
}

func TestFuncDeps_MakeApply(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT *
	//   FROM abcde
	//   INNER JOIN LATERAL (SELECT * FROM mnpq WHERE m=a LIMIT 1)
	//   ON True
	abcde := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	mnpq.MakeMax1Row(util.MakeFastIntSet(10, 11, 12, 13))
	verifyFD(t, mnpq, "(): ()-->(10-13)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "(1): (1)-->(2-5,10-13), (2,3)~~>(1,4,5)")

	// SELECT *
	// FROM abcde
	// INNER JOIN LATERAL (SELECT * FROM mnpq WHERE m=a AND p=1)
	// ON True
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(util.MakeFastIntSet(10, 12))
	verifyFD(t, mnpq, "(11): ()-->(10,12), (11)-->(13)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "(1,11): (1)-->(2-5), (2,3)~~>(1,4,5), (1,11)-->(10,12,13)")

	// SELECT *
	// FROM abcde
	// INNER JOIN LATERAL (SELECT * FROM mnpq WHERE m=a AND p=q)
	// ON True
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(util.MakeFastIntSet(10))
	mnpq.AddEquivalency(12, 13)
	verifyFD(t, mnpq, "(11): ()-->(10), (11)-->(12,13), (12)==(13), (13)==(12)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "(1,11): (1)-->(2-5), (2,3)~~>(1,4,5), (1,11)-->(10,12,13), (12)==(13), (13)==(12)")

	// No key in outer relation.
	//   SELECT *
	//   FROM (SELECT b, c, d, e FROM abcde)
	//   INNER JOIN LATERAL (SELECT * FROM mnpq WHERE p=q AND n=1)
	//   ON True
	abcde = makeAbcdeFD(t)
	abcde.ProjectCols(util.MakeFastIntSet(2, 3, 4, 5))
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(util.MakeFastIntSet(11))
	mnpq.AddEquivalency(12, 13)
	verifyFD(t, mnpq, "(10): ()-->(11), (10)-->(12,13), (12)==(13), (13)==(12)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "(2,3)~~>(4,5), (12)==(13), (13)==(12)")

	// No key in inner relation.
	//   SELECT *
	//   FROM abcde
	//   INNER JOIN LATERAL (SELECT n, p, q FROM mnpq WHERE n=a AND p=1)
	//   ON True
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(util.MakeFastIntSet(11, 12))
	mnpq.ProjectCols(util.MakeFastIntSet(11, 12, 13))
	verifyFD(t, mnpq, "()-->(11,12)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "(1)-->(2-5), (2,3)~~>(1,4,5)")
}

func TestFuncDeps_MakeOuter(t *testing.T) {
	// All determinant columns in null-supplying side are nullable.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde LEFT OUTER JOIN (SELECT *, p+q FROM mnpq) ON True
	nullExtendedCols := util.MakeFastIntSet(10, 11, 12, 13, 14)
	loj := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	mnpq.AddSynthesizedCol(util.MakeFastIntSet(12, 13), 14)
	loj.MakeProduct(mnpq)
	verifyFD(t, loj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12,13)-->(14)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, loj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12,13)~~>(14), (1,10,11)-->(14)")

	// One determinant column in null-supplying side is not null.
	//   SELECT * FROM abcde LEFT OUTER JOIN (SELECT *, m+q FROM mnpq) ON True
	nullExtendedCols = util.MakeFastIntSet(10, 11, 12, 13, 14)
	loj = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(util.MakeFastIntSet(10, 13), 14)
	loj.MakeProduct(mnpq)
	verifyFD(t, loj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (10,13)-->(14)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, loj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (10,13)-->(14)")

	// Add constants on both sides of outer join.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON b=1 AND c=1 AND p=1
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	roj := makeAbcdeFD(t)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddConstants(util.MakeFastIntSet(2, 3, 12))
	roj.MakeNotNull(util.MakeFastIntSet(2, 3, 12))
	verifyFD(t, roj, "(10,11): ()-->(2,3,12), (1)-->(4,5), (2,3)-->(1,4,5), (10,11)-->(13)")
	roj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 3, 10, 11, 12))
	verifyFD(t, roj, "(10,11): ()-->(12), (1)-->(4,5), (2,3)-->(1,4,5), (10,11)-->(1-5,13), ()~~>(2,3)")

	// Test equivalency on both sides of outer join.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON b=c AND c=d AND m=p AND m=q
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	roj = makeAbcdeFD(t)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddEquivalency(2, 3)
	roj.AddEquivalency(3, 4)
	roj.AddEquivalency(10, 12)
	roj.AddEquivalency(10, 13)
	verifyFD(t, roj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,5), (10,11)-->(12,13), (2)==(3,4), (3)==(2,4), (4)==(2,3), (10)==(12,13), (12)==(10,13), (13)==(10,12)")
	roj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 3, 10, 11, 13))
	verifyFD(t, roj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,5), (10,11)-->(12,13), (2)==(3,4), (3)==(2,4), (4)==(2,3), (10)==(12,13), (12)==(10,13), (13)==(10,12)")

	// Test equivalency that crosses join boundary.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON a=m
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	roj = makeAbcdeFD(t)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddEquivalency(1, 10)
	verifyFD(t, roj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	roj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, roj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1-5,12,13), (1)~~>(10)")

	// Test equivalency that includes columns from both sides of join boundary.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON a=m AND a=b
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	roj = makeAbcdeFD(t)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddEquivalency(1, 10)
	roj.AddEquivalency(1, 2)
	verifyFD(t, roj, "(10,11): (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(2,10), (10)==(1,2), (2)==(1,10)")
	roj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 10, 11))
	verifyFD(t, roj, "(10,11): (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(1-5,12,13), (1)==(2), (2)==(1), (1)~~>(10), (2)~~>(10)")

	// Test multiple calls to MakeOuter, where the first creates determinant with
	// columns from both sides of join.
	//   SELECT * FROM (SELECT * FROM abcde WHERE b=1) FULL JOIN mnpq ON True
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	nullExtendedCols2 := util.MakeFastIntSet(10, 11, 12, 13)
	roj = makeAbcdeFD(t)
	roj.AddConstants(util.MakeFastIntSet(2))
	roj.MakeProduct(makeMnpqFD(t))
	verifyFD(t, roj, "(1,10,11): ()-->(2), (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	roj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 10, 11))
	verifyFD(t, roj, "(1,10,11): (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), ()~~>(2), (1,10,11)-->(2)")
	roj.MakeOuter(nullExtendedCols2, util.MakeFastIntSet(1, 2, 10, 11))
	verifyFD(t, roj, "(1,10,11): (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), ()~~>(2), (1,10,11)-->(2)")

	// Join keyless relations with nullable columns.
	//   SELECT * FROM (SELECT d, e, d+e FROM abcde) LEFT JOIN (SELECT p, q, p+q FROM mnpq) ON True
	nullExtendedCols = util.MakeFastIntSet(12, 13, 14)
	loj = makeAbcdeFD(t)
	loj.AddSynthesizedCol(util.MakeFastIntSet(4, 5), 6)
	loj.ProjectCols(util.MakeFastIntSet(4, 5, 6))
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(util.MakeFastIntSet(12, 13), 14)
	mnpq.ProjectCols(util.MakeFastIntSet(12, 13, 14))
	loj.MakeProduct(mnpq)
	verifyFD(t, loj, "(4,5)-->(6), (12,13)-->(14)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet())
	verifyFD(t, loj, "(4,5)-->(6), (12,13)~~>(14)")
	testColsAreStrictKey(t, loj, util.MakeFastIntSet(4, 5, 6, 12, 13, 14), false)

	// Join keyless relations with not-null columns.
	//   SELECT * FROM (SELECT d, e, d+e FROM abcde WHERE d>e) LEFT JOIN (SELECT p, q, p+q FROM mnpq WHERE p>q) ON True
	nullExtendedCols = util.MakeFastIntSet(12, 13, 14)
	loj = makeAbcdeFD(t)
	loj.AddSynthesizedCol(util.MakeFastIntSet(4, 5), 6)
	loj.ProjectCols(util.MakeFastIntSet(4, 5, 6))
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(util.MakeFastIntSet(12, 13), 14)
	mnpq.ProjectCols(util.MakeFastIntSet(12, 13, 14))
	loj.MakeProduct(mnpq)
	verifyFD(t, loj, "(4,5)-->(6), (12,13)-->(14)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(4, 5, 12, 13))
	verifyFD(t, loj, "(4,5)-->(6), (12,13)-->(14)")
	testColsAreStrictKey(t, loj, util.MakeFastIntSet(4, 5, 6, 12, 13, 14), false)

	// SELECT * FROM abcde LEFT JOIN LATERAL (SELECT p, q, p+q FROM mnpq) ON True
	nullExtendedCols = util.MakeFastIntSet(12, 13, 14)
	loj = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(util.MakeFastIntSet(12, 13), 14)
	mnpq.ProjectCols(util.MakeFastIntSet(12, 13, 14))
	verifyFD(t, mnpq, "(12,13)-->(14)")
	loj.MakeApply(mnpq)
	verifyFD(t, loj, "(1)-->(2-5), (2,3)~~>(1,4,5), (1,12,13)-->(14)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1))
	verifyFD(t, loj, "(1)-->(2-5), (2,3)~~>(1,4,5)")
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
//   CREATE UNIQUE INDEX ON abcde (b, c)
func makeAbcdeFD(t *testing.T) *props.FuncDepSet {
	// Set Key to all cols to start, and ensure it's overridden in AddStrictKey.
	allCols := util.MakeFastIntSet(1, 2, 3, 4, 5)
	abcde := &props.FuncDepSet{}
	abcde.AddStrictKey(util.MakeFastIntSet(1), allCols)
	verifyFD(t, abcde, "(1): (1)-->(2-5)")
	abcde.AddLaxKey(util.MakeFastIntSet(2, 3), allCols)
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(1), true)
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(2, 3), false)
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(1, 2), true)
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(1, 2, 3, 4, 5), true)
	testColsAreStrictKey(t, abcde, util.MakeFastIntSet(4, 5), false)
	testColsAreLaxKey(t, abcde, util.MakeFastIntSet(2, 3), true)
	return abcde
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
func makeMnpqFD(t *testing.T) *props.FuncDepSet {
	allCols := util.MakeFastIntSet(10, 11, 12, 13)
	mnpq := &props.FuncDepSet{}
	mnpq.AddStrictKey(util.MakeFastIntSet(10, 11), allCols)
	mnpq.MakeNotNull(util.MakeFastIntSet(10, 11))
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13)")
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(10), false)
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(10, 11), true)
	testColsAreStrictKey(t, mnpq, util.MakeFastIntSet(10, 11, 12), true)
	return mnpq
}

// Construct cartesian product FD from figure 3.6, page 122:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
//   SELECT * FROM abcde, mnpq
func makeProductFD(t *testing.T) *props.FuncDepSet {
	product := makeAbcdeFD(t)
	product.MakeProduct(makeMnpqFD(t))
	verifyFD(t, product, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	testColsAreStrictKey(t, product, util.MakeFastIntSet(1), false)
	testColsAreStrictKey(t, product, util.MakeFastIntSet(10, 11), false)
	testColsAreStrictKey(t, product, util.MakeFastIntSet(1, 10, 11), true)
	testColsAreStrictKey(t, product, util.MakeFastIntSet(1, 2, 3, 10, 11, 12), true)
	testColsAreStrictKey(t, product, util.MakeFastIntSet(2, 3, 10, 11), false)
	return product
}

// Construct inner join FD:
//   SELECT * FROM abcde, mnpq WHERE a=m
func makeJoinFD(t *testing.T) *props.FuncDepSet {
	// Start with cartesian product FD and add equivalency to it.
	join := makeProductFD(t)
	join.AddEquivalency(1, 10)
	verifyFD(t, join, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	join.ProjectCols(util.MakeFastIntSet(1, 2, 3, 4, 5, 10, 11, 12, 13))
	verifyFD(t, join, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, join, util.MakeFastIntSet(1, 11), true)
	testColsAreStrictKey(t, join, util.MakeFastIntSet(1, 10), false)
	testColsAreStrictKey(t, join, util.MakeFastIntSet(1, 10, 11), true)
	testColsAreStrictKey(t, join, util.MakeFastIntSet(1), false)
	testColsAreStrictKey(t, join, util.MakeFastIntSet(10, 11), true)
	testColsAreStrictKey(t, join, util.MakeFastIntSet(2, 3, 11), false)
	testColsAreLaxKey(t, join, util.MakeFastIntSet(2, 3, 11), true)
	return join
}

func verifyFD(t *testing.T, f *props.FuncDepSet, expected string) {
	t.Helper()
	actual := f.String()
	if actual != expected {
		t.Errorf("\nexpected: %s\nactual  : %s", expected, actual)
	}

	f.Verify()

	if key, ok := f.Key(); ok {
		testColsAreStrictKey(t, f, key, true)
		if !key.Empty() {
			testColsAreStrictKey(t, f, opt.ColSet{}, false)
		}
		closure := f.ComputeClosure(key)
		testColsAreStrictKey(t, f, closure, true)
	} else {
		testColsAreStrictKey(t, f, opt.ColSet{}, false)
	}
}

func testColsAreStrictKey(t *testing.T, f *props.FuncDepSet, cols opt.ColSet, expected bool) {
	t.Helper()
	actual := f.ColsAreStrictKey(cols)
	if actual != expected {
		if expected {
			t.Errorf("%s is not a strict key for %s", cols, f)
		} else {
			t.Errorf("%s is a strict key for %s", cols, f)
		}
	}
}

func testColsAreLaxKey(t *testing.T, f *props.FuncDepSet, cols opt.ColSet, expected bool) {
	t.Helper()
	actual := f.ColsAreLaxKey(cols)
	if actual != expected {
		if expected {
			t.Errorf("%s is not a lax key for %s", cols, f)
		} else {
			t.Errorf("%s is a lax key for %s", cols, f)
		}
	}
}
