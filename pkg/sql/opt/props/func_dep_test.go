// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/stretchr/testify/require"
)

func TestFuncDeps_ConstCols(t *testing.T) {
	fd := &props.FuncDepSet{}
	require.Equal(t, "()", fd.ConstantCols().String())
	fd.AddConstants(c(1, 2))
	require.Equal(t, "(1,2)", fd.ConstantCols().String())

	fd2 := makeAbcdeFD(t)
	require.Equal(t, "()", fd2.ConstantCols().String())
	fd2.AddConstants(c(1, 2))
	require.Equal(t, "(1,2)", fd.ConstantCols().String())
}

// Other tests also exercise the ColsAreKey methods.
func TestFuncDeps_ColsAreKey(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// This case wouldn't actually happen with a real world query.
	var loj props.FuncDepSet
	preservedCols := c(1, 2, 3, 4, 5)
	nullExtendedCols := c(10, 11, 12, 13, 14)
	abcde := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	mnpq.AddSynthesizedCol(c(12, 13), 14)
	loj.CopyFrom(abcde)
	loj.MakeProduct(mnpq)
	loj.AddConstants(c(3))
	loj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 10, 11))
	loj.AddEquivalency(1, 10)
	verifyFD(t, &loj, "key(10,11); ()-->(3), (1)-->(2,4,5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12,13)~~>(14), (1,10,11)-->(14), (1)==(10), (10)==(1)")

	testcases := []struct {
		cols   opt.ColSet
		strict bool
		lax    bool
	}{
		{cols: c(1, 2, 3, 4, 5, 10, 11, 12, 13, 14), strict: true, lax: true},
		{cols: c(1, 2, 3, 4, 5, 10, 12, 13, 14), strict: false, lax: false},
		{cols: c(1, 11), strict: true, lax: true},
		{cols: c(10, 11), strict: true, lax: true},
		{cols: c(1), strict: false, lax: false},
		{cols: c(10), strict: false, lax: false},
		{cols: c(11), strict: false, lax: false},
		{cols: c(), strict: false, lax: false},

		// This case is interesting: if we take into account that 3 is a constant,
		// we could put 2 and 3 together and use (2,3)~~>(1,4,5) and (1)==(10) to
		// prove that (2,3) is a lax key. But this is only true when that constant
		// value for 3 is not NULL. We would have to pass non-null information to
		// the check. See #42731.
		{cols: c(2, 11), strict: false, lax: false},
	}

	for _, tc := range testcases {
		testColsAreStrictKey(t, &loj, tc.cols, tc.strict)
		testColsAreLaxKey(t, &loj, tc.cols, tc.lax)
	}
}

func TestFuncDeps_ComputeClosure(t *testing.T) {
	// (a)-->(b,c,d)
	// (b,c,e)-->(f)
	// (d)==(e)
	// (e)==(d)
	fd1 := &props.FuncDepSet{}
	fd1.AddSynthesizedCol(c(1), 2)
	fd1.AddSynthesizedCol(c(1), 3)
	fd1.AddSynthesizedCol(c(1), 4)
	fd1.AddSynthesizedCol(c(2, 3, 5), 6)
	fd1.AddEquivalency(4, 5)
	verifyFD(t, fd1, "(1)-->(2-4), (2,3,5)-->(6), (4)==(5), (5)==(4)")

	// (a)~~>(d)
	// ()-->(b)
	// (b)==(c)
	// (c)==(b)
	// (d)-->(e)
	fd2 := &props.FuncDepSet{}
	// This isn't intended to create a real lax key; just a lax dependency.
	fd2.AddLaxKey(c(1), c(1, 4))
	fd2.AddConstants(c(2))
	fd2.AddEquivalency(2, 3)
	fd2.AddSynthesizedCol(c(4), 5)
	verifyFD(t, fd2, "lax-key(1); ()-->(2,3), (1)~~>(4), (2)==(3), (3)==(2), (4)-->(5)")

	testcases := []struct {
		fd       *props.FuncDepSet
		in       opt.ColSet
		expected opt.ColSet
	}{
		{fd: fd1, in: c(), expected: c()},
		{fd: fd1, in: c(1), expected: c(1, 2, 3, 4, 5, 6)},
		{fd: fd1, in: c(2), expected: c(2)},
		{fd: fd1, in: c(2, 3, 4), expected: c(2, 3, 4, 5, 6)},
		{fd: fd1, in: c(4), expected: c(4, 5)},

		{fd: fd2, in: c(), expected: c(2, 3)},
		{fd: fd2, in: c(1), expected: c(1, 2, 3)},
		{fd: fd2, in: c(1, 4), expected: c(1, 2, 3, 4, 5)},
	}

	for _, tc := range testcases {
		closure := tc.fd.ComputeClosure(tc.in)
		if !closure.Equals(tc.expected) {
			t.Errorf("in: %s, expected: %s, actual: %s", tc.in, tc.expected, closure)
		}
	}
}

func TestFuncDeps_InClosureOf(t *testing.T) {
	// (a)~~>(d)
	// ()-->(b)
	// (b)==(c)
	// (c)==(b)
	// (d)-->(e)
	fd := &props.FuncDepSet{}
	fd.AddConstants(c(2))
	// This isn't intended to create a real lax key; just a lax dependency.
	fd.AddLaxKey(c(1), c(1, 4))
	fd.AddEquivalency(2, 3)
	fd.AddSynthesizedCol(c(4), 5)
	verifyFD(t, fd, "lax-key(1); ()-->(2,3), (1)~~>(4), (2)==(3), (3)==(2), (4)-->(5)")

	testcases := []struct {
		cols     []opt.ColumnID
		in       []opt.ColumnID
		expected bool
	}{
		{cols: []opt.ColumnID{}, in: []opt.ColumnID{}, expected: true},
		{cols: []opt.ColumnID{}, in: []opt.ColumnID{1}, expected: true},
		{cols: []opt.ColumnID{2, 3}, in: []opt.ColumnID{}, expected: true},
		{cols: []opt.ColumnID{2}, in: []opt.ColumnID{3}, expected: true},
		{cols: []opt.ColumnID{3}, in: []opt.ColumnID{2}, expected: true},
		{cols: []opt.ColumnID{3, 5}, in: []opt.ColumnID{2, 4}, expected: true},

		{cols: []opt.ColumnID{1}, in: []opt.ColumnID{}, expected: false},
		{cols: []opt.ColumnID{4}, in: []opt.ColumnID{5}, expected: false},
		{cols: []opt.ColumnID{2, 3, 4}, in: []opt.ColumnID{1, 2, 3}, expected: false},
	}

	for _, tc := range testcases {
		cols := c(tc.cols...)
		in := c(tc.in...)
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

func TestFuncDepSet_AreColsEquiv(t *testing.T) {
	fd := &props.FuncDepSet{}

	// (a) == (b)
	// (b) == (c)
	// (d) == (e)
	// (a) --> (f)
	// (e) == (g)
	fd.AddEquivalency(1, 2)
	fd.AddEquivalency(2, 3)
	fd.AddEquivalency(4, 5)
	fd.AddSynthesizedCol(c(1), 6)

	testcases := []struct {
		col1, col2 opt.ColumnID
		expected   bool
	}{
		{col1: 1, col2: 2, expected: true},
		{col1: 2, col2: 3, expected: true},
		{col1: 2, col2: 1, expected: true},
		{col1: 1, col2: 3, expected: true},
		{col1: 3, col2: 2, expected: true},
		{col1: 1, col2: 4, expected: false},
		{col1: 1, col2: 6, expected: false},
	}

	for _, tc := range testcases {
		col1 := tc.col1
		col2 := tc.col2
		actual := fd.AreColsEquiv(col1, col2)
		if actual != tc.expected {
			if tc.expected {
				t.Errorf("expected %v to be equal to %v", col1, col2)
			} else {
				t.Errorf("expected %v to not be equal to %v", col1, col2)
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
	// This isn't intended to create a real lax key; just a lax dependency.
	fd1.AddLaxKey(c(1), c(1, 5))
	fd1.AddSynthesizedCol(c(1), 6)
	fd1.AddEquivalency(1, 2)
	fd1.AddEquivalency(2, 3)
	fd1.AddEquivalency(1, 4)
	verifyFD(t, fd1, "lax-key(1); (1)~~>(5), (1)-->(6), (1)==(2-4), (2)==(1,3,4), (3)==(1,2,4), (4)==(1-3)")

	testcases := []struct {
		fd       *props.FuncDepSet
		in       opt.ColSet
		expected opt.ColSet
	}{
		{fd: fd1, in: c(), expected: c()},
		{fd: fd1, in: c(1), expected: c(1, 2, 3, 4)},
		{fd: fd1, in: c(2), expected: c(1, 2, 3, 4)},
		{fd: fd1, in: c(3), expected: c(1, 2, 3, 4)},
		{fd: fd1, in: c(4), expected: c(1, 2, 3, 4)},
		{fd: fd1, in: c(5, 6), expected: c(5, 6)},
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
	// This isn't intended to create a real lax key; just a lax dependency.
	fd1.AddLaxKey(c(1), c(1, 5))
	fd1.AddSynthesizedCol(c(1), 6)
	fd1.AddEquivalency(1, 2)
	fd1.AddEquivalency(2, 3)
	verifyFD(t, fd1, "lax-key(1); (1)~~>(5), (1)-->(6), (1)==(2,3), (2)==(1,3), (3)==(1,2)")

	// (a)==(b,d)
	// (b)==(a,c)
	// (c)==(b)
	// (d)==(a)
	// (a)~~>(e)
	// (a)-->(f)
	fd2 := &props.FuncDepSet{}
	fd2.CopyFrom(fd1)
	fd2.AddEquivalency(1, 4)
	verifyFD(t, fd2, "lax-key(1); (1)~~>(5), (1)-->(6), (1)==(2-4), (2)==(1,3,4), (3)==(1,2,4), (4)==(1-3)")

	// (a)==(b,d)
	// (b)==(a,c)
	// (c)==(b)
	// (d)==(e)
	// (a)~~>(e)
	// (a)-->(f)
	fd3 := &props.FuncDepSet{}
	fd3.CopyFrom(fd1)
	fd3.AddEquivalency(4, 5)
	verifyFD(t, fd3, "lax-key(1); (1)~~>(5), (1)-->(6), (1)==(2,3), (2)==(1,3), (3)==(1,2), (4)==(5), (5)==(4)")

	testcases := []struct {
		fd       *props.FuncDepSet
		expected opt.ColSet
	}{
		{fd: fd1, expected: c(1)},
		{fd: fd2, expected: c(1)},
		{fd: fd3, expected: c(1, 4)},
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
	allCols := c(10, 11, 12, 13)
	mnpq.AddStrictKey(c(12), allCols)
	verifyFD(t, mnpq, "key(12); (10,11)-->(12,13), (12)-->(10,11,13)")
	testColsAreStrictKey(t, mnpq, c(12), true)
	testColsAreStrictKey(t, mnpq, c(13), false)
	testColsAreStrictKey(t, mnpq, c(10, 11), true)

	// SELECT DISTINCT ON (m, n, p) m, n, p, q FROM mnpq
	mnpq = makeMnpqFD(t)
	mnpq.AddStrictKey(c(10, 11, 12), allCols)
	verifyFD(t, mnpq, "key(10,11); (10,11)-->(12,13)")
	testColsAreStrictKey(t, mnpq, c(10, 11), true)
	testColsAreStrictKey(t, mnpq, c(11, 12), false)

	// SELECT DISTINCT ON (n, p, q) m, n, p, q FROM mnpq
	mnpq = makeMnpqFD(t)
	mnpq.AddStrictKey(c(11, 12, 13), allCols)
	verifyFD(t, mnpq, "key(10,11); (10,11)-->(12,13), (11-13)-->(10)")
	testColsAreStrictKey(t, mnpq, c(11, 12, 13), true)
	testColsAreStrictKey(t, mnpq, c(11, 12), false)
	testColsAreStrictKey(t, mnpq, c(10, 11), true)

	// All columns together form a key.
	//   CREATE TABLE ab (a INT, b INT, PRIMARY KEY (a, b))
	allCols = c(1, 2)
	ab := &props.FuncDepSet{}
	ab.AddStrictKey(allCols, allCols)
	verifyFD(t, ab, "key(1,2)")
	testColsAreStrictKey(t, ab, c(1, 2), true)
	testColsAreStrictKey(t, ab, c(1), false)

	// Empty key.
	empty := &props.FuncDepSet{}
	empty.AddStrictKey(opt.ColSet{}, c(1))
	verifyFD(t, empty, "key(); ()-->(1)")
	testColsAreStrictKey(t, empty, c(), true)
	testColsAreStrictKey(t, empty, c(1), true)
}

func TestFuncDeps_AddLaxKey(t *testing.T) {
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// CREATE UNIQUE INDEX idx ON mnpq (p)
	mnpq := makeMnpqFD(t)
	allCols := c(10, 11, 12, 13)
	mnpq.AddLaxKey(c(12), allCols)
	verifyFD(t, mnpq, "key(10,11); (10,11)-->(12,13), (12)~~>(10,11,13)")
	testColsAreStrictKey(t, mnpq, c(12), false)
	testColsAreLaxKey(t, mnpq, c(12), true)
	testColsAreLaxKey(t, mnpq, c(10, 11), true)

	// CREATE UNIQUE INDEX idx ON mnpq (m, n, p)
	mnpq = makeMnpqFD(t)
	mnpq.AddLaxKey(c(10, 11, 12), allCols)
	verifyFD(t, mnpq, "key(10,11); (10,11)-->(12,13)")
	testColsAreStrictKey(t, mnpq, c(10, 11), true)
	testColsAreLaxKey(t, mnpq, c(10, 11), true)
	testColsAreLaxKey(t, mnpq, c(10, 11, 12), true)

	// Verify that a shorter lax key overwrites a longer lax key (but not
	// vice-versa).
	abcde := &props.FuncDepSet{}
	abcde.AddLaxKey(c(2, 3), c(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "lax-key(2,3); (2,3)~~>(1,4,5)")
	abcde.AddLaxKey(c(1), c(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "lax-key(1); (2,3)~~>(1,4,5), (1)~~>(2-5)")
	abcde.AddLaxKey(c(4, 5), c(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "lax-key(1); (2,3)~~>(1,4,5), (1)~~>(2-5), (4,5)~~>(1-3)")
}

func TestFuncDeps_MakeMax1Row(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde LIMIT 1
	abcde := makeAbcdeFD(t)
	abcde.MakeMax1Row(c(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "key(); ()-->(1-5)")
	testColsAreStrictKey(t, abcde, c(), true)

	// No columns.
	abcde = makeAbcdeFD(t)
	abcde.MakeMax1Row(opt.ColSet{})
	verifyFD(t, abcde, "key()")
	testColsAreStrictKey(t, abcde, c(), true)

	// Retain equivalencies.
	abcde = makeAbcdeFD(t)
	abcde.AddEquivalency(1, 2)
	abcde.AddEquivalency(3, 4)
	abcde.MakeMax1Row(c(1, 2, 3))
	verifyFD(t, abcde, "key(); ()-->(1-3), (2)==(1), (1)==(2)")
	testColsAreStrictKey(t, abcde, c(), true)

	// Retain partial equivalencies. (1)==(2) is extracted from (1)==(2,4).
	abcde = makeAbcdeFD(t)
	abcde.AddEquivalency(1, 2)
	abcde.AddEquivalency(1, 4)
	abcde.MakeMax1Row(c(1, 2, 3))
	verifyFD(t, abcde, "key(); ()-->(1-3), (2)==(1), (1)==(2)")
	testColsAreStrictKey(t, abcde, c(), true)

	// No columns with equivalencies.
	abcde = makeAbcdeFD(t)
	abcde.AddEquivalency(1, 2)
	abcde.AddEquivalency(3, 4)
	abcde.MakeMax1Row(opt.ColSet{})
	verifyFD(t, abcde, "key()")
	testColsAreStrictKey(t, abcde, c(), true)
}

func TestFuncDeps_MakeNotNull(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde WHERE b IS NOT NULL
	abcde := makeAbcdeFD(t)
	abcde.MakeNotNull(c(2))
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5)")

	// SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL
	abcde.MakeNotNull(c(2, 3))
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)-->(1,4,5)")

	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT * FROM (SELECT * FROM abcde WHERE a=1 AND b=1)
	//   LEFT OUTER JOIN (SELECT * FROM mnpq WHERE m=1 AND p=1) ON True
	//   WHERE p IS NOT NULL
	preservedCols := c(1, 2, 3, 4, 5)
	nullExtendedCols := c(10, 11, 12, 13)
	loj := makeProductFD(t)
	loj.AddConstants(c(1, 2, 10, 12))
	verifyFD(t, loj, "key(11); ()-->(1-5,10,12), (11)-->(13)")
	loj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 2, 10, 11, 12))
	verifyFD(t, loj, "key(11); ()-->(1-5), (11)-->(10,12,13)")
	loj.MakeNotNull(c(1, 2, 12))
	verifyFD(t, loj, "key(11); ()-->(1-5), (11)-->(10,12,13)")

	// Test MakeNotNull triggering key reduction.
	//   SELECT * FROM (SELECT DISTINCT b, c, d, e FROM abcde) WHERE b IS NOT NULL AND c IS NOT NULL
	allCols := c(2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	abcde.ProjectCols(allCols)
	abcde.AddStrictKey(allCols, allCols)
	verifyFD(t, abcde, "key(2-5); (2,3)~~>(4,5)")
	abcde.MakeNotNull(c(2, 3))
	verifyFD(t, abcde, "key(2,3); (2,3)-->(4,5)")

	// Test lax key to strong key conversion.
	abc := &props.FuncDepSet{}
	abc.AddLaxKey(c(2, 3), c(1, 2, 3))
	verifyFD(t, abc, "lax-key(2,3); (2,3)~~>(1)")
	abc.MakeNotNull(c(2))
	verifyFD(t, abc, "lax-key(2,3); (2,3)~~>(1)")
	abc.MakeNotNull(c(2, 3))
	verifyFD(t, abc, "key(2,3); (2,3)-->(1)")
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
	verifyFD(t, &bmcn, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (2)==(10), (10)==(2), (3)==(11), (11)==(3)")
	testColsAreStrictKey(t, &bmcn, c(2, 3, 4, 5, 10, 11, 12, 13), false)

	// SELECT * FROM abcde, mnpq WHERE a=m AND a=n
	var amn props.FuncDepSet
	amn.CopyFrom(product)
	amn.AddEquivalency(1, 10)
	amn.AddEquivalency(1, 11)
	verifyFD(t, &amn, "key(11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10,11), (10)==(1,11), (11)==(1,10)")
	testColsAreStrictKey(t, &amn, c(1), true)
	testColsAreStrictKey(t, &amn, c(10), true)
	testColsAreStrictKey(t, &amn, c(11), true)

	// Override weaker dependencies with equivalency.
	//   CREATE TABLE ab (a INT PRIMARY KEY, b INT, UNIQUE(b))
	//   SELECT * FROM ab WHERE a=b
	allCols := c(1, 2)
	ab := &props.FuncDepSet{}
	ab.AddStrictKey(c(1), allCols)
	ab.AddLaxKey(c(2), allCols)
	verifyFD(t, ab, "key(1); (1)-->(2), (2)~~>(1)")
	ab.AddEquivalency(1, 2)
	verifyFD(t, ab, "key(1); (1)==(2), (2)==(1)")
	testColsAreStrictKey(t, ab, c(2), true)

	// Multiple equivalencies + constant.
	//   SELECT * FROM abcde, mnpq ON a=m WHERE m=n AND n=1
	cnst := makeJoinFD(t)
	cnst.AddEquivalency(10, 11)
	cnst.AddConstants(c(11))
	verifyFD(t, cnst, "key(); ()-->(1-5,10-13), (1)==(10,11), (10)==(1,11), (11)==(1,10)")
}

func TestFuncDeps_AddConstants(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde WHERE c>2
	abcde := makeAbcdeFD(t)
	abcde.AddConstants(c(2))
	verifyFD(t, abcde, "key(1); ()-->(2), (1)-->(3-5), (2,3)~~>(1,4,5)")
	abcde.MakeNotNull(c(2, 3))
	verifyFD(t, abcde, "key(1); ()-->(2), (1)-->(3-5), (3)-->(1,4,5)")
	testColsAreStrictKey(t, abcde, c(3), true)

	// CREATE TABLE wxyz (w INT, x INT, y INT, z INT, PRIMARY KEY(w, x, y, z))
	// SELECT * FROM wxyz WHERE x IS NULL AND y IS NULL
	allCols := c(1, 2, 3, 4)
	xyz := &props.FuncDepSet{}
	xyz.AddStrictKey(allCols, allCols)
	xyz.AddConstants(c(2, 3))
	verifyFD(t, xyz, "key(1,4); ()-->(2,3)")
	testColsAreStrictKey(t, xyz, c(2, 3), false)

	// SELECT * FROM (SELECT * FROM wxyz WHERE x=1) WHERE y=2
	allCols = c(1, 2, 3, 4)
	xyz = &props.FuncDepSet{}
	xyz.AddStrictKey(allCols, allCols)
	xyz.AddConstants(c(2))
	xyz.MakeNotNull(c(2))
	xyz.AddConstants(c(3))
	xyz.MakeNotNull(c(2, 3))
	verifyFD(t, xyz, "key(1,4); ()-->(2,3)")

	// SELECT * FROM (SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL) WHERE b=1
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(c(2, 3))
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)-->(1,4,5)")
	abcde.AddConstants(c(2))
	verifyFD(t, abcde, "key(1); ()-->(2), (1)-->(3-5), (3)-->(1,4,5)")

	// SELECT * FROM (SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL) WHERE b=1 AND c=2
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(c(2, 3))
	abcde.AddConstants(c(2, 3))
	verifyFD(t, abcde, "key(); ()-->(1-5)")

	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT a, m, n FROM abcde, mnpq WHERE a=m AND n IS NULL
	var am props.FuncDepSet
	am.CopyFrom(makeJoinFD(t))
	am.AddConstants(c(11))
	verifyFD(t, &am, "key(10); ()-->(11), (1)-->(2-5), (2,3)~~>(1,4,5), (10)-->(12,13), (1)==(10), (10)==(1)")
	am.ProjectCols(c(1, 10, 11))
	verifyFD(t, &am, "key(10); ()-->(11), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, &am, c(1), true)
	testColsAreStrictKey(t, &am, c(1, 10), true)

	// Equivalency, with one of equivalent columns set to constant.
	//   SELECT * FROM abcde, mnpq WHERE a=m AND m=5
	var eqConst props.FuncDepSet
	eqConst.CopyFrom(makeJoinFD(t))
	eqConst.AddConstants(c(10))
	eqConst.MakeNotNull(c(10))
	verifyFD(t, &eqConst, "key(11); ()-->(1-5,10), (11)-->(12,13), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, &eqConst, c(1, 2, 3, 10, 12), false)
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
	abdef.AddSynthesizedCol(c(2, 3), 6)
	verifyFD(t, &abdef, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5), (2,3)-->(6)")
	abdef.ProjectCols(c(1, 2, 4, 5, 6))
	verifyFD(t, &abdef, "key(1); (1)-->(2,4-6)")

	// Add another synthesized column, based on the first synthesized column.
	abdef.AddSynthesizedCol(c(6), 7)
	verifyFD(t, &abdef, "key(1); (1)-->(2,4-6), (6)-->(7)")
	testColsAreStrictKey(t, &abdef, c(2, 3), false)

	// Add a constant synthesized column, not based on any other column.
	abdef.AddSynthesizedCol(opt.ColSet{}, 8)
	verifyFD(t, &abdef, "key(1); ()-->(8), (1)-->(2,4-6), (6)-->(7)")
	testColsAreStrictKey(t, &abdef, c(2, 3, 4, 5, 6, 7, 8), false)

	// Remove columns and add computed column.
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde, mnpq WHERE a=m
	//   SELECT a, n, b+1 FROM abcde, mnpq WHERE a=m
	var anb1 props.FuncDepSet
	anb1.CopyFrom(makeJoinFD(t))
	anb1.AddSynthesizedCol(c(2), 100)
	verifyFD(t, &anb1, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1), (2)-->(100)")
	anb1.ProjectCols(c(1, 11, 100))
	verifyFD(t, &anb1, "key(1,11); (1)-->(100)")
	testColsAreStrictKey(t, &anb1, c(1, 11, 100), true)

	// Test that we are reducing the "from" columns.
	fd := &props.FuncDepSet{}
	fd.AddStrictKey(opt.MakeColSet(1), opt.MakeColSet(1, 2))
	fd.AddSynthesizedCol(opt.MakeColSet(1, 2), 3)
	verifyFD(t, fd, "key(1); (1)-->(2,3)")
}

func TestFuncDeps_ProjectCols(t *testing.T) {
	foo := &props.FuncDepSet{}
	all := c(1, 2, 3, 4)
	foo.AddStrictKey(c(1), all)
	foo.AddLaxKey(c(2, 3), all)
	foo.AddLaxKey(c(4), all)
	verifyFD(t, foo, "key(1); (1)-->(2-4), (2,3)~~>(1,4), (4)~~>(1-3)")
	foo.ProjectCols(c(2, 3, 4))
	verifyFD(t, foo, "lax-key(2-4); (2,3)~~>(4), (4)~~>(2,3)")
	foo.MakeNotNull(c(2, 3, 4))
	verifyFD(t, foo, "key(4); (2,3)-->(4), (4)-->(2,3)")

	x := makeAbcdeFD(t)
	x.ProjectCols(c(2, 3))
	verifyFD(t, x, "lax-key(2,3)")

	x = makeAbcdeFD(t)
	x.MakeNotNull(c(2, 3))
	x.ProjectCols(c(2, 3))
	verifyFD(t, x, "key(2,3)")

	// Remove column from lax dependency.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   SELECT a, c, d, e FROM abcde
	abde := makeAbcdeFD(t)
	abde.ProjectCols(c(1, 3, 4, 5))
	verifyFD(t, abde, "key(1); (1)-->(3-5)")

	// Try removing columns that are only dependants (i.e. never determinants).
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde, mnpq WHERE a=m
	//   SELECT a, b, c, m, n FROM abcde, mnpq WHERE a=m
	var abcmn props.FuncDepSet
	abcmn.CopyFrom(makeJoinFD(t))
	abcmn.ProjectCols(c(1, 2, 3, 10, 11))
	verifyFD(t, &abcmn, "key(10,11); (1)-->(2,3), (2,3)~~>(1,10), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, &abcmn, c(1, 11), true)
	testColsAreStrictKey(t, &abcmn, c(2, 3), false)

	// Remove column that is constant and part of multi-column determinant.
	//   SELECT a, c, d, e FROM abcde WHERE b=1
	abcde := makeAbcdeFD(t)
	abcde.AddConstants(c(2))
	abcde.MakeNotNull(c(2, 3))
	verifyFD(t, abcde, "key(1); ()-->(2), (1)-->(3-5), (3)-->(1,4,5)")
	abcde.ProjectCols(c(1, 3, 4, 5))
	verifyFD(t, abcde, "key(1); (1)-->(3-5), (3)-->(1,4,5)")

	// Remove key columns, but expect another key to be found.
	//   SELECT b, c, n FROM abcde, mnpq WHERE a=m AND b IS NOT NULL AND c IS NOT NULL
	switchKey := makeJoinFD(t)
	switchKey.MakeNotNull(c(2, 3))
	verifyFD(t, switchKey, "key(10,11); (1)-->(2-5), (2,3)-->(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	switchKey.ProjectCols(c(2, 3, 11))
	verifyFD(t, switchKey, "key(2,3,11)")

	// Remove column from every determinant and ensure that all FDs go away.
	//   SELECT d FROM abcde, mnpq WHERE a=m AND 1=1 AND n=2
	noKey := makeJoinFD(t)
	verifyFD(t, noKey, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	noKey.ProjectCols(c(2, 11))
	verifyFD(t, noKey, "")
	testColsAreStrictKey(t, noKey, c(), false)

	// Remove columns so that there is no longer a key.
	//   SELECT b, c, d, e, n, p, q FROM abcde, mnpq WHERE a=m
	var bcden props.FuncDepSet
	bcden.CopyFrom(makeJoinFD(t))
	bcden.ProjectCols(c(2, 3, 4, 5, 11, 12, 13))
	verifyFD(t, &bcden, "lax-key(2-5,11-13); (2,3)~~>(4,5)")
	testColsAreStrictKey(t, &bcden, c(2, 3, 4, 5, 11, 12, 13), false)
	testColsAreLaxKey(t, &bcden, c(2, 3, 4, 5, 11, 12, 13), true)

	// Remove remainder of columns (N rows, 0 cols projected).
	bcden.ProjectCols(opt.ColSet{})
	verifyFD(t, &bcden, "")

	// Project single column.
	//   SELECT d FROM abcde, mnpq WHERE a=m AND a=1 AND n=1
	oneRow := makeJoinFD(t)
	oneRow.AddConstants(c(1, 11))
	verifyFD(t, oneRow, "key(); ()-->(1-5,10-13), (1)==(10), (10)==(1)")
	oneRow.ProjectCols(c(4))
	verifyFD(t, oneRow, "key(); ()-->(4)")

	// Remove column that has equivalent substitute.
	//   SELECT e, one FROM (SELECT *, d+1 AS one FROM abcde) WHERE d=e
	abcde = makeAbcdeFD(t)
	abcde.AddSynthesizedCol(c(4), 6)
	abcde.AddEquivalency(4, 5)
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5), (4)-->(6), (4)==(5), (5)==(4)")
	abcde.ProjectCols(c(5, 6))
	verifyFD(t, abcde, "(5)-->(6)")

	// Remove column that has equivalent substitute and is part of composite
	// determinant.
	//   SELECT d, e FROM abcde WHERE b=d AND c=e
	abcde = makeAbcdeFD(t)
	abcde.AddEquivalency(2, 4)
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5), (2)==(4), (4)==(2)")
	abcde.ProjectCols(c(3, 4, 5))
	verifyFD(t, abcde, "lax-key(3-5); (3,4)~~>(5)")

	// Equivalent substitution results in (4,5)~~>(4,5), which is eliminated.
	//   SELECT d, e FROM abcde WHERE b=d AND c=e
	abcde = makeAbcdeFD(t)
	abcde.AddEquivalency(2, 4)
	abcde.AddEquivalency(3, 5)
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5), (2)==(4), (4)==(2), (3)==(5), (5)==(3)")
	abcde.ProjectCols(c(4, 5))
	verifyFD(t, abcde, "lax-key(4,5)")

	// Use ProjectCols to add columns (make sure key is extended).
	//   SELECT d, e FROM abcde WHERE b=d AND c=e
	abcde = makeAbcdeFD(t)
	abcde.ProjectCols(c(1, 2, 3, 4, 5, 6, 7))
	verifyFD(t, abcde, "key(1); (1)-->(2-7), (2,3)~~>(1,4,5)")

	// Verify lax keys are retained (and can later become keys) when the key is
	// projected away.
	abcde = &props.FuncDepSet{}
	abcde.AddStrictKey(c(1), c(1, 2, 3, 4, 5))
	abcde.AddLaxKey(c(2), c(1, 2, 3, 4, 5))
	abcde.AddLaxKey(c(3, 4), c(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2)~~>(1,3-5), (3,4)~~>(1,2,5)")
	abcde.ProjectCols(c(2, 3, 4, 5))
	verifyFD(t, abcde, "lax-key(2-5); (2)~~>(3-5), (3,4)~~>(2,5)")
	// 2 on its own is not necessarily a lax key: even if it determines the other
	// columns, any of them can still be NULL.
	testColsAreLaxKey(t, abcde, c(2), false)
	testColsAreLaxKey(t, abcde, c(3, 4), false)

	copy := &props.FuncDepSet{}
	copy.CopyFrom(abcde)

	// Verify that lax keys convert to strong keys.
	abcde.MakeNotNull(c(2, 3, 4, 5))
	verifyFD(t, abcde, "key(3,4); (2)-->(3-5), (3,4)-->(2,5)")

	// Regression test for #56358: ProjectCols was creating FD relations with
	// overlapping from/to sets.
	fd := &props.FuncDepSet{}
	fd.AddConstants(c(2, 3))
	fd.AddSynthesizedCol(c(4), 1)
	fd.AddStrictKey(c(1), c(1, 2, 3, 4))
	fd.AddEquivalency(2, 3)
	verifyFD(t, fd, "key(1); ()-->(2,3), (4)-->(1), (1)-->(2-4), (2)==(3), (3)==(2)")
	// Now project away column 3, and make sure we don't end up with (1)->(1,4).
	fd.ProjectCols(c(1, 2, 4))
	verifyFD(t, fd, "key(1); ()-->(2), (4)-->(1), (1)-->(4)")
}

func TestFuncDeps_AddFrom(t *testing.T) {
	// Remove lax dependency, then add it back.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	abcde := makeAbcdeFD(t)
	abcde.ProjectCols(c(1, 2, 4))
	verifyFD(t, abcde, "key(1); (1)-->(2,4)")
	abcde.AddFrom(makeAbcdeFD(t))
	abcde.AddStrictKey(c(1), c(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreStrictKey(t, abcde, c(1), true)

	// Remove strict dependency, then add it back.
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(c(2, 3))
	abcde.ProjectCols(c(2, 3))
	verifyFD(t, abcde, "key(2,3)")
	abcde.AddFrom(makeAbcdeFD(t))
	abcde.AddStrictKey(c(2, 3), c(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "key(2,3); (1)-->(2-5), (2,3)-->(1,4,5)")
	testColsAreStrictKey(t, abcde, c(1), true)
}

func TestFuncDeps_AddEquivFrom(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT * FROM abcde, mnpq WHERE a=m AND b=n
	product := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	product.MakeProduct(mnpq)
	product.AddEquivalency(1, 10)
	verifyFD(t, product, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")

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
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM (SELECT a, b, c FROM abcde WHERE d=e), (SELECT m, n FROM mnpq WHERE p=q)
	product := makeAbcdeFD(t)
	product.AddEquivalency(4, 5)
	product.ProjectCols(c(1, 2, 3))
	mnpq := makeMnpqFD(t)
	mnpq.AddEquivalency(12, 13)
	mnpq.ProjectCols(c(10, 11))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "key(1,10,11); (1)-->(2,3), (2,3)~~>(1)")

	// Constants on both sides.
	//   SELECT * FROM (SELECT * FROM abcde b=1), (SELECT * FROM mnpq WHERE p=1)
	product = makeAbcdeFD(t)
	product.AddConstants(c(2))
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(c(12))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "key(1,10,11); ()-->(2,12), (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(13)")

	// Strict key on left side, no key on right side:
	//   SELECT * FROM abcde, (SELECT p, q FROM mnpq)
	product = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.ProjectCols(c(12, 13))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreStrictKey(t, product, c(1, 2, 3, 4, 5, 12, 13), false)
	testColsAreLaxKey(t, product, c(1, 2, 3, 4, 5, 12, 13), false)

	// No key on left side, Strict key on right side.
	//   SELECT * FROM (SELECT d, e FROM abcde), mnpq
	product = makeAbcdeFD(t)
	product.ProjectCols(c(4, 5))
	product.MakeProduct(makeMnpqFD(t))
	verifyFD(t, product, "(10,11)-->(12,13)")
	testColsAreStrictKey(t, product, c(4, 5, 10, 11, 12, 13), false)
	testColsAreLaxKey(t, product, c(1, 2, 3, 4, 5, 12, 13), false)

	// Strict key on left side, lax key on right side:
	//   CREATE UNIQUE INDEX ON mnpq (p)
	//   SELECT * FROM abcde, (SELECT p, q FROM mnpq)
	product = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddLaxKey(c(12), c(10, 11, 12, 13))
	mnpq.ProjectCols(c(12, 13))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "lax-key(1,12,13); (1)-->(2-5), (2,3)~~>(1,4,5), (12)~~>(13)")
	testColsAreStrictKey(t, product, c(1, 2, 3, 4, 5, 12, 13), false)
	testColsAreLaxKey(t, product, c(1, 12, 13), true)

	// Lax key on left side, strict key on right side:
	//   SELECT * FROM (SELECT b, c, d, e FROM abcde), mnpq
	product = makeAbcdeFD(t)
	product.ProjectCols(c(2, 3, 4, 5))
	mnpq = makeMnpqFD(t)
	product.MakeProduct(mnpq)
	verifyFD(t, product, "lax-key(2-5,10,11); (2,3)~~>(4,5), (10,11)-->(12,13)")
	testColsAreStrictKey(t, product, c(1, 2, 3, 4, 5, 10, 11, 12, 13), false)
	testColsAreLaxKey(t, product, c(2, 3, 4, 5, 10, 11), true)

	// Lax key on left side, lax key on right side:
	//   CREATE UNIQUE INDEX ON mnpq (p)
	//   SELECT * FROM (SELECT b, c, d, e FROM abcde), (SELECT p, q FROM mnpq)
	product = makeAbcdeFD(t)
	product.ProjectCols(c(2, 3, 4, 5))
	mnpq = makeMnpqFD(t)
	mnpq.AddLaxKey(c(12), c(10, 11, 12, 13))
	mnpq.ProjectCols(c(12, 13))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "lax-key(2-5,12,13); (2,3)~~>(4,5), (12)~~>(13)")
	testColsAreStrictKey(t, product, c(2, 3, 4, 5, 12, 13), false)

	// Lax key on left side, no key on right side:
	//   SELECT * FROM (SELECT b, c, d, e FROM abcde), (SELECT p, q FROM mnpq)
	product = makeAbcdeFD(t)
	product.ProjectCols(c(2, 3, 4, 5))
	mnpq = makeMnpqFD(t)
	mnpq.ProjectCols(c(12, 13))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(2,3)~~>(4,5)")
	testColsAreStrictKey(t, product, c(2, 3, 4, 5, 12, 13), false)
	testColsAreLaxKey(t, product, c(2, 3, 4, 5, 12, 13), false)

	// No key on left side, lax key on right side:
	//   CREATE UNIQUE INDEX ON mnpq (p)
	//   SELECT * FROM (SELECT d, e FROM abcde), (SELECT p, q FROM mnpq)
	product = makeAbcdeFD(t)
	product.ProjectCols(c(4, 5))
	mnpq = makeMnpqFD(t)
	mnpq.AddLaxKey(c(12), c(10, 11, 12, 13))
	mnpq.ProjectCols(c(12, 13))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(12)~~>(13)")
	testColsAreStrictKey(t, product, c(4, 5, 12, 13), false)
	testColsAreLaxKey(t, product, c(4, 5, 12, 13), false)
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
	mnpq.MakeMax1Row(c(10, 11, 12, 13))
	verifyFD(t, mnpq, "key(); ()-->(10-13)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "key(1); (1)-->(2-5,10-13), (2,3)~~>(1,4,5)")

	// SELECT *
	// FROM abcde
	// INNER JOIN LATERAL (SELECT * FROM mnpq WHERE m=a AND p=1)
	// ON True
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(c(10, 12))
	verifyFD(t, mnpq, "key(11); ()-->(10,12), (11)-->(13)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "key(1,11); (1)-->(2-5), (2,3)~~>(1,4,5), (1,11)-->(10,12,13)")

	// SELECT *
	// FROM abcde
	// INNER JOIN LATERAL (SELECT * FROM mnpq WHERE m=a AND p=q)
	// ON True
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(c(10))
	mnpq.AddEquivalency(12, 13)
	verifyFD(t, mnpq, "key(11); ()-->(10), (11)-->(12,13), (12)==(13), (13)==(12)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "key(1,11); (1)-->(2-5), (2,3)~~>(1,4,5), (1,11)-->(10,12,13), (12)==(13), (13)==(12)")

	// No key in outer relation.
	//   SELECT *
	//   FROM (SELECT b, c, d, e FROM abcde)
	//   INNER JOIN LATERAL (SELECT * FROM mnpq WHERE p=q AND n=1)
	//   ON True
	abcde = makeAbcdeFD(t)
	abcde.ProjectCols(c(2, 3, 4, 5))
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(c(11))
	mnpq.AddEquivalency(12, 13)
	verifyFD(t, mnpq, "key(10); ()-->(11), (10)-->(12,13), (12)==(13), (13)==(12)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "(2,3)~~>(4,5), (12)==(13), (13)==(12)")

	// No key in inner relation.
	//   SELECT *
	//   FROM abcde
	//   INNER JOIN LATERAL (SELECT n, p, q FROM mnpq WHERE n=a AND p=1)
	//   ON True
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddConstants(c(11, 12))
	mnpq.ProjectCols(c(11, 12, 13))
	verifyFD(t, mnpq, "()-->(11,12)")
	abcde.MakeApply(mnpq)
	verifyFD(t, abcde, "(1)-->(2-5), (2,3)~~>(1,4,5)")
}

func TestFuncDeps_MakeLeftOuter(t *testing.T) {
	// All determinant columns in null-extended side are nullable.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde LEFT OUTER JOIN (SELECT *, p+q FROM mnpq) ON True
	var loj props.FuncDepSet
	preservedCols := c(1, 2, 3, 4, 5)
	nullExtendedCols := c(10, 11, 12, 13, 14)
	abcde := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	mnpq.AddSynthesizedCol(c(12, 13), 14)
	loj.CopyFrom(abcde)
	loj.MakeProduct(mnpq)
	verifyFD(t, &loj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12,13)-->(14)")
	loj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 10, 11))
	verifyFD(t, &loj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12,13)~~>(14), (1,10,11)-->(14)")

	// One determinant column in null-extended side is not null.
	//   SELECT * FROM abcde LEFT OUTER JOIN (SELECT *, m+q FROM mnpq) ON True
	preservedCols = c(1, 2, 3, 4, 5)
	nullExtendedCols = c(10, 11, 12, 13, 14)
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(c(10, 13), 14)
	loj.CopyFrom(abcde)
	loj.MakeProduct(mnpq)
	verifyFD(t, &loj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (10,13)-->(14)")
	loj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 10, 11))
	verifyFD(t, &loj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (10,13)-->(14)")

	// Inputs have constant columns. Constant columns on the row-supplying side
	// stay constant, while constant columns on the null-supplying side are not
	// constant after null-extension.
	var roj props.FuncDepSet
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddConstants(c(2, 3, 12))
	roj.MakeNotNull(c(2, 3, 12))
	verifyFD(t, &roj, "key(10,11); ()-->(1-5,12), (10,11)-->(13)")
	roj.MakeLeftOuter(mnpq, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 2, 3, 10, 11, 12))
	verifyFD(t, &roj, "key(10,11); ()-->(12), (10,11)-->(1-5,13)")

	// Add constants on both sides of outer join. None of the resulting columns
	// are constant, because rows are added back after filtering on the
	// row-supplying side, and the null-supplying side is null-extended.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON b=1 AND c=1 AND p=1
	filters := props.FuncDepSet{}
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	filters.AddConstants(c(2, 3, 12))
	filters.MakeNotNull(c(2, 3, 12))
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	roj.MakeLeftOuter(mnpq, &filters, preservedCols, nullExtendedCols, c(1, 2, 3, 10, 11, 12))
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")

	// Test equivalency on both sides of outer join.
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddEquivalency(2, 3)
	roj.AddEquivalency(3, 4)
	roj.AddEquivalency(10, 12)
	roj.AddEquivalency(10, 13)
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,5), (10,11)-->(12,13), (2)==(3,4), (3)==(2,4), (4)==(2,3), (10)==(12,13), (12)==(10,13), (13)==(10,12)")
	roj.MakeLeftOuter(mnpq, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 2, 3, 10, 11, 13))
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,5), (10,11)-->(12,13), (2)==(3,4), (3)==(2,4), (4)==(2,3), (10)==(12,13), (12)==(10,13), (13)==(10,12)")

	// Test equivalencies on both sides of outer join in filters.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON b=c AND c=d AND m=p AND m=q
	filters = props.FuncDepSet{}
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	filters.AddEquivalency(2, 3)
	filters.AddEquivalency(3, 4)
	filters.AddEquivalency(10, 12)
	filters.AddEquivalency(10, 13)
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	roj.MakeLeftOuter(mnpq, &filters, preservedCols, nullExtendedCols, c(1, 2, 3, 10, 11, 13))
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")

	// Test equivalency that crosses join boundary.
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddEquivalency(1, 10)
	verifyFD(t, &roj, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	roj.MakeLeftOuter(mnpq, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 10, 11))
	verifyFD(t, &roj, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1-5,12,13), (1)~~>(10)")

	// Test filter equivalency that crosses join boundary.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON a=m
	filters = props.FuncDepSet{}
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	filters.AddEquivalency(1, 10)
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	roj.MakeLeftOuter(mnpq, &filters, preservedCols, nullExtendedCols, c(1, 10, 11))
	verifyFD(t, &roj, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1-5,12,13)")

	// Test equivalency that includes columns from both sides of join boundary.
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	roj.AddEquivalency(1, 10)
	roj.AddEquivalency(1, 2)
	verifyFD(t, &roj, "key(10,11); (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(2,10), (10)==(1,2), (2)==(1,10)")
	roj.MakeLeftOuter(mnpq, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 2, 10, 11))
	verifyFD(t, &roj, "key(10,11); (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(1-5,12,13), (1)==(2), (2)==(1), (1)~~>(10), (2)~~>(10)")

	// Test filter equivalency that includes columns from both sides of join
	// boundary.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON a=m AND a=b
	filters = props.FuncDepSet{}
	preservedCols = c(10, 11, 12, 13)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.MakeProduct(makeMnpqFD(t))
	filters.AddEquivalency(1, 10)
	filters.AddEquivalency(1, 2)
	verifyFD(t, &roj, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	roj.MakeLeftOuter(mnpq, &filters, preservedCols, nullExtendedCols, c(1, 2, 10, 11))
	verifyFD(t, &roj, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1-5,12,13)")

	// Test multiple calls to MakeLeftOuter, where the first creates determinant with
	// columns from both sides of join.
	//   SELECT * FROM (SELECT * FROM abcde WHERE b=1) FULL JOIN mnpq ON True
	preservedCols = c(10, 11, 12, 13)
	preservedCols2 := c(1, 2, 3, 4, 5)
	nullExtendedCols = c(1, 2, 3, 4, 5)
	nullExtendedCols2 := c(10, 11, 12, 13)
	abcde = makeAbcdeFD(t)
	roj.CopyFrom(abcde)
	roj.AddConstants(c(2))
	roj.MakeProduct(makeMnpqFD(t))
	verifyFD(t, &roj, "key(1,10,11); ()-->(2), (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	roj.MakeLeftOuter(mnpq, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1, 2, 10, 11))
	verifyFD(t, &roj, "key(1,10,11); (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10,11)-->(2)")
	roj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols2, nullExtendedCols2, c(1, 2, 10, 11))
	verifyFD(t, &roj, "key(1,10,11); (1)-->(3-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1,10,11)-->(2)")

	// Join keyless relations with nullable columns.
	//   SELECT * FROM (SELECT d, e, d+e FROM abcde) LEFT JOIN (SELECT p, q, p+q FROM mnpq) ON True
	preservedCols = c(4, 5, 6)
	nullExtendedCols = c(12, 13, 14)
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(c(12, 13), 14)
	mnpq.ProjectCols(c(12, 13, 14))
	loj.CopyFrom(abcde)
	loj.AddSynthesizedCol(c(4, 5), 6)
	loj.ProjectCols(c(4, 5, 6))
	loj.MakeProduct(mnpq)
	verifyFD(t, &loj, "(4,5)-->(6), (12,13)-->(14)")
	loj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c())
	verifyFD(t, &loj, "(4,5)-->(6), (12,13)~~>(14)")
	testColsAreStrictKey(t, &loj, c(4, 5, 6, 12, 13, 14), false)

	// Join keyless relations with not-null columns.
	//   SELECT * FROM (SELECT d, e, d+e FROM abcde WHERE d>e) LEFT JOIN (SELECT p, q, p+q FROM mnpq WHERE p>q) ON True
	preservedCols = c(4, 5, 6)
	nullExtendedCols = c(12, 13, 14)
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(c(12, 13), 14)
	mnpq.ProjectCols(c(12, 13, 14))
	loj.CopyFrom(abcde)
	loj.AddSynthesizedCol(c(4, 5), 6)
	loj.ProjectCols(c(4, 5, 6))
	loj.MakeProduct(mnpq)
	verifyFD(t, &loj, "(4,5)-->(6), (12,13)-->(14)")
	loj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(4, 5, 12, 13))
	verifyFD(t, &loj, "(4,5)-->(6), (12,13)-->(14)")
	testColsAreStrictKey(t, &loj, c(4, 5, 6, 12, 13, 14), false)

	// SELECT * FROM abcde LEFT JOIN LATERAL (SELECT p, q, p+q FROM mnpq) ON True
	preservedCols = c(1, 2, 3, 4, 5)
	nullExtendedCols = c(12, 13, 14)
	abcde = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.AddSynthesizedCol(c(12, 13), 14)
	mnpq.ProjectCols(c(12, 13, 14))
	loj.CopyFrom(abcde)
	verifyFD(t, mnpq, "(12,13)-->(14)")
	loj.MakeApply(mnpq)
	verifyFD(t, &loj, "(1)-->(2-5), (2,3)~~>(1,4,5), (1,12,13)-->(14)")
	loj.MakeLeftOuter(abcde, &props.FuncDepSet{}, preservedCols, nullExtendedCols, c(1))
	verifyFD(t, &loj, "(1)-->(2-5), (2,3)~~>(1,4,5)")
}

func TestFuncDeps_MakeFullOuter(t *testing.T) {
	mk := func(left, right *props.FuncDepSet, notNullInputCols opt.ColSet) *props.FuncDepSet {
		var outer props.FuncDepSet
		outer.CopyFrom(left)
		outer.MakeProduct(right)
		outer.MakeFullOuter(left.ColSet(), right.ColSet(), notNullInputCols)
		return &outer
	}

	abcde := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)

	outer := mk(abcde, mnpq, c(1, 10, 11))
	verifyFD(t, outer, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")

	// With partial null column info, some FDs become lax.
	outer = mk(abcde, mnpq, c(1))
	verifyFD(t, outer, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)~~>(12,13), (1,10,11)-->(12,13)")

	// Without null column info, the key becomes lax.
	outer = mk(abcde, mnpq, c())
	verifyFD(t, outer, "lax-key(1,10,11); (2,3)~~>(1,4,5), (1)~~>(2-5), (10,11)~~>(12,13), (1,10,11)~~>(2-5,12,13)")

	// Test case with empty key on both sides; the result should not have a key.
	abcde.MakeMax1Row(abcde.ColSet())
	mnpq.MakeMax1Row(mnpq.ColSet())
	outer = mk(abcde, mnpq, c())
	verifyFD(t, outer, "")
}

func TestFuncDeps_RemapFrom(t *testing.T) {
	var res props.FuncDepSet
	abcde := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)

	from := opt.ColList{1, 2, 3, 4, 5, 10, 11, 12, 13}
	to := make(opt.ColList, len(from))
	for i := range from {
		to[i] = from[i] * 10
	}
	res.RemapFrom(abcde, from, to)
	verifyFD(t, &res, "key(10); (10)-->(20,30,40,50), (20,30)~~>(10,40,50)")
	res.RemapFrom(mnpq, from, to)
	verifyFD(t, &res, "key(100,110); (100,110)-->(120,130)")

	// Test where not all columns in the FD are present in the mapping.
	from = opt.ColList{1, 3, 4, 5}
	to = opt.ColList{10, 30, 40, 50}
	res.RemapFrom(abcde, from, to)
	verifyFD(t, &res, "key(10); (10)-->(30,40,50)")
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
//   CREATE UNIQUE INDEX ON abcde (b, c)
func makeAbcdeFD(t *testing.T) *props.FuncDepSet {
	// Set Key to all cols to start, and ensure it's overridden in AddStrictKey.
	allCols := c(1, 2, 3, 4, 5)
	abcde := &props.FuncDepSet{}
	abcde.AddStrictKey(c(1), allCols)
	verifyFD(t, abcde, "key(1); (1)-->(2-5)")
	abcde.AddLaxKey(c(2, 3), allCols)
	verifyFD(t, abcde, "key(1); (1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreStrictKey(t, abcde, c(1), true)
	testColsAreStrictKey(t, abcde, c(2, 3), false)
	testColsAreStrictKey(t, abcde, c(1, 2), true)
	testColsAreStrictKey(t, abcde, c(1, 2, 3, 4, 5), true)
	testColsAreStrictKey(t, abcde, c(4, 5), false)
	testColsAreLaxKey(t, abcde, c(2, 3), true)
	return abcde
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
func makeMnpqFD(t *testing.T) *props.FuncDepSet {
	allCols := c(10, 11, 12, 13)
	mnpq := &props.FuncDepSet{}
	mnpq.AddStrictKey(c(10, 11), allCols)
	mnpq.MakeNotNull(c(10, 11))
	verifyFD(t, mnpq, "key(10,11); (10,11)-->(12,13)")
	testColsAreStrictKey(t, mnpq, c(10), false)
	testColsAreStrictKey(t, mnpq, c(10, 11), true)
	testColsAreStrictKey(t, mnpq, c(10, 11, 12), true)
	return mnpq
}

// Construct cartesian product FD from figure 3.6, page 122:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
//   SELECT * FROM abcde, mnpq
func makeProductFD(t *testing.T) *props.FuncDepSet {
	product := makeAbcdeFD(t)
	product.MakeProduct(makeMnpqFD(t))
	verifyFD(t, product, "key(1,10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	testColsAreStrictKey(t, product, c(1), false)
	testColsAreStrictKey(t, product, c(10, 11), false)
	testColsAreStrictKey(t, product, c(1, 10, 11), true)
	testColsAreStrictKey(t, product, c(1, 2, 3, 10, 11, 12), true)
	testColsAreStrictKey(t, product, c(2, 3, 10, 11), false)
	return product
}

// Construct inner join FD:
//   SELECT * FROM abcde, mnpq WHERE a=m
func makeJoinFD(t *testing.T) *props.FuncDepSet {
	// Start with cartesian product FD and add equivalency to it.
	join := makeProductFD(t)
	join.AddEquivalency(1, 10)
	verifyFD(t, join, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	join.ProjectCols(c(1, 2, 3, 4, 5, 10, 11, 12, 13))
	verifyFD(t, join, "key(10,11); (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	testColsAreStrictKey(t, join, c(1, 11), true)
	testColsAreStrictKey(t, join, c(1, 10), false)
	testColsAreStrictKey(t, join, c(1, 10, 11), true)
	testColsAreStrictKey(t, join, c(1), false)
	testColsAreStrictKey(t, join, c(10, 11), true)
	testColsAreStrictKey(t, join, c(2, 3, 11), false)
	testColsAreLaxKey(t, join, c(2, 3, 11), true)
	return join
}

func verifyFD(t *testing.T, f *props.FuncDepSet, expected string) {
	t.Helper()
	actual := f.String()
	if actual != expected {
		t.Errorf("\nexpected: %s\nactual  : %s", expected, actual)
	}

	f.Verify()

	if key, ok := f.StrictKey(); ok {
		testColsAreStrictKey(t, f, key, true)
		if !key.Empty() {
			testColsAreStrictKey(t, f, opt.ColSet{}, false)
		}
		closure := f.ComputeClosure(key)
		testColsAreStrictKey(t, f, closure, true)
	} else if key, ok := f.LaxKey(); ok {
		testColsAreLaxKey(t, f, key, true)
		if !key.Empty() {
			testColsAreLaxKey(t, f, opt.ColSet{}, false)
		}
		closure := f.ComputeClosure(key)
		testColsAreLaxKey(t, f, closure, true)
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

func c(cols ...opt.ColumnID) opt.ColSet {
	return opt.MakeColSet(cols...)
}
