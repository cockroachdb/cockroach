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

func TestFuncDeps_AddStrictKey(t *testing.T) {
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT DISTINCT ON (p) m, n, p, q FROM mnpq
	mnpq := makeMnpqFD(t)
	allCols := util.MakeFastIntSet(10, 11, 12, 13)
	mnpq.AddStrictKey(util.MakeFastIntSet(12), allCols)
	verifyFD(t, mnpq, "(12): (10,11)-->(12,13), (12)-->(10,11,13)")
	testColsAreKey(t, mnpq, util.MakeFastIntSet(12), true)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(13), false)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10, 11), true)

	// SELECT DISTINCT ON (m, n, p) m, n, p, q FROM mnpq
	mnpq = makeMnpqFD(t)
	mnpq.AddStrictKey(util.MakeFastIntSet(10, 11, 12), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13)")
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10, 11), true)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(11, 12), false)

	// SELECT DISTINCT ON (n, p, q) m, n, p, q FROM mnpq
	mnpq = makeMnpqFD(t)
	mnpq.AddStrictKey(util.MakeFastIntSet(11, 12, 13), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13), (11-13)-->(10)")
	testColsAreKey(t, mnpq, util.MakeFastIntSet(11, 12, 13), true)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(11, 12), false)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10, 11), true)

	// All columns together form a key.
	//   CREATE TABLE ab (a INT, b INT, PRIMARY KEY (a, b))
	allCols = util.MakeFastIntSet(1, 2)
	ab := &props.FuncDepSet{}
	ab.AddStrictKey(allCols, allCols)
	verifyFD(t, ab, "(1,2): ")
	testColsAreKey(t, ab, util.MakeFastIntSet(1, 2), true)
	testColsAreKey(t, ab, util.MakeFastIntSet(1), false)
}

func TestFuncDeps_AddLaxKey(t *testing.T) {
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// CREATE UNIQUE INDEX idx ON mnpq (p)
	mnpq := makeMnpqFD(t)
	allCols := util.MakeFastIntSet(10, 11, 12, 13)
	mnpq.AddLaxKey(util.MakeFastIntSet(12), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13), (12)~~>(10,11,13)")
	testColsAreKey(t, mnpq, util.MakeFastIntSet(12), false)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10, 11, 12), true)

	// CREATE UNIQUE INDEX idx ON mnpq (m, n, p)
	mnpq = makeMnpqFD(t)
	mnpq.AddLaxKey(util.MakeFastIntSet(10, 11, 12), allCols)
	verifyFD(t, mnpq, "(10,11): (10,11)-->(12,13)")
	testColsAreKey(t, mnpq, util.MakeFastIntSet(12), false)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10, 11), true)
}

func TestFuncDeps_MakeMax1Row(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde LIMIT 1
	abcde := makeAbcdeFD(t)
	abcde.MakeMax1Row(util.MakeFastIntSet(1, 2, 3, 4, 5))
	verifyFD(t, abcde, "(): ()-->(1-5)")
	testColsAreKey(t, abcde, util.MakeFastIntSet(), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(1, 2, 3, 4, 5), true)

	// No columns.
	abcde = makeAbcdeFD(t)
	abcde.MakeMax1Row(opt.ColSet{})
	verifyFD(t, abcde, "(): ")
	testColsAreKey(t, abcde, util.MakeFastIntSet(), true)
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
	// SELECT * FROM abcde LEFT OUTER JOIN mnpq ON a=m WHERE m IS NOT NULL
	loj := makeLeftJoinFD(t)
	loj.MakeNotNull(util.MakeFastIntSet(1, 10))
	verifyFD(t, loj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1,12,13), (1)-->(10), (10)-->(1)")
	testColsAreKey(t, loj, util.MakeFastIntSet(1, 11), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(1, 10), false)

	// Make one of two equivalencies not null (i.e. "m" but "n").
	//   SELECT * FROM abcde LEFT OUTER JOIN mnpq ON a=m AND a=n WHERE m IS NOT NULL
	nullExtendedCols := util.MakeFastIntSet(10, 11, 12, 13)
	loj = makeJoinFD(t)
	loj.AddEquivalency(1, 11)
	verifyFD(t, loj, "(11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10,11), (10)==(1), (11)==(1)")
	testColsAreKey(t, loj, util.MakeFastIntSet(1), true)
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, loj, "(11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)-->(10,11), (10)~~>(1), (11)-->(1)")
	loj.MakeNotNull(util.MakeFastIntSet(1, 10))
	verifyFD(t, loj, "(11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)-->(10,11), (10)-->(1), (11)-->(1)")
	testColsAreKey(t, loj, util.MakeFastIntSet(1), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(10), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(11), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(), false)

	// SELECT * FROM abcde LEFT OUTER JOIN mnpq ON a=m AND m=1 AND b=2 AND p=3 WHERE p IS NOT NULL
	// TODO(andyk): Notice that the left outer join loses the knowledge that "a"
	//              is a constant, via its equivalency with "m".
	loj = makeJoinFD(t)
	loj.AddConstants(util.MakeFastIntSet(2, 10, 12))
	verifyFD(t, loj, "(11): ()-->(2,10,12), (1)-->(2-5), (2,3)~~>(1,4,5), (11)-->(12,13), (1)==(10), (10)==(1)")
	testColsAreKey(t, loj, util.MakeFastIntSet(10, 11), true)
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 10, 11))
	verifyFD(t, loj, "(11): ()-->(2), (1)-->(2-5), (2,3)~~>(1,4,5), (11)-->(1,10,12,13), (1)-->(10), (10)~~>(1), ()~~>(10,12)")
	loj.MakeNotNull(util.MakeFastIntSet(1, 12))
	verifyFD(t, loj, "(11): ()-->(2), (1)-->(2-5), (2,3)~~>(1,4,5), (11)-->(1,10,12,13), (1)-->(10), (10)~~>(1), ()-->(12), ()~~>(10)")
	testColsAreKey(t, loj, util.MakeFastIntSet(11), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(1), false)
	testColsAreKey(t, loj, util.MakeFastIntSet(10), false)

	// SELECT * FROM abcde LEFT OUTER JOIN mnpq ON a=m AND m=1 AND b=2 AND p=3 WHERE p IS NOT NULL AND m IS NOT NULL
	loj.MakeNotNull(util.MakeFastIntSet(1, 10, 12))
	verifyFD(t, loj, "(11): ()-->(2), (1)-->(2-5), (2,3)~~>(1,4,5), (11)-->(1,10,12,13), (1)-->(10), (10)-->(1), ()-->(12), ()-->(10)")
	testColsAreKey(t, loj, util.MakeFastIntSet(11), true)
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
	testColsAreKey(t, &bmcn, util.MakeFastIntSet(1), true)
	testColsAreKey(t, &bmcn, util.MakeFastIntSet(2, 3, 4, 5, 10, 11, 12, 13), false)
	testColsAreKey(t, &bmcn, util.MakeFastIntSet(), false)

	// SELECT * FROM abcde, mnpq WHERE a=m AND a=n
	var amn props.FuncDepSet
	amn.CopyFrom(product)
	amn.AddEquivalency(1, 10)
	amn.AddEquivalency(1, 11)
	verifyFD(t, &amn, "(11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10,11), (10)==(1), (11)==(1)")
	testColsAreKey(t, &amn, util.MakeFastIntSet(1), true)
	testColsAreKey(t, &amn, util.MakeFastIntSet(10), true)
	testColsAreKey(t, &amn, util.MakeFastIntSet(11), true)
	testColsAreKey(t, &amn, util.MakeFastIntSet(2, 3, 4, 5), false)
	testColsAreKey(t, &amn, util.MakeFastIntSet(), false)

	// Override weaker dependencies with equivalency.
	//   SELECT * FROM abcde LEFT OUTER JOIN mnpq ON a=m WHERE a=m AND a=m
	loj := makeLeftJoinFD(t)
	verifyFD(t, loj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1,12,13), (1)-->(10), (10)~~>(1)")
	loj.AddEquivalency(1, 10)
	loj.AddEquivalency(1, 10)
	verifyFD(t, loj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1,12,13), (1)==(10), (10)==(1)")
	testColsAreKey(t, loj, util.MakeFastIntSet(1, 11), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(10, 11), true)

	// Multiple equivalencies + constant.
	//   SELECT * FROM abcde, mnpq ON a=m WHERE m=n AND n=1
	cnst := makeJoinFD(t)
	cnst.AddEquivalency(10, 11)
	cnst.AddConstants(util.MakeFastIntSet(11))
	verifyFD(t, cnst, "(): ()-->(11), (1)-->(2-5), (2,3)~~>(1,4,5), (10)-->(12,13), (1)==(10), (10)==(1,11), (11)==(10)")
	testColsAreKey(t, cnst, util.MakeFastIntSet(), true)
}

func TestFuncDeps_AddConstants(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// SELECT * FROM abcde WHERE c>2
	abcde := makeAbcdeFD(t)
	abcde.AddConstants(util.MakeFastIntSet(2))
	verifyFD(t, abcde, "(1): ()-->(2), (1)-->(2-5), (2,3)~~>(1,4,5)")
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(1): ()-->(2), (1)-->(2-5), (2,3)-->(1,4,5)")
	testColsAreKey(t, abcde, util.MakeFastIntSet(3), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(1), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(), false)

	// SELECT * FROM abcde WHERE b IS NULL AND c IS NULL
	abcde = makeAbcdeFD(t)
	abcde.SetKey(util.MakeFastIntSet(1, 2, 3, 4, 5))
	abcde.AddConstants(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(1): ()-->(2,3), (1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreKey(t, abcde, util.MakeFastIntSet(2, 3), false)
	testColsAreKey(t, abcde, util.MakeFastIntSet(1), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(), false)

	// SELECT * FROM (SELECT * FROM abcde WHERE b=1) WHERE c=2
	abcde = makeAbcdeFD(t)
	abcde.SetKey(util.MakeFastIntSet(1, 2, 3, 4, 5))
	abcde.AddConstants(util.MakeFastIntSet(2))
	abcde.MakeNotNull(util.MakeFastIntSet(2))
	abcde.AddConstants(util.MakeFastIntSet(3))
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(): ()-->(2,3), (1)-->(2-5), (2,3)-->(1,4,5)")
	testColsAreKey(t, abcde, util.MakeFastIntSet(), true)

	// SELECT * FROM (SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL) WHERE b=1
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)-->(1,4,5)")
	abcde.AddConstants(util.MakeFastIntSet(2))
	verifyFD(t, abcde, "(1): ()-->(2), (1)-->(2-5), (3)-->(1,4,5)")
	testColsAreKey(t, abcde, util.MakeFastIntSet(1), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(3), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(2), false)

	// SELECT * FROM (SELECT * FROM abcde WHERE b IS NOT NULL AND c IS NOT NULL) WHERE b=1 AND c=2
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	abcde.AddConstants(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(): ()-->(1-5), (1)-->(2-5)")
	testColsAreKey(t, abcde, util.MakeFastIntSet(), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(2, 3), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(1, 2, 3, 4, 5), true)

	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// SELECT a, m, n FROM abcde, mnpq WHERE a=m AND n IS NULL
	var am props.FuncDepSet
	am.CopyFrom(makeJoinFD(t))
	am.AddConstants(util.MakeFastIntSet(11))
	am.ProjectCols(util.MakeFastIntSet(1, 10, 11))
	am.SetKey(util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, &am, "(1,10,11): ()-->(11), (1)==(10), (10)==(1)")
	testColsAreKey(t, &am, util.MakeFastIntSet(1), true)
	testColsAreKey(t, &am, util.MakeFastIntSet(10), true)
	testColsAreKey(t, &am, util.MakeFastIntSet(1, 10), true)
	testColsAreKey(t, &am, util.MakeFastIntSet(), false)

	// Equivalency, with one of equivalent columns set to constant.
	//   SELECT * FROM abcde, mnpq WHERE a=m AND m=5
	var eqConst props.FuncDepSet
	eqConst.CopyFrom(makeJoinFD(t))
	eqConst.AddConstants(util.MakeFastIntSet(10))
	eqConst.MakeNotNull(util.MakeFastIntSet(10))
	verifyFD(t, &eqConst, "(11): ()-->(10), (1)-->(2-5), (2,3)~~>(1,4,5), (11)-->(12,13), (1)==(10), (10)==(1)")
	testColsAreKey(t, &eqConst, util.MakeFastIntSet(11), true)
	testColsAreKey(t, &eqConst, util.MakeFastIntSet(1), false)
	testColsAreKey(t, &eqConst, util.MakeFastIntSet(1, 2, 3, 10, 12), false)
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
	verifyFD(t, &abdef, "(1): (1)-->(2-5), (2,3)-->(6) [removed: (3)]")
	abdef.SetKey(util.MakeFastIntSet(1, 2, 4, 5, 6))
	verifyFD(t, &abdef, "(1,2,4-6): (1)-->(2-5), (2,3)-->(6) [removed: (3)]")
	testColsAreKey(t, &abdef, util.MakeFastIntSet(1), true)
	testColsAreKey(t, &abdef, util.MakeFastIntSet(2, 4, 5, 6), false)
	testColsAreKey(t, &abdef, util.MakeFastIntSet(1, 2), true)
	testColsAreKey(t, &abdef, util.MakeFastIntSet(), false)

	// Add another synthesized column, based on the first synthesized column.
	abdef.AddSynthesizedCol(util.MakeFastIntSet(6), 7)
	verifyFD(t, &abdef, "(1,2,4-6): (1)-->(2-5), (2,3)-->(6), (6)-->(7) [removed: (3)]")
	testColsAreKey(t, &abdef, util.MakeFastIntSet(1), true)
	testColsAreKey(t, &abdef, util.MakeFastIntSet(2, 3), false)

	// Add a constant synthesized column, not based on any other column.
	abdef.AddSynthesizedCol(opt.ColSet{}, 8)
	verifyFD(t, &abdef, "(1): ()-->(8), (1)-->(2-5), (2,3)-->(6), (6)-->(7) [removed: (3)]")
	testColsAreKey(t, &abdef, util.MakeFastIntSet(1), true)
	testColsAreKey(t, &abdef, util.MakeFastIntSet(2, 3, 4, 5, 6, 7, 8), false)

	// Remove columns and add computed column.
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde, mnpq WHERE a=m
	//   SELECT a, n, b+1 FROM abcde, mnpq WHERE a=m
	var anb1 props.FuncDepSet
	anb1.CopyFrom(makeJoinFD(t))
	anb1.AddSynthesizedCol(util.MakeFastIntSet(2), 100)
	verifyFD(t, &anb1, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1), (2)-->(100)")
	anb1.ProjectCols(util.MakeFastIntSet(1, 11, 100))
	verifyFD(t, &anb1, "(1,11): (1)-->(2), (1)==(10), (10)==(1), (2)-->(100) [removed: (2,10)]")
	anb1.SetKey(util.MakeFastIntSet(1, 11, 100))
	verifyFD(t, &anb1, "(1,11,100): (1)-->(2), (1)==(10), (10)==(1), (2)-->(100) [removed: (2,10)]")
	testColsAreKey(t, &anb1, util.MakeFastIntSet(1, 11), true)
	testColsAreKey(t, &anb1, util.MakeFastIntSet(1, 11, 100), true)
	testColsAreKey(t, &anb1, util.MakeFastIntSet(1, 100), false)
	testColsAreKey(t, &anb1, util.MakeFastIntSet(11, 100), false)
}

func TestFuncDeps_ProjectCols(t *testing.T) {
	// Remove column from lax dependency.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   SELECT a, c, d, e FROM abcde
	abde := makeAbcdeFD(t)
	abde.ProjectCols(util.MakeFastIntSet(1, 3, 4, 5))
	verifyFD(t, abde, "(1): (1)-->(3-5)")
	testColsAreKey(t, abde, util.MakeFastIntSet(1), true)

	// Try removing columns that are only dependants (i.e. never determinants).
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde, mnpq WHERE a=m
	//   SELECT a, b, c, m, n FROM abcde, mnpq WHERE a=m
	var abcmn props.FuncDepSet
	abcmn.CopyFrom(makeJoinFD(t))
	abcmn.ProjectCols(util.MakeFastIntSet(1, 2, 3, 10, 11))
	verifyFD(t, &abcmn, "(10,11): (1)-->(2,3), (2,3)~~>(1), (1)==(10), (10)==(1)")
	abcmn.SetKey(util.MakeFastIntSet(1, 2, 3, 10, 11))
	verifyFD(t, &abcmn, "(1-3,10,11): (1)-->(2,3), (2,3)~~>(1), (1)==(10), (10)==(1)")
	testColsAreKey(t, &abcmn, util.MakeFastIntSet(1, 11), true)
	testColsAreKey(t, &abcmn, util.MakeFastIntSet(10, 11), true)
	testColsAreKey(t, &abcmn, util.MakeFastIntSet(2, 3), false)

	// Remove key columns, but expect another key to be found.
	//   SELECT b, c, n FROM abcde, mnpq WHERE a=m AND b IS NOT NULL AND c IS NOT NULL
	switchKey := makeJoinFD(t)
	switchKey.MakeNotNull(util.MakeFastIntSet(2, 3))
	verifyFD(t, switchKey, "(10,11): (1)-->(2-5), (2,3)-->(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	switchKey.ProjectCols(util.MakeFastIntSet(2, 3, 11))
	verifyFD(t, switchKey, "(2,3,11): (1)-->(2,3), (2,3)-->(1), (1)==(10), (10)==(1) [removed: (1,10)]")
	testColsAreKey(t, switchKey, util.MakeFastIntSet(2, 3, 11), true)
	testColsAreKey(t, switchKey, util.MakeFastIntSet(2, 3), false)

	// Remove column from every determinant and ensure that all FDs go away.
	//   SELECT d FROM abcde, mnpq WHERE a=m AND 1=1 AND n=2
	noKey := makeJoinFD(t)
	verifyFD(t, noKey, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1)")
	noKey.ProjectCols(util.MakeFastIntSet(2, 11))
	verifyFD(t, noKey, "")
	testColsAreKey(t, noKey, util.MakeFastIntSet(), false)
	testColsAreKey(t, noKey, util.MakeFastIntSet(2, 11), false)

	// Remove columns so that there is no longer a key.
	//   SELECT b, c, d, e, n, p, q FROM abcde, mnpq WHERE a=m
	var bcden props.FuncDepSet
	bcden.CopyFrom(makeJoinFD(t))
	bcden.ProjectCols(util.MakeFastIntSet(2, 3, 4, 5, 11, 12, 13))
	verifyFD(t, &bcden, "(1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(10), (10)==(1) [removed: (1,10)]")
	testColsAreKey(t, &bcden, util.MakeFastIntSet(2, 3, 4, 5, 11, 12, 13), false)
	testColsAreKey(t, &bcden, util.MakeFastIntSet(), false)

	// Remove remainder of columns (N rows, 0 cols projected).
	bcden.ProjectCols(opt.ColSet{})
	verifyFD(t, &bcden, "")

	testColsAreKey(t, &bcden, util.MakeFastIntSet(), false)

	// Project single row.
	//   SELECT d FROM abcde, mnpq WHERE a=m AND 1=1 AND n=2
	oneRow := makeJoinFD(t)
	oneRow.AddConstants(util.MakeFastIntSet(1, 11))
	verifyFD(t, oneRow, "(): ()-->(1-5,11), (2,3)~~>(1,4,5), (10)-->(12,13), (1)==(10), (10)==(1)")
	oneRow.ProjectCols(util.MakeFastIntSet(4))
	verifyFD(t, oneRow, "(): ()-->(4)")
	testColsAreKey(t, oneRow, util.MakeFastIntSet(), true)
}

func TestFuncDeps_AddFrom(t *testing.T) {
	// Remove lax dependency, then add it back.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	abcde := makeAbcdeFD(t)
	abcde.ProjectCols(util.MakeFastIntSet(1, 2, 4))
	verifyFD(t, abcde, "(1): (1)-->(2,4)")
	abcde.AddFrom(makeAbcdeFD(t))
	verifyFD(t, abcde, "(1): (1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreKey(t, abcde, util.MakeFastIntSet(1), true)

	// Remove strict dependency, then add it back.
	abcde = makeAbcdeFD(t)
	abcde.MakeNotNull(util.MakeFastIntSet(2, 3))
	abcde.ProjectCols(util.MakeFastIntSet(2, 3))
	verifyFD(t, abcde, "(2,3): (1)-->(2,3), (2,3)-->(1) [removed: (1)]")
	abcde.AddFrom(makeAbcdeFD(t))
	verifyFD(t, abcde, "(2,3): (1)-->(2-5), (2,3)-->(1), (2,3)~~>(1,4,5) [removed: (1)]")
	testColsAreKey(t, abcde, util.MakeFastIntSet(1), true)
}

func TestFuncDeps_MakeProduct(t *testing.T) {
	// Union dependencies and removed columns and keys:
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM (SELECT a, b, c FROM abcde WHERE d=e), (SELECT m, n FROM mnpq WHERE p=q)
	product := makeAbcdeFD(t)
	product.AddEquivalency(4, 5)
	product.ProjectCols(util.MakeFastIntSet(1, 2, 3))
	mnpq := makeMnpqFD(t)
	mnpq.AddEquivalency(12, 13)
	mnpq.ProjectCols(util.MakeFastIntSet(10, 11))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (4)==(5), (5)==(4), (10,11)-->(12,13), (12)==(13), (13)==(12) [removed: (4,5,12,13)]")
	testColsAreKey(t, product, util.MakeFastIntSet(1, 10, 11), true)

	// Key only on left side:
	//   SELECT * FROM abcde, (SELECT p, q FROM mnpq)
	product = makeAbcdeFD(t)
	mnpq = makeMnpqFD(t)
	mnpq.ProjectCols(util.MakeFastIntSet(12, 13))
	product.MakeProduct(mnpq)
	verifyFD(t, product, "(1)-->(2-5), (2,3)~~>(1,4,5)")
	testColsAreKey(t, product, util.MakeFastIntSet(1, 2, 3, 4, 5, 12, 13), false)

	// Key only on right side:
	//   SELECT * FROM (SELECT d, e FROM abcde), mnpq
	product = makeAbcdeFD(t)
	product.ProjectCols(util.MakeFastIntSet(4, 5))
	product.MakeProduct(makeMnpqFD(t))
	verifyFD(t, product, "(10,11)-->(12,13)")
	testColsAreKey(t, product, util.MakeFastIntSet(4, 5, 10, 11, 12, 13), false)
}

func TestFuncDeps_MakeOuter(t *testing.T) {
	// Determinant columns in null-supplying side are nullable.
	//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	//   CREATE UNIQUE INDEX ON abcde (b, c)
	//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	//   SELECT * FROM abcde LEFT OUTER JOIN (SELECT * FROM mnpq WHERE p=q) ON True
	nullExtendedCols := util.MakeFastIntSet(10, 11, 12, 13)
	loj := makeAbcdeFD(t)
	mnpq := makeMnpqFD(t)
	mnpq.AddEquivalency(12, 13)
	loj.MakeProduct(mnpq)
	verifyFD(t, loj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12)==(13), (13)==(12)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, loj, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (12)~~>(13), (13)~~>(12)")
	testColsAreKey(t, loj, util.MakeFastIntSet(1, 10, 11), true)

	// One determinant column in null-supplying side is not null.
	//   SELECT * FROM (SELECT DISTINCT ON (b, c) b, c, d, e FROM abcde WHERE b IS NOT NULL) RIGHT OUTER JOIN mnpq ON True
	nullExtendedCols = util.MakeFastIntSet(2, 3, 4, 5)
	loj = makeAbcdeFD(t)
	loj.MakeNotNull(util.MakeFastIntSet(2))
	loj.ProjectCols(util.MakeFastIntSet(2, 3, 4, 5))
	loj.AddStrictKey(util.MakeFastIntSet(2, 3), util.MakeFastIntSet(2, 3, 4, 5))
	loj.MakeProduct(makeMnpqFD(t))
	verifyFD(t, loj, "(2,3,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (2,3)-->(4,5), (10,11)-->(12,13) [removed: (1)]")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(2, 10, 11))
	verifyFD(t, loj, "(2,3,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (2,3)-->(4,5), (10,11)-->(12,13) [removed: (1)]")
	testColsAreKey(t, loj, util.MakeFastIntSet(2, 3, 10, 11), true)

	// Null-extended constants become lax, even if they are not null.
	//   SELECT * FROM (SELECT * FROM abcde WHERE b=1) RIGHT OUTER JOIN mnpq ON True
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	loj = makeAbcdeFD(t)
	loj.AddConstants(util.MakeFastIntSet(2))
	loj.MakeNotNull(util.MakeFastIntSet(2))
	loj.MakeProduct(makeMnpqFD(t))
	verifyFD(t, loj, "(1,10,11): ()-->(2), (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 10, 11))
	verifyFD(t, loj, "(1,10,11): ()~~>(2), (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	testColsAreKey(t, loj, util.MakeFastIntSet(1, 10, 11), true)

	// Add constants on both sides of outer join.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON b=1 AND c=1 AND p=1
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	loj = makeAbcdeFD(t)
	loj.MakeProduct(makeMnpqFD(t))
	loj.AddConstants(util.MakeFastIntSet(2, 3, 12))
	loj.MakeNotNull(util.MakeFastIntSet(2, 3, 12))
	verifyFD(t, loj, "(10,11): ()-->(2,3,12), (1)-->(2-5), (2,3)-->(1,4,5), (10,11)-->(12,13)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 3, 10, 11, 12))
	verifyFD(t, loj, "(10,11): ()-->(12), (1)-->(2-5), (2,3)-->(1,4,5), (10,11)-->(2,3,12,13), ()~~>(2,3)")
	testColsAreKey(t, loj, util.MakeFastIntSet(10, 11), true)

	// Test equivalency on both sides of outer join, and across divide.
	//   SELECT * FROM abcde RIGHT OUTER JOIN mnpq ON b=c AND m=q AND a=m AND d=m
	nullExtendedCols = util.MakeFastIntSet(1, 2, 3, 4, 5)
	loj = makeAbcdeFD(t)
	loj.MakeProduct(makeMnpqFD(t))
	loj.AddEquivalency(1, 3)
	loj.AddEquivalency(10, 13)
	loj.AddEquivalency(1, 10)
	loj.AddEquivalency(4, 10)
	loj.MakeNotNull(util.MakeFastIntSet(3, 4, 12, 13))
	verifyFD(t, loj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(3,10), (3)==(1), (10)==(1,4,13), (13)==(10), (4)==(10)")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 2, 3, 10, 11, 12, 13))
	verifyFD(t, loj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13), (1)==(3), (3)==(1), (10)-->(1,4), (13)==(10), (4)~~>(10), (1)~~>(10), (10)==(13)")
	testColsAreKey(t, loj, util.MakeFastIntSet(10, 11), true)

	// Join keyless relations with nullable columns.
	//   SELECT * FROM (SELECT b, c FROM abcde WHERE b=c) LEFT JOIN (SELECT p, q FROM mnpq WHERE p=q) ON True
	nullExtendedCols = util.MakeFastIntSet(12, 13)
	loj = makeAbcdeFD(t)
	loj.AddEquivalency(2, 3)
	loj.ProjectCols(util.MakeFastIntSet(2, 3))
	mnpq = makeMnpqFD(t)
	mnpq.AddEquivalency(12, 13)
	mnpq.ProjectCols(util.MakeFastIntSet(12, 13))
	loj.MakeProduct(mnpq)
	verifyFD(t, loj, "(1)-->(2,3), (2,3)~~>(1), (2)==(3), (3)==(2), (12)==(13), (13)==(12) [removed: (1)]")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet())
	verifyFD(t, loj, "(1)-->(2,3), (2,3)~~>(1), (2)==(3), (3)==(2), (12)~~>(13), (13)~~>(12) [removed: (1)]")
	testColsAreKey(t, loj, util.MakeFastIntSet(), false)

	// Join keyless relations with not-null columns.
	//   SELECT * FROM (SELECT b, c FROM abcde WHERE b=c) LEFT JOIN (SELECT p, q FROM mnpq WHERE p=q) ON True
	nullExtendedCols = util.MakeFastIntSet(12, 13)
	loj = makeAbcdeFD(t)
	loj.AddEquivalency(2, 3)
	loj.ProjectCols(util.MakeFastIntSet(2, 3))
	mnpq = makeMnpqFD(t)
	mnpq.AddEquivalency(12, 13)
	mnpq.ProjectCols(util.MakeFastIntSet(12, 13))
	loj.MakeProduct(mnpq)
	verifyFD(t, loj, "(1)-->(2,3), (2,3)~~>(1), (2)==(3), (3)==(2), (12)==(13), (13)==(12) [removed: (1)]")
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(2, 3, 12, 13))
	verifyFD(t, loj, "(1)-->(2,3), (2,3)~~>(1), (2)==(3), (3)==(2), (12)==(13), (13)==(12) [removed: (1)]")
	testColsAreKey(t, loj, util.MakeFastIntSet(), false)
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
	testColsAreKey(t, abcde, util.MakeFastIntSet(1), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(2, 3), false)
	testColsAreKey(t, abcde, util.MakeFastIntSet(1, 2), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(1, 2, 3, 4, 5), true)
	testColsAreKey(t, abcde, util.MakeFastIntSet(4, 5), false)
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
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10), false)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10, 11), true)
	testColsAreKey(t, mnpq, util.MakeFastIntSet(10, 11, 12), true)
	return mnpq
}

// Construct cartesian product FD from figure 3.6, page 122:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
//   SELECT * FROM abcde, mnpq
func makeProductFD(t *testing.T) *props.FuncDepSet {
	product := makeAbcdeFD(t)
	product.MakeProduct(makeMnpqFD(t))
	verifyFD(t, product, "(1,10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(12,13)")
	testColsAreKey(t, product, util.MakeFastIntSet(1), false)
	testColsAreKey(t, product, util.MakeFastIntSet(10, 11), false)
	testColsAreKey(t, product, util.MakeFastIntSet(1, 10, 11), true)
	testColsAreKey(t, product, util.MakeFastIntSet(1, 2, 3, 10, 11, 12), true)
	testColsAreKey(t, product, util.MakeFastIntSet(2, 3, 10, 11), false)
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
	testColsAreKey(t, join, util.MakeFastIntSet(1, 11), true)
	testColsAreKey(t, join, util.MakeFastIntSet(1, 10), false)
	testColsAreKey(t, join, util.MakeFastIntSet(1, 10, 11), true)
	testColsAreKey(t, join, util.MakeFastIntSet(1), false)
	testColsAreKey(t, join, util.MakeFastIntSet(10, 11), true)
	return join
}

// Construct left outer join FD:
//   SELECT * FROM abcde LEFT JOIN mnpq ON a=m
func makeLeftJoinFD(t *testing.T) *props.FuncDepSet {
	nullExtendedCols := util.MakeFastIntSet(10, 11, 12, 13)
	loj := makeJoinFD(t)
	loj.MakeOuter(nullExtendedCols, util.MakeFastIntSet(1, 10, 11))
	verifyFD(t, loj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1,12,13), (1)-->(10), (10)~~>(1)")
	loj.MakeNotNull(util.MakeFastIntSet(1))
	verifyFD(t, loj, "(10,11): (1)-->(2-5), (2,3)~~>(1,4,5), (10,11)-->(1,12,13), (1)-->(10), (10)~~>(1)")
	testColsAreKey(t, loj, util.MakeFastIntSet(10, 11), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(1, 11), true)
	testColsAreKey(t, loj, util.MakeFastIntSet(1, 10), false)
	return loj
}

func verifyFD(t *testing.T, f *props.FuncDepSet, expected string) {
	t.Helper()
	actual := f.String()
	if actual != expected {
		t.Errorf("\nexpected: %s\nactual  : %s", expected, actual)
	}
}

func testColsAreKey(t *testing.T, f *props.FuncDepSet, cols opt.ColSet, expected bool) {
	t.Helper()
	actual := f.ColsAreKey(cols)
	if actual != expected {
		if expected {
			t.Errorf("%s is not a key for %s", cols, f)
		} else {
			t.Errorf("%s is a key for %s", cols, f)
		}
	}
}
