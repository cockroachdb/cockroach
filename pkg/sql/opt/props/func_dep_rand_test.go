// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

// This file is the home of TestFuncDepOpsRandom, a randomized FD tester.

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

const debug = false

// testVal is a value in a row in a test relation.
// Value of 0 is special and is treated as NULL.
type testVal uint8

const null testVal = 0

// A testRow in a test relation. Value of 0 is special and is treated as NULL.
// The first value corresponds to ColumnID 0, and so on.
// A testRow is immutable after initial construction.
type testRow []testVal

func (tr testRow) value(col opt.ColumnID) testVal {
	return tr[col-1]
}

func (tr testRow) String() string {
	var b strings.Builder
	for i, v := range tr {
		if i > 0 {
			b.WriteByte(',')
		}
		if v == null {
			b.WriteString("NULL")
		} else {
			fmt.Fprintf(&b, "%d", v)
		}
	}
	return b.String()
}

// rowKey encodes the values of at most 8 columns.
type rowKey uint64

// key generates a rowKey from the values on the given columns. Also returns
// whether there were any null values.
func (tr testRow) key(cols opt.ColSet) (_ rowKey, hasNulls bool) {
	var key rowKey
	cols.ForEach(func(c opt.ColumnID) {
		val := tr.value(c)
		if val == null {
			hasNulls = true
		}
		key = (key << 8) | rowKey(val)
	})
	return key, hasNulls
}

// hasNulls returns true if the row has a null value on any of the given
// columns.
func (tr testRow) hasNulls(cols opt.ColSet) bool {
	var res bool
	cols.ForEach(func(c opt.ColumnID) {
		res = res || tr.value(c) == null
	})
	return res
}

// equalOn returns true if the two rows are equal on the given columns.
func (tr testRow) equalOn(other testRow, cols opt.ColSet) bool {
	eq := true
	cols.ForEach(func(c opt.ColumnID) {
		if tr.value(c) != other.value(c) {
			eq = false
		}
	})
	return eq
}

type testRelation []testRow

// checkKey verifies that a certain key (strict or lax) is valid for the test
// relation.
func (tr testRelation) checkKey(key opt.ColSet, typ keyType) error {
	m := make(map[rowKey]testRow)
	for _, r := range tr {
		k, hasNulls := r.key(key)
		// If it is a lax key, we can ignore any rows that contain a NULL on the
		// key columns.
		if typ == laxKey && hasNulls {
			continue
		}
		if existingRow, ok := m[k]; ok {
			keyStr := ""
			if typ == laxKey {
				keyStr = "lax-"
			}
			return fmt.Errorf("%skey%s doesn't hold on rows %s and %s", keyStr, key, r, existingRow)
		}
		m[k] = r
	}
	return nil
}

// checkFD verifies that a certain FD holds for the test relation.
func (tr testRelation) checkFD(dep funcDep) error {
	if dep.equiv {
		// An equivalence FD is easy to check row-by-row.
		for _, r := range tr {
			c, _ := dep.from.Next(0)
			val := r.value(c)
			fail := false
			dep.to.ForEach(func(col opt.ColumnID) {
				if r.value(col) != val {
					fail = true
				}
			})
			if fail {
				return fmt.Errorf("FD %s doesn't hold on row %s", &dep, r)
			}
		}
		return nil
	}

	// We split the rows into groups (keyed on the `from` columns), picking the
	// first row in each group as the "representative" of that group. All other
	// rows in the group are checked against the representative row.
	m := make(map[rowKey]testRow)
	for _, r := range tr {
		k, hasNulls := r.key(dep.from)
		// If it is not a strict FD, we can ignore any rows that contain a NULL on
		// the 'from' columns.
		if !dep.strict && hasNulls {
			continue
		}

		if first, ok := m[k]; ok {
			if !first.equalOn(r, dep.to) {
				return fmt.Errorf("FD %s doesn't hold on rows %s and %s", &dep, first, r)
			}
		} else {
			m[k] = r
		}
	}
	return nil
}

// checkFDs verifies that the given FDs hold against the test relation. It also
// verifies the results of ColsAreLaxKey/ColsAreStrictKey.
func (tr testRelation) checkFDs(fd *FuncDepSet) error {
	// Check deps.
	for _, dep := range fd.deps {
		if err := tr.checkFD(dep); err != nil {
			return err
		}
	}

	// Check keys.
	if fd.hasKey != noKey {
		if err := tr.checkKey(fd.key, fd.hasKey); err != nil {
			return err
		}
	}

	return nil
}

// testOp is an interface implemented by test operations. Each test operation
// makes a call to an FD API and filters out some of the rows in the current
// test relation in order for that API call to be correct. The resulting FDs are
// checked to hold on the updated test relations (and various FD APIs are
// checked as well).
type testOp interface {
	fmt.Stringer

	// FilterRelation returns a new subset testRelation that is consistent with
	// the FD operation.
	FilterRelation(tr testRelation) testRelation

	// ApplyToFDs returns a new FuncDepSet after the operation.
	ApplyToFDs(fd FuncDepSet) FuncDepSet
}

var _ testOp = &addKeyOp{}

type testConfig struct {
	// numCols is the number of columns in the relation.
	// Can be at most 8 (see rowKey).
	numCols int
	// valRange is the range of values in the test relation.
	valRange testVal
}

func (tc *testConfig) randCol() opt.ColumnID {
	return opt.ColumnID(rand.Intn(tc.numCols) + 1)
}

func (tc *testConfig) randColSet(minLen, maxLen int) opt.ColSet {
	if maxLen > tc.numCols {
		panic(errors.AssertionFailedf("maxLen > numCols"))
	}
	length := rand.Intn(maxLen-minLen+1) + minLen
	// Use Robert Floyd's algorithm to generate <length> distinct integers between
	// 0 and numCols-1, just because it's so cool!
	var res opt.ColSet
	for j := tc.numCols - length; j < tc.numCols; j++ {
		if t := rand.Intn(j + 1); !res.Contains(opt.ColumnID(t + 1)) {
			res.Add(opt.ColumnID(t + 1))
		} else {
			res.Add(opt.ColumnID(j + 1))
		}
	}
	return res
}

func (tc *testConfig) allCols() opt.ColSet {
	var allCols opt.ColSet
	for i := opt.ColumnID(1); i <= opt.ColumnID(tc.numCols); i++ {
		allCols.Add(i)
	}
	return allCols
}

// initTestRelation creates a testRelation with all possible combinations of
// values in the set {0 (null), 1, 2, ... valRange}.
func (tc *testConfig) initTestRelation() testRelation {
	var tr testRelation

	// genRows takes a row prefix and recursively generates all rows with that
	// prefix, appending them to tr.
	var genRows func(vals []testVal)
	genRows = func(vals []testVal) {
		n := len(vals)
		if n == tc.numCols {
			// Add each row twice to have duplicates.
			tr = append(tr, vals, vals)
			return
		}
		for i := null; i <= tc.valRange; i++ {
			genRows(append(vals[:n:n], i))
		}
	}
	genRows(nil)
	return tr
}

func (tc *testConfig) checkAPIs(fd *FuncDepSet, tr testRelation) error {
	if fd.HasMax1Row() && len(tr) > 1 {
		return fmt.Errorf("HasMax1Row() incorrectly returns true")
	}

	for t := 0; t < 5; t++ {
		cols := tc.randColSet(1, tc.numCols)

		if fd.ColsAreLaxKey(cols) {
			if err := tr.checkKey(cols, laxKey); err != nil {
				return fmt.Errorf("ColsAreLaxKey%s incorrectly returns true", cols)
			}
		}

		if fd.ColsAreStrictKey(cols) {
			if err := tr.checkKey(cols, strictKey); err != nil {
				return fmt.Errorf("ColsAreStrictKey%s incorrectly returns true", cols)
			}
		}

		closure := fd.ComputeClosure(cols)
		if err := tr.checkFD(funcDep{
			from:   cols,
			to:     closure,
			strict: true,
		}); err != nil {
			return fmt.Errorf("ComputeClosure%s incorrectly returns %s: %s", cols, closure, err)
		}

		reduced := fd.ReduceCols(cols)
		if err := tr.checkFD(funcDep{
			from:   reduced,
			to:     cols,
			strict: true,
		}); err != nil {
			return fmt.Errorf("ReduceCols%s incorrectly returns %s: %s", cols, reduced, err)
		}

		var proj FuncDepSet
		proj.CopyFrom(fd)
		proj.ProjectCols(cols)
		// The FDs after projection should still hold on the table.
		if err := tr.checkFDs(&proj); err != nil {
			return fmt.Errorf("ProjectCols%s incorrectly returns %s: %s", cols, proj.String(), err)
		}
	}

	return nil
}

// testOpGenerator generates a testOp, given the number of columns in the
// relation.
type testOpGenerator = func(tc *testConfig) testOp

// addKeyOp is a test operation corresponding to AddStrictKey / AddLaxKey.
type addKeyOp struct {
	allCols opt.ColSet
	key     opt.ColSet
	typ     keyType
}

func genAddKey(minKeyCols, maxKeyCols int) testOpGenerator {
	return func(tc *testConfig) testOp {
		cols := tc.randColSet(minKeyCols, maxKeyCols)
		typ := strictKey
		if !cols.Empty() && rand.Int()%2 == 0 {
			typ = laxKey
		}
		return &addKeyOp{
			allCols: tc.allCols(),
			key:     cols,
			typ:     typ,
		}
	}
}

func (o *addKeyOp) String() string {
	if o.typ == strictKey {
		return fmt.Sprintf("AddStrictKey%s", o.key)
	}
	return fmt.Sprintf("AddLaxKey%s", o.key)
}

func (o *addKeyOp) FilterRelation(tr testRelation) testRelation {
	var out testRelation
	// Process the rows in random order and remove any duplicate rows.
	m := make(map[rowKey]bool)
	perm := rand.Perm(len(tr))
	for _, rowIdx := range perm {
		r := tr[perm[rowIdx]]
		key, hasNulls := r.key(o.key)
		// If it is a lax key, we can leave all rows that contain a NULL on the
		// key columns.
		if o.typ == laxKey && hasNulls {
			out = append(out, r)
			continue
		}
		if !m[key] {
			out = append(out, r)
			m[key] = true
		}
	}
	return out
}

func (o *addKeyOp) ApplyToFDs(fd FuncDepSet) FuncDepSet {
	var out FuncDepSet
	out.CopyFrom(&fd)
	if o.typ == strictKey {
		out.AddStrictKey(o.key, o.allCols)
	} else {
		out.AddLaxKey(o.key, o.allCols)
	}
	return out
}

// makeNotNullOp is a test operation corresponding to MakeNotNull.
type makeNotNullOp struct {
	cols opt.ColSet
}

func genMakeNotNull(minCols, maxCols int) testOpGenerator {
	return func(tc *testConfig) testOp {
		return &makeNotNullOp{
			cols: tc.randColSet(minCols, maxCols),
		}
	}
}

func (o *makeNotNullOp) String() string {
	return fmt.Sprintf("MakeNotNull%s", o.cols)
}

func (o *makeNotNullOp) FilterRelation(tr testRelation) testRelation {
	var out testRelation
	for _, r := range tr {
		if !r.hasNulls(o.cols) {
			out = append(out, r)
		}
	}
	return out
}

func (o *makeNotNullOp) ApplyToFDs(fd FuncDepSet) FuncDepSet {
	var out FuncDepSet
	out.CopyFrom(&fd)
	out.MakeNotNull(o.cols)
	return out
}

// addConstOp is a test operation corresponding to AddConstants.
type addConstOp struct {
	cols opt.ColSet
	vals []testVal
}

func genAddConst(minCols, maxCols int) testOpGenerator {
	return func(tc *testConfig) testOp {
		cols := tc.randColSet(minCols, maxCols)
		vals := make([]testVal, cols.Len())
		for i := range vals {
			vals[i] = testVal(rand.Intn(int(tc.valRange + 1)))
		}
		return &addConstOp{
			cols: cols,
			vals: vals,
		}
	}
}

func (o *addConstOp) String() string {
	return fmt.Sprintf("AddConstants%s values {%v}", o.cols, testRow(o.vals).String())
}

func (o *addConstOp) FilterRelation(tr testRelation) testRelation {
	var out testRelation
	for _, r := range tr {
		idx := 0
		ok := true
		o.cols.ForEach(func(c opt.ColumnID) {
			if r[c-1] != o.vals[idx] {
				ok = false
			}
			idx++
		})
		if ok {
			out = append(out, r)
		}
	}
	return out
}

func (o *addConstOp) ApplyToFDs(fd FuncDepSet) FuncDepSet {
	var out FuncDepSet
	out.CopyFrom(&fd)
	out.AddConstants(o.cols)
	return out
}

// addEquivOp is a test operation corresponding to AddEquivalency.
type addEquivOp struct {
	a, b opt.ColumnID
}

func genAddEquiv() testOpGenerator {
	return func(tc *testConfig) testOp {
		return &addEquivOp{
			a: tc.randCol(),
			b: tc.randCol(),
		}
	}
}

func (o *addEquivOp) String() string {
	return fmt.Sprintf("AddEquivalency(%d,%d)", o.a, o.b)
}

func (o *addEquivOp) FilterRelation(tr testRelation) testRelation {
	// Filter out rows where the equivalency doesn't hold.
	var out testRelation
	for _, r := range tr {
		if r.value(o.a) == r.value(o.b) {
			out = append(out, r)
		}
	}
	return out
}

func (o *addEquivOp) ApplyToFDs(fd FuncDepSet) FuncDepSet {
	var out FuncDepSet
	out.CopyFrom(&fd)
	out.AddEquivalency(o.a, o.b)
	return out
}

// addSynthOp is a test operation corresponding to AddSynthesizedCol.
type addSynthOp struct {
	from opt.ColSet
	to   opt.ColumnID
}

func genAddSynth(minCols, maxCols int) testOpGenerator {
	return func(tc *testConfig) testOp {
		from := tc.randColSet(minCols, maxCols)
		to := tc.randCol()
		from.Remove(to)
		return &addSynthOp{
			from: from,
			to:   to,
		}
	}
}

func (o *addSynthOp) String() string {
	return fmt.Sprintf("AddSynthesizedCol(%s, %d)", o.from, o.to)
}

func (o *addSynthOp) FilterRelation(tr testRelation) testRelation {
	// Filter out rows where the from->to FD doesn't hold. The code here parallels
	// that in testRelation.checkKey.
	//
	// We split the rows into groups (keyed on the `from` columns), picking the
	// first row in each group as the "representative" of that group. All other
	// rows in the group are checked against the representative row.
	var out testRelation
	m := make(map[rowKey]testRow)
	perm := rand.Perm(len(tr))
	for _, rowIdx := range perm {
		r := tr[rowIdx]
		k, _ := r.key(o.from)
		if first, ok := m[k]; ok {
			if first.value(o.to) != r.value(o.to) {
				// Filter out row.
				continue
			}
		} else {
			m[k] = r
		}
		out = append(out, r)
	}
	return out
}

func (o *addSynthOp) ApplyToFDs(fd FuncDepSet) FuncDepSet {
	var out FuncDepSet
	out.CopyFrom(&fd)
	out.AddSynthesizedCol(o.from, o.to)
	return out
}

// TestFuncDepOpsRandom performs random FD operations and maintains a test
// relation in parallel, making sure that the FDs always hold w.r.t the test
// table. We start with a "full" table (all possible combinations of values in a
// certain range) and each operation filters out rows in conformance with the
// operation (e.g. if we add a key, we remove duplicate rows). We also test
// various FuncDepSet APIs at each stage.
//
// To reuse work, instead of generating one chain of operations at a time, we
// generate a tree of operations; each path from root to a leaf is a chain that
// is getting tested.
//
func TestFuncDepOpsRandom(t *testing.T) {
	type testParams struct {
		testConfig

		// maxDepth is the maximum length of a chain of test operations.
		maxDepth int

		// branching is the number of test operations, generated at each level.
		// The number of total operations is branching ^ maxDepth.
		branching int

		ops []testOpGenerator
	}

	testConfigs := []testParams{
		{
			testConfig: testConfig{
				numCols:  3,
				valRange: 3,
			},
			maxDepth:  5,
			branching: 3,
			ops: []testOpGenerator{
				genAddKey(0 /* minKeyCols */, 2 /* maxKeyCols */),
				genMakeNotNull(1 /* minCols */, 3 /* maxCols */),
				genAddConst(1 /* minCols */, 3 /* maxCols */),
				genAddEquiv(),
				genAddSynth(0 /* minCols */, 3 /* maxCols */),
			},
		},

		{
			testConfig: testConfig{
				numCols:  5,
				valRange: 2,
			},
			maxDepth:  8,
			branching: 2,
			ops: []testOpGenerator{
				genAddKey(1 /* minKeyCols */, 5 /* maxKeyCols */),
				genMakeNotNull(1 /* minCols */, 4 /* maxCols */),
				genAddConst(1 /* minCols */, 4 /* maxCols */),
				genAddEquiv(),
				genAddSynth(0 /* minCols */, 3 /* maxCols */),
			},
		},
	}

	const repeats = 100

	for _, cfg := range testConfigs {
		for r := 0; r < repeats; r++ {
			ops := make([]testOp, cfg.maxDepth+1)
			rels := make([]testRelation, cfg.maxDepth+1)
			rels[0] = cfg.initTestRelation()
			fds := make([]FuncDepSet, cfg.maxDepth+1)

			var run func(depth int)
			run = func(depth int) {
				for i := 0; i < cfg.branching; i++ {
					opGenIdx := rand.Intn(len(cfg.ops))
					op := cfg.ops[opGenIdx](&cfg.testConfig)

					ops[depth] = op
					rels[depth] = op.FilterRelation(rels[depth-1])

					// Zero out in case ApplyToFDs below panics.
					fds[depth] = FuncDepSet{}
					err := func() (err error) {
						defer func() {
							if r := recover(); r != nil {
								err = errors.AssertionFailedf("%v", r)
							}
						}()
						fds[depth] = op.ApplyToFDs(fds[depth-1])
						fds[depth].Verify()
						if err = rels[depth].checkFDs(&fds[depth]); err != nil {
							return err
						}
						if err = cfg.checkAPIs(&fds[depth], rels[depth]); err != nil {
							return err
						}
						return nil
					}()
					if err != nil {
						// Generate a useful error string that shows the entire chain of
						// operations. For example:
						//   initial numCols=3 valRange=3
						//    => MakeNotNull(2)
						//       FDs:
						//    => AddConstants(1,3) values {NULL,1}
						//       FDs: ()-->(1,3)
						//    => AddLaxKey(3)
						//       FDs: ()-->(1-3)
						var b strings.Builder
						fmt.Fprintf(&b, "details below\n")
						fmt.Fprintf(&b, "initial numCols=%d valRange=%d\n", cfg.numCols, cfg.valRange)
						for j := 1; j <= depth; j++ {
							fmt.Fprintf(&b, " => %s\n", ops[j].String())
							fmt.Fprintf(&b, "    FDs: %s\n", fds[j].String())
						}
						fmt.Fprintf(&b, "\n%+v", err)
						t.Fatal(b.String())
					}

					if debug {
						fmt.Printf("%d: %s %s\n", depth, op, fds[depth])
						for _, r := range rels[depth] {
							fmt.Printf("  %s\n", r)
						}
					}

					if depth < cfg.maxDepth {
						run(depth + 1)
					}
				}
			}
			run(1)
		}
	}
}

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	os.Exit(m.Run())
}
