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
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

const debug = false

// testVal is a value in a row in a test relation.
// Value of 0 is special and is treated as NULL.
type testVal uint8

const null testVal = 0

func (v testVal) String() string {
	if v == null {
		return "NULL"
	}
	return fmt.Sprintf("%d", v)
}

// A testRow in a test relation. Value of 0 is special and is treated as NULL.
// The first value corresponds to ColumnID 1, and so on.
// A testRow is immutable after initial construction.
type testRow []testVal

func (tr testRow) value(col opt.ColumnID) testVal {
	return tr[col-1]
}

func (tr testRow) String() string {
	var b strings.Builder
	for i, v := range tr {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(v.String())
	}
	return b.String()
}

// rowKey encodes the values of at most 16 columns.
type rowKey uint64

// key generates a rowKey from the values on the given columns. Also returns
// whether there were any null values.
func (tr testRow) key(cols opt.ColSet) (_ rowKey, hasNulls bool) {
	if cols.Len() > 16 {
		panic(errors.AssertionFailedf("max 16 columns supported"))
	}
	var key rowKey
	cols.ForEach(func(c opt.ColumnID) {
		val := tr.value(c)
		if val == null {
			hasNulls = true
		}
		if val > 15 {
			panic(errors.AssertionFailedf("testVal must be <= 15"))
		}
		key = (key << 4) | rowKey(val)
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

// String prints out the test relation in the following format:
//
//   1     2     3
//   -------------
//   NULL  1     2
//   3     NULL  4
//
func (tr testRelation) String() string {
	if len(tr) == 0 {
		return "  <empty>\n"
	}
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	for i := range tr[0] {
		fmt.Fprintf(tw, "%d\t", i+1)
	}
	fmt.Fprint(tw, "\n")
	for _, r := range tr {
		for _, v := range r {
			fmt.Fprintf(tw, "%s\t", v)
		}
		fmt.Fprint(tw, "\n")
	}
	_ = tw.Flush()

	rows := strings.Split(buf.String(), "\n")
	buf.Reset()
	fmt.Fprintf(&buf, "  %s\n  ", strings.TrimRight(rows[0], " "))
	for range rows[0] {
		buf.WriteByte('-')
	}
	buf.WriteString("\n")
	for _, r := range rows[1:] {
		fmt.Fprintf(&buf, "  %s\n", strings.TrimRight(r, " "))
	}
	return buf.String()
}

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
			return fmt.Errorf(
				"%skey%s doesn't hold on rows:\n%s", keyStr, key, testRelation{r, existingRow},
			)
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
				return fmt.Errorf("FD %s doesn't hold on rows:\n%s", &dep, testRelation{first, r})
			}
		} else {
			m[k] = r
		}
	}
	return nil
}

// checkFDs verifies that the given FDs hold against the test relation.
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

// notNullCols returns the set columns that have no nulls in the test relation.
func (tr testRelation) notNullCols(numCols int) opt.ColSet {
	var res opt.ColSet
	for c := opt.ColumnID(1); c <= opt.ColumnID(numCols); c++ {
		res.Add(c)
		for _, r := range tr {
			if r.value(c) == null {
				res.Remove(c)
				break
			}
		}
	}
	return res
}

// joinTestRelations creates a possible result of joining two testRelations,
// specifically:
//  - an inner join if both leftOuter and rightOuter are false;
//  - a left/right outer join if one of them is true;
//  - a full outer join if both are true.
func joinTestRelations(
	numLeftCols int,
	left testRelation,
	numRightCols int,
	right testRelation,
	filters []testOp,
	leftOuter bool,
	rightOuter bool,
) testRelation {
	var res testRelation

	// Adds the given rows to the join result.
	add := func(l, r testRow) {
		newRow := make(testRow, numLeftCols+numRightCols)
		if l != nil {
			copy(newRow, l)
		}
		if r != nil {
			copy(newRow[numLeftCols:], r)
		}
		res = append(res, newRow)
	}

	// Perform a cross join between the left and right relations.
	for _, leftRow := range left {
		for _, rightRow := range right {
			add(leftRow, rightRow)
		}
	}

	// Apply the filters to the result of the cross join.
	for i := range filters {
		res = filters[i].FilterRelation(res)
	}

	// Walk through the now-filtered cartesian product and keep track of all
	// unique left and right rows.
	leftCols := makeCols(numLeftCols)
	rightCols := makeCols(numRightCols)
	matchedLeftRows := map[rowKey]struct{}{}
	matchedRightRows := map[rowKey]struct{}{}
	for _, row := range res {
		leftKey, _ := row.key(leftCols)
		rightKey, _ := row.key(shiftSet(rightCols, numLeftCols))
		matchedLeftRows[leftKey] = struct{}{}
		matchedRightRows[rightKey] = struct{}{}
	}

	// If leftOuter is true, add back any left rows that were filtered out,
	// null-extending the right side.
	if leftOuter {
		for _, row := range left {
			key, _ := row.key(leftCols)
			if _, ok := matchedLeftRows[key]; !ok {
				add(row, nil)
			}
		}
	}

	// If rightOuter is true, add back any right rows that were filtered out,
	// null-extending the left side.
	if rightOuter {
		for _, row := range right {
			key, _ := row.key(rightCols)
			if _, ok := matchedRightRows[key]; !ok {
				add(nil, row)
			}
		}
	}

	if debug {
		fmt.Printf("left:\n%s", left)
		fmt.Printf("right:\n%s", right)
		fmt.Printf("filters:\n%s", filters)
		fmt.Printf("join(leftOuter=%t, rightOuter=%t):\n%s", leftOuter, rightOuter, res)
	}
	return res
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
	return makeCols(tc.numCols)
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

// testState corresponds to a chain of applied test operations. The head of a
// testStates chain has no parent and no op and just corresponds to the initial
// (empty) FDs and test relation.
type testState struct {
	parent *testState
	cfg    *testConfig

	op  testOp
	fds FuncDepSet
	rel testRelation
}

func (ts *testState) format(b *strings.Builder) {
	if ts.parent == nil {
		fmt.Fprintf(b, "initial numCols=%d valRange=%d\n", ts.cfg.numCols, ts.cfg.valRange)
	} else {
		ts.parent.format(b)
		fmt.Fprintf(b, " => %s\n", ts.op.String())
		fmt.Fprintf(b, "    FDs: %s\n", ts.fds.String())
	}
}

// String describes the chain of operations and corresponding FDs.
// For example:
//   initial numCols=3 valRange=3
//    => MakeNotNull(2)
//       FDs:
//    => AddConstants(1,3) values {NULL,1}
//       FDs: ()-->(1,3)
//    => AddLaxKey(3)
//       FDs: ()-->(1-3)
//
func (ts *testState) String() string {
	var b strings.Builder
	ts.format(&b)
	return b.String()
}

func newTestState(cfg *testConfig) *testState {
	state := &testState{cfg: cfg}
	state.rel = cfg.initTestRelation()
	return state
}

func (ts *testState) child(t *testing.T, op testOp) *testState {
	child := &testState{
		parent: ts,
		cfg:    ts.cfg,
		op:     op,
		rel:    op.FilterRelation(ts.rel),
	}

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errors.AssertionFailedf("%v", r)
			}
		}()
		child.fds = op.ApplyToFDs(ts.fds)
		child.fds.Verify()
		if err = child.rel.checkFDs(&child.fds); err != nil {
			return err
		}
		if err = ts.cfg.checkAPIs(&child.fds, child.rel); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		t.Fatalf("details below\n%s\n%+v", child.String(), err)
	}

	return child
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
				valRange: 2,
			},
			maxDepth:  2,
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
			maxDepth:  5,
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

	filterConfigs := []testParams{
		{
			testConfig: testConfig{
				numCols:  6,
				valRange: 2,
			},
			maxDepth:  3,
			branching: 3,
			ops: []testOpGenerator{
				genMakeNotNull(1 /* minCols */, 4 /* maxCols */),
				genAddConst(0 /* minCols */, 6 /* maxCols */),
				genAddEquiv(),
			},
		},

		{
			testConfig: testConfig{
				numCols:  8,
				valRange: 2,
			},
			maxDepth:  2,
			branching: 3,
			ops: []testOpGenerator{
				genMakeNotNull(0 /* minCols */, 8 /* maxCols */),
				genAddConst(3 /* minCols */, 5 /* maxCols */),
				genAddEquiv(),
			},
		},

		{
			testConfig: testConfig{
				numCols:  10,
				valRange: 2,
			},
			maxDepth:  3,
			branching: 6,
			ops: []testOpGenerator{
				genMakeNotNull(2 /* minCols */, 7 /* maxCols */),
				genAddConst(2 /* minCols */, 3 /* maxCols */),
				genAddEquiv(),
			},
		},
	}

	// Allows a set of filters to be chosen based on the total number of columns
	// in the input relations.
	filterIndex := map[int]int{6: 0, 8: 1, 10: 2}

	const repeats = 100

	for _, cfg := range testConfigs {
		for r := 0; r < repeats; r++ {
			var run func(state *testState, depth int)
			run = func(state *testState, depth int) {
				for i := 0; i < cfg.branching; i++ {
					opGenIdx := rand.Intn(len(cfg.ops))
					op := cfg.ops[opGenIdx](&cfg.testConfig)

					child := state.child(t, op)

					if debug {
						fmt.Printf("%d: %s %s\n", depth, op, child.fds.String())
						for _, r := range child.rel {
							fmt.Printf("  %s\n", r)
						}
					}

					if depth < cfg.maxDepth {
						run(child, depth+1)
					}
				}
			}
			run(newTestState(&cfg.testConfig), 1 /* depth */)
		}
	}

	// Run tests for joins.
	for r := 0; r < repeats; r++ {
		// Generate left and right input op chains.
		genRandChain := func() *testState {
			cfg := testConfigs[rand.Intn(len(testConfigs))]
			state := newTestState(&cfg.testConfig)

			steps := 1 + rand.Intn(cfg.maxDepth)
			for i := 0; i < steps; i++ {
				opGenIdx := rand.Intn(len(cfg.ops))
				op := cfg.ops[opGenIdx](&cfg.testConfig)
				state = state.child(t, op)
			}
			return state
		}
		left := genRandChain()
		nLeft := left.cfg.numCols
		leftCols := left.cfg.allCols()
		right := genRandChain()
		nRight := right.cfg.numCols
		rightCols := shiftSet(right.cfg.allCols(), nLeft)
		rightFDs := shiftColumns(right.fds, nLeft)

		// Generate filter ops.
		var filters []testOp
		filtersFDs := FuncDepSet{}
		cfg := filterConfigs[filterIndex[nLeft+nRight]]
		steps := 1 + rand.Intn(cfg.maxDepth)
		for i := 0; i < steps; i++ {
			opGenIdx := rand.Intn(len(cfg.ops))
			op := cfg.ops[opGenIdx](&cfg.testConfig)
			filters = append(filters, op)
			filtersFDs = op.ApplyToFDs(filtersFDs)
		}

		// Test inner join.
		join := joinTestRelations(
			nLeft, left.rel, nRight, right.rel, filters, false /* leftOuter */, false, /* rightOuter */
		)

		var fd FuncDepSet
		fd.CopyFrom(&left.fds)
		fd.MakeProduct(&rightFDs)
		fd.AddFrom(&filtersFDs)
		if err := join.checkFDs(&fd); err != nil {
			t.Fatalf(
				"MakeProduct and AddFrom returned incorrect FDs\n"+
					"left:    %s\n"+
					"right:   %s\n"+
					"filters: %s\n"+
					"result:  %s\n"+
					"error:   %+v\n\n"+
					"left side: %s\n"+
					"right side (cols shifted by %d): %s\n"+
					"filter steps: %s",
				&left.fds, &rightFDs, &filtersFDs, &fd, err, left, nLeft, right, filters,
			)
		}

		leftNotNullCols := left.rel.notNullCols(nLeft)
		rightNotNullCols := shiftSet(right.rel.notNullCols(nRight), nLeft)
		notNullInputCols := leftNotNullCols.Union(rightNotNullCols)

		// Test left join.
		join = joinTestRelations(
			nLeft, left.rel, nRight, right.rel, filters, true /* leftOuter */, false, /* rightOuter */
		)

		fd.CopyFrom(&left.fds)
		fd.MakeProduct(&rightFDs)
		fd.MakeLeftOuter(&left.fds, &filtersFDs, leftCols, rightCols, notNullInputCols)
		if err := join.checkFDs(&fd); err != nil {
			t.Fatalf(
				"MakeLeftOuter(..., %s, %s, %s) returned incorrect FDs\n"+
					"left:    %s\n"+
					"right:   %s\n"+
					"filters: %s\n"+
					"result:  %s\n"+
					"error:   %+v\n\n"+
					"left side: %s\n"+
					"right side (cols shifted by %d): %s\n"+
					"filter steps: %s",
				leftCols, rightCols, notNullInputCols,
				&left.fds, &rightFDs, &filtersFDs, &fd, err, left, nLeft, right, filters,
			)
		}

		// Test outer join.
		join = joinTestRelations(
			nLeft, left.rel, nRight, right.rel, nil, true /* leftOuter */, true, /* rightOuter */
		)

		fd.CopyFrom(&left.fds)
		fd.MakeProduct(&rightFDs)
		fd.MakeFullOuter(leftCols, rightCols, notNullInputCols)
		if err := join.checkFDs(&fd); err != nil {
			t.Fatalf(
				"MakeFullOuter(%s, %s, %s) returned incorrect FDs\n"+
					"left:   %s\n"+
					"right:  %s\n"+
					"result: %s\n"+
					"error:  %+v\n\n"+
					"left side: %s\n"+
					"right side (cols shifted by %d): %s",
				leftCols, rightCols, notNullInputCols,
				&left.fds, &rightFDs, &fd, err, left, nLeft, right,
			)
		}
	}
}

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	os.Exit(m.Run())
}

func shiftSet(cols opt.ColSet, delta int) opt.ColSet {
	var res opt.ColSet
	cols.ForEach(func(col opt.ColumnID) {
		res.Add(col + opt.ColumnID(delta))
	})
	return res
}

func shiftColumns(fd FuncDepSet, delta int) FuncDepSet {
	var res FuncDepSet
	res.CopyFrom(&fd)
	for i := range res.deps {
		d := &res.deps[i]
		d.from = shiftSet(d.from, delta)
		d.to = shiftSet(d.to, delta)
	}
	res.key = shiftSet(res.key, delta)
	return res
}

func makeCols(numCols int) opt.ColSet {
	var allCols opt.ColSet
	for i := opt.ColumnID(1); i <= opt.ColumnID(numCols); i++ {
		allCols.Add(i)
	}
	return allCols
}
