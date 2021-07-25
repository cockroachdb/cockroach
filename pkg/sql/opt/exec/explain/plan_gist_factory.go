// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// PlanGist is a compact representation of a logical plan meant to be used as
// a key and log for different plans used to implement a particular query.  A
// fingerprint doesn't change for the following:
// - literal constant values
// - alias names
// - grouping column names
// - constraint values
// - estimated rows stats
// The notion is that the fingerprint is the rough shape of the plan that
// represents the way the plan operators are put together and what tables and
// indexes they use.
// PlanGist starts out with just the []bytes and the string rep and hash are
// lazily calculated.   The rationale is that we can lazily create those AFTER
// query execution and returning results to the client if possible.
type PlanGist struct {
	bytes []byte
	str   string
	hash  uint64
}

// String returns the fingerprint bytes as a base64 encoded string.
func (fp *PlanGist) String() string {
	if len(fp.str) == 0 {
		fp.str = base64.StdEncoding.EncodeToString(fp.bytes)
		h := fnv.New64()
		h.Write([]byte(fp.str))
		fp.hash = h.Sum64()
	}
	return fp.str
}

// Hash returns a 64 bit hash of the fingerprint.   Note that this is
// symbolically stable across table/index ids, ie indexes from two different
// databases with different ids but the same name will have the same hash.
func (fp *PlanGist) Hash() uint64 {
	_ = fp.String()
	return fp.hash
}

var version = 1

// PlanGistFactory is an exec.Factory that produces a fingerprint by eaves
// dropping on the exec builder phase of compilation.
// TODO: do we need all the result column stuff?
// TODO: am I doing error handling right?
type PlanGistFactory struct {
	wrappedFactory exec.Factory
	buffer         *bytes.Buffer
	nodeStack      []*Node
	catalog        cat.Catalog
}

var _ exec.Factory = &PlanGistFactory{}

// NewPlanGistFactory creates a new fingerprint factory.
func NewPlanGistFactory(wrappedFactory exec.Factory) *PlanGistFactory {
	f := new(PlanGistFactory)
	f.wrappedFactory = wrappedFactory
	f.buffer = new(bytes.Buffer)
	f.encodeInt(version)
	return f
}

// ConstructPlan delegates to the wrapped factory.
func (f *PlanGistFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, cascades []exec.Cascade, checks []exec.Node,
) (exec.Plan, error) {
	return f.wrappedFactory.ConstructPlan(root, subqueries, cascades, checks)
}

// PlanGist returns a pointer to a PlanGist.
func (f *PlanGistFactory) PlanGist() *PlanGist {
	// This just returns a slice into the buffer, should we do a new right sized
	// allocation?
	return &PlanGist{bytes: f.buffer.Bytes()}
}

// DecodePlanGist constructs an explain.Node tree from a fingerprint.
// FIXME: why do we need a factory to decode?
func (f *PlanGistFactory) DecodePlanGist(s string, cat cat.Catalog) (*Plan, error) {
	f.catalog = cat
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	f.buffer = bytes.NewBuffer(b)
	plan := &Plan{}
	ver := f.decodeInt()
	if ver != version {
		return nil, errors.Errorf("invalid plan gist version %d", ver)
	}
	for {
		op := f.decodeOp()
		if op == unknownOp {
			break
		}
		switch op {
		case errorIfRowsOp:
			plan.Checks = append(plan.Checks, f.popChild())
		}
	}

	plan.Root = f.popChild()

	for _, n := range f.nodeStack {
		subquery := exec.Subquery{
			Root: n,
		}
		plan.Subqueries = append(plan.Subqueries, subquery)
	}

	return plan, nil
}

func (f *PlanGistFactory) decodeOp() execOperator {
	val, err := f.buffer.ReadByte()
	if err != nil || val == 0 {
		return unknownOp
	}
	n, err := f.decodeOperatorBody(execOperator(val))
	if err != nil {
		panic(err)
	}
	f.nodeStack = append(f.nodeStack, n)
	return n.op
}

func (f *PlanGistFactory) peekChild() *Node {
	l := len(f.nodeStack)
	n := f.nodeStack[l-1]
	return n
}

func (f *PlanGistFactory) popChild() *Node {
	l := len(f.nodeStack)
	n := f.nodeStack[l-1]
	f.nodeStack = f.nodeStack[:l-1]
	return n
}

func (f *PlanGistFactory) encodeOperator(op execOperator) {
	f.encodeByte(byte(op))
}

func (f *PlanGistFactory) encodeInt(i int) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(i))
	n, err := f.buffer.Write(buf[:n])
	if err != nil {
		panic(err)
	}
}

func (f *PlanGistFactory) decodeInt() int {
	val, err := binary.ReadVarint(f.buffer)
	if err != nil {
		panic(err)
	}
	return int(val)
}

func (f *PlanGistFactory) encodeID(id cat.StableID) {
	f.encodeInt(int(id))
}

func (f *PlanGistFactory) decodeID() cat.StableID {
	return cat.StableID(f.decodeInt())
}

func (f *PlanGistFactory) decodeTable() cat.Table {
	id := f.decodeID()
	ds, _, err := f.catalog.ResolveDataSourceByID(context.TODO(), cat.Flags{}, id)
	if err != nil {
		fmt.Errorf("no table matching id %d", id)
		return nil
	}
	// store table for index id lookups
	return ds.(cat.Table)
}

func (f *PlanGistFactory) decodeIndex(tbl cat.Table) cat.Index {
	id := f.decodeID()
	if tbl == nil {
		return nil
	}
	for i, n := 0, tbl.IndexCount(); i < n; i++ {
		if tbl.Index(i).ID() == id {
			return tbl.Index(i)
		}
	}
	fmt.Errorf("no index matching id %d", id)
	return nil
}

// TODO: implement this and figure out how to test...
func (f *PlanGistFactory) decodeSchema() cat.Schema {
	id := f.decodeID()
	_ = id
	return nil
}

func (f *PlanGistFactory) encodeNodeColumnOrdinals(vals []exec.NodeColumnOrdinal) {
	f.encodeInt(0)
	//f.encodeInt(len(vals))
	//for _, val := range vals {
	//	f.encodeInt(int(val))
	//	}
}

func (f *PlanGistFactory) decodeNodeColumnOrdinals() []exec.NodeColumnOrdinal {
	l := f.decodeInt()
	var vals []exec.NodeColumnOrdinal
	for i := 0; i < int(l); i++ {
		ival := f.decodeInt()
		vals = append(vals, exec.NodeColumnOrdinal(ival))
	}
	return vals
}

func (f *PlanGistFactory) encodeResultColumns(vals colinfo.ResultColumns) {
	f.encodeInt(0)
	//f.encodeInt(len(vals))
}

func (f *PlanGistFactory) decodeResultColumns() colinfo.ResultColumns {
	l := f.decodeInt()
	var cols colinfo.ResultColumns
	for i := 0; i < l; i++ {
		// TODO: do we need these?
		var col colinfo.ResultColumn
		cols = append(cols, col)
	}
	return cols
}

func (f *PlanGistFactory) encodeByte(b byte) {
	err := f.buffer.WriteByte(b)
	if err != nil {
		panic(err)
	}
}

func (f *PlanGistFactory) decodeByte() byte {
	val, err := f.buffer.ReadByte()
	if err != nil {
		panic(err)
	}
	return val
}

func (f *PlanGistFactory) decodeJoinType() descpb.JoinType {
	val := f.decodeByte()
	return descpb.JoinType(val)
}

func (f *PlanGistFactory) encodeBool(b bool) {
	if b {
		f.encodeByte(1)
	} else {
		f.encodeByte(0)
	}
}

func (f *PlanGistFactory) decodeBool() bool {
	val := f.decodeByte()
	if val == 0 {
		return false
	}
	return true
}

func (f *PlanGistFactory) encodeColumnOrdering(cols colinfo.ColumnOrdering) {
	f.encodeInt(0)
	//f.encodeInt(len(cols))
	//for _, col := range cols {
	//	f.encodeInt(int(col.ColIdx))
	//}
}

func (f *PlanGistFactory) decodeColumnOrdering() colinfo.ColumnOrdering {
	l := f.decodeInt()
	var vals []colinfo.ColumnOrderInfo
	for i := 0; i < int(l); i++ {
		idx := f.decodeInt()
		var val colinfo.ColumnOrderInfo
		val.ColIdx = int(idx)
		vals = append(vals, val)
	}
	return vals
}

func (f *PlanGistFactory) encodeScanParams(params exec.ScanParams) {
	// TODO: we need to be able to write out a small FastIntSet efficiently!
	s := params.NeededCols
	err := s.Encode(f.buffer)
	if err != nil {
		panic(err)
	}

	if params.IndexConstraint != nil {
		f.encodeInt(params.IndexConstraint.Spans.Count())
	} else {
		f.encodeInt(0)
	}

	if params.InvertedConstraint != nil {
		f.encodeInt(params.InvertedConstraint.Len())
	} else {
		f.encodeInt(0)
	}
}

func (f *PlanGistFactory) decodeScanParams() exec.ScanParams {
	s := util.FastIntSet{}
	err := s.Decode(f.buffer)
	if err != nil {
		panic(err)
	}

	var idxConstraint *constraint.Constraint
	l := f.decodeInt()
	if l > 0 {
		idxConstraint = new(constraint.Constraint)
		idxConstraint.Spans.Alloc(int(l))
	}

	var invertedConstraint inverted.Spans
	l = f.decodeInt()
	if l > 0 {
		invertedConstraint = make([]inverted.Span, l)
	}

	return exec.ScanParams{NeededCols: s, IndexConstraint: idxConstraint, InvertedConstraint: invertedConstraint}
}
