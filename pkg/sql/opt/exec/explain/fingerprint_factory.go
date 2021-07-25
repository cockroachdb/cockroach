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

// Fingerprint is a compact representation of a logical plan meant to be used as
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
type Fingerprint struct {
	bytes []byte
	str   string
	hash  uint64
}

// String returns the fingerprint bytes as a base64 encoded string.
func (fp Fingerprint) String() string {
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
func (fp Fingerprint) Hash() uint64 {
	_ = fp.String()
	return fp.hash
}

var version = 1

// FingerprintFactory is an exec.Factory that produces a fingerprint by eaves
// dropping on the exec builder phase of compilation.
type FingerprintFactory struct {
	wrappedFactory exec.Factory
	buffer         *bytes.Buffer
	nodeStack      []*Node
	catalog        cat.Catalog
	lastTable      cat.Table
}

var _ exec.Factory = &FingerprintFactory{}

// NewFingerprintFactory creates a new fingerprint factory.
func NewFingerprintFactory(wrappedFactory exec.Factory) *FingerprintFactory {
	f := new(FingerprintFactory)
	f.wrappedFactory = wrappedFactory
	f.buffer = new(bytes.Buffer)
	f.encodeInt(version)
	return f
}

func (f *FingerprintFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, cascades []exec.Cascade, checks []exec.Node,
) (exec.Plan, error) {
	return f.wrappedFactory.ConstructPlan(root, subqueries, cascades, checks)
}

func (f *FingerprintFactory) Fingerprint() Fingerprint {
	return Fingerprint{bytes: f.buffer.Bytes()}
}

// DecodeFingerprint constructs an explain.Node tree from a fingerprint.
func (f *FingerprintFactory) DecodeFingerprint(s string, cat cat.Catalog) (*Plan, error) {
	f.catalog = cat
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	f.buffer = bytes.NewBuffer(b)
	plan := &Plan{}
	ver := f.decodeInt()
	if ver != version {
		return nil, errors.Errorf("invalid fingerprint version %d", ver)
	}
	for {
		op := f.decodeOp()
		if op == unknownOp {
			break
		}
		switch op {
		case bufferOp:
			subquery := exec.Subquery{
				Root: f.popChild(),
			}
			plan.Subqueries = append(plan.Subqueries, subquery)
		}
	}
	plan.Root = f.popChild()

	if len(f.nodeStack) > 0 {
		fmt.Printf("nodes didn't make a tree")
	}
	return plan, nil
}

func (f *FingerprintFactory) decodeOp() execOperator {
	val, err := f.buffer.ReadByte()
	if err != nil {
		return unknownOp
	}
	n, err := f.decodeOperatorBody(execOperator(val))
	if err != nil {
		panic(err)
	}
	switch n.op {
	//case serializingProjectOp:
	//	args := n.args.(*serializingProjectArgs)
	//	args.ColNames = make([]string, len(args.Cols))
	//	for i := range args.Cols {
	//		args.ColNames[i] = fmt.Sprintf("col%d", args.Cols[i])
	//	}
	}
	f.nodeStack = append(f.nodeStack, n)
	return n.op
}

func (f *FingerprintFactory) encodeOperator(op execOperator) {
	f.encodeByte(byte(op))
}

func (f *FingerprintFactory) encodeInt(i int) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(i))
	n, err := f.buffer.Write(buf[:n])
	if err != nil {
		panic(err)
	}
}

func (f *FingerprintFactory) decodeInt() int {
	val, err := binary.ReadVarint(f.buffer)
	if err != nil {
		panic(err)
	}
	return int(val)
}

func (f *FingerprintFactory) encodeId(id cat.StableID) {
	f.encodeInt(int(id))
}

func (f *FingerprintFactory) decodeId() cat.StableID {
	return cat.StableID(f.decodeInt())
}

func (f *FingerprintFactory) decodeTable() cat.Table {
	id := f.decodeId()
	ds, _, err := f.catalog.ResolveDataSourceByID(context.TODO(), cat.Flags{}, id)
	if err != nil {
		panic(err)
	}
	f.lastTable = ds.(cat.Table)
	return f.lastTable
}

func (f *FingerprintFactory) decodeIndex() cat.Index {
	id := f.decodeId()
	for i, n := 0, f.lastTable.IndexCount(); i < n; i++ {
		if f.lastTable.Index(i).ID() == id {
			return f.lastTable.Index(i)
		}
	}
	// error?
	return nil
}

func (f *FingerprintFactory) decodeSchema() cat.Schema {
	id := f.decodeId()
	_ = id
	return nil
}

func (f *FingerprintFactory) encodeNodeColumnOrdinals(vals []exec.NodeColumnOrdinal) {
	f.encodeInt(len(vals))
	for _, val := range vals {
		f.encodeInt(int(val))
	}
}

func (f *FingerprintFactory) decodeNodeColumnOrdinals() []exec.NodeColumnOrdinal {
	l := f.decodeInt()
	var vals []exec.NodeColumnOrdinal
	for i := 0; i < int(l); i++ {
		ival := f.decodeInt()
		vals = append(vals, exec.NodeColumnOrdinal(ival))
	}
	return vals
}

func (f *FingerprintFactory) encodeResultColumns(vals colinfo.ResultColumns) {
	f.encodeInt(len(vals))
}

func (f *FingerprintFactory) decodeResultColumns() colinfo.ResultColumns {
	l := f.decodeInt()
	var cols colinfo.ResultColumns
	for i := 0; i < l; i++ {
		// TODO: do we need these?
		var col colinfo.ResultColumn
		cols = append(cols, col)
	}
	return cols
}

func (f *FingerprintFactory) encodeByte(b byte) {
	err := f.buffer.WriteByte(b)
	if err != nil {
		panic(err)
	}
}

func (f *FingerprintFactory) decodeByte() byte {
	val, err := f.buffer.ReadByte()
	if err != nil {
		panic(err)
	}
	return val
}

func (f *FingerprintFactory) decodeJoinType() descpb.JoinType {
	val := f.decodeByte()
	return descpb.JoinType(val)
}

func (f *FingerprintFactory) encodeBool(b bool) {
	if b {
		f.encodeByte(1)
	} else {
		f.encodeByte(0)
	}
}

func (f *FingerprintFactory) decodeBool() bool {
	val := f.decodeByte()
	if val == 0 {
		return false
	}
	return true
}

func (f *FingerprintFactory) encodeColumnOrdering(cols colinfo.ColumnOrdering) {
	f.encodeInt(len(cols))
	for _, col := range cols {
		f.encodeInt(int(col.ColIdx))
	}
}

func (f *FingerprintFactory) decodeColumnOrdering() colinfo.ColumnOrdering {
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

func (f *FingerprintFactory) encodeScanParams(params exec.ScanParams) {
	// TODO: we need to be able to write out a small FastIntSet efficiently!
	s := params.NeededCols
	_, err := s.Encode(f.buffer)
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

func (f *FingerprintFactory) decodeScanParams() exec.ScanParams {
	s := util.FastIntSet{}
	s.Decode(f.buffer)

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

func (f *FingerprintFactory) popChild() *Node {
	l := len(f.nodeStack)
	n := f.nodeStack[l-1]
	f.nodeStack = f.nodeStack[:l-1]
	return n
}

// FIXME: remove, copied from explain_factory
func (f *FingerprintFactory) newNode(
	op execOperator, args interface{}, ordering exec.OutputOrdering, children ...*Node,
) (*Node, error) {
	inputNodeCols := make([]colinfo.ResultColumns, len(children))
	for i := range children {
		inputNodeCols[i] = children[i].Columns()
	}
	columns, err := getResultColumns(op, args, inputNodeCols...)
	if err != nil {
		return nil, err
	}
	return &Node{
		f:        nil,
		op:       op,
		args:     args,
		columns:  columns,
		ordering: colinfo.ColumnOrdering(ordering),
		children: children,
	}, nil
}
