// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

func init() {
	if numOperators != 60 {
		// This error occurs when an operator has been added or removed in
		// pkg/sql/opt/exec/explain/factory.opt. If an operator is added at the
		// end of factory.opt, simply adjust the hardcoded value above. If an
		// operator is removed or added anywhere else in factory.opt, increment
		// gistVersion below. Note that we currently do not have a mechanism for
		// decoding gists of older versions. This means that if gistVersion is
		// incremented in a release, upgrading a cluster to that release will
		// cause decoding errors for any previously generated plan gists.
		panic(errors.AssertionFailedf("Operator field changed (%d), please update check and consider incrementing version", numOperators))
	}
}

// gistVersion tracks major changes to how we encode plans or to the operator
// set. It isn't necessary to increment it when adding a single operator but if
// we remove an operator or change the operator set or decide to use a more
// efficient encoding version should be incremented.
//
// Version history:
//   1. Initial version.
var gistVersion = 1

// PlanGist is a compact representation of a logical plan meant to be used as
// a key and log for different plans used to implement a particular query. A
// gist doesn't change for the following:
//
// - literal constant values
// - alias names
// - grouping column names
// - constraint values
// - estimated rows stats
//
// The notion is that the gist is the rough shape of the plan that represents
// the way the plan operators are put together and what tables and indexes they
// use.
type PlanGist struct {
	gist string
	hash uint64
}

// String returns the gist bytes as a base64 encoded string.
func (fp PlanGist) String() string {
	return fp.gist
}

// Hash returns a 64 bit hash of the gist. Note that this is symbolically stable
// across table/index ids, i.e. indexes from two different databases with
// different ids but the same name will have the same hash.
func (fp PlanGist) Hash() uint64 {
	return fp.hash
}

// PlanGistFactory is an exec.Factory that produces a gist by eaves
// dropping on the exec builder phase of compilation.
type PlanGistFactory struct {
	wrappedFactory exec.Factory

	// buffer is used for reading and writing (i.e. decoding and encoding).
	// Data that is written to the buffer is also added to the hash. The
	// exception is when we're dealing with ids where we will write the id to the
	// buffer and the "string" to the hash. This allows the hash to be id agnostic
	// (ie hash's will be stable across plans from different databases with
	// different DDL history).
	buffer bytes.Buffer
	hash   util.FNV64

	nodeStack []*Node

	// catalog is used to resolve table and index ids that are stored in the gist.
	// catalog can be nil when decoding gists via decode_external_plan_gist in which
	// case we don't attempt to resolve tables or indexes.
	catalog cat.Catalog
}

var _ exec.Factory = &PlanGistFactory{}

// writeAndHash writes an arbitrary slice of bytes to the buffer and hashes each
// byte.
func (f *PlanGistFactory) writeAndHash(data []byte) int {
	i, _ := f.buffer.Write(data)
	f.updateHash(data)
	return i
}

func (f *PlanGistFactory) updateHash(data []byte) {
	for _, b := range data {
		f.hash.Add(uint64(b))
	}
}

// NewPlanGistFactory creates a new PlanGistFactory.
func NewPlanGistFactory(wrappedFactory exec.Factory) *PlanGistFactory {
	f := new(PlanGistFactory)
	f.wrappedFactory = wrappedFactory
	f.hash.Init()
	f.encodeInt(gistVersion)
	return f
}

// ConstructPlan delegates to the wrapped factory.
func (f *PlanGistFactory) ConstructPlan(
	root exec.Node,
	subqueries []exec.Subquery,
	cascades []exec.Cascade,
	checks []exec.Node,
	rootRowCount int64,
) (exec.Plan, error) {
	plan, err := f.wrappedFactory.ConstructPlan(root, subqueries, cascades, checks, rootRowCount)
	return plan, err
}

// PlanGist returns a pointer to a PlanGist.
func (f *PlanGistFactory) PlanGist() PlanGist {
	return PlanGist{gist: base64.StdEncoding.EncodeToString(f.buffer.Bytes()),
		hash: f.hash.Sum()}
}

// DecodePlanGistToRows converts a gist to a logical plan and returns the rows.
func DecodePlanGistToRows(gist string, catalog cat.Catalog) (_ []string, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having
			// to add error checks everywhere throughout the code. This is only
			// possible because the code does not update shared state and does
			// not manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				retErr = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	flags := Flags{HideValues: true, Redact: RedactAll}
	ob := NewOutputBuilder(flags)
	explainPlan, err := DecodePlanGistToPlan(gist, catalog)
	if err != nil {
		return nil, err
	}
	err = Emit(explainPlan, ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" })
	if err != nil {
		return nil, err
	}
	return ob.BuildStringRows(), nil
}

// DecodePlanGistToPlan constructs an explain.Node tree from a gist.
func DecodePlanGistToPlan(s string, cat cat.Catalog) (plan *Plan, retErr error) {
	f := NewPlanGistFactory(exec.StubFactory{})
	f.catalog = cat
	bytes, err := base64.StdEncoding.DecodeString(s)

	if err != nil {
		return nil, err
	}

	// Clear out buffer which will have version in it from NewPlanGistFactory.
	f.buffer.Reset()
	f.buffer.Write(bytes)
	plan = &Plan{}

	ver := f.decodeInt()
	if ver != gistVersion {
		return nil, errors.Errorf("unsupported gist version %d (expected %d)", ver, gistVersion)
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
	f.writeAndHash(buf[:n])
}

func (f *PlanGistFactory) decodeInt() int {
	val, err := binary.ReadVarint(&f.buffer)
	if err != nil {
		panic(err)
	}

	return int(val)
}

// encodeDataSource encodes tables and indexes and does a numeric id based
// encoding to the gist and a symbolic encoding to the hash.
func (f *PlanGistFactory) encodeDataSource(id cat.StableID, name tree.Name) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(id))
	_, err := f.buffer.Write(buf[:n])
	if err != nil {
		panic(err)
	}
	f.updateHash([]byte(string(name)))
}

func (f *PlanGistFactory) encodeID(id cat.StableID) {
	f.encodeInt(int(id))
}

func (f *PlanGistFactory) decodeID() cat.StableID {
	return cat.StableID(f.decodeInt())
}

func (f *PlanGistFactory) decodeTable() cat.Table {
	if f.catalog == nil {
		return &unknownTable{}
	}
	id := f.decodeID()
	ds, _, err := f.catalog.ResolveDataSourceByID(context.TODO(), cat.Flags{}, id)
	if err == nil {
		return ds.(cat.Table)
	}
	if pgerror.GetPGCode(err) == pgcode.UndefinedTable {
		return &unknownTable{}
	}
	panic(err)
}

func (f *PlanGistFactory) decodeIndex(tbl cat.Table) cat.Index {
	id := f.decodeID()
	for i, n := 0, tbl.IndexCount(); i < n; i++ {
		if tbl.Index(i).ID() == id {
			return tbl.Index(i)
		}
	}
	return &unknownIndex{}
}

// TODO: implement this and figure out how to test...
func (f *PlanGistFactory) decodeSchema() cat.Schema {
	id := f.decodeID()
	_ = id
	return nil
}

func (f *PlanGistFactory) encodeNodeColumnOrdinals(vals []exec.NodeColumnOrdinal) {
	f.encodeInt(len(vals))
}

func (f *PlanGistFactory) decodeNodeColumnOrdinals() []exec.NodeColumnOrdinal {
	l := f.decodeInt()
	vals := make([]exec.NodeColumnOrdinal, l)
	return vals
}

func (f *PlanGistFactory) encodeResultColumns(vals colinfo.ResultColumns) {
	f.encodeInt(len(vals))
}

func (f *PlanGistFactory) decodeResultColumns() colinfo.ResultColumns {
	numCols := f.decodeInt()
	return make(colinfo.ResultColumns, numCols)
}

func (f *PlanGistFactory) encodeByte(b byte) {
	f.buffer.WriteByte(b)
	f.hash.Add(uint64(b))
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
	return val != 0
}

func (f *PlanGistFactory) encodeFastIntSet(s util.FastIntSet) {
	lenBefore := f.buffer.Len()
	if err := s.Encode(&f.buffer); err != nil {
		panic(err)
	}
	f.updateHash(f.buffer.Bytes()[lenBefore:])
}

// TODO: enable this or remove it...
func (f *PlanGistFactory) encodeColumnOrdering(cols colinfo.ColumnOrdering) {
}

func (f *PlanGistFactory) decodeColumnOrdering() colinfo.ColumnOrdering {
	return nil
}

func (f *PlanGistFactory) encodeScanParams(params exec.ScanParams) {
	f.encodeFastIntSet(params.NeededCols)

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

	f.encodeInt(int(params.HardLimit))
}

func (f *PlanGistFactory) decodeScanParams() exec.ScanParams {
	neededCols := util.FastIntSet{}
	err := neededCols.Decode(&f.buffer)
	if err != nil {
		panic(err)
	}

	var idxConstraint *constraint.Constraint
	l := f.decodeInt()
	if l > 0 {
		idxConstraint = new(constraint.Constraint)
		idxConstraint.Spans.Alloc(l)
		for i := 0; i < l; i++ {
			var sp constraint.Span
			idxConstraint.Spans.Append(&sp)
		}
	}

	var invertedConstraint inverted.Spans
	l = f.decodeInt()
	if l > 0 {
		invertedConstraint = make([]inverted.Span, l)
	}

	hardLimit := f.decodeInt()

	return exec.ScanParams{NeededCols: neededCols, IndexConstraint: idxConstraint, InvertedConstraint: invertedConstraint, HardLimit: int64(hardLimit)}
}

func (f *PlanGistFactory) encodeRows(rows [][]tree.TypedExpr) {
	f.encodeInt(len(rows))
}

func (f *PlanGistFactory) decodeRows() [][]tree.TypedExpr {
	numRows := f.decodeInt()
	return make([][]tree.TypedExpr, numRows)
}

// unknownTable implements the cat.Table interface and is used to represent
// tables that cannot be decoded. This can happen when tables in plan gists are
// dropped and are no longer in the catalog.
type unknownTable struct{}

func (u *unknownTable) ID() cat.StableID {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) PostgresDescriptorID() catid.DescID {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) Equals(other cat.Object) bool {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) Name() tree.Name {
	return "?"
}

func (u *unknownTable) CollectTypes(ord int) (descpb.IDs, error) {
	return nil, errors.AssertionFailedf("not implemented")
}

func (u *unknownTable) IsVirtualTable() bool {
	return false
}

func (u *unknownTable) IsSystemTable() bool {
	return false
}

func (u *unknownTable) IsMaterializedView() bool {
	return false
}

func (u *unknownTable) ColumnCount() int {
	return 0
}

func (u *unknownTable) Column(i int) *cat.Column {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) IndexCount() int {
	return 0
}

func (u *unknownTable) WritableIndexCount() int {
	return 0
}

func (u *unknownTable) DeletableIndexCount() int {
	return 0
}

func (u *unknownTable) Index(i cat.IndexOrdinal) cat.Index {
	return &unknownIndex{}
}

func (u *unknownTable) StatisticCount() int {
	return 0
}

func (u *unknownTable) Statistic(i int) cat.TableStatistic {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) CheckCount() int {
	return 0
}

func (u *unknownTable) Check(i int) cat.CheckConstraint {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) FamilyCount() int {
	return 0
}

func (u *unknownTable) Family(i int) cat.Family {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) OutboundForeignKeyCount() int {
	return 0
}

func (u *unknownTable) OutboundForeignKey(i int) cat.ForeignKeyConstraint {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) InboundForeignKeyCount() int {
	return 0
}

func (u *unknownTable) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) UniqueCount() int {
	return 0
}

func (u *unknownTable) Unique(i cat.UniqueOrdinal) cat.UniqueConstraint {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownTable) Zone() cat.Zone {
	return cat.EmptyZone()
}

func (u *unknownTable) IsPartitionAllBy() bool {
	return false
}

func (u *unknownTable) IsRefreshViewRequired() bool {
	return false
}

var _ cat.Table = &unknownTable{}

// unknownTable implements the cat.Index interface and is used to represent
// indexes that cannot be decoded. This can happen when indexes in plan gists
// are dropped and are no longer in the catalog.
type unknownIndex struct{}

func (u *unknownIndex) ID() cat.StableID {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownIndex) Name() tree.Name {
	return "?"
}

func (u *unknownIndex) Table() cat.Table {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownIndex) Ordinal() int {
	return 0
}

func (u *unknownIndex) IsUnique() bool {
	return false
}

func (u *unknownIndex) IsInverted() bool {
	return false
}

func (u *unknownIndex) IsNotVisible() bool {
	return false
}

func (u *unknownIndex) ColumnCount() int {
	return 0
}

func (u *unknownIndex) ExplicitColumnCount() int {
	return 0
}

func (u *unknownIndex) KeyColumnCount() int {
	return 0
}

func (u *unknownIndex) LaxKeyColumnCount() int {
	return 0
}

func (u *unknownIndex) NonInvertedPrefixColumnCount() int {
	return 0
}

func (u *unknownIndex) Column(i int) cat.IndexColumn {
	var col cat.Column
	col.Init(
		i,
		cat.StableID(0),
		"?",
		cat.Ordinary,
		types.Int,
		true, /* nullable */
		cat.Visible,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil, /* generatedAsIdentitySequenceOption */
	)
	return cat.IndexColumn{
		Column:     &col,
		Descending: false,
	}
}

func (u *unknownIndex) InvertedColumn() cat.IndexColumn {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownIndex) Predicate() (string, bool) {
	return "", false
}

func (u *unknownIndex) Zone() cat.Zone {
	return cat.EmptyZone()
}

func (u *unknownIndex) Span() roachpb.Span {
	panic(errors.AssertionFailedf("not implemented"))
}

func (u *unknownIndex) ImplicitColumnCount() int {
	return 0
}

func (u *unknownIndex) ImplicitPartitioningColumnCount() int {
	return 0
}

func (u *unknownIndex) GeoConfig() geoindex.Config {
	return geoindex.Config{}
}

func (u *unknownIndex) Version() descpb.IndexDescriptorVersion {
	return descpb.LatestIndexDescriptorVersion
}

func (u *unknownIndex) PartitionCount() int {
	return 0
}

func (u *unknownIndex) Partition(i int) cat.Partition {
	panic(errors.AssertionFailedf("not implemented"))
}

var _ cat.Index = &unknownIndex{}
