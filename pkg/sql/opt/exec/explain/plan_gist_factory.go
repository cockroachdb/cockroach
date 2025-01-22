// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/base64"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

func init() {
	if numOperators != 63 {
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
//  1. Initial version.
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

	// enc accumulates the encoded gist. Data that is written to enc is also
	// added to hash. The exception is when we're dealing with ids where we will
	// write the id to buf and the "string" to hash. This allows the hash to be
	// id agnostic (ie hash's will be stable across plans from different
	// databases with different DDL history).
	enc  base64.Encoder
	hash util.FNV64
}

var _ exec.Factory = &PlanGistFactory{}

// Ctx implements the Factory interface.
func (f *PlanGistFactory) Ctx() context.Context {
	return f.wrappedFactory.Ctx()
}

// writeAndHash writes an arbitrary slice of bytes to the buffer and hashes each
// byte.
func (f *PlanGistFactory) writeAndHash(data []byte) {
	f.enc.Write(data)
	f.updateHash(data)
}

func (f *PlanGistFactory) updateHash(data []byte) {
	for _, b := range data {
		f.hash.Add(uint64(b))
	}
}

// Init initializes a PlanGistFactory.
func (f *PlanGistFactory) Init(factory exec.Factory) {
	*f = PlanGistFactory{
		wrappedFactory: factory,
	}
	f.enc.Init(b64.StdEncoding)
	f.hash.Init()
	f.encodeInt(gistVersion)
}

// Initialized returns true if the PlanGistFactory has been initialized.
func (f *PlanGistFactory) Initialized() bool {
	return f.wrappedFactory != nil
}

// Reset resets the PlanGistFactory, clearing references to the wrapped factory.
func (f *PlanGistFactory) Reset() {
	*f = PlanGistFactory{}
}

// ConstructPlan delegates to the wrapped factory.
func (f *PlanGistFactory) ConstructPlan(
	root exec.Node,
	subqueries []exec.Subquery,
	cascades, triggers []exec.PostQuery,
	checks []exec.Node,
	rootRowCount int64,
	flags exec.PlanFlags,
) (exec.Plan, error) {
	plan, err := f.wrappedFactory.ConstructPlan(root, subqueries, cascades, triggers, checks, rootRowCount, flags)
	return plan, err
}

// PlanGist returns an encoded PlanGist. It should only be called once after
// ConstructPlan.
func (f *PlanGistFactory) PlanGist() PlanGist {
	return PlanGist{
		gist: f.enc.String(),
		hash: f.hash.Sum(),
	}
}

// planGistDecoder is used to decode a plan gist into a logical plan.
type planGistDecoder struct {
	buf       bytes.Reader
	nodeStack []*Node

	// catalog is used to resolve table and index ids that are stored in the gist.
	// catalog can be nil when decoding gists via decode_external_plan_gist in which
	// case we don't attempt to resolve tables or indexes.
	catalog cat.Catalog
}

// DecodePlanGistToRows converts a gist to a logical plan and returns the rows.
func DecodePlanGistToRows(
	ctx context.Context, evalCtx *eval.Context, gist string, catalog cat.Catalog,
) (_ []string, retErr error) {
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

	flags := Flags{HideValues: true, Deflake: DeflakeAll, OnlyShape: true}
	ob := NewOutputBuilder(flags)
	explainPlan, err := DecodePlanGistToPlan(gist, catalog)
	if err != nil {
		return nil, err
	}
	err = Emit(ctx, evalCtx, explainPlan, ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" }, false /* createPostQueryPlanIfMissing */)
	if err != nil {
		return nil, err
	}
	return ob.BuildStringRows(), nil
}

// DecodePlanGistToPlan constructs an explain.Node tree from a gist.
func DecodePlanGistToPlan(s string, cat cat.Catalog) (plan *Plan, err error) {
	b, err := b64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	var d planGistDecoder
	d.buf.Reset(b)
	d.catalog = cat

	plan = &Plan{}

	ver := d.decodeInt()
	if ver != gistVersion {
		return nil, errors.Errorf("unsupported gist version %d (expected %d)", ver, gistVersion)
	}

	for {
		op := d.decodeOp()
		if op == unknownOp {
			break
		}
		switch op {
		case errorIfRowsOp:
			plan.Checks = append(plan.Checks, d.popChild())
		}
	}

	plan.Root = d.popChild()

	for _, n := range d.nodeStack {
		subquery := exec.Subquery{
			Root: n,
		}
		plan.Subqueries = append(plan.Subqueries, subquery)
	}

	return plan, nil
}

func (d *planGistDecoder) decodeOp() execOperator {
	val, err := d.buf.ReadByte()
	if err != nil || val == 0 {
		return unknownOp
	}
	n, err := d.decodeOperatorBody(execOperator(val))
	if err != nil {
		panic(err)
	}
	d.nodeStack = append(d.nodeStack, n)

	return n.op
}

func (d *planGistDecoder) popChild() *Node {
	l := len(d.nodeStack)
	if l == 0 {
		return nil
	}
	n := d.nodeStack[l-1]
	d.nodeStack = d.nodeStack[:l-1]

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

func (d *planGistDecoder) decodeInt() int {
	val, err := binary.ReadVarint(&d.buf)
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
	f.enc.Write(buf[:n])
	f.updateHash([]byte(string(name)))
}

func (f *PlanGistFactory) encodeID(id cat.StableID) {
	f.encodeInt(int(id))
}

func (d *planGistDecoder) decodeID() cat.StableID {
	return cat.StableID(d.decodeInt())
}

func (d *planGistDecoder) decodeTable() cat.Table {
	if d.catalog == nil {
		return &unknownTable{}
	}
	id := d.decodeID()
	// TODO(mgartner): Do not use the background context.
	ds, _, err := d.catalog.ResolveDataSourceByID(context.Background(), cat.Flags{}, id)
	if err == nil {
		return ds.(cat.Table)
	}
	if pgerror.GetPGCode(err) == pgcode.UndefinedTable {
		return &unknownTable{}
	}
	panic(err)
}

func (d *planGistDecoder) decodeIndex(tbl cat.Table) cat.Index {
	id := d.decodeID()
	for i, n := 0, tbl.IndexCount(); i < n; i++ {
		if tbl.Index(i).ID() == id {
			return tbl.Index(i)
		}
	}
	return &unknownIndex{}
}

// TODO: implement this and figure out how to test...
func (d *planGistDecoder) decodeSchema() cat.Schema {
	id := d.decodeID()
	_ = id
	return nil
}

func (f *PlanGistFactory) encodeNodeColumnOrdinals(vals []exec.NodeColumnOrdinal) {
	f.encodeInt(len(vals))
}

func (d *planGistDecoder) decodeNodeColumnOrdinals() []exec.NodeColumnOrdinal {
	l := d.decodeInt()
	if l < 0 {
		return nil
	}
	vals := make([]exec.NodeColumnOrdinal, l)
	return vals
}

func (f *PlanGistFactory) encodeResultColumns(vals colinfo.ResultColumns) {
	f.encodeInt(len(vals))
}

func (d *planGistDecoder) decodeResultColumns() colinfo.ResultColumns {
	numCols := d.decodeInt()
	if numCols < 0 {
		return nil
	}
	return make(colinfo.ResultColumns, numCols)
}

func (f *PlanGistFactory) encodeByte(b byte) {
	f.enc.Write([]byte{b})
	f.hash.Add(uint64(b))
}

func (d *planGistDecoder) decodeByte() byte {
	val, err := d.buf.ReadByte()
	if err != nil {
		panic(err)
	}
	return val
}

func (d *planGistDecoder) decodeJoinType() descpb.JoinType {
	val := d.decodeByte()
	return descpb.JoinType(val)
}

func (f *PlanGistFactory) encodeBool(b bool) {
	if b {
		f.encodeByte(1)
	} else {
		f.encodeByte(0)
	}
}

func (d *planGistDecoder) decodeBool() bool {
	val := d.decodeByte()
	return val != 0
}

func (f *PlanGistFactory) encodeFastIntSet(s intsets.Fast) {
	if err := s.EncodeBase64(&f.enc, &f.hash); err != nil {
		panic(err)
	}
}

// TODO: enable this or remove it...
func (f *PlanGistFactory) encodeColumnOrdering(cols colinfo.ColumnOrdering) {
}

func (d *planGistDecoder) decodeColumnOrdering() colinfo.ColumnOrdering {
	return nil
}

func (f *PlanGistFactory) encodeScanParams(params exec.ScanParams) {
	f.encodeFastIntSet(params.NeededCols)

	if params.IndexConstraint != nil {
		// Encode 1 to represent one or more spans. We don't encode the exact
		// number of spans so that two queries with the same plan but a
		// different number of spans have the same gist.
		f.encodeInt(1)
	} else {
		f.encodeInt(0)
	}

	if params.InvertedConstraint != nil {
		// Encode 1 to represent one or more spans. We don't encode the exact
		// number of spans so that two queries with the same plan but a
		// different number of spans have the same gist.
		f.encodeInt(1)
	} else {
		f.encodeInt(0)
	}

	if params.HardLimit > 0 {
		f.encodeInt(1)
	} else {
		f.encodeInt(0)
	}
}

func (d *planGistDecoder) decodeScanParams() exec.ScanParams {
	neededCols := intsets.Fast{}
	err := neededCols.Decode(&d.buf)
	if err != nil {
		panic(err)
	}

	var idxConstraint *constraint.Constraint
	l := d.decodeInt()
	if l > 0 {
		idxConstraint = new(constraint.Constraint)
		idxConstraint.Spans.Alloc(l)
		var sp constraint.Span
		idxConstraint.Spans.Append(&sp)
	}

	var invertedConstraint inverted.Spans
	l = d.decodeInt()
	if l > 0 {
		invertedConstraint = make([]inverted.Span, l)
	}

	hardLimit := d.decodeInt()

	// Since we no longer record the limit value and its just a bool tell the emit code
	// to just print "limit", instead the misleading "limit: 1".
	if hardLimit > 0 {
		hardLimit = -1
	}

	return exec.ScanParams{
		NeededCols:         neededCols,
		IndexConstraint:    idxConstraint,
		InvertedConstraint: invertedConstraint,
		HardLimit:          int64(hardLimit),
	}
}

func (f *PlanGistFactory) encodeRows(rows [][]tree.TypedExpr) {
	f.encodeInt(len(rows))
}

func (d *planGistDecoder) decodeRows() [][]tree.TypedExpr {
	numRows := d.decodeInt()
	if numRows < 0 {
		return nil
	}
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

// HomeRegion is part of the cat.Table interface.
func (u *unknownTable) HomeRegion() (region string, ok bool) {
	return "", false
}

// IsGlobalTable is part of the cat.Table interface.
func (u *unknownTable) IsGlobalTable() bool {
	return false
}

// IsRegionalByRow is part of the cat.Table interface.
func (u *unknownTable) IsRegionalByRow() bool {
	return false
}

// IsMultiregion is part of the cat.Table interface.
func (u *unknownTable) IsMultiregion() bool {
	return false
}

// HomeRegionColName is part of the cat.Table interface.
func (u *unknownTable) HomeRegionColName() (colName string, ok bool) {
	return "", false
}

// GetDatabaseID is part of the cat.Table interface.
func (u *unknownTable) GetDatabaseID() descpb.ID {
	return 0
}

// IsHypothetical is part of the cat.Table interface.
func (u *unknownTable) IsHypothetical() bool {
	return false
}

// TriggerCount is part of the cat.Table interface.
func (u *unknownTable) TriggerCount() int {
	return 0
}

// Trigger is part of the cat.Table interface.
func (u *unknownTable) Trigger(i int) cat.Trigger {
	panic(errors.AssertionFailedf("not implemented"))
}

// IsRowLevelSecurityEnabled is part of the cat.Table interface
func (u *unknownTable) IsRowLevelSecurityEnabled() bool { return false }

// PolicyCount is part of the cat.Table interface
func (u *unknownTable) PolicyCount(polType tree.PolicyType) int { return 0 }

// Policy is part of the cat.Table interface
func (u *unknownTable) Policy(polType tree.PolicyType, i int) cat.Policy {
	panic(errors.AssertionFailedf("not implemented"))
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
	return &unknownTable{}
}

func (u *unknownIndex) Ordinal() cat.IndexOrdinal {
	return 0
}

func (u *unknownIndex) IsUnique() bool {
	return false
}

func (u *unknownIndex) IsInverted() bool {
	return false
}

func (u *unknownIndex) IsVector() bool {
	return false
}

func (u *unknownIndex) GetInvisibility() float64 {
	return 0.0
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

func (u *unknownIndex) PrefixColumnCount() int {
	return 0
}

func (u *unknownIndex) Column(i int) cat.IndexColumn {
	var col cat.Column
	col.Init(
		i,
		cat.DefaultStableID,
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

func (u *unknownIndex) VectorColumn() cat.IndexColumn {
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

func (u *unknownIndex) GeoConfig() geopb.Config {
	return geopb.Config{}
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
