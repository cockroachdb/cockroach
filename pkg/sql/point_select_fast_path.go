// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/prep"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// PointSelectFastPathEnabled gates the experimental hardcoded fast path
// for prepared point-SELECTs against a single-column primary key.
//
// See experiments/point-select-fast-path/PLAN.md. This is a research
// shortcut, not a production feature: detection is intentionally narrow
// (one query shape) and execution skips most of the connExecutor /
// optimizer / DistSQL stack to estimate the upper bound on per-execute
// SQL processing overhead.
var PointSelectFastPathEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.fast_path.point_select.enabled",
	"if true, prepared SELECT statements matching the shape "+
		"'SELECT cols FROM table WHERE pk_col = $1' against a single-column "+
		"primary key are executed via a hardcoded path that bypasses the "+
		"optimizer, exec-builder, distSQL, and row.Fetcher",
	false,
)

// pointSelectFastPath is the cached metadata used to execute one
// prepared point-SELECT without going through the full SQL pipeline.
// Populated once at first execute, reused for every subsequent execute
// of the same prepared statement.
//
// Fields are immutable after construction. Concurrent reads are safe.
//
// Four concrete shapes are supported, distinguished by lookupIndexID
// (primary vs secondary), pkFetch, and joinFetch:
//   - PK lookup:               primary index, pkFetch=false, joinFetch=false.
//     One Get; value is family-0 tuple.
//   - Covering secondary:      secondary index, pkFetch=false, joinFetch=false.
//     One Get; value is BYTES with key-suffix prefix followed by
//     storing-column tuple.
//   - Non-covering secondary:  secondary index, pkFetch=true, joinFetch=false.
//     Two Gets: index Get returns the PK suffix bytes, then PK Get
//     returns the row's family-0 tuple.
//   - Two-table lookup join:   secondary index, pkFetch=true, joinFetch=true.
//     Three Gets: index Get on table 1, PK Get on table 1 (to read
//     the FK column from family 0), then PK Get on table 2 (the
//     join target) to fetch the requested columns from its family-0
//     tuple.
type pointSelectFastPath struct {
	tableID     descpb.ID
	descVersion descpb.DescriptorVersion

	// First-stage lookup (always issued).
	lookupIndexID   descpb.IndexID
	lookupKeyPrefix []byte
	lookupFamilyID  descpb.FamilyID
	pkColType       *types.T

	// suffixColTypes is non-empty when the lookup index is a unique
	// secondary index; it holds the types of the key-suffix columns
	// (the implicit PK) that prefix the lookup value's bytes. Empty
	// for primary-index lookups.
	suffixColTypes []*types.T

	// pkFetch is set when the lookup index does not cover the
	// requested columns. The lookup value's suffix bytes become the
	// key body of a second Get against the primary index.
	pkFetch     bool
	pkKeyPrefix []byte
	pkFamilyID  descpb.FamilyID

	// joinFetch is set when the recognized shape is a two-table
	// lookup join: after the second Get on table 1's primary index,
	// we decode an intermediate (FK) column from the family-0 tuple,
	// re-encode it as a key into table 2's primary index, and Get
	// the joined row. Implies pkFetch.
	joinFetch           bool
	joinPKKeyPrefix     []byte
	joinPKFamilyID      descpb.FamilyID
	intermediateColType *types.T
	intermediateDecoder valueside.Decoder

	resultColumns   colinfo.ResultColumns
	resultColumnIDs []descpb.ColumnID
	// decoder decodes the final tuple bytes that carry the requested
	// columns. The "final" Get differs by shape — for PK and covering
	// secondary it's the lookup Get; for non-covering it's the PK
	// Get on table 1; for the join shape it's the PK Get on table 2.
	decoder valueside.Decoder
}

// pointSelectCacheEntry is what we store in pointSelectCache. We cache
// both successes (plan != nil) and failures (notApplicable=true) so we
// don't re-pay detection cost on every execute when the statement
// shape is not eligible.
type pointSelectCacheEntry struct {
	plan          *pointSelectFastPath
	notApplicable bool
}

// pointSelectCache associates fast-path metadata with a prepared
// statement. Keyed by *prep.Statement; entries persist for the life of
// the prepared statement. The cache leaks entries when prepared
// statements are deallocated — acceptable for the experiment, would
// need refcount tie-in for production use.
var pointSelectCache sync.Map

// tryGetPointSelectFastPath returns the cached fast-path plan for the
// given prepared statement, performing detection lazily on first call.
// Returns nil if the prepared statement does not match the recognized
// shape or if detection determined the statement is ineligible.
//
// Safe to call when the cluster setting is disabled — caller is
// expected to gate on the setting. We don't gate here so that a
// previously cached "not applicable" decision is still cheap to skip
// when the setting is on.
func (ex *connExecutor) tryGetPointSelectFastPath(
	ctx context.Context, p *planner, prepared *prep.Statement,
) *pointSelectFastPath {
	if prepared == nil || prepared.AST == nil {
		return nil
	}
	if cached, ok := pointSelectCache.Load(prepared); ok {
		return cached.(pointSelectCacheEntry).plan
	}
	plan := buildPointSelectFastPath(ctx, p, prepared.AST)
	pointSelectCache.Store(prepared, pointSelectCacheEntry{
		plan:          plan,
		notApplicable: plan == nil,
	})
	return plan
}

// buildPointSelectFastPath inspects the parsed AST and, if it matches
// the recognized shape (`SELECT cols FROM t WHERE pk = $1` against a
// single-column primary key), resolves the table descriptor and builds
// the metadata needed to execute the statement directly. Returns nil
// if any check fails — the caller falls through to the slow path.
//
// Strict on shape by design: anything we don't perfectly recognize
// returns nil. Correctness is preserved by construction because the
// slow path remains the default.
func buildPointSelectFastPath(
	ctx context.Context, p *planner, ast tree.Statement,
) *pointSelectFastPath {
	sel, ok := ast.(*tree.Select)
	if !ok {
		return nil
	}
	if sel.With != nil || sel.OrderBy != nil || sel.Limit != nil || sel.Locking != nil {
		return nil
	}
	clause, ok := sel.Select.(*tree.SelectClause)
	if !ok {
		return nil
	}
	if clause.Distinct || clause.DistinctOn != nil ||
		clause.GroupBy != nil || clause.Having != nil || clause.Window != nil {
		return nil
	}
	if len(clause.From.Tables) != 1 {
		return nil
	}
	// Two-table inner JOIN dispatch: handled by a dedicated builder
	// because both AST shape and descriptor wiring differ enough that
	// inlining would obscure the single-table path.
	if _, isJoin := clause.From.Tables[0].(*tree.JoinTableExpr); isJoin {
		return buildPointSelectJoinFastPath(ctx, p, sel, clause)
	}
	aliased, ok := clause.From.Tables[0].(*tree.AliasedTableExpr)
	if !ok {
		return nil
	}
	if aliased.IndexFlags != nil || aliased.Lateral || aliased.Ordinality ||
		aliased.As.Alias != "" {
		return nil
	}
	tn, ok := aliased.Expr.(*tree.TableName)
	if !ok {
		return nil
	}
	if clause.Where == nil || clause.Where.Type != tree.AstWhere {
		return nil
	}
	cmp, ok := clause.Where.Expr.(*tree.ComparisonExpr)
	if !ok {
		return nil
	}
	if cmp.Operator.Symbol != treecmp.EQ {
		return nil
	}
	pkColName, ok := unqualifiedColumnRef(cmp.Left)
	if !ok {
		return nil
	}
	placeholder, ok := cmp.Right.(*tree.Placeholder)
	if !ok || placeholder.Idx != 0 {
		return nil
	}
	resultColNames := make([]string, len(clause.Exprs))
	for i, e := range clause.Exprs {
		if e.As != "" {
			return nil
		}
		colName, ok := unqualifiedColumnRef(e.Expr)
		if !ok {
			return nil
		}
		resultColNames[i] = colName
	}
	if len(resultColNames) == 0 {
		return nil
	}

	// Resolve the table.
	flags := tree.ObjectLookupFlags{
		Required:          true,
		DesiredObjectKind: tree.TableObject,
	}
	_, desc, err := resolver.ResolveExistingTableObject(ctx, p, tn, flags)
	if err != nil || desc == nil {
		return nil
	}

	// Find an index whose first (and only) key column matches the
	// WHERE column. Walk all active public indexes — the primary index
	// works for the original `WHERE pk = $1` case; a unique secondary
	// index covers the `WHERE non_pk_col = $1` case when its STORING
	// columns include everything the SELECT requested. We require
	// uniqueness because the fast-path execution issues a single Get
	// and emits at most one row.
	var lookupIdx catalog.Index
	for _, idx := range desc.ActiveIndexes() {
		if idx.NumKeyColumns() != 1 || !idx.IsUnique() {
			continue
		}
		if idx.GetKeyColumnName(0) != pkColName {
			continue
		}
		lookupIdx = idx
		break
	}
	if lookupIdx == nil {
		return nil
	}
	lookupColID := lookupIdx.GetKeyColumnID(0)
	lookupCol, err := catalog.MustFindColumnByID(desc, lookupColID)
	if err != nil {
		return nil
	}

	// Compute two coverage sets: the columns reachable from a single
	// Get on the lookup index, and (for secondary indexes) the columns
	// reachable from a follow-up Get on the primary index using the
	// suffix bytes as the PK key.
	primaryFamilyCols := make(map[descpb.ColumnID]struct{})
	if len(desc.GetFamilies()) == 0 || desc.GetFamilies()[0].ID != descpb.FamilyID(0) {
		return nil
	}
	for _, id := range desc.GetFamilies()[0].ColumnIDs {
		primaryFamilyCols[id] = struct{}{}
	}

	isPrimary := lookupIdx.GetID() == desc.GetPrimaryIndex().GetID()
	indexCoveredCols := make(map[descpb.ColumnID]struct{})
	if isPrimary {
		indexCoveredCols = primaryFamilyCols
	} else {
		lookupIdx.CollectKeyColumnIDs().ForEach(func(id descpb.ColumnID) {
			indexCoveredCols[id] = struct{}{}
		})
		lookupIdx.CollectKeySuffixColumnIDs().ForEach(func(id descpb.ColumnID) {
			indexCoveredCols[id] = struct{}{}
		})
		lookupIdx.CollectSecondaryStoredColumnIDs().ForEach(func(id descpb.ColumnID) {
			indexCoveredCols[id] = struct{}{}
		})
	}

	// Resolve every requested column. Decide between a single-Get
	// covering plan and a non-covering two-Get plan based on whether
	// every requested column is covered by the lookup index alone.
	// Non-covering only makes sense for secondary indexes, where the
	// suffix bytes carry the PK we can re-Get.
	resultCols := make([]catalog.Column, len(resultColNames))
	resultColIDs := make([]descpb.ColumnID, len(resultColNames))
	resultColumns := make(colinfo.ResultColumns, len(resultColNames))
	needsPKFetch := false
	for i, name := range resultColNames {
		col, err := catalog.MustFindColumnByName(desc, name)
		if err != nil {
			return nil
		}
		if col.IsSystemColumn() || col.IsHidden() {
			return nil
		}
		if _, covered := indexCoveredCols[col.GetID()]; !covered {
			// Not in the lookup index; can we get it from the PK row?
			if isPrimary {
				return nil
			}
			if _, inPK := primaryFamilyCols[col.GetID()]; !inPK {
				return nil
			}
			needsPKFetch = true
		}
		resultCols[i] = col
		resultColIDs[i] = col.GetID()
		resultColumns[i] = colinfo.ResultColumn{
			Name: col.GetName(),
			Typ:  col.GetType(),
		}
	}

	// PK-only-result restriction (primary-index path only). If the
	// only requested column is the PK column itself, the family-0 row
	// might not even exist on disk for some encodings, and the value's
	// tuple decode would return nothing. Skip the fast path. Doesn't
	// apply to secondary-index lookups, which always have a value
	// entry (the suffix / storing payload).
	if isPrimary {
		allPK := true
		for _, id := range resultColIDs {
			if id != lookupColID {
				allPK = false
				break
			}
		}
		if allPK {
			return nil
		}
	}

	// For unique secondary indexes, the value's bytes start with the
	// key-side-encoded suffix columns (the implicit PK) followed by
	// the tuple-encoded storing columns. Capture the suffix-column
	// types here so the executor can skip past them before invoking
	// the value-side decoder. Primary-index values have no such
	// prefix; the slice stays empty for that case.
	var suffixColTypes []*types.T
	if !isPrimary {
		n := lookupIdx.NumKeySuffixColumns()
		if n > 0 {
			suffixColTypes = make([]*types.T, n)
			for i := 0; i < n; i++ {
				colID := lookupIdx.GetKeySuffixColumnID(i)
				col, err := catalog.MustFindColumnByID(desc, colID)
				if err != nil {
					return nil
				}
				suffixColTypes[i] = col.GetType()
			}
		}
	}

	plan := &pointSelectFastPath{
		tableID:         desc.GetID(),
		descVersion:     desc.GetVersion(),
		lookupIndexID:   lookupIdx.GetID(),
		lookupKeyPrefix: rowenc.MakeIndexKeyPrefix(p.execCfg.Codec, desc.GetID(), lookupIdx.GetID()),
		lookupFamilyID:  descpb.FamilyID(0),
		pkColType:       lookupCol.GetType(),
		suffixColTypes:  suffixColTypes,
		resultColumns:   resultColumns,
		resultColumnIDs: resultColIDs,
		decoder:         valueside.MakeDecoder(resultCols),
	}

	if needsPKFetch {
		// Capture the PK index info for the second Get. The suffix
		// bytes from the secondary lookup are already key-encoded in
		// PK column order, so we can append them directly to the PK
		// prefix without decoding-and-re-encoding.
		pkIdx := desc.GetPrimaryIndex()
		plan.pkFetch = true
		plan.pkKeyPrefix = rowenc.MakeIndexKeyPrefix(p.execCfg.Codec, desc.GetID(), pkIdx.GetID())
		plan.pkFamilyID = descpb.FamilyID(0)
	}
	return plan
}

// unqualifiedColumnRef returns the column name if expr is an
// unqualified column reference (a single-part *tree.UnresolvedName),
// and ok=false otherwise. This is the form parser output uses for bare
// `colName` references; once the optimizer resolves names they become
// *tree.ColumnItem instead, but our detection runs against the parsed
// AST so we only see UnresolvedName here.
func unqualifiedColumnRef(expr tree.Expr) (name string, ok bool) {
	un, isName := expr.(*tree.UnresolvedName)
	if !isName {
		return "", false
	}
	if un.NumParts != 1 || un.Star {
		return "", false
	}
	return un.Parts[0], true
}

// qualifiedColumnRef returns (alias, column) if expr is a two-part
// qualified column reference (e.g. `f.id`). UnresolvedName stores
// parts in reverse order: Parts[0] is the column, Parts[1] is the
// table or alias.
func qualifiedColumnRef(expr tree.Expr) (alias, name string, ok bool) {
	un, isName := expr.(*tree.UnresolvedName)
	if !isName {
		return "", "", false
	}
	if un.NumParts != 2 || un.Star {
		return "", "", false
	}
	return un.Parts[1], un.Parts[0], true
}

// buildPointSelectJoinFastPath recognizes the two-table lookup-join
// shape:
//
//	SELECT t2alias.col, ... FROM t1 t1alias JOIN t2 t2alias
//	  ON t1alias.fk = t2alias.pkcol
//	WHERE t1alias.indexedcol = $1
//
// where:
//   - the join is an INNER join with an explicit ON clause,
//   - both tables are aliased,
//   - t1alias.indexedcol is the single key column of a UNIQUE
//     SECONDARY index on t1 (so the lookup hits two-stage non-
//     covering shape on t1 to read the FK column),
//   - t2alias.pkcol is the single PK column of t2,
//   - t1alias.fk lives in t1's column family 0 (so we can decode it
//     from the family-0 tuple after the second Get),
//   - every SELECT column is qualified with t2alias and lives in t2's
//     column family 0.
//
// The resulting plan executes three KV Gets in series: lookup index
// on t1, PK row on t1 (to read fk), PK row on t2.
//
// Returns nil for any AST shape we don't perfectly recognize, so the
// slow path remains the default.
func buildPointSelectJoinFastPath(
	ctx context.Context, p *planner, sel *tree.Select, clause *tree.SelectClause,
) *pointSelectFastPath {
	join, ok := clause.From.Tables[0].(*tree.JoinTableExpr)
	if !ok {
		return nil
	}
	if join.JoinType != "" && join.JoinType != tree.AstInner {
		return nil
	}
	if join.Hint != "" {
		return nil
	}
	leftAliased, ok := join.Left.(*tree.AliasedTableExpr)
	if !ok {
		return nil
	}
	rightAliased, ok := join.Right.(*tree.AliasedTableExpr)
	if !ok {
		return nil
	}
	if leftAliased.IndexFlags != nil || leftAliased.Lateral || leftAliased.Ordinality ||
		rightAliased.IndexFlags != nil || rightAliased.Lateral || rightAliased.Ordinality {
		return nil
	}
	leftAlias := string(leftAliased.As.Alias)
	rightAlias := string(rightAliased.As.Alias)
	if leftAlias == "" || rightAlias == "" || leftAlias == rightAlias {
		return nil
	}
	leftTN, ok := leftAliased.Expr.(*tree.TableName)
	if !ok {
		return nil
	}
	rightTN, ok := rightAliased.Expr.(*tree.TableName)
	if !ok {
		return nil
	}

	onCond, ok := join.Cond.(*tree.OnJoinCond)
	if !ok {
		return nil
	}
	onCmp, ok := onCond.Expr.(*tree.ComparisonExpr)
	if !ok || onCmp.Operator.Symbol != treecmp.EQ {
		return nil
	}
	leftOnAlias, leftOnCol, ok := qualifiedColumnRef(onCmp.Left)
	if !ok {
		return nil
	}
	rightOnAlias, rightOnCol, ok := qualifiedColumnRef(onCmp.Right)
	if !ok {
		return nil
	}

	// The ON clause can be written either way around. Identify which
	// side is the FK (t1) and which is the join target (t2).
	var fkAlias, fkColName, targetAlias, targetColName string
	switch {
	case leftOnAlias == leftAlias && rightOnAlias == rightAlias:
		fkAlias, fkColName = leftOnAlias, leftOnCol
		targetAlias, targetColName = rightOnAlias, rightOnCol
	case leftOnAlias == rightAlias && rightOnAlias == leftAlias:
		fkAlias, fkColName = rightOnAlias, rightOnCol
		targetAlias, targetColName = leftOnAlias, leftOnCol
	default:
		return nil
	}

	// WHERE clause: alias.col = $1; alias must be the FK side.
	if clause.Where == nil || clause.Where.Type != tree.AstWhere {
		return nil
	}
	whereCmp, ok := clause.Where.Expr.(*tree.ComparisonExpr)
	if !ok || whereCmp.Operator.Symbol != treecmp.EQ {
		return nil
	}
	whereAlias, whereCol, ok := qualifiedColumnRef(whereCmp.Left)
	if !ok || whereAlias != fkAlias {
		return nil
	}
	placeholder, ok := whereCmp.Right.(*tree.Placeholder)
	if !ok || placeholder.Idx != 0 {
		return nil
	}

	// SELECT exprs: every column qualified with the target alias.
	resultColNames := make([]string, len(clause.Exprs))
	for i, e := range clause.Exprs {
		if e.As != "" {
			return nil
		}
		a, c, ok := qualifiedColumnRef(e.Expr)
		if !ok || a != targetAlias {
			return nil
		}
		resultColNames[i] = c
	}
	if len(resultColNames) == 0 {
		return nil
	}

	// Map aliases back to TableNames.
	var fkTN, targetTN *tree.TableName
	if fkAlias == leftAlias {
		fkTN, targetTN = leftTN, rightTN
	} else {
		fkTN, targetTN = rightTN, leftTN
	}

	flags := tree.ObjectLookupFlags{
		Required:          true,
		DesiredObjectKind: tree.TableObject,
	}
	_, fkDesc, err := resolver.ResolveExistingTableObject(ctx, p, fkTN, flags)
	if err != nil || fkDesc == nil {
		return nil
	}
	_, targetDesc, err := resolver.ResolveExistingTableObject(ctx, p, targetTN, flags)
	if err != nil || targetDesc == nil {
		return nil
	}

	// Target table: must have a single-column PK matching targetColName.
	targetPKIdx := targetDesc.GetPrimaryIndex()
	if targetPKIdx == nil || targetPKIdx.NumKeyColumns() != 1 {
		return nil
	}
	if targetPKIdx.GetKeyColumnName(0) != targetColName {
		return nil
	}

	// FK table: find a UNIQUE SECONDARY index keyed solely on whereCol.
	var lookupIdx catalog.Index
	for _, idx := range fkDesc.ActiveIndexes() {
		if idx.NumKeyColumns() != 1 || !idx.IsUnique() {
			continue
		}
		if idx.GetID() == fkDesc.GetPrimaryIndex().GetID() {
			// Need a secondary index — we have to fetch the FK
			// column separately from the PK row.
			continue
		}
		if idx.GetKeyColumnName(0) != whereCol {
			continue
		}
		lookupIdx = idx
		break
	}
	if lookupIdx == nil {
		return nil
	}
	lookupCol, err := catalog.MustFindColumnByID(fkDesc, lookupIdx.GetKeyColumnID(0))
	if err != nil {
		return nil
	}

	// FK column must live in t1's family 0 (the family fetched by
	// the second Get).
	if len(fkDesc.GetFamilies()) == 0 || fkDesc.GetFamilies()[0].ID != descpb.FamilyID(0) {
		return nil
	}
	fkPrimaryFamilyCols := make(map[descpb.ColumnID]struct{})
	for _, id := range fkDesc.GetFamilies()[0].ColumnIDs {
		fkPrimaryFamilyCols[id] = struct{}{}
	}
	fkColumn, err := catalog.MustFindColumnByName(fkDesc, fkColName)
	if err != nil {
		return nil
	}
	if _, ok := fkPrimaryFamilyCols[fkColumn.GetID()]; !ok {
		return nil
	}

	// Result columns must live in t2's family 0.
	if len(targetDesc.GetFamilies()) == 0 || targetDesc.GetFamilies()[0].ID != descpb.FamilyID(0) {
		return nil
	}
	targetPrimaryFamilyCols := make(map[descpb.ColumnID]struct{})
	for _, id := range targetDesc.GetFamilies()[0].ColumnIDs {
		targetPrimaryFamilyCols[id] = struct{}{}
	}
	resultCols := make([]catalog.Column, len(resultColNames))
	resultColIDs := make([]descpb.ColumnID, len(resultColNames))
	resultColumns := make(colinfo.ResultColumns, len(resultColNames))
	for i, name := range resultColNames {
		col, err := catalog.MustFindColumnByName(targetDesc, name)
		if err != nil {
			return nil
		}
		if col.IsSystemColumn() || col.IsHidden() {
			return nil
		}
		if _, ok := targetPrimaryFamilyCols[col.GetID()]; !ok {
			return nil
		}
		resultCols[i] = col
		resultColIDs[i] = col.GetID()
		resultColumns[i] = colinfo.ResultColumn{
			Name: col.GetName(),
			Typ:  col.GetType(),
		}
	}

	// Suffix column types for the secondary lookup value's BYTES
	// payload (the implicit PK suffix).
	n := lookupIdx.NumKeySuffixColumns()
	if n == 0 {
		return nil
	}
	suffixColTypes := make([]*types.T, n)
	for i := 0; i < n; i++ {
		col, err := catalog.MustFindColumnByID(fkDesc, lookupIdx.GetKeySuffixColumnID(i))
		if err != nil {
			return nil
		}
		suffixColTypes[i] = col.GetType()
	}

	return &pointSelectFastPath{
		tableID:     fkDesc.GetID(),
		descVersion: fkDesc.GetVersion(),

		lookupIndexID:   lookupIdx.GetID(),
		lookupKeyPrefix: rowenc.MakeIndexKeyPrefix(p.execCfg.Codec, fkDesc.GetID(), lookupIdx.GetID()),
		lookupFamilyID:  descpb.FamilyID(0),
		pkColType:       lookupCol.GetType(),
		suffixColTypes:  suffixColTypes,

		pkFetch:     true,
		pkKeyPrefix: rowenc.MakeIndexKeyPrefix(p.execCfg.Codec, fkDesc.GetID(), fkDesc.GetPrimaryIndex().GetID()),
		pkFamilyID:  descpb.FamilyID(0),

		joinFetch:           true,
		joinPKKeyPrefix:     rowenc.MakeIndexKeyPrefix(p.execCfg.Codec, targetDesc.GetID(), targetPKIdx.GetID()),
		joinPKFamilyID:      descpb.FamilyID(0),
		intermediateColType: fkColumn.GetType(),
		intermediateDecoder: valueside.MakeDecoder([]catalog.Column{fkColumn}),

		resultColumns:   resultColumns,
		resultColumnIDs: resultColIDs,
		decoder:         valueside.MakeDecoder(resultCols),
	}
}

// execPointSelectFastPath executes a prepared point-SELECT by issuing
// one or two KV Gets and emitting the decoded row directly to res.
// Returns handled=true if the statement was fully processed (success
// or terminal user error written to res), handled=false to indicate
// the caller should fall back to the normal execution path.
func (ex *connExecutor) execPointSelectFastPath(
	ctx context.Context,
	plan *pointSelectFastPath,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
) (handled bool, err error) {
	// Bail out on anything we don't yet support.
	if pinfo == nil || len(pinfo.Values) != 1 {
		return false, nil
	}
	// QueryArguments is []TypedExpr; the bound value is the Datum case.
	val, ok := pinfo.Values[0].(tree.Datum)
	if !ok || val == nil {
		return false, nil
	}
	if !plan.pkColType.Equivalent(val.ResolvedType()) {
		return false, nil
	}

	// Skip the fast path if the descriptor has changed since detection.
	// A version change can mean a column was added/dropped, the family
	// layout changed, etc. — rather than try to re-detect inline, fall
	// through and let the slow path (which goes through the full
	// resolver) do the right thing. We deliberately swallow the lookup
	// error here for the same reason: any failure means we can't safely
	// run the fast path, so the slow path will produce the appropriate
	// user-visible error if there is one.
	desc, lookupErr := ex.planner.LookupTableByID(ctx, plan.tableID)
	if lookupErr != nil {
		return false, nil //nolint:returnerrcheck
	}
	if desc.GetVersion() != plan.descVersion {
		return false, nil
	}

	// Encode the lookup key from the bound placeholder. An encoding
	// error here means the placeholder type doesn't round-trip through
	// the key encoder, which we treat as "unsupported" and let the
	// slow path handle (it will generate a proper type-mismatch error).
	lookupKey := make([]byte, 0, len(plan.lookupKeyPrefix)+16)
	lookupKey = append(lookupKey, plan.lookupKeyPrefix...)
	lookupKey, err = keyside.Encode(lookupKey, val, encoding.Ascending)
	if err != nil {
		return false, nil //nolint:returnerrcheck
	}
	lookupKey = keys.MakeFamilyKey(lookupKey, uint32(plan.lookupFamilyID))

	// First-stage Get against the lookup index.
	var b1 kv.Batch
	b1.Get(lookupKey)
	if err := ex.state.mu.txn.Run(ctx, &b1); err != nil {
		return false, nil //nolint:returnerrcheck
	}
	getRes1 := b1.Results[0]
	if getRes1.Err != nil {
		return false, nil //nolint:returnerrcheck
	}
	res.SetColumns(ctx, plan.resultColumns, false /* skipRowDescription */)
	if len(getRes1.Rows) == 0 || !getRes1.Rows[0].Value.IsPresent() {
		// No row matched — empty result, zero rows affected.
		res.SetRowsAffected(ctx, 0)
		return true, nil
	}

	// The lookup value's encoding depends on the index shape:
	//
	// * Primary index (suffixColTypes empty, !pkFetch): value tag is
	//   TUPLE; bytes are the tuple-encoded non-PK columns of family 0.
	// * Unique secondary index (suffixColTypes non-empty): value tag
	//   is BYTES; bytes start with the key-side-encoded suffix columns
	//   (the implicit PK), followed by either tuple-encoded storing
	//   columns (covering case) or nothing (non-covering).
	//
	// In the non-covering case, we use the suffix bytes directly as
	// the body of a primary-key Get to fetch the row.
	alloc := &tree.DatumAlloc{}
	var tupleBytes []byte
	if len(plan.suffixColTypes) == 0 {
		var tupleErr error
		tupleBytes, tupleErr = getRes1.Rows[0].Value.GetTuple()
		if tupleErr != nil {
			return false, nil //nolint:returnerrcheck
		}
	} else {
		valueBytes, bytesErr := getRes1.Rows[0].Value.GetBytes()
		if bytesErr != nil {
			return false, nil //nolint:returnerrcheck
		}
		if plan.pkFetch {
			// Non-covering: valueBytes IS the key-encoded PK suffix.
			// Append it to the PK prefix and Get the primary row.
			pkKey := make([]byte, 0, len(plan.pkKeyPrefix)+len(valueBytes)+4)
			pkKey = append(pkKey, plan.pkKeyPrefix...)
			pkKey = append(pkKey, valueBytes...)
			pkKey = keys.MakeFamilyKey(pkKey, uint32(plan.pkFamilyID))

			var b2 kv.Batch
			b2.Get(pkKey)
			if err := ex.state.mu.txn.Run(ctx, &b2); err != nil {
				return false, nil //nolint:returnerrcheck
			}
			getRes2 := b2.Results[0]
			if getRes2.Err != nil {
				return false, nil //nolint:returnerrcheck
			}
			if len(getRes2.Rows) == 0 || !getRes2.Rows[0].Value.IsPresent() {
				// Anomaly: the secondary index pointed at a PK row
				// that doesn't exist. Defer to the slow path so the
				// optimizer / row.Fetcher can decide what to do
				// (raise a consistency error, fall through, etc.).
				return false, nil
			}
			tupleBytes, err = getRes2.Rows[0].Value.GetTuple()
			if err != nil {
				return false, nil //nolint:returnerrcheck
			}
		} else {
			// Covering: skip past the suffix columns; the rest is the
			// tuple-encoded storing payload we need to decode.
			for _, suffixType := range plan.suffixColTypes {
				_, valueBytes, err = keyside.Decode(alloc, suffixType, valueBytes, encoding.Ascending)
				if err != nil {
					return false, nil //nolint:returnerrcheck
				}
			}
			tupleBytes = valueBytes
		}
	}

	// Two-table lookup join: the tuple bytes we have so far come from
	// table 1's PK row. Decode the intermediate FK column out of them,
	// re-encode it as a key into table 2's primary index, and Get the
	// joined row. The final decode below then runs against table 2's
	// family-0 tuple.
	if plan.joinFetch {
		intDatums, intErr := plan.intermediateDecoder.Decode(alloc, tupleBytes)
		if intErr != nil {
			return false, nil //nolint:returnerrcheck
		}
		if len(intDatums) != 1 {
			return false, nil
		}
		fkDatum := intDatums[0]
		if fkDatum == tree.DNull {
			// FK is NULL — no joined row exists. Empty result.
			res.SetRowsAffected(ctx, 0)
			return true, nil
		}

		joinKey := make([]byte, 0, len(plan.joinPKKeyPrefix)+16)
		joinKey = append(joinKey, plan.joinPKKeyPrefix...)
		joinKey, err = keyside.Encode(joinKey, fkDatum, encoding.Ascending)
		if err != nil {
			return false, nil //nolint:returnerrcheck
		}
		joinKey = keys.MakeFamilyKey(joinKey, uint32(plan.joinPKFamilyID))

		var b3 kv.Batch
		b3.Get(joinKey)
		if err := ex.state.mu.txn.Run(ctx, &b3); err != nil {
			return false, nil //nolint:returnerrcheck
		}
		getRes3 := b3.Results[0]
		if getRes3.Err != nil {
			return false, nil //nolint:returnerrcheck
		}
		if len(getRes3.Rows) == 0 || !getRes3.Rows[0].Value.IsPresent() {
			// FK pointed at a missing row. Defer to slow path —
			// could be a consistency anomaly the optimizer would
			// surface differently.
			return false, nil
		}
		tupleBytes, err = getRes3.Rows[0].Value.GetTuple()
		if err != nil {
			return false, nil //nolint:returnerrcheck
		}
	}

	datums, decodeErr := plan.decoder.Decode(alloc, tupleBytes)
	if decodeErr != nil {
		return false, nil //nolint:returnerrcheck
	}

	if err := res.AddRow(ctx, datums); err != nil {
		// Real user-visible error from pgwire — surface it.
		return true, err
	}
	res.SetRowsAffected(ctx, 1)
	return true, nil
}
