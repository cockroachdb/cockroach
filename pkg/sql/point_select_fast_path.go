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
type pointSelectFastPath struct {
	tableID         descpb.ID
	indexID         descpb.IndexID
	descVersion     descpb.DescriptorVersion
	familyID        descpb.FamilyID
	keyPrefix       []byte
	pkColType       *types.T
	resultColumns   colinfo.ResultColumns
	resultColumnIDs []descpb.ColumnID
	decoder         valueside.Decoder
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

	// Single-column primary index.
	pkIndex := desc.GetPrimaryIndex()
	if pkIndex == nil || pkIndex.NumKeyColumns() != 1 {
		return nil
	}
	if pkIndex.GetKeyColumnName(0) != pkColName {
		return nil
	}
	pkColID := pkIndex.GetKeyColumnID(0)
	pkCol, err := catalog.MustFindColumnByID(desc, pkColID)
	if err != nil {
		return nil
	}

	// Resolve each requested output column. Reject if any column lives
	// outside the primary column family — the prototype only handles
	// the case where a single Get against the family-0 row returns
	// every requested column.
	if len(desc.GetFamilies()) == 0 {
		return nil
	}
	primaryFamily := desc.GetFamilies()[0]
	if primaryFamily.ID != descpb.FamilyID(0) {
		return nil
	}
	primaryFamilyCols := make(map[descpb.ColumnID]struct{}, len(primaryFamily.ColumnIDs))
	for _, id := range primaryFamily.ColumnIDs {
		primaryFamilyCols[id] = struct{}{}
	}
	resultCols := make([]catalog.Column, len(resultColNames))
	resultColIDs := make([]descpb.ColumnID, len(resultColNames))
	resultColumns := make(colinfo.ResultColumns, len(resultColNames))
	for i, name := range resultColNames {
		col, err := catalog.MustFindColumnByName(desc, name)
		if err != nil {
			return nil
		}
		if _, ok := primaryFamilyCols[col.GetID()]; !ok {
			return nil
		}
		// We never want to return a hidden / system column; user
		// queries can't reference them anyway, but be explicit.
		if col.IsSystemColumn() || col.IsHidden() {
			return nil
		}
		resultCols[i] = col
		resultColIDs[i] = col.GetID()
		resultColumns[i] = colinfo.ResultColumn{
			Name: col.GetName(),
			Typ:  col.GetType(),
		}
	}

	// We require there to be at least one stored column in the family.
	// If the only requested columns were the PK, the family-0 row
	// might not even exist on disk for some encodings; bail out.
	allPK := true
	for _, id := range resultColIDs {
		if id != pkColID {
			allPK = false
			break
		}
	}
	if allPK {
		return nil
	}

	keyPrefix := rowenc.MakeIndexKeyPrefix(p.execCfg.Codec, desc.GetID(), pkIndex.GetID())
	return &pointSelectFastPath{
		tableID:         desc.GetID(),
		indexID:         pkIndex.GetID(),
		descVersion:     desc.GetVersion(),
		familyID:        primaryFamily.ID,
		keyPrefix:       keyPrefix,
		pkColType:       pkCol.GetType(),
		resultColumns:   resultColumns,
		resultColumnIDs: resultColIDs,
		decoder:         valueside.MakeDecoder(resultCols),
	}
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

// execPointSelectFastPath executes a prepared point-SELECT against a
// single-column primary key by issuing one KV Get and emitting the
// decoded row directly to res. Returns handled=true if the statement
// was fully processed (success or terminal user error written to res),
// handled=false to indicate the caller should fall back to the normal
// execution path.
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

	// Encode the primary key from the bound placeholder. An encoding
	// error here means the placeholder type doesn't round-trip through
	// the key encoder, which we treat as "unsupported" and let the
	// slow path handle (it will generate a proper type-mismatch error).
	keyBuf := make([]byte, 0, len(plan.keyPrefix)+16)
	keyBuf = append(keyBuf, plan.keyPrefix...)
	keyBuf, err = keyside.Encode(keyBuf, val, encoding.Ascending)
	if err != nil {
		return false, nil //nolint:returnerrcheck
	}
	keyBuf = keys.MakeFamilyKey(keyBuf, uint32(plan.familyID))

	// Issue a single Get against the open transaction. KV errors are
	// surfaced through the slow path so they go through the regular
	// pgwire error translation pipeline.
	var b kv.Batch
	b.Get(keyBuf)
	if err := ex.state.mu.txn.Run(ctx, &b); err != nil {
		return false, nil //nolint:returnerrcheck
	}
	// The Batch always populates Results when Run returns nil.
	getRes := b.Results[0]
	if getRes.Err != nil {
		return false, nil //nolint:returnerrcheck
	}
	res.SetColumns(ctx, plan.resultColumns, false /* skipRowDescription */)
	if len(getRes.Rows) == 0 || !getRes.Rows[0].Value.IsPresent() {
		// No row matched — empty result, zero rows affected.
		res.SetRowsAffected(ctx, 0)
		return true, nil
	}

	// Decode the value (single column family, tuple-encoded). A decode
	// failure indicates the table's storage encoding has shifted away
	// from what we cached at detection time; defer to the slow path.
	tupleBytes, tupleErr := getRes.Rows[0].Value.GetTuple()
	if tupleErr != nil {
		return false, nil //nolint:returnerrcheck
	}
	datums, decodeErr := plan.decoder.Decode(&tree.DatumAlloc{}, tupleBytes)
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
