// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	randworkload "github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/cockroachdb/errors"
)

// dbTx is the subset of *sql.Tx that the Execute* helpers need. Production
// code passes a real *sql.Tx; tests pass a recording wrapper to capture SQL
// for inspection. Keeping the surface narrow lets test wrappers remain small.
type dbTx interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (gosql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*gosql.Rows, error)
}

// emittedRow holds the column values written for one table within a single
// transaction. Keys are column names; values are the Go-SQL representations
// passed to the driver. The full row is stored (not just the PK) because FK
// constraints may reference non-PK unique constraint columns.
type emittedRow map[string]interface{}

// emittedSet maps table name → row written for that table in the current txn.
type emittedSet map[string]emittedRow

// droppedEdgeSet indexes edges dropped by RandomSubDAG by referencing table.
// FK columns belonging to a dropped edge are written as NULL in the initial
// UPSERT and patched with an UPDATE after the topological walk completes.
type droppedEdgeSet map[string][]FKEdge

func newDroppedEdgeSet(edges []FKEdge) droppedEdgeSet {
	s := make(droppedEdgeSet, len(edges))
	for _, e := range edges {
		s[e.ReferencingTable] = append(s[e.ReferencingTable], e)
	}
	return s
}

// hasColumn reports whether col is a column on this edge's referencing side.
func (s droppedEdgeSet) hasColumn(table, col string) bool {
	for _, e := range s[table] {
		for _, c := range e.ReferencingColumns {
			if c == col {
				return true
			}
		}
	}
	return false
}

// ExecuteUpsert generates and executes an UPSERT walk over sorted in
// topological order, writing one row per table. PK columns are read from
// pools.PKs[T][pkIdx] (stable across the chain). UC columns are always
// re-rolled fresh per write from pools.
//
// repointFKs controls non-PK FK column behavior:
//
//   - false: pin FK columns to pkIdx. Used for the chain's first UPSERT
//     (when the parent rows are being written in the same walk and only
//     exist at pkIdx, so a fresh parentIdx would dangle).
//   - true: roll fresh parentIdx per write for non-PK FK columns. Used for
//     UPSERT-as-update (Exists → Exists), where the chain's parent rows
//     already exist in the database, and other slots are populated by other
//     chains. This is what produces real replicable diffs — the row's FK
//     columns mutate between writes.
//
// PK-overlapping FK columns are always pinned to pkIdx regardless; rolling
// them would change the row's identity.
//
// pools may be nil for the legacy code path, in which case PK and UC columns
// fall back to per-write type-domain sampling. FK columns in that mode are
// resolved from the parent's emitted row (parents are guaranteed to have
// been visited first by the topological order).
//
// Returns the set of rows emitted. On failure the partial emitted set is
// returned alongside the error so callers can inspect what was attempted.
func ExecuteUpsert(
	ctx context.Context,
	tx dbTx,
	rng *rand.Rand,
	sorted []*Table,
	sub *FKGraph,
	droppedEdges []FKEdge,
	pools *Pools,
	pkIdx int,
	repointFKs bool,
) (emittedSet, error) {
	emitted := make(emittedSet, len(sorted))
	dropped := newDroppedEdgeSet(droppedEdges)

	for _, t := range sorted {
		row, err := buildRow(rng, t, sub, dropped, emitted, pools, pkIdx, repointFKs)
		if err != nil {
			return emitted, errors.Wrapf(err, "building row for %s", t.Name)
		}
		emitted[t.Name] = row
		if err := execUpsertRow(ctx, tx, t, row); err != nil {
			return emitted, errors.Wrapf(err, "upserting into %s", t.Name)
		}
		// If any in-sub child FK references a computed column on t, read it
		// back so downstream buildRow calls can resolve the FK value from
		// emitted rather than from pools (computed columns aren't pool-backed).
		if err := readBackComputedFKTargets(ctx, tx, t, sub, row); err != nil {
			return emitted, errors.Wrapf(err, "reading back computed FK targets for %s", t.Name)
		}
	}

	if err := patchDroppedEdges(ctx, tx, sorted, droppedEdges, emitted); err != nil {
		return emitted, errors.Wrap(err, "patching dropped edges")
	}
	return emitted, nil
}

// readBackComputedFKTargets fills computed columns on t into row when the
// columns are referenced by any in-sub FK. Without this, downstream child
// UPSERTs can't resolve their FK values — the parent's computed column was
// populated by the DB on the just-executed UPSERT, but the workload never
// chose a value for it client-side.
//
// Issues one SELECT per parent table that has at least one FK-referenced
// computed column. The SELECT runs against the same tx as the UPSERT, so it
// observes the just-written row.
func readBackComputedFKTargets(
	ctx context.Context, tx dbTx, t *Table, sub *FKGraph, row emittedRow,
) error {
	computed := make(map[string]bool)
	for _, c := range t.Columns {
		if c.Computed {
			computed[c.Name] = true
		}
	}
	if len(computed) == 0 {
		return nil
	}

	// Collect computed columns on t that are referenced by some in-sub FK.
	needed := make(map[string]bool)
	for _, e := range sub.Edges {
		if e.ReferencedTable != t.Name {
			continue
		}
		for _, refCol := range e.ReferencedColumns {
			if computed[refCol] {
				needed[refCol] = true
			}
		}
	}
	if len(needed) == 0 {
		return nil
	}

	pkCols := primaryKeyColumns(t)
	colNames := make([]string, 0, len(needed))
	for c := range needed {
		colNames = append(colNames, c)
	}
	sort.Strings(colNames)
	selectCols := make([]string, len(colNames))
	for i, c := range colNames {
		selectCols[i] = tree.NameString(c)
	}
	whereClauses := make([]string, len(pkCols))
	args := make([]interface{}, len(pkCols))
	for i, c := range pkCols {
		whereClauses[i] = fmt.Sprintf("%s = $%d", tree.NameString(c), i+1)
		v, ok := row[c]
		if !ok {
			return errors.AssertionFailedf(
				"PK column %s missing from emitted row for %s", c, t.Name,
			)
		}
		args[i] = v
	}
	stmt := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		strings.Join(selectCols, ", "),
		tree.NameString(t.Name),
		strings.Join(whereClauses, " AND "),
	)
	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return errors.AssertionFailedf(
			"no row found in %s after UPSERT; read-back of computed columns failed", t.Name,
		)
	}
	dest := make([]interface{}, len(colNames))
	for i := range dest {
		var v interface{}
		dest[i] = &v
	}
	if err := rows.Scan(dest...); err != nil {
		return err
	}
	for i, c := range colNames {
		row[c] = *(dest[i].(*interface{}))
	}
	return rows.Err()
}

// buildRow generates one row's worth of column values for t. Column sources
// in priority order (first match wins):
//
//  1. Computed: skipped (DB fills them; downstream FKs resolve via read-back).
//  2. Dropped or self-ref FK column: nil (NULL); patched later.
//  3. FK column with in-sub parent: parent's referenced-column value. PK-
//     overlapping FKs read the parent's PKs[pkIdx] (stable per chain); non-
//     PK FKs read from a fresh parentIdx (changes per write — drives diffs).
//  4. PK column: pools.PKs[t][pkIdx] (stable per chain).
//  5. Non-PK UC column: pools.UCs[uc.Name][freshIdx] (changes per write).
//  6. Plain column: random per write via randgen.RandDatum.
//
// When pools is nil, PK columns fall back to type-domain sampling and FK
// columns are resolved from emitted (the pre-pool behavior).
func buildRow(
	rng *rand.Rand,
	t *Table,
	sub *FKGraph,
	dropped droppedEdgeSet,
	emitted emittedSet,
	pools *Pools,
	pkIdx int,
	repointFKs bool,
) (emittedRow, error) {
	pkCols := primaryKeyColumns(t)
	pkSet := make(map[string]bool, len(pkCols))
	for _, c := range pkCols {
		pkSet[c] = true
	}
	pkValByCol, err := pkColumnValues(t, sub, pools, pkIdx, rng)
	if err != nil {
		return nil, err
	}
	fkValByCol, err := fkColumnValues(rng, t, sub, emitted, pools, pkIdx, pkSet, repointFKs)
	if err != nil {
		return nil, err
	}
	ucValByCol := ucColumnValues(rng, t, pools, pkSet)

	row := make(emittedRow, len(t.Columns))
	for _, col := range t.Columns {
		if col.Computed {
			continue
		}
		switch {
		case dropped.hasColumn(t.Name, col.Name) || isSelfRefColumn(t, col.Name):
			row[col.Name] = nil
		default:
			if v, ok := fkValByCol[col.Name]; ok {
				row[col.Name] = v
				continue
			}
			if v, ok := pkValByCol[col.Name]; ok {
				row[col.Name] = v
				continue
			}
			if v, ok := ucValByCol[col.Name]; ok {
				row[col.Name] = v
				continue
			}
			val, err := randomColumnValue(rng, col)
			if err != nil {
				return nil, err
			}
			row[col.Name] = val
		}
	}
	return row, nil
}

// pkColumnValues returns the PK column values for t at the chain's pkIdx.
// When pools is non-nil, values come from PKValuesFor (which handles PK-
// overlapping-FK propagation from the parent's pool). When pools is nil,
// values are sampled fresh from each PK column's type domain.
func pkColumnValues(
	t *Table, sub *FKGraph, pools *Pools, pkIdx int, rng *rand.Rand,
) (map[string]interface{}, error) {
	pkCols := primaryKeyColumns(t)
	out := make(map[string]interface{}, len(pkCols))
	if pools != nil {
		vals, err := PKValuesFor(t, sub, pools, pkIdx)
		if err != nil {
			return nil, err
		}
		for i, c := range pkCols {
			out[c] = vals[i]
		}
		return out, nil
	}
	colByName := make(map[string]Column, len(t.Columns))
	for _, c := range t.Columns {
		colByName[c.Name] = c
	}
	for _, c := range pkCols {
		col, ok := colByName[c]
		if !ok {
			return nil, errors.AssertionFailedf("PK column %s.%s not in column list", t.Name, c)
		}
		v, err := randomColumnValue(rng, col)
		if err != nil {
			return nil, err
		}
		out[c] = v
	}
	return out, nil
}

// fkColumnValues resolves, for each child column on t that participates in
// an in-sub FK, the value the column should take. PK-overlapping FK columns
// always pin to pkIdx (stable per chain).
//
// repointFKs controls non-PK FK columns:
//   - false: pin to pkIdx (the parent row at pkIdx is being written in this
//     same walk).
//   - true: roll a fresh parentIdx per call (the parent's row at pkIdx
//     already exists in the database from a prior chain event, and other
//     slots are populated by other chains). This produces real replicable
//     diffs on UPSERT-as-update.
//
// Self-referential edges are skipped (handled via NULL + backfill). When
// multiple FK edges reference the same child column, all parents must agree
// on the value; disagreement indicates an uncoordinated PK assignment.
func fkColumnValues(
	rng *rand.Rand,
	t *Table,
	sub *FKGraph,
	emitted emittedSet,
	pools *Pools,
	pkIdx int,
	pkSet map[string]bool,
	repointFKs bool,
) (map[string]interface{}, error) {
	subEdges := make(map[string]bool, len(sub.Edges))
	for _, e := range sub.Edges {
		subEdges[e.Name] = true
	}

	// Pre-pick one freshIdx per table when repointing. All of t's non-PK FKs
	// share this index so child columns referenced by multiple FKs (e.g.
	// diamond's projects.org_id, referenced by both depts and teams FKs) get
	// values from a single shared parent slot — independent picks per FK
	// would split the shared column across two parent rows and trigger an
	// FK conflict. Using one freshIdx per table keeps the row consistent.
	//
	// We bias freshIdx away from pkIdx so the rewrite produces a real diff
	// against the chain's previous state. Pool size of 1 collapses to the
	// pinned case, which is fine.
	freshIdx := pkIdx
	if repointFKs && pools != nil {
		for _, tuples := range pools.PKs {
			if len(tuples) > 1 {
				freshIdx = (pkIdx + 1 + rng.Intn(len(tuples)-1)) % len(tuples)
			}
			break
		}
	}

	type candidate struct {
		parentTable string
		parentCol   string
		val         interface{}
	}
	candidates := make(map[string][]candidate)

	for _, e := range t.OutboundFKs {
		if e.ReferencingTable == e.ReferencedTable {
			continue
		}
		if !subEdges[e.Name] {
			continue
		}
		if len(e.ReferencingColumns) != len(e.ReferencedColumns) {
			return nil, errors.AssertionFailedf(
				"FK %s has %d referencing cols but %d referenced cols",
				e.Name, len(e.ReferencingColumns), len(e.ReferencedColumns),
			)
		}

		parentIdx, mode, err := pickParentIdx(e, pools, pkIdx, pkSet, freshIdx, repointFKs)
		if err != nil {
			return nil, err
		}

		parentVals, err := fkParentValues(sub, e, mode, parentIdx, pools, emitted)
		if err != nil {
			return nil, err
		}
		if parentVals == nil {
			continue
		}
		for i, refCol := range e.ReferencingColumns {
			candidates[refCol] = append(candidates[refCol], candidate{
				parentTable: e.ReferencedTable,
				parentCol:   e.ReferencedColumns[i],
				val:         parentVals[i],
			})
		}
	}

	resolved := make(map[string]interface{}, len(candidates))
	for childCol, cands := range candidates {
		first := cands[0]
		for _, c := range cands[1:] {
			if c.val != first.val {
				return nil, errors.Newf(
					"FK column %s.%s has conflicting parent values: %s.%s=%v vs %s.%s=%v",
					t.Name, childCol,
					first.parentTable, first.parentCol, first.val,
					c.parentTable, c.parentCol, c.val,
				)
			}
		}
		resolved[childCol] = first.val
	}
	return resolved, nil
}

// fkSourceMode tags how an FK's parent values are sourced. poolPK and poolUC
// read from the corresponding pool at parentIdx; emitted reads from the
// parent's row already written in this transaction (legacy nil-pool path).
type fkSourceMode int

const (
	fkSourcePoolPK fkSourceMode = iota
	fkSourcePoolUC
	fkSourceEmitted
)

// pickParentIdx chooses the parent-side index to use for FK edge e and
// reports which pool to read from. freshIdx is the caller-supplied
// parent index to use for non-PK FKs when repointing; -1 (or repointFKs=false)
// pins to pkIdx instead.
//
// PK-overlapping FKs always pin to pkIdx — re-pointing them would change the
// row's identity and break chain stability. When pools is nil, the mode is
// fkSourceEmitted and the returned index is unused.
//
// Why the caller passes freshIdx instead of letting this helper roll its
// own: a single table's FKs may share child columns (e.g. diamond's
// projects.org_id is referenced by both depts and teams FKs). Independent
// per-FK rolls would split the shared column across two parent rows; the
// caller picks one freshIdx per table and uses it across all that table's
// FKs to keep the row consistent.
func pickParentIdx(
	e FKEdge, pools *Pools, pkIdx int, pkSet map[string]bool, freshIdx int, repointFKs bool,
) (int, fkSourceMode, error) {
	if pools == nil {
		return 0, fkSourceEmitted, nil
	}
	overlapsPK := false
	for _, c := range e.ReferencingColumns {
		if pkSet[c] {
			overlapsPK = true
			break
		}
	}
	// Decide the parent pool: FKs to a PRIMARY KEY land in pools.PKs; FKs to
	// a non-PK UC land in pools.UCs. The schema-discover path stamps the
	// referenced constraint name on every FK, which is what we key on.
	parentTbl := e.ReferencedTable
	if pkTuples, isPK := pools.PKs[parentTbl]; isPK && fkTargetsPrimary(e, pools) {
		size := len(pkTuples)
		if size == 0 {
			return 0, fkSourcePoolPK, errors.AssertionFailedf("empty PK pool for %s", parentTbl)
		}
		if overlapsPK || !repointFKs {
			return pkIdx, fkSourcePoolPK, nil
		}
		return freshIdx % size, fkSourcePoolPK, nil
	}
	ucTuples, ok := pools.UCs[e.ReferencedConstraint]
	if !ok {
		// Pool wasn't built for this UC (shouldn't happen for in-sub edges,
		// but tolerate by falling back to emitted-row resolution).
		return 0, fkSourceEmitted, nil
	}
	if len(ucTuples) == 0 {
		return 0, fkSourcePoolUC, errors.AssertionFailedf(
			"empty UC pool for %s", e.ReferencedConstraint,
		)
	}
	if overlapsPK || !repointFKs {
		return pkIdx, fkSourcePoolUC, nil
	}
	return freshIdx % len(ucTuples), fkSourcePoolUC, nil
}

// fkTargetsPrimary reports whether FK e references the parent's primary key
// (as opposed to a non-PK unique constraint). Determined by checking whether
// the referenced constraint name appears in pools.UCs (non-PK) or not.
func fkTargetsPrimary(e FKEdge, pools *Pools) bool {
	_, isUC := pools.UCs[e.ReferencedConstraint]
	return !isUC
}

// fkParentValues returns the parent's values for FK e, in the order of
// e.ReferencedColumns. The source depends on mode: poolPK reads from
// pools.PKs[parent][parentIdx]; poolUC reads from pools.UCs[ucName][parentIdx];
// emitted reads from the parent's row already written in this transaction.
//
// Returns (nil, nil) when the parent isn't available (e.g. the FK's parent
// table is not in the sub-DAG — sub.Tables lookup misses).
func fkParentValues(
	sub *FKGraph, e FKEdge, mode fkSourceMode, parentIdx int, pools *Pools, emitted emittedSet,
) ([]interface{}, error) {
	switch mode {
	case fkSourcePoolPK:
		// Read the parent's PK values via PKValuesFor so the propagation of
		// PK-overlapping FK columns through the parent (and its parents,
		// transitively) is applied. Reading raw pools.PKs[parent][parentIdx]
		// would return uncoordinated pool values that don't match the row
		// actually written to the parent table.
		parentTbl, ok := sub.Tables[e.ReferencedTable]
		if !ok {
			return nil, nil
		}
		parentPK, err := PKValuesFor(parentTbl, sub, pools, parentIdx)
		if err != nil {
			return nil, err
		}
		// Map the FK's referenced columns onto positions in the parent's PK
		// tuple. FKs may reference a subset of the parent's PK columns or in
		// a different order (composite-key schemas), so look up by name.
		pkCols := primaryKeyColumns(parentTbl)
		pkPos := make(map[string]int, len(pkCols))
		for i, c := range pkCols {
			pkPos[c] = i
		}
		out := make([]interface{}, len(e.ReferencedColumns))
		for i, refCol := range e.ReferencedColumns {
			pos, ok := pkPos[refCol]
			if !ok {
				return nil, errors.AssertionFailedf(
					"FK %s referenced column %s.%s not in parent PK",
					e.Name, e.ReferencedTable, refCol,
				)
			}
			out[i] = parentPK[pos]
		}
		return out, nil
	case fkSourcePoolUC:
		tuples, ok := pools.UCs[e.ReferencedConstraint]
		if !ok {
			return nil, errors.AssertionFailedf("no UC pool for %s", e.ReferencedConstraint)
		}
		// UC pool tuples are stored in UC column order, which matches
		// e.ReferencedColumns by construction (DiscoverSchema stamps
		// ReferencedColumns from the parent UC's column list).
		tuple := tuples[parentIdx]
		out := make([]interface{}, len(tuple))
		copy(out, tuple)
		return out, nil
	case fkSourceEmitted:
		parentRow, ok := emitted[e.ReferencedTable]
		if !ok {
			return nil, errors.AssertionFailedf(
				"FK %s parent %s has not been emitted", e.Name, e.ReferencedTable,
			)
		}
		out := make([]interface{}, len(e.ReferencedColumns))
		for i, refCol := range e.ReferencedColumns {
			v, ok := parentRow[refCol]
			if !ok {
				return nil, errors.AssertionFailedf(
					"parent column %s.%s missing from emitted row", e.ReferencedTable, refCol,
				)
			}
			out[i] = v
		}
		return out, nil
	default:
		return nil, errors.AssertionFailedf("unknown fk source mode %d", mode)
	}
}

// ucColumnValues returns, for each non-PK UC column on t that is not also a
// PK column, a value sampled from a fresh ucIdx into pools.UCs. Returns an
// empty map when pools is nil — the caller falls back to randomColumnValue
// for these columns.
//
// PK-column overlap is excluded so PK columns aren't overwritten with
// arbitrary UC values: PK columns are always sourced from pools.PKs[T][pkIdx]
// (or from FK propagation when overlapping).
func ucColumnValues(
	rng *rand.Rand, t *Table, pools *Pools, pkSet map[string]bool,
) map[string]interface{} {
	if pools == nil {
		return nil
	}
	out := make(map[string]interface{})
	for _, uc := range t.UniqueConstraints {
		if uc.IsPrimary {
			continue
		}
		tuples, ok := pools.UCs[uc.Name]
		if !ok || len(tuples) == 0 {
			continue
		}
		ucIdx := rng.Intn(len(tuples))
		for i, c := range uc.Columns {
			if pkSet[c] {
				continue
			}
			out[c] = tuples[ucIdx][i]
		}
	}
	return out
}

// randomColumnValue produces a Go-SQL value for col via randgen.RandDatum. If
// the column type was not discovered (e.g. user-defined type), the column is
// treated as opaque and NULL is returned — the workload tolerates this rather
// than failing on unknown types.
func randomColumnValue(rng *rand.Rand, col Column) (interface{}, error) {
	if col.Type == nil {
		return nil, nil
	}
	d := randgen.RandDatum(rng, col.Type, false /* nullOk */)
	return randworkload.DatumToGoSQL(d)
}

// isSelfRefColumn reports whether col is part of a self-referential FK on t.
// Self-ref FK columns are written as NULL initially because the row they
// would point to does not yet exist.
func isSelfRefColumn(t *Table, col string) bool {
	for _, e := range t.OutboundFKs {
		if e.ReferencingTable != e.ReferencedTable {
			continue
		}
		for _, c := range e.ReferencingColumns {
			if c == col {
				return true
			}
		}
	}
	return false
}

// primaryKeyColumns returns the column names that form t's primary key, or
// nil if t has no PK (which is unexpected for tables we operate on).
func primaryKeyColumns(t *Table) []string {
	for _, uc := range t.UniqueConstraints {
		if uc.IsPrimary {
			return uc.Columns
		}
	}
	return nil
}

// execUpsertRow executes an UPSERT INTO t with the columns and values in row.
// Column order is taken from t.Columns so the SQL is deterministic.
func execUpsertRow(ctx context.Context, tx dbTx, t *Table, row emittedRow) error {
	cols := make([]string, 0, len(t.Columns))
	args := make([]interface{}, 0, len(t.Columns))
	for _, col := range t.Columns {
		v, ok := row[col.Name]
		if !ok {
			// A column not in the row means we couldn't generate a value for
			// it (e.g. unknown type). Skip it and let the table default fill in.
			continue
		}
		cols = append(cols, tree.NameString(col.Name))
		args = append(args, v)
	}
	if len(cols) == 0 {
		return nil
	}
	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	stmt := fmt.Sprintf(
		"UPSERT INTO %s (%s) VALUES (%s)",
		tree.NameString(t.Name),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := tx.ExecContext(ctx, stmt, args...)
	return err
}

// ExecuteDelete deletes one row per table in the sub-DAG, identified by the
// row PKs derived from pools at the chain's pkIdx. The walk locks rows in
// topological order (parents first) with SELECT ... FOR UPDATE, then deletes
// in reverse order (leaves first) so child rows are gone before their parents.
//
// Returns (false, nil) and rolls nothing back if any target row is missing
// — typically because another worker in the same shard already deleted it.
// The caller is expected to commit the transaction either way; the partial
// SELECT FOR UPDATE locks acquired so far are released on commit.
func ExecuteDelete(
	ctx context.Context, tx dbTx, sorted []*Table, sub *FKGraph, pools *Pools, pkIdx int,
) (deleted bool, err error) {
	pkVals := make(map[string][]interface{}, len(sorted))
	for _, t := range sorted {
		vals, err := PKValuesFor(t, sub, pools, pkIdx)
		if err != nil {
			return false, errors.Wrapf(err, "resolving PK for %s", t.Name)
		}
		pkVals[t.Name] = vals
	}

	for _, t := range sorted {
		found, err := lockRow(ctx, tx, t, pkVals[t.Name])
		if err != nil {
			return false, errors.Wrapf(err, "locking row in %s", t.Name)
		}
		if !found {
			return false, nil
		}
	}

	for i := len(sorted) - 1; i >= 0; i-- {
		t := sorted[i]
		if err := deleteRow(ctx, tx, t, pkVals[t.Name]); err != nil {
			return false, errors.Wrapf(err, "deleting row from %s", t.Name)
		}
	}
	return true, nil
}

// lockRow runs SELECT ... FOR UPDATE on t's PK and returns whether a row
// matched. The selected columns are unused — only the lock matters.
func lockRow(ctx context.Context, tx dbTx, t *Table, pkVals []interface{}) (bool, error) {
	pkCols := primaryKeyColumns(t)
	if len(pkVals) != len(pkCols) {
		return false, errors.AssertionFailedf(
			"table %s expects %d PK values, got %d", t.Name, len(pkCols), len(pkVals),
		)
	}
	whereClauses := make([]string, len(pkCols))
	for i, c := range pkCols {
		whereClauses[i] = fmt.Sprintf("%s = $%d", tree.NameString(c), i+1)
	}
	stmt := fmt.Sprintf(
		"SELECT 1 FROM %s WHERE %s FOR UPDATE",
		tree.NameString(t.Name),
		strings.Join(whereClauses, " AND "),
	)
	rows, err := tx.QueryContext(ctx, stmt, pkVals...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), rows.Err()
}

// deleteRow runs DELETE FROM t WHERE pk = ... using pkVals. Missing rows are
// not an error — we may have raced with another worker between the lock walk
// and the delete walk. (The lock walk's FOR UPDATE makes this race rare but
// not impossible if the txn was retried.)
func deleteRow(ctx context.Context, tx dbTx, t *Table, pkVals []interface{}) error {
	pkCols := primaryKeyColumns(t)
	whereClauses := make([]string, len(pkCols))
	for i, c := range pkCols {
		whereClauses[i] = fmt.Sprintf("%s = $%d", tree.NameString(c), i+1)
	}
	stmt := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		tree.NameString(t.Name),
		strings.Join(whereClauses, " AND "),
	)
	_, err := tx.ExecContext(ctx, stmt, pkVals...)
	return err
}

// patchDroppedEdges issues UPDATE statements to fill in FK columns that were
// written as NULL during the topological walk because their parent had not
// yet been visited (cycle-breaking edges from RandomSubDAG). Both sides must
// be present in emitted; otherwise the edge is left as NULL.
func patchDroppedEdges(
	ctx context.Context, tx dbTx, sorted []*Table, droppedEdges []FKEdge, emitted emittedSet,
) error {
	if len(droppedEdges) == 0 {
		return nil
	}
	pkByTable := make(map[string][]string, len(sorted))
	for _, t := range sorted {
		pkByTable[t.Name] = primaryKeyColumns(t)
	}

	for _, e := range droppedEdges {
		childRow, ok := emitted[e.ReferencingTable]
		if !ok {
			continue
		}
		parentRow, ok := emitted[e.ReferencedTable]
		if !ok {
			continue
		}
		if err := execPatch(ctx, tx, e, childRow, parentRow, pkByTable[e.ReferencingTable]); err != nil {
			return errors.Wrapf(err, "patching FK %s", e.Name)
		}
	}
	return nil
}

// execPatch runs an UPDATE that sets the FK columns on the child row to the
// parent's referenced columns, identifying the child by its PK.
func execPatch(
	ctx context.Context, tx dbTx, e FKEdge, childRow, parentRow emittedRow, childPKCols []string,
) error {
	if len(childPKCols) == 0 {
		return errors.AssertionFailedf("table %s has no primary key", e.ReferencingTable)
	}

	setClauses := make([]string, 0, len(e.ReferencingColumns))
	args := make([]interface{}, 0, len(e.ReferencingColumns)+len(childPKCols))
	argIdx := 1
	for i, refCol := range e.ReferencingColumns {
		parentVal, ok := parentRow[e.ReferencedColumns[i]]
		if !ok {
			return errors.AssertionFailedf(
				"parent column %s missing from emitted row", e.ReferencedColumns[i],
			)
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", tree.NameString(refCol), argIdx))
		args = append(args, parentVal)
		argIdx++
	}

	whereClauses := make([]string, 0, len(childPKCols))
	for _, pkCol := range childPKCols {
		pkVal, ok := childRow[pkCol]
		if !ok {
			return errors.AssertionFailedf(
				"PK column %s missing from emitted row for %s", pkCol, e.ReferencingTable,
			)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", tree.NameString(pkCol), argIdx))
		args = append(args, pkVal)
		argIdx++
	}

	stmt := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		tree.NameString(e.ReferencingTable),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	_, err := tx.ExecContext(ctx, stmt, args...)
	return err
}
