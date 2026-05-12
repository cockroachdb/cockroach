// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

// generateSchema produces numTables random CREATE TABLE statements wired with
// random FK constraints, returning two parallel results: the workload.Table
// entries that init applies via CREATE TABLE, and the ALTER TABLE FK
// statements to apply afterward (collected for the PostLoad hook).
//
// FKs are added by fkConflictMutator, a per-pair-probability mutator that
// always emits NO ACTION. fkDensity is the per-ordered-pair probability of
// attempting an FK; with N tables there are N*(N-1) ordered pairs (self-refs
// excluded), so E[F] = N*(N-1)*fkDensity. Stock randgen.ForeignKeyMutator
// uses a geometric outer loop that produces ~1 FK per call regardless of
// table count, which leaves most tables in singleton FKGraphs after sub-DAG
// trimming.
//
// LDR-targeted constraints applied during generation:
//   - WithSkipColumnFamilyMutations: LDR rejects tables with multiple
//     column families.
//   - WithColumnFilter (ldrSafeColumn): drops columns of types LDR refuses
//     to replicate (e.g. RefCursor).
//   - WithPrimaryIndexFilter (ldrSafePK): rejects PKs containing
//     composite-encoded columns (FLOAT/DECIMAL/JSON/COLLATEDSTRING/...) or
//     virtual computed columns. LDR refuses both shapes.
//
// generateSchema returns ok=false when any generated table's primary key
// contains a computed column. PK-as-computed breaks the workload's
// client-side PK assignment and FK propagation; the caller (ensureSchema)
// retries with a fresh seed. Non-PK computed columns are accepted — the txn
// generator skips them in writes and reads back FK-referenced ones after
// parent UPSERTs.
func generateSchema(
	seed int64, numTables int, fkDensity float64,
) (tables []workload.Table, fkStmts []tree.Statement, ok bool) {
	rng := rand.New(rand.NewSource(seed))
	stmts := randgen.RandCreateTables(
		context.Background(),
		rng,
		"t",
		numTables,
		[]randgen.TableOption{
			randgen.WithPrimaryIndexRequired(),
			randgen.WithSkipColumnFamilyMutations(),
			randgen.WithColumnFilter(ldrSafeColumn),
			randgen.WithPrimaryIndexFilter(ldrSafePK),
			randgen.WithIndexFilter(ComputedColumnFilter),
		},
		fkConflictMutator{density: fkDensity},
	)

	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *tree.CreateTable:
			if hasComputedPKColumn(s) {
				return nil, nil, false
			}
			fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
			s.FormatBody(fmtCtx)
			tables = append(tables, workload.Table{
				Name:   string(s.Table.ObjectName),
				Schema: fmtCtx.CloseAndGetString(),
			})
		case *tree.AlterTable:
			fkStmts = append(fkStmts, s)
		}
	}
	return tables, fkStmts, true
}

// ldrSafePK returns false if the candidate primary index contains a column
// LDR can't accept as a PK column:
//   - Composite-encoded types (FLOAT, DECIMAL, JSON, COLLATEDSTRING, ...).
//   - Virtual computed columns.
//
// LDR rejects either shape with "table T has a primary key column ... with
// composite encoding" or "table T has a virtual computed column ... that
// appears in the primary key".
func ldrSafePK(_ *tree.IndexTableDef, columns []*tree.ColumnTableDef) bool {
	for _, c := range columns {
		if c.Computed.Virtual {
			return false
		}
		typ, ok := tree.GetStaticallyKnownType(c.Type)
		if !ok {
			return false
		}
		if colinfo.CanHaveCompositeKeyEncoding(typ) {
			return false
		}
	}
	return true
}

// ComputedColumnFilter rejects unique indexes whose key references a
// computed column, either explicitly or via an expression key (which creates
// a hidden virtual computed column). LDR's lock synthesis hashes datums from
// the rangefeed-decoded row, which does not include computed columns —
// hashing them yields a zero hash that collides arbitrarily, breaking
// dependency tracking. Non-unique indexes don't participate in lock
// synthesis, so they're left alone.
func ComputedColumnFilter(def tree.TableDef, columns []*tree.ColumnTableDef) bool {
	uc, ok := def.(*tree.UniqueConstraintTableDef)
	if !ok {
		return true
	}
	computed := make(map[tree.Name]bool, len(columns))
	for _, c := range columns {
		if c.Computed.Computed {
			computed[c.Name] = true
		}
	}
	for _, elem := range uc.Columns {
		if elem.Expr != nil {
			return false
		}
		if computed[elem.Column] {
			return false
		}
	}
	return true
}

// ldrSafeColumn returns false for column types LDR refuses to replicate at
// all. RefCursor is the known offender today; add others here as we hit them.
//
// Computed columns are intentionally NOT filtered here: randgen falls back to
// generating computed columns whenever the column-filter rejects a candidate,
// so rejecting them here would loop forever. Tables containing any computed
// column are dropped after generation by hasComputedColumn instead.
func ldrSafeColumn(c *tree.ColumnTableDef) bool {
	typ, ok := tree.GetStaticallyKnownType(c.Type)
	if !ok {
		return true
	}
	switch typ.Family() {
	case types.RefCursorFamily:
		return false
	}
	return true
}

// hasComputedPKColumn reports whether the CREATE TABLE statement's primary
// key contains any computed column. The workload pre-assigns PK values
// client-side (AssignPKs) so it can coordinate diamond-shaped FK chains; a
// computed PK column has no client-known value and breaks that flow.
//
// Non-PK computed columns are fine: the txn generator skips them in writes,
// and FK-referenced computed UC columns get read back after the parent
// UPSERT (see readBackComputedFKTargets).
func hasComputedPKColumn(ct *tree.CreateTable) bool {
	pkCols := make(map[tree.Name]bool)
	for _, def := range ct.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.PrimaryKey.IsPrimaryKey {
				pkCols[d.Name] = true
			}
		case *tree.UniqueConstraintTableDef:
			if d.PrimaryKey {
				for _, c := range d.Columns {
					pkCols[c.Column] = true
				}
			}
		}
	}
	for _, def := range ct.Defs {
		c, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		if c.Computed.Computed && pkCols[c.Name] {
			return true
		}
	}
	return false
}

// fkConflictMutator adds FK constraints to a list of CREATE TABLE statements.
// It mirrors stock randgen.ForeignKeyMutator's column-matching and cycle
// detection but replaces the geometric outer loop with an explicit per-pair
// probability so callers can control coverage.
//
// Compared to the stock mutator:
//   - Per ordered (child, parent) pair, attempt one FK with probability
//     density. Stock uses `for rng.Intn(2) == 0` which is geometric in the
//     number of FKs, independent of table count.
//   - Always emits NO ACTION on both ON DELETE / ON UPDATE. The workload's
//     transaction generator drives explicit child-first deletes and parent-
//     first inserts and does not branch on reference action type; emitting
//     CASCADE / SET NULL / SET DEFAULT would silently violate that
//     invariant.
//   - Cycles across multiple tables are allowed; the workload's runtime
//     breaks them.
type fkConflictMutator struct {
	density float64
}

// Mutate implements randgen.Mutator.
func (m fkConflictMutator) Mutate(
	rng *rand.Rand, stmts []tree.Statement,
) (mutated []tree.Statement, changed bool) {
	// Index tables by name; collect their non-virtual columns. Same shape as
	// stock foreignKeyMutator so the column-matching logic below is a direct
	// port.
	var cols [][]*tree.ColumnTableDef
	var tableNames []tree.TableName
	tableNameToColIdx := func(t tree.TableName) (int, bool) {
		for i, name := range tableNames {
			if t == name {
				return i, true
			}
		}
		return 0, false
	}
	byName := map[tree.TableName]*tree.CreateTable{}
	usedCols := map[tree.TableName]map[tree.Name]bool{}

	var tables []*tree.CreateTable
	for _, stmt := range stmts {
		table, ok := stmt.(*tree.CreateTable)
		if !ok {
			continue
		}
		// Skip partitioned tables; stock mutator skips them too because FK
		// constraints + partitioning don't yield a usable filter.
		var skip bool
		for _, def := range table.Defs {
			switch def := def.(type) {
			case *tree.IndexTableDef:
				if def.PartitionByIndex != nil {
					skip = true
				}
			case *tree.UniqueConstraintTableDef:
				if def.IndexTableDef.PartitionByIndex != nil {
					skip = true
				}
			}
		}
		if skip {
			continue
		}
		tables = append(tables, table)
		byName[table.Table] = table
		usedCols[table.Table] = map[tree.Name]bool{}
		idx := len(cols)
		cols = append(cols, nil)
		tableNames = append(tableNames, table.Table)
		for _, def := range table.Defs {
			c, ok := def.(*tree.ColumnTableDef)
			if !ok {
				continue
			}
			if c.Computed.Virtual {
				// Stock mutator skips virtual columns (#59671).
				continue
			}
			cols[idx] = append(cols[idx], c)
		}
	}
	if len(tables) == 0 {
		return stmts, false
	}

	// For each ordered (child, parent) pair (excluding self-pairs), attempt
	// one FK with probability density. Stock mutator's `for rng.Intn(2) == 0`
	// produces ~1 FK total regardless of table count; this loop scales with
	// N*(N-1) pairs and gives the caller direct coverage control. Cycles
	// across multiple tables are allowed — the workload's runtime breaks
	// them.
	// TODO(darrylwong): allow self refs as well
	for ci, child := range tables {
		for pi, parent := range tables {
			if ci == pi {
				continue
			}
			if rng.Float64() >= m.density {
				continue
			}
			if m.tryAddFK(rng, child, parent, cols, tableNameToColIdx, usedCols, byName, &stmts) {
				changed = true
			}
		}
	}
	return stmts, changed
}

// tryAddFK attempts to add one FK from child to parent. Returns true if an
// edge was added. Mirrors the column-matching logic in stock
// foreignKeyMutator but skips the cycle check — the workload's runtime can
// break cycles itself.
func (m fkConflictMutator) tryAddFK(
	rng *rand.Rand,
	child, parent *tree.CreateTable,
	cols [][]*tree.ColumnTableDef,
	tableNameToColIdx func(tree.TableName) (int, bool),
	usedCols map[tree.TableName]map[tree.Name]bool,
	byName map[tree.TableName]*tree.CreateTable,
	stmts *[]tree.Statement,
) bool {
	childIdx, _ := tableNameToColIdx(child.Table)
	parentIdx, _ := tableNameToColIdx(parent.Table)

	// Build the candidate FK column list for child: all non-virtual columns
	// not already used by another FK on this table.
	var fkCols []*tree.ColumnTableDef
	for _, c := range cols[childIdx] {
		if usedCols[child.Table][c.Name] {
			continue
		}
		fkCols = append(fkCols, c)
	}
	if len(fkCols) == 0 {
		return false
	}
	rng.Shuffle(len(fkCols), func(i, j int) { fkCols[i], fkCols[j] = fkCols[j], fkCols[i] })

	// Geometric short-prefix: 50% one column, 25% two, etc. Same as stock.
	prefixLen := 1
	for len(fkCols) > prefixLen && rng.Intn(2) == 0 {
		prefixLen++
	}
	fkCols = fkCols[:prefixLen]

	if len(cols[parentIdx]) < len(fkCols) {
		return false
	}

	// Match each fkCol to a type-identical parent column. Stock uses
	// Type.Equivalent which is family-only and pairs INT2↔INT8 / FLOAT4↔FLOAT8
	// / etc. The workload then propagates the parent's wider value into the
	// child column unchanged and the driver-side encode step rejects it. Use
	// Identical for an exact width/locale match instead.
	availCols := append([]*tree.ColumnTableDef(nil), cols[parentIdx]...)
	var usingCols []*tree.ColumnTableDef
	for _, fkCol := range fkCols {
		fkColType := tree.MustBeStaticallyKnownType(fkCol.Type)
		found := false
		for refI, refCol := range availCols {
			if refCol.Computed.Virtual {
				continue
			}
			refColType := tree.MustBeStaticallyKnownType(refCol.Type)
			if fkColType.Identical(refColType) && colinfo.ColumnTypeIsIndexable(refColType) {
				usingCols = append(usingCols, refCol)
				availCols = append(availCols[:refI], availCols[refI+1:]...)
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Append a UNIQUE constraint to the parent on the referenced columns so
	// the FK has something to reference. Stock mutator does the same — even
	// when one already exists, the duplicate is harmless (#unneeded-unique).
	refColumns := make(tree.IndexElemList, len(usingCols))
	names := make(tree.NameList, len(usingCols))
	for i, c := range usingCols {
		refColumns[i].Column = c.Name
		names[i] = c.Name
	}
	parent.Defs = append(parent.Defs, &tree.UniqueConstraintTableDef{
		IndexTableDef: tree.IndexTableDef{Columns: refColumns},
	})

	fromNames := make(tree.NameList, len(fkCols))
	for i, c := range fkCols {
		fromNames[i] = c.Name
		usedCols[child.Table][c.Name] = true
	}

	*stmts = append(*stmts, &tree.AlterTable{
		Table: child.Table.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{&tree.AlterTableAddConstraint{
			ConstraintDef: &tree.ForeignKeyConstraintTableDef{
				Table:    byName[parent.Table].Table,
				FromCols: fromNames,
				ToCols:   names,
				Actions:  tree.ReferenceActions{},
				Match:    tree.MatchSimple,
			},
		}},
	})
	return true
}
