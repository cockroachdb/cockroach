// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// lockingItem represents a single FOR UPDATE / FOR SHARE item in a locking
// clause (perhaps with multiple targets). It wraps a tree.LockingItem with
// extra information needed for semantic analysis and plan building.
//
// A locking item specifies several locking properties.
//
// The first property is locking strength (see tree.LockingStrength). Locking
// strength represents the degree of protection that a row-level lock provides.
// The stronger the lock, the more protection it provides for the lock holder
// but the more restrictive it is to concurrent transactions attempting to
// access the same row. In order from weakest to strongest, the lock strength
// variants are:
//
//	FOR KEY SHARE
//	FOR SHARE
//	FOR NO KEY UPDATE
//	FOR UPDATE
//
// The second property is the locking wait policy (see tree.LockingWaitPolicy).
// A locking wait policy represents the policy a table scan uses to interact
// with row-level locks held by other transactions. Unlike locking strength,
// locking wait policy is optional to specify in a locking clause. If not
// specified, the policy defaults to blocking and waiting for locks to become
// available. The non-standard policies instruct scans to take other approaches
// to handling locks held by other transactions. These non-standard policies
// are:
//
//	SKIP LOCKED
//	NOWAIT
//
// In addition to these properties, locking items can contain an optional list
// of target relations. When provided, the locking item applies only to those
// relations in the target list. When not provided, the locking item applies to
// all relations in the current scope.
//
// Locking clauses consist of multiple locking items.
//
// For example, a complex locking clause might look like:
//
//	SELECT ... FROM ... FOR SHARE NOWAIT FOR UPDATE OF t1, t2
//
// which would be represented as two locking items:
//
//	[ {ForShare, LockWaitError, []}, {ForUpdate, LockWaitBlock, [t1, t2]} ]
type lockingItem struct {
	item *tree.LockingItem

	// targetsFound is used to validate that we matched all of the lock targets.
	targetsFound intsets.Fast

	// builders has one lockBuilder for each data source that matched this
	// item. Each lockBuilder here will become one Lock operator in the plan.
	builders []*lockBuilder
}

// lockBuilder is a helper for building Lock operators for a single data
// source. It keeps track of the PK columns of the table. The same lockBuilder
// may be referenced by multiple lockingItems.
type lockBuilder struct {
	table   opt.TableID
	keyCols opt.ColList
}

// newLockBuilder constructs a lockBuilder for the passed table.
func newLockBuilder(tabMeta *opt.TableMeta) *lockBuilder {
	primaryIndex := tabMeta.Table.Index(cat.PrimaryIndex)
	lb := &lockBuilder{
		table:   tabMeta.MetaID,
		keyCols: make(opt.ColList, primaryIndex.KeyColumnCount()),
	}
	for i := range lb.keyCols {
		lb.keyCols[i] = lb.table.IndexColumnID(primaryIndex, i)
	}
	return lb
}

// lockingSpec maintains a collection of FOR [KEY] UPDATE/SHARE items that apply
// to the current scope. Locking items can apply as they come into scope in the
// AST, or as data sources match locking targets within FROM lists.
//
// For example, for a statement like:
//
//	SELECT * FROM a, (SELECT * FROM b, c FOR SHARE NOWAIT FOR UPDATE OF c) FOR SHARE
//
// while building each scan, the lockingSpec would look different:
//
//   - while building a it would be:
//     [{ForShare, LockWaitBlock, []}]
//
//   - while building b it would be:
//     [{ForShare, LockWaitBlock, []}, {ForShare, LockWaitError, []}]
//
//   - while building c it would be:
//     [{ForShare, LockWaitBlock, []}, {ForShare, LockWaitError, []}, {ForUpdate, LockWaitBlock, [c]}]
type lockingSpec []*lockingItem

// noRowLocking indicates that no row-level locking applies to the current
// scope.
var noRowLocking lockingSpec

// isSet returns whether the spec contains any row-level locking modes.
func (lm lockingSpec) isSet() bool {
	return len(lm) != 0
}

// get returns the combined row-level locking mode from all currently-applied
// locking items.
func (lm lockingSpec) get() opt.Locking {
	var l opt.Locking
	for _, li := range lm {
		spec := li.item
		l = l.Max(opt.Locking{
			Strength:   spec.Strength,
			WaitPolicy: spec.WaitPolicy,
			Form:       spec.Form,
		})
	}
	return l
}

// lockingContext holds the locking information for the current scope. It is
// passed down into subexpressions by value so that it automatically "pops" back
// to its previous value on return.
type lockingContext struct {
	// lockScope is the stack of locking items that are currently in scope. This
	// might include locking items that do not currently apply because they have
	// an unmatched target.
	lockScope []*lockingItem

	// locking is the stack of locking items that apply to the current scope,
	// either because they did not have a target or because one of their targets
	// matched an ancestor of this scope.
	locking lockingSpec

	// isNullExtended is set to true if this lockingContext is being passed down
	// to the null-extended side of an outer join. This is needed so that we can
	// return an error if the locking is set when we are building a table scan and
	// isNullExtended is true.
	isNullExtended bool

	// safeUpdate is set to true if this lockingContext is being passed down from
	// a select statement with either a WHERE clause or a LIMIT clause. This is
	// needed so that we can return an error if we're locking without a WHERE
	// clause or LIMIT clause and sql_safe_updates is true.
	safeUpdate bool
}

// noLocking indicates that no row-level locking has been specified.
var noLocking lockingContext

// push pushes a locking item onto the scope stack, and also applies it if it
// has no targets.
func (lockCtx *lockingContext) push(li *tree.LockingItem) {
	item := &lockingItem{
		item: li,
	}
	lockCtx.lockScope = append(lockCtx.lockScope, item)
	if len(li.Targets) == 0 {
		lockCtx.locking = append(lockCtx.locking, item)
	}
}

// pop removes and returns the topmost locking item from the scope stack.
func (lockCtx *lockingContext) pop() *lockingItem {
	n := len(lockCtx.lockScope)
	if n == 0 {
		panic(errors.AssertionFailedf("tried to pop non-existent locking item"))
	}
	item := lockCtx.lockScope[n-1]
	lockCtx.lockScope = lockCtx.lockScope[:n-1]
	// For now we do not bother explicitly popping the lockingSpec stack. Instead
	// we rely on passing lockingContext by value in optbuilder, meaning
	// lockingSpec is implicitly popped when returning.
	return item
}

// blankLockingScope is a sentinel locking item that, when pushed, prevents
// lockCtx.filter from matching targets outside it.
var blankLockingScope lockingItem = lockingItem{item: &tree.LockingItem{}}

// filter applies any locking items that match the specified data source alias.
func (lockCtx *lockingContext) filter(alias tree.Name) {
	// Search backward through locking scopes to find all of the matching items
	// inside the innermost blankLockingScope. Unlike for variable scopes, for
	// locking scopes *all* of the matching items apply, not just the first. This
	// means in some cases we might apply the same locking item multiple times, if
	// it has multiple targets and they match more than once. This is fine.
	for i := len(lockCtx.lockScope) - 1; i >= 0; i-- {
		item := lockCtx.lockScope[i]
		if item == &blankLockingScope {
			break
		}
		// Only consider locking items with targets. (Locking items without targets
		// were already applied in push.)
		for i, target := range item.item.Targets {
			if target.ObjectName == alias {
				lockCtx.locking = append(lockCtx.locking, item)
				item.targetsFound.Add(i)
			}
		}
	}
}

// withoutTargets hides all unapplied locking items in scope, so that they
// cannot be applied. Already applied locking items remain applied.
func (lockCtx *lockingContext) withoutTargets() {
	lockCtx.lockScope = append(lockCtx.lockScope, &blankLockingScope)
	// Reset isNullExtended if no lock applies at this point, since we shouldn't
	// throw an error if locking is introduced lower in the plan tree.
	if !lockCtx.locking.isSet() {
		lockCtx.isNullExtended = false
	}
}

// ignoreLockingForCTE is a placeholder for the following comment:
//
// We intentionally do not propagate any row-level locking information from the
// current scope to the CTE. This mirrors Postgres' behavior. It also avoids a
// number of awkward questions like how row-level locking would interact with
// mutating common table expressions.
//
// From https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
// > these clauses do not apply to WITH queries referenced by the primary query.
// > If you want row locking to occur within a WITH query, specify a locking
// > clause within the WITH query.
func (lm lockingSpec) ignoreLockingForCTE() {}

// analyzeLockArgs analyzes all locking clauses currently in scope and adds the
// PK columns needed for those clauses to lockScope.
func (b *Builder) analyzeLockArgs(
	lockCtx lockingContext, inScope, projectionsScope *scope,
) (lockScope *scope) {
	if !b.shouldBuildLockOp() {
		return nil
	}

	// Get all the PK cols of all lockBuilders in scope.
	var pkCols opt.ColSet
	for _, item := range lockCtx.lockScope {
		for _, lb := range item.builders {
			for _, col := range lb.keyCols {
				pkCols.Add(col)
			}
		}
	}

	if pkCols.Empty() {
		return nil
	}

	lockScope = inScope.push()
	lockScope.cols = make([]scopeColumn, 0, pkCols.Len())

	// Make sure to check extra columns, since the primary key columns may not
	// have been explicitly projected.
	inScope.forEachColWithExtras(func(col *scopeColumn) {
		if pkCols.Contains(col.id) {
			lockScope.appendColumn(col)
		}
	})
	return lockScope
}

// buildLockArgs adds the PK columns needed for all locking clauses currently in
// scope to the projectionsScope.
func (b *Builder) buildLockArgs(inScope, projectionsScope, lockScope *scope) {
	if lockScope == nil {
		return
	}
	projectionsScope.addExtraColumns(lockScope.cols)
}

// validate checks that the locking item is well-formed, and that all of its
// targets matched a data source in the FROM clause.
func (item *lockingItem) validate() {
	li := item.item

	// Validate locking strength.
	switch li.Strength {
	case tree.ForNone:
		// AST nodes should not be created with this locking strength.
		panic(errors.AssertionFailedf("locking item without strength"))
	case tree.ForUpdate:
		// Exclusive locking on the entire row.
	case tree.ForNoKeyUpdate:
		// Exclusive locking on only non-key(s) of the row. Currently unimplemented
		// and treated identically to ForUpdate.
	case tree.ForShare:
		// Shared locking on the entire row.
	case tree.ForKeyShare:
		// Shared locking on only key(s) of the row. Currently unimplemented and
		// treated identically to ForShare.
	default:
		panic(errors.AssertionFailedf("unknown locking strength: %d", li.Strength))
	}

	// Validating locking wait policy.
	switch li.WaitPolicy {
	case tree.LockWaitBlock:
		// Default. Block on conflicting locks.
	case tree.LockWaitSkipLocked:
		// Skip rows that can't be locked.
	case tree.LockWaitError:
		// Raise an error on conflicting locks.
	default:
		panic(errors.AssertionFailedf("unknown locking wait policy: %d", li.WaitPolicy))
	}

	// Validate locking form.
	switch li.Form {
	case tree.LockRecord:
		// Default. Only lock existing rows.
	case tree.LockPredicate:
		// Lock both existing rows and gaps between rows.
	default:
		panic(errors.AssertionFailedf("unknown locking form: %d", li.Form))
	}

	// Validate locking targets by checking that all targets are well-formed and
	// all were found somewhere in the FROM clause.
	for i, target := range li.Targets {
		// Insist on unqualified alias names here. We could probably do
		// something smarter, but it's better to just mirror Postgres
		// exactly. See transformLockingClause in Postgres' source.
		if target.CatalogName != "" || target.SchemaName != "" {
			panic(pgerror.Newf(pgcode.Syntax,
				"%s must specify unqualified relation names", li.Strength))
		}
		// Validate that at some point we found this target.
		if !item.targetsFound.Contains(i) {
			panic(pgerror.Newf(
				pgcode.UndefinedTable,
				"relation %q in %s clause not found in FROM clause",
				target.ObjectName, li.Strength,
			))
		}
	}
}

// shouldUseGuaranteedDurability returns whether we should use
// guaranteed-durable locking for SELECT FOR UPDATE, SELECT FOR SHARE, or
// constraint checks.
func (b *Builder) shouldUseGuaranteedDurability() bool {
	return b.evalCtx.TxnIsoLevel != isolation.Serializable ||
		b.evalCtx.SessionData().DurableLockingForSerializable
}

// shouldBuildLockOp returns whether we should use the Lock operator for SELECT
// FOR UPDATE or SELECT FOR SHARE.
func (b *Builder) shouldBuildLockOp() bool {
	return b.evalCtx.TxnIsoLevel != isolation.Serializable ||
		b.evalCtx.SessionData().OptimizerUseLockOpForSerializable
}

// lockingSpecForTableScan adjusts the lockingSpec for a Scan depending on
// whether locking will be implemented by a Lock operator, and also creates
// lockBuilders as a side-effect.
func (b *Builder) lockingSpecForTableScan(locking lockingSpec, tabMeta *opt.TableMeta) lockingSpec {
	if locking.isSet() {
		lb := newLockBuilder(tabMeta)
		for _, item := range locking {
			item.builders = append(item.builders, lb)
		}
	}
	if b.shouldBuildLockOp() {
		// If we're implementing FOR UPDATE / FOR SHARE with a Lock operator on top
		// of the plan, then this can be an unlocked scan. But if the locking uses
		// SKIP LOCKED or NOWAIT then we still need this unlocked scan to use SKIP
		// LOCKED or NOWAIT behavior, respectively, even if it does not take any
		// locks itself.
		if waitPolicy := locking.get().WaitPolicy; waitPolicy != tree.LockWaitBlock &&
			// In isolation levels weaker than Serializable, unlocked scans read
			// underneath locks without blocking. For these weaker isolation levels we
			// do not strictly need unlocked scans to use SKIP LOCKED or NOWAIT
			// behavior. We keep the SKIP LOCKED behavior anyway as an optimization,
			// but avoid NOWAIT in order to prevent false positive locking errors.
			(b.evalCtx.TxnIsoLevel == isolation.Serializable ||
				waitPolicy == tree.LockWaitSkipLocked) {
			// Create a dummy lockingSpec to get just the lock wait behavior.
			locking = lockingSpec{&lockingItem{
				item: &tree.LockingItem{
					WaitPolicy: waitPolicy,
				},
			}}
		} else {
			locking = nil
		}
	}
	return locking
}

// buildLocking constructs one Lock operator for each data source that this
// lockingItem applied to.
func (b *Builder) buildLocking(item *lockingItem, inScope *scope) {
	locking := lockingSpec{item}.get()
	// Under weaker isolation levels we use fully-durable locks for SELECT FOR
	// UPDATE.
	if b.shouldUseGuaranteedDurability() {
		locking.Durability = tree.LockDurabilityGuaranteed
	}
	for i := range item.builders {
		b.buildLock(item.builders[i], locking, inScope)
	}
}

// buildLock constructs a Lock operator for a single data source at a single
// locking strength.
func (b *Builder) buildLock(lb *lockBuilder, locking opt.Locking, inScope *scope) {
	md := b.factory.Metadata()
	tab := md.Table(lb.table)
	// We need to use a fresh table reference to have control over the exact
	// column families locked.
	newTabID := md.DuplicateTable(lb.table, b.factory.RemapCols)
	newTab := md.Table(newTabID)
	// Add remapped columns for the new table reference. For now we lock all
	// non-virtual columns of all families of the primary index of the table, so
	// include all ordinary and mutation columns.
	ordinals := tableOrdinals(newTab, columnKinds{
		includeMutations: true,
		includeSystem:    false,
		includeInverted:  false,
	})
	var lockCols opt.ColSet
	for _, ord := range ordinals {
		if !tab.Column(ord).IsVirtualComputed() {
			lockCols.Add(newTabID.ColumnID(ord))
		}
	}
	private := &memo.LockPrivate{
		Table:     newTabID,
		KeySource: lb.table,
		Locking:   locking,
		KeyCols:   lb.keyCols,
		LockCols:  lockCols,
		// ExtraCols might include some of the primary key columns needed to lock the
		// row, if they weren't in the output of the SELECT FOR UPDATE.
		Cols: inScope.colSetWithExtraCols(),
	}
	// Validate that all of the PK cols are found within the input scope.
	scopeCols := private.Cols
	for _, keyCol := range private.KeyCols {
		if !scopeCols.Contains(keyCol) {
			panic(errors.AssertionFailedf("cols missing key column %d", keyCol))
		}
	}
	if private.Locking.WaitPolicy == tree.LockWaitSkipLocked && tab.FamilyCount() > 1 {
		// TODO(rytaft): We may be able to support this if enough columns are
		// pruned that only a single family is scanned.
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"SKIP LOCKED cannot be used for tables with multiple column families",
		))
	}
	inScope.expr = b.factory.ConstructLock(inScope.expr, private)
}

// lockingSpecForClause converts a lockingClause to a lockingSpec.
func lockingSpecForClause(lockingClause tree.LockingClause) (lm lockingSpec) {
	for _, li := range lockingClause {
		lm = append(lm, &lockingItem{
			item: li,
		})
	}
	return lm
}
