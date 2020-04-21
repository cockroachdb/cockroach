// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// lockingSpec maintains a collection of FOR [KEY] UPDATE/SHARE items that apply
// to a given scope. Locking clauses can be applied to the lockingSpec as they
// come into scope in the AST. The lockingSpec can then be consolidated down to
// a single row-level locking specification for different tables to determine
// how scans over those tables should perform row-level locking, if at all.
//
// A SELECT statement may contain zero, one, or more than one row-level locking
// clause. Each of these clauses consist of two different properties.
//
// The first property is locking strength (see tree.LockingStrength). Locking
// strength represents the degree of protection that a row-level lock provides.
// The stronger the lock, the more protection it provides for the lock holder
// but the more restrictive it is to concurrent transactions attempting to
// access the same row. In order from weakest to strongest, the lock strength
// variants are:
//
//   FOR KEY SHARE
//   FOR SHARE
//   FOR NO KEY UPDATE
//   FOR UPDATE
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
//   SKIP LOCKED
//   NOWAIT
//
// In addition to these two properties, locking clauses can contain an optional
// list of target relations. When provided, the locking clause applies only to
// those relations in the target list. When not provided, the locking clause
// applies to all relations in the current scope.
//
// Put together, a complex locking spec might look like:
//
//   SELECT ... FROM ... FOR SHARE NOWAIT FOR UPDATE OF t1, t2
//
// which would be represented as:
//
//   [ {ForShare, LockWaitError, []}, {ForUpdate, LockWaitBlock, [t1, t2]} ]
//
type lockingSpec []*tree.LockingItem

// noRowLocking indicates that no row-level locking has been specified.
var noRowLocking lockingSpec

// isSet returns whether the spec contains any row-level locking modes.
func (lm lockingSpec) isSet() bool {
	return len(lm) != 0
}

// get returns the first row-level locking mode in the spec. If the spec was the
// outcome of filter operation, this will be the only locking mode in the spec.
func (lm lockingSpec) get() *tree.LockingItem {
	if lm.isSet() {
		return lm[0]
	}
	return nil
}

// apply merges the locking clause into the current locking spec. The effect of
// applying new locking clauses to an existing spec is always to strengthen the
// locking approaches it represents, either through increasing locking strength
// or using more aggressive wait policies.
func (lm *lockingSpec) apply(locking tree.LockingClause) {
	// TODO(nvanbenschoten): If we wanted to eagerly prune superfluous locking
	// items so that they don't need to get merged away in each call to filter,
	// this would be the place to do it. We don't expect to see multiple FOR
	// UPDATE clauses very often, so it's probably not worth it.
	if len(*lm) == 0 {
		// NB: avoid allocation, but also prevent future mutation of AST.
		l := len(locking)
		*lm = lockingSpec(locking[:l:l])
		return
	}
	*lm = append(*lm, locking...)
}

// filter returns the desired row-level locking mode for the specified table as
// a new consolidated lockingSpec. If no matching locking mode is found then the
// resulting spec will remain un-set. If a matching locking mode for the table
// is found then the resulting spec will contain exclusively that locking mode
// and will no longer be restricted to specific target relations.
func (lm lockingSpec) filter(alias tree.Name) lockingSpec {
	var ret lockingSpec
	var copied bool
	updateRet := func(li *tree.LockingItem, len1 []*tree.LockingItem) {
		if ret == nil && len(li.Targets) == 0 {
			// Fast-path. We don't want the resulting spec to include targets,
			// so we only allow this if the item we want to copy has none.
			ret = len1
			return
		}
		if !copied {
			retCpy := make(lockingSpec, 1)
			retCpy[0] = new(tree.LockingItem)
			if len(ret) == 1 {
				*retCpy[0] = *ret[0]
			}
			ret = retCpy
			copied = true
		}
		// From https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
		// > If the same table is mentioned (or implicitly affected) by more
		// > than one locking clause, then it is processed as if it was only
		// > specified by the strongest one.
		ret[0].Strength = ret[0].Strength.Max(li.Strength)
		// > Similarly, a table is processed as NOWAIT if that is specified in
		// > any of the clauses affecting it. Otherwise, it is processed as SKIP
		// > LOCKED if that is specified in any of the clauses affecting it.
		ret[0].WaitPolicy = ret[0].WaitPolicy.Max(li.WaitPolicy)
	}

	for i, li := range lm {
		len1 := lm[i : i+1 : i+1]
		if len(li.Targets) == 0 {
			// If no targets are specified, the clause affects all tables.
			updateRet(li, len1)
		} else {
			// If targets are specified, the clause affects only those tables.
			for _, target := range li.Targets {
				if target.ObjectName == alias {
					updateRet(li, len1)
					break
				}
			}
		}
	}
	return ret
}

// withoutTargets returns a new lockingSpec with all locking clauses that apply
// only to a subset of tables removed.
func (lm lockingSpec) withoutTargets() lockingSpec {
	return lm.filter("")
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
