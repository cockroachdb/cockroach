// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// addRowLevelSecurityFilter adds a filter based on the expressions of
// applicable RLS policies. If RLS is enabled but no policies are applicable,
// all rows will be filtered out.
func (b *Builder) addRowLevelSecurityFilter(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope,
) {
	scalar, _ := b.maybeBuildRLSUsingExpr(tabMeta, tableScope, cmdScope, nil)
	if scalar != nil {
		tableScope.expr = b.factory.ConstructSelect(tableScope.expr,
			memo.FiltersExpr{b.factory.ConstructFiltersItem(scalar)})
	}
}

// maybeBuildRLSUsingExpr attempts to construct a scalar expression for
// applicable RLS USING policies. It is intended for filtering rows during
// read operations. Returns nil if the user is exempt from RLS enforcement.
func (b *Builder) maybeBuildRLSUsingExpr(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope, colRefs *opt.ColSet,
) (scalar opt.ScalarExpr, colIDs opt.ColSet) {
	if b.isExemptFromRLSPolicies(tabMeta, cmdScope) {
		return nil, opt.ColSet{}
	}
	return b.buildRowLevelSecurityUsingExpression(tabMeta, tableScope, cmdScope, colRefs)
}

// isExemptFromRLSPolicies will check if the given user is exempt from RLS policies.
func (b *Builder) isExemptFromRLSPolicies(
	tabMeta *opt.TableMeta, cmdScope cat.PolicyCommandScope,
) bool {
	if !tabMeta.Table.IsRowLevelSecurityEnabled() || cmdScope == cat.PolicyScopeExempt {
		return true
	}

	// Check for cases where users are exempt from policies.
	isAdmin, err := b.catalog.UserHasAdminRole(b.ctx, b.checkPrivilegeUser)
	if err != nil {
		panic(err)
	}
	isOwnerAndNotForced, err := b.isTableOwnerAndRLSNotForced(tabMeta)
	if err != nil {
		panic(err)
	}
	bypassRLS, err := b.catalog.UserHasGlobalPrivilegeOrRoleOption(b.ctx, privilege.BYPASSRLS, b.checkPrivilegeUser)
	if err != nil {
		panic(err)
	}
	b.factory.Metadata().SetRLSEnabled(b.checkPrivilegeUser, isAdmin, tabMeta.MetaID,
		isOwnerAndNotForced, bypassRLS)
	// Check if RLS filtering is exempt.
	if isAdmin || isOwnerAndNotForced || bypassRLS {
		return true
	}
	return false
}

// buildRowLevelSecurityUsingExpression generates a scalar expression for read
// operations by combining all applicable RLS policies. An expression is always
// returned; if no policies apply, a 'false' expression is returned.
func (b *Builder) buildRowLevelSecurityUsingExpression(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope, colRefs *opt.ColSet,
) (scalar opt.ScalarExpr, colIDs opt.ColSet) {
	combinedExpr, colIDs := b.combinePolicyUsingExpressionForCommand(tabMeta, tableScope, cmdScope)
	if combinedExpr == nil {
		// No policies, filter out all rows by adding a "false" expression.
		b.factory.Metadata().GetRLSMeta().NoPoliciesApplied = true
		return memo.FalseSingleton, opt.ColSet{}
	}
	scalar = b.buildScalar(combinedExpr, tableScope, nil, nil, colRefs)
	return scalar, colIDs
}

// combinePolicyUsingExpressionForCommand generates a `tree.TypedExpr` command
// for use in the scan. Depending on the policy command scope, it may simply
// pass through to `buildRowLevelSecurityUsingExpressionForCommand`.
// For other commands, the process is more complex and may involve multiple
// calls to that function to generate the expression.
func (b *Builder) combinePolicyUsingExpressionForCommand(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope,
) (tree.TypedExpr, opt.ColSet) {
	// For DELETE and UPDATE, we always apply SELECT/ALL policies because
	// reading the existing row is necessary before performing the write.
	switch cmdScope {
	case cat.PolicyScopeUpdate, cat.PolicyScopeDelete:
		selectExpr, selectColIDs := b.buildRowLevelSecurityUsingExpressionForCommand(tabMeta, tableScope, cat.PolicyScopeSelect)
		if selectExpr == nil {
			return selectExpr, opt.ColSet{}
		}
		updateExpr, updateColIDs := b.buildRowLevelSecurityUsingExpressionForCommand(tabMeta, tableScope, cmdScope)
		if updateExpr == nil {
			return nil, opt.ColSet{}
		}
		updateColIDs.UnionWith(selectColIDs)
		return tree.NewTypedAndExpr(selectExpr, updateExpr), updateColIDs
	default:
		return b.buildRowLevelSecurityUsingExpressionForCommand(tabMeta, tableScope, cmdScope)
	}
}

// buildRowLevelSecurityUsingExpressionForCommand will generate a tree.TypedExpr
// for all policies that apply to the given policy command scope.
func (b *Builder) buildRowLevelSecurityUsingExpressionForCommand(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope,
) (tree.TypedExpr, opt.ColSet) {
	var policiesUsed opt.PolicyIDSet
	var combinedExpr tree.TypedExpr
	var colIDs opt.ColSet
	policies := tabMeta.Table.Policies()

	// Create a closure to handle building the expression for one policy.
	buildForPolicy := func(policy cat.Policy, restrictive bool) {
		if !policy.AppliesToRole(b.checkPrivilegeUser) || !policyAppliesToCommandScope(policy, cmdScope) {
			return
		}
		strExpr := policy.UsingExpr
		if strExpr == "" {
			return
		}
		for _, id := range policy.UsingColumnIDs {
			colIDs.Add(opt.ColumnID(id))
		}
		policiesUsed.Add(policy.ID)
		parsedExpr, err := parser.ParseExpr(strExpr)
		if err != nil {
			panic(err)
		}
		typedExpr := tableScope.resolveType(parsedExpr, types.AnyElement)
		if combinedExpr != nil {
			// Restrictive policies are combined using AND, while permissive
			// policies are combined using OR.
			if restrictive {
				combinedExpr = tree.NewTypedAndExpr(combinedExpr, typedExpr)
			} else {
				combinedExpr = tree.NewTypedOrExpr(combinedExpr, typedExpr)
			}
		} else {
			combinedExpr = typedExpr
		}
	}

	for _, policy := range policies.Permissive {
		buildForPolicy(policy, false /* restrictive */)
	}
	if combinedExpr == nil {
		// No permissive policies. Return an empty expr to force the caller to generate a deny-all expression.
		return nil, colIDs
	}
	for _, policy := range policies.Restrictive {
		buildForPolicy(policy, true /* restrictive */)
	}

	// We should have already exited early if there were no permissive policies.
	if combinedExpr == nil {
		panic(errors.AssertionFailedf("at least one applicable policy should have been found"))
	}
	b.factory.Metadata().GetRLSMeta().AddPoliciesUsed(tabMeta.MetaID, policiesUsed, true /* applyFilterExpr */)
	return combinedExpr, colIDs
}

// policyAppliesToCommandScope checks whether a given PolicyCommandScope applies
// to the specified policy. It returns true if the policy is applicable and
// false otherwise.
func policyAppliesToCommandScope(policy cat.Policy, cmdScope cat.PolicyCommandScope) bool {
	if cmdScope == cat.PolicyScopeExempt {
		return true
	}
	cmd := policy.Command
	switch cmd {
	case catpb.PolicyCommand_ALL:
		return true
	case catpb.PolicyCommand_SELECT:
		return cmdScope == cat.PolicyScopeSelect
	case catpb.PolicyCommand_INSERT:
		return cmdScope == cat.PolicyScopeInsert
	case catpb.PolicyCommand_UPDATE:
		return cmdScope == cat.PolicyScopeUpdate
	case catpb.PolicyCommand_DELETE:
		return cmdScope == cat.PolicyScopeDelete
	default:
		panic(errors.AssertionFailedf("unknown policy command %v", cmd))
	}
}

// isTableOwnerAndRLSNotForced returns true iff the user is the table owner and
// the NO FORCE option is set.
func (b *Builder) isTableOwnerAndRLSNotForced(tabMeta *opt.TableMeta) (bool, error) {
	if tabMeta.Table.IsRowLevelSecurityForced() {
		return false, nil
	}
	return b.catalog.IsOwner(b.ctx, tabMeta.Table, b.checkPrivilegeUser)
}

// buildRLSCheckExpr builds the expression that will be used within the check
// constraint built for RLS. The built expression is returned along with the
// column IDs referenced in the check (indexed by external column ordinal).
func (b *Builder) buildRLSCheckExpr(
	tabMeta *opt.TableMeta,
	cmdScope cat.PolicyCommandScope,
	needsSelectPriv bool,
	fetchScope *scope,
	outScope *scope,
	canaryColID opt.ColumnID,
	referencedCols *opt.ColSet,
) (opt.ScalarExpr, opt.ColSet) {
	if b.isExemptFromRLSPolicies(tabMeta, cmdScope) {
		return memo.TrueSingleton, opt.ColSet{}
	}

	var combinedExpr opt.ScalarExpr
	var colIDs opt.ColSet
	// UPSERT is complex because the applied policies depend on whether a conflict
	// occurred. This is handled separately because of that complexity.
	if cmdScope == cat.PolicyScopeUpsert {
		combinedExpr, colIDs = b.combinePolicyWithCheckExprForUpsert(
			tabMeta, needsSelectPriv, fetchScope, outScope, canaryColID, referencedCols)
	} else {
		combinedExpr, colIDs = b.combinePolicyWithCheckExpr(tabMeta, cmdScope, needsSelectPriv, outScope, referencedCols)
	}
	// If no policies apply, then we will add a false check as nothing is allowed
	// to be written.
	if combinedExpr == nil {
		b.factory.Metadata().GetRLSMeta().NoPoliciesApplied = true
		return memo.FalseSingleton, colIDs
	}
	return combinedExpr, colIDs
}

// combinePolicyWithCheckExpr will build a combined expression depending if this
// is INSERT or UPDATE.
func (b *Builder) combinePolicyWithCheckExpr(
	tabMeta *opt.TableMeta,
	cmdScope cat.PolicyCommandScope,
	needsSelectPriv bool,
	exprScope *scope,
	referencedCols *opt.ColSet,
) (scalar opt.ScalarExpr, colIDs opt.ColSet) {
	var selExpr opt.ScalarExpr
	var selColIDs opt.ColSet
	if needsSelectPriv {
		selExpr, selColIDs = b.genPolicyUsingExprForCommand(tabMeta, cat.PolicyScopeSelect, exprScope, referencedCols)
		if selExpr == nil {
			return selExpr, selColIDs
		}
		colIDs.UnionWith(selColIDs)
	}

	writeExpr, writeColIDs := b.genPolicyWithCheckExprForCommand(tabMeta, cmdScope, exprScope, referencedCols)
	if writeExpr == nil {
		return writeExpr, writeColIDs
	}
	if selExpr == nil {
		return writeExpr, writeColIDs
	}
	colIDs.UnionWith(writeColIDs)
	return b.factory.ConstructAnd(selExpr, writeExpr), colIDs
}

// genPolicyWithCheckExprForCommand will build a WITH CHECK expression for the
// given policy command.
func (b *Builder) genPolicyWithCheckExprForCommand(
	tabMeta *opt.TableMeta,
	cmdScope cat.PolicyCommandScope,
	exprScope *scope,
	referencedCols *opt.ColSet,
) (opt.ScalarExpr, opt.ColSet) {
	return b.genCheckExprForCommand(tabMeta, cmdScope, exprScope, referencedCols, false /* onlyUsingExpr */)
}

// genPolicyUsingExprForCommand will build a USING expression for the given policy command.
func (b *Builder) genPolicyUsingExprForCommand(
	tabMeta *opt.TableMeta,
	cmdScope cat.PolicyCommandScope,
	exprScope *scope,
	referencedCols *opt.ColSet,
) (opt.ScalarExpr, opt.ColSet) {
	return b.genCheckExprForCommand(tabMeta, cmdScope, exprScope, referencedCols, true /* onlyUsingExpr */)
}

// genCheckExprForCommand will build a check expression for the given policy command.
func (b *Builder) genCheckExprForCommand(
	tabMeta *opt.TableMeta,
	cmdScope cat.PolicyCommandScope,
	exprScope *scope,
	referencedCols *opt.ColSet,
	onlyUsingExpr bool,
) (opt.ScalarExpr, opt.ColSet) {
	// colIDs tracks the column IDs referenced in all the policy expressions
	// that are applied. We use a set as we need to combine the columns used
	// for multiple policies.
	var colIDs opt.ColSet
	var scalar opt.ScalarExpr
	var policiesUsed opt.PolicyIDSet
	policies := tabMeta.Table.Policies()

	// Create a closure to handle building the expression for one policy.
	buildForPolicy := func(p cat.Policy, restrictive bool) {
		if !p.AppliesToRole(b.checkPrivilegeUser) || !policyAppliesToCommandScope(p, cmdScope) {
			return
		}
		policiesUsed.Add(p.ID)

		var expr string
		// The USING expression is used in two scenarios:
		// - When the WITH CHECK expression is not defined
		// - When the caller explicitly requests only the USING expression (e.g.,
		// during UPSERT)
		//
		// Note: If both expressions are missing, the policy does not apply and can
		// be skipped.
		if p.WithCheckExpr == "" || onlyUsingExpr {
			if p.UsingExpr == "" {
				return
			}
			expr = p.UsingExpr
			for _, id := range p.UsingColumnIDs {
				colIDs.Add(opt.ColumnID(id))
			}
		} else {
			expr = p.WithCheckExpr
			for _, id := range p.WithCheckColumnIDs {
				colIDs.Add(opt.ColumnID(id))
			}
		}
		pexpr, err := parser.ParseExpr(expr)
		if err != nil {
			panic(err)
		}
		texpr := exprScope.resolveAndRequireType(pexpr, types.Bool)
		singleExprScalar := b.buildScalar(texpr, exprScope, nil, nil, referencedCols)

		// Build up a scalar expression of all singleExprScalar's combined.
		if scalar != nil {
			if restrictive {
				scalar = b.factory.ConstructAnd(scalar, singleExprScalar)
			} else {
				scalar = b.factory.ConstructOr(scalar, singleExprScalar)
			}
		} else {
			scalar = singleExprScalar
		}
	}

	for _, policy := range policies.Permissive {
		buildForPolicy(policy, false /* restrictive */)
	}
	// If no permissive policies apply, then we will add a false check as
	// nothing is allowed to be written.
	if scalar == nil {
		b.factory.Metadata().GetRLSMeta().NoPoliciesApplied = true
		return memo.FalseSingleton, colIDs
	}
	for _, policy := range policies.Restrictive {
		buildForPolicy(policy, true /* restrictive */)
	}

	if scalar == nil {
		panic(errors.AssertionFailedf("at least one applicable policy should have been included"))
	}
	b.factory.Metadata().GetRLSMeta().AddPoliciesUsed(tabMeta.MetaID, policiesUsed, false /* applyFilterExpr */)
	return scalar, colIDs
}

// combinePolicyWithCheckExprForUpsert is function to build up the WITH CHECK
// expression for an UPSERT operation.
func (b *Builder) combinePolicyWithCheckExprForUpsert(
	tabMeta *opt.TableMeta,
	needsSelectPriv bool,
	fetchScope *scope,
	exprScope *scope,
	canaryColID opt.ColumnID,
	referencedCols *opt.ColSet,
) (scalar opt.ScalarExpr, colIDs opt.ColSet) {
	conflictScanScalar, fetchColIDs := b.maybeBuildRLSUsingExpr(
		tabMeta, fetchScope, cat.PolicyScopeUpdate, referencedCols)
	if conflictScanScalar == nil {
		conflictScanScalar = memo.TrueSingleton
	}
	colIDs.UnionWith(fetchColIDs)

	conflictUpdateScalar, updateColIDs := b.combinePolicyWithCheckExpr(
		tabMeta, cat.PolicyScopeUpdate, needsSelectPriv, exprScope, referencedCols)
	colIDs.UnionWith(updateColIDs)
	noConflictScalar, insertColIDs := b.combinePolicyWithCheckExpr(
		tabMeta, cat.PolicyScopeInsert, needsSelectPriv, exprScope, referencedCols)
	colIDs.UnionWith(insertColIDs)

	isNotConflict := b.factory.ConstructIs(
		b.factory.ConstructVariable(canaryColID),
		memo.NullSingleton,
	)
	isConflict := b.factory.ConstructNot(isNotConflict)
	scalar = b.factory.ConstructAnd(
		b.factory.ConstructAnd(
			b.factory.ConstructOr(
				isNotConflict,
				conflictScanScalar,
			),
			b.factory.ConstructOr(
				isNotConflict,
				conflictUpdateScalar,
			),
		),
		b.factory.ConstructOr(
			isConflict,
			noConflictScalar,
		),
	)
	return scalar, colIDs
}

// rlsCheckConstraint is an implementation of cat.CheckConstraint for the
// check constraint built to enforce the RLS policies on write.
type rlsCheckConstraint struct {
	constraint string
	colIDs     opt.ColList
	tab        cat.Table
}

// Constraint implements the cat.CheckConstraint interface.
func (r *rlsCheckConstraint) Constraint() string { return r.constraint }

// Validated implements the cat.CheckConstraint interface.
func (r *rlsCheckConstraint) Validated() bool { return true }

// ColumnCount implements the cat.CheckConstraint interface.
func (r *rlsCheckConstraint) ColumnCount() int { return len(r.colIDs) }

// ColumnOrdinal implements the cat.CheckConstraint interface.
func (r *rlsCheckConstraint) ColumnOrdinal(i int) int {
	ord, err := r.tab.LookupColumnOrdinal(descpb.ColumnID(r.colIDs[i]))
	if err != nil {
		panic(err)
	}
	return ord
}

// IsRLSConstraint implements the cat.CheckConstraint interface.
func (r *rlsCheckConstraint) IsRLSConstraint() bool { return true }
