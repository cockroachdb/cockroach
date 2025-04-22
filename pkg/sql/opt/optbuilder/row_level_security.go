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
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// addRowLevelSecurityFilter adds a filter based on the expressions of
// applicable RLS policies. If RLS is enabled but no policies are applicable,
// all rows will be filtered out.
func (b *Builder) addRowLevelSecurityFilter(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope,
) {
	if b.isExemptFromRLSPolicies(tabMeta, cmdScope) {
		return
	}
	scalar := b.buildRowLevelSecurityUsingExpression(tabMeta, tableScope, cmdScope)
	if scalar != nil {
		tableScope.expr = b.factory.ConstructSelect(tableScope.expr,
			memo.FiltersExpr{b.factory.ConstructFiltersItem(scalar)})
	}
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
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope,
) opt.ScalarExpr {
	combinedExpr := b.combinePolicyUsingExpressionForCommand(tabMeta, tableScope, cmdScope)
	if combinedExpr == nil {
		// No policies, filter out all rows by adding a "false" expression.
		b.factory.Metadata().GetRLSMeta().NoPoliciesApplied = true
		return memo.FalseSingleton
	}
	return b.buildScalar(combinedExpr, tableScope, nil, nil, nil)
}

// combinePolicyUsingExpressionForCommand generates a `tree.TypedExpr` command
// for use in the scan. Depending on the policy command scope, it may simply
// pass through to `buildRowLevelSecurityUsingExpressionForCommand`.
// For other commands, the process is more complex and may involve multiple
// calls to that function to generate the expression.
func (b *Builder) combinePolicyUsingExpressionForCommand(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope,
) tree.TypedExpr {
	// For DELETE and UPDATE, we always apply SELECT/ALL policies because
	// reading the existing row is necessary before performing the write.
	switch cmdScope {
	case cat.PolicyScopeUpdate, cat.PolicyScopeDelete:
		selectExpr := b.buildRowLevelSecurityUsingExpressionForCommand(tabMeta, tableScope, cat.PolicyScopeSelect)
		if selectExpr == nil {
			return selectExpr
		}
		updateExpr := b.buildRowLevelSecurityUsingExpressionForCommand(tabMeta, tableScope, cmdScope)
		if updateExpr == nil {
			return nil
		}
		return tree.NewTypedAndExpr(selectExpr, updateExpr)
	default:
		return b.buildRowLevelSecurityUsingExpressionForCommand(tabMeta, tableScope, cmdScope)
	}
}

// buildRowLevelSecurityUsingExpressionForCommand will generate a tree.TypedExpr
// for all policies that apply to the given policy command scope.
func (b *Builder) buildRowLevelSecurityUsingExpressionForCommand(
	tabMeta *opt.TableMeta, tableScope *scope, cmdScope cat.PolicyCommandScope,
) tree.TypedExpr {
	var policiesUsed opt.PolicyIDSet
	var combinedExpr tree.TypedExpr
	policies := tabMeta.Table.Policies()

	// Create a closure to handle building the expression for one policy.
	buildForPolicy := func(policy cat.Policy, restrictive bool) {
		if !policy.AppliesToRole(b.ctx, b.catalog, b.checkPrivilegeUser) || !policyAppliesToCommandScope(policy, cmdScope) {
			return
		}
		strExpr := policy.UsingExpr
		if strExpr == "" {
			return
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
		return nil
	}
	for _, policy := range policies.Restrictive {
		buildForPolicy(policy, true /* restrictive */)
	}

	// We should have already exited early if there were no permissive policies.
	if combinedExpr == nil {
		panic(errors.AssertionFailedf("at least one applicable policy should have been found"))
	}
	b.factory.Metadata().GetRLSMeta().AddPoliciesUsed(tabMeta.MetaID, policiesUsed, true /* applyFilterExpr */)
	return combinedExpr
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

// getColIDsFromPoliciesUsed returns the column ordinals referenced by all
// policies used in the RLS meta cache.
func (b *Builder) getColIDsFromPoliciesUsed(tabMeta *opt.TableMeta) []int {
	filters, checks := b.factory.Metadata().GetRLSMeta().GetPoliciesUsed(tabMeta.MetaID)
	var colIDs intsets.Fast
	extractColIDsForPolicies := func(policies []cat.Policy) {
		for _, policy := range policies {
			if checks.Contains(policy.ID) && policy.WithCheckExpr != "" {
				for _, id := range policy.WithCheckColumnIDs {
					colIDs.Add(int(id))
				}
			}
			// Include columns from the USING expression. Note: if a WITH CHECK expression
			// is present but empty, the USING expression will be applied instead.
			if filters.Contains(policy.ID) || (checks.Contains(policy.ID) && policy.WithCheckExpr == "") {
				for _, id := range policy.UsingColumnIDs {
					colIDs.Add(int(id))
				}
			}
		}
	}
	policies := tabMeta.Table.Policies()
	extractColIDsForPolicies(policies.Permissive)
	extractColIDsForPolicies(policies.Restrictive)
	return colIDs.Ordered()
}

// rlsCheckConstraint is an implementation of cat.CheckConstraint for the
// check constraint built to enforce the RLS policies on write.
type rlsCheckConstraint struct {
	constraint string
	colIDs     []int
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
