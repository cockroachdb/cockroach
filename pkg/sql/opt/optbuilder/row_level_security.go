// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
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
	if !tabMeta.Table.IsRowLevelSecurityEnabled() || cmdScope == cat.PolicyScopeExempt {
		return
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
	b.factory.Metadata().SetRLSEnabled(b.checkPrivilegeUser, isAdmin, tabMeta.MetaID, isOwnerAndNotForced)
	// Check if RLS filtering is exempt.
	if isAdmin || isOwnerAndNotForced {
		return
	}

	scalar := b.buildRowLevelSecurityUsingExpression(tabMeta, tableScope, cmdScope)
	tableScope.expr = b.factory.ConstructSelect(tableScope.expr,
		memo.FiltersExpr{b.factory.ConstructFiltersItem(scalar)})
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
		if !policy.AppliesToRole(b.checkPrivilegeUser) || !policyAppliesToCommandScope(policy, cmdScope) {
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

// optRLSConstraintBuilder is used synthesize a check constraint to enforce the
// RLS policies for new rows.
type optRLSConstraintBuilder struct {
	tab      cat.Table
	md       *opt.Metadata
	tabMeta  *opt.TableMeta
	oc       cat.Catalog
	user     username.SQLUsername
	isUpdate bool
}

// Build will construct a CheckConstraint to enforce the policies for the
// current user and command.
func (r *optRLSConstraintBuilder) Build(ctx context.Context) cat.CheckConstraint {
	expr, colIDs := r.genExpression(ctx)
	if expr == "" {
		panic(fmt.Sprintf("must return some expression but empty string returned for user: %v", r.user))
	}
	return &rlsCheckConstraint{
		constraint: expr,
		colIDs:     colIDs,
		tab:        r.tab,
	}
}

// genExpression builds the expression that will be used within the check
// constraint built for RLS.
func (r *optRLSConstraintBuilder) genExpression(ctx context.Context) (string, []int) {
	// colIDs tracks the column IDs referenced in all the policy expressions
	// that are applied. We use a set as we need to combine the columns used
	// for multiple policies.
	var colIDs intsets.Fast

	// Check for cases where users are exempt from policies.
	isAdmin, err := r.oc.UserHasAdminRole(ctx, r.user)
	if err != nil {
		panic(err)
	}
	isOwnerAndNotForced, err := r.isTableOwnerAndRLSNotForced(ctx)
	if err != nil {
		panic(err)
	}
	r.md.SetRLSEnabled(r.user, isAdmin, r.tabMeta.MetaID, isOwnerAndNotForced)
	if isAdmin || isOwnerAndNotForced {
		// Return a constraint check that always passes.
		return "true", nil
	}

	combinedExpr := r.combinePolicyWithCheckExpr(&colIDs)
	// If no policies apply, then we will add a false check as nothing is allowed
	// to be written.
	if combinedExpr == "" {
		r.md.GetRLSMeta().NoPoliciesApplied = true
		return "false", nil
	}
	return combinedExpr, colIDs.Ordered()
}

// combinePolicyWithCheckExpr will build a combined expression depending if this
// is INSERT or UPDATE.
func (r *optRLSConstraintBuilder) combinePolicyWithCheckExpr(colIDs *intsets.Fast) string {
	// When handling UPDATE, we need to add the SELECT/ALL using expressions
	// first, then apply any UPDATE policy.
	if r.isUpdate {
		selExpr := r.genPolicyWithCheckExprForCommand(colIDs, cat.PolicyScopeSelect)
		if selExpr == "" {
			return ""
		}
		updExpr := r.genPolicyWithCheckExprForCommand(colIDs, cat.PolicyScopeUpdate)
		if updExpr == "" {
			return ""
		}
		return fmt.Sprintf("(%s) and (%s)", selExpr, updExpr)
	}
	return r.genPolicyWithCheckExprForCommand(colIDs, cat.PolicyScopeInsert)
}

// genPolicyWithCheckExprForCommand will build a WITH CHECK expression for the
// given policy command.
func (r *optRLSConstraintBuilder) genPolicyWithCheckExprForCommand(
	colIDs *intsets.Fast, cmdScope cat.PolicyCommandScope,
) string {
	var sb strings.Builder
	var policiesUsed opt.PolicyIDSet
	policies := r.tabMeta.Table.Policies()

	// Create a closure to handle building the expression for one policy.
	buildForPolicy := func(p cat.Policy, restrictive bool) {
		if !p.AppliesToRole(r.user) || !policyAppliesToCommandScope(p, cmdScope) {
			return
		}
		policiesUsed.Add(p.ID)

		var expr string
		// If the WITH CHECK expression is missing, we default to the USING
		// expression. If both are missing, then this policy doesn't apply and can
		// be skipped.
		if p.WithCheckExpr == "" {
			if p.UsingExpr == "" {
				return
			}
			expr = p.UsingExpr
			for _, id := range p.UsingColumnIDs {
				colIDs.Add(int(id))
			}
		} else {
			expr = p.WithCheckExpr
			for _, id := range p.WithCheckColumnIDs {
				colIDs.Add(int(id))
			}
		}
		if sb.Len() != 0 {
			if restrictive {
				sb.WriteString(" AND ")
			} else {
				sb.WriteString(" OR ")
			}
		} else {
			sb.WriteString("(") // Add the outer parenthesis that surrounds all permissive policies
		}
		sb.WriteString("(")
		sb.WriteString(expr)
		sb.WriteString(")")
	}

	for _, policy := range policies.Permissive {
		buildForPolicy(policy, false /* restrictive */)
	}
	// If no permissive policies apply, then we will add a false check as
	// nothing is allowed to be written.
	if sb.Len() == 0 {
		r.md.GetRLSMeta().NoPoliciesApplied = true
		return "false"
	}
	sb.WriteString(")") // Close the outer parenthesis that surrounds all permissive policies
	for _, policy := range policies.Restrictive {
		buildForPolicy(policy, true /* restrictive */)
	}

	if sb.Len() == 0 {
		panic(errors.AssertionFailedf("at least one applicable policy should have been included"))
	}
	r.md.GetRLSMeta().AddPoliciesUsed(r.tabMeta.MetaID, policiesUsed, false /* applyFilterExpr */)
	return sb.String()
}

// isTableOwnerAndRLSNotForced returns true iff the user is the table owner and
// the NO FORCE option is set.
func (r *optRLSConstraintBuilder) isTableOwnerAndRLSNotForced(ctx context.Context) (bool, error) {
	if r.tabMeta.Table.IsRowLevelSecurityForced() {
		return false, nil
	}
	return r.oc.IsOwner(ctx, r.tabMeta.Table, r.user)
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
