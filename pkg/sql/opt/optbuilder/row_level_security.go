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

	// Admin users are exempt from any RLS filtering.
	isAdmin, err := b.catalog.UserHasAdminRole(b.ctx, b.checkPrivilegeUser)
	if err != nil {
		panic(err)
	}
	b.factory.Metadata().SetRLSEnabled(b.checkPrivilegeUser, isAdmin, tabMeta.MetaID)
	if isAdmin {
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
	var policiesUsed opt.PolicyIDSet
	policies := tabMeta.Table.Policies()
	for _, policy := range policies.Permissive {
		if !policy.AppliesToRole(b.checkPrivilegeUser) || !b.policyAppliesToCommandScope(policy, cmdScope) {
			continue
		}
		strExpr := policy.UsingExpr
		if strExpr == "" {
			continue
		}
		policiesUsed.Add(policy.ID)
		parsedExpr, err := parser.ParseExpr(strExpr)
		if err != nil {
			panic(err)
		}
		typedExpr := tableScope.resolveType(parsedExpr, types.AnyElement)
		scalar := b.buildScalar(typedExpr, tableScope, nil, nil, nil)
		// TODO(136742): Apply multiple RLS policies.
		b.factory.Metadata().GetRLSMeta().AddPoliciesUsed(tabMeta.MetaID, policiesUsed, true /* applyFilterExpr */)
		return scalar
	}

	// TODO(136742): Add support for restrictive policies.

	// If no permissive policies apply, filter out all rows by adding a "false" expression.
	b.factory.Metadata().GetRLSMeta().NoPoliciesApplied = true
	return memo.FalseSingleton
}

// policyAppliesToCommandScope checks whether a given PolicyCommandScope applies
// to the specified policy. It returns true if the policy is applicable and
// false otherwise.
func (b *Builder) policyAppliesToCommandScope(
	policy cat.Policy, cmdScope cat.PolicyCommandScope,
) bool {
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
	var sb strings.Builder

	// colIDs tracks the column IDs referenced in all the policy expressions
	// that are applied. We use a set as we need to combine the columns used
	// for multiple policies.
	var colIDs intsets.Fast

	// Admin users are exempt from any RLS policies.
	isAdmin, err := r.oc.UserHasAdminRole(ctx, r.user)
	if err != nil {
		panic(err)
	}
	r.md.SetRLSEnabled(r.user, isAdmin, r.tabMeta.MetaID)
	if isAdmin {
		// Return a constraint check that always passes.
		return "true", nil
	}

	var policiesUsed opt.PolicyIDSet
	for i := range r.tab.Policies().Permissive {
		p := &r.tab.Policies().Permissive[i]

		if !p.AppliesToRole(r.user) || !r.policyAppliesToCommand(p, r.isUpdate) {
			continue
		}
		policiesUsed.Add(p.ID)
		var expr string
		// If the WITH CHECK expression is missing, we default to the USING
		// expression. If both are missing, then this policy doesn't apply and can
		// be skipped.
		if p.WithCheckExpr == "" {
			if p.UsingExpr == "" {
				continue
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
			sb.WriteString(" OR ")
		}
		sb.WriteString("(")
		sb.WriteString(expr)
		sb.WriteString(")")
		// TODO(136742): Add support for multiple policies.
		r.md.GetRLSMeta().AddPoliciesUsed(r.tabMeta.MetaID, policiesUsed, false /* applyFilterExpr */)
		break
	}

	// TODO(136742): Add support for restrictive policies.

	// If no policies apply, then we will add a false check as nothing is allowed
	// to be written.
	if sb.Len() == 0 {
		r.md.GetRLSMeta().NoPoliciesApplied = true
		return "false", nil
	}

	return sb.String(), colIDs.Ordered()
}

// policyAppliesToCommand will return true iff the command set in the policy
// applies to the current mutation action.
func (r *optRLSConstraintBuilder) policyAppliesToCommand(policy *cat.Policy, isUpdate bool) bool {
	switch policy.Command {
	case catpb.PolicyCommand_ALL:
		return true
	case catpb.PolicyCommand_SELECT, catpb.PolicyCommand_DELETE:
		return false
	case catpb.PolicyCommand_INSERT:
		return !isUpdate
	case catpb.PolicyCommand_UPDATE:
		return isUpdate
	default:
		panic(errors.AssertionFailedf("unknown policy command %v", policy.Command))
	}
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
