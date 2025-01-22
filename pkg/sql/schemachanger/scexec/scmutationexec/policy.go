// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) AddPolicy(ctx context.Context, op scop.AddPolicy) error {
	tbl, err := i.checkOutTable(ctx, op.Policy.TableID)
	if err != nil {
		return err
	}
	if op.Policy.PolicyID >= tbl.NextPolicyID {
		tbl.NextPolicyID = op.Policy.PolicyID + 1
	}
	tbl.Policies = append(tbl.Policies, descpb.PolicyDescriptor{
		ID:      op.Policy.PolicyID,
		Type:    op.Policy.Type,
		Command: op.Policy.Command,
	})
	return nil
}

func (i *immediateVisitor) SetPolicyName(ctx context.Context, op scop.SetPolicyName) error {
	policy, err := i.checkOutPolicy(ctx, op.TableID, op.PolicyID)
	if err != nil {
		return err
	}
	policy.Name = op.Name
	return nil
}

func (i *immediateVisitor) RemovePolicy(ctx context.Context, op scop.RemovePolicy) error {
	tbl, err := i.checkOutTable(ctx, op.Policy.TableID)
	if err != nil {
		return err
	}
	var found bool
	for idx := range tbl.Policies {
		if tbl.Policies[idx].ID == op.Policy.PolicyID {
			tbl.Policies = append(tbl.Policies[:idx], tbl.Policies[idx+1:]...)
			found = true
			break
		}
	}
	if !found {
		return errors.AssertionFailedf("failed to find policy with ID %d in table %q (%d)",
			op.Policy.PolicyID, tbl.GetName(), tbl.GetID())
	}
	return nil
}

func (i *immediateVisitor) AddPolicyRole(ctx context.Context, op scop.AddPolicyRole) error {
	policy, err := i.checkOutPolicy(ctx, op.Role.TableID, op.Role.PolicyID)
	if err != nil {
		return err
	}
	// Verify that the role doesn't already exist in the policy.
	for _, r := range policy.RoleNames {
		if r == op.Role.RoleName {
			return errors.AssertionFailedf(
				"role %q already exists in policy %d on table %d",
				op.Role.RoleName, op.Role.PolicyID, op.Role.TableID)
		}
	}
	// For convenience, if the public role is part of the policy, it will always be
	// positioned as the first entry in the slice. This simplifies query runtime checks
	// by allowing a quick determination of whether the policy applies to all roles
	// (i.e., the public role). If the role is not public, it is appended to the end
	// of the slice.
	if op.Role.RoleName == username.PublicRole {
		policy.RoleNames = append([]string{op.Role.RoleName}, policy.RoleNames...)
	} else {
		policy.RoleNames = append(policy.RoleNames, op.Role.RoleName)
	}
	return nil
}

func (i *immediateVisitor) RemovePolicyRole(ctx context.Context, op scop.RemovePolicyRole) error {
	policy, err := i.checkOutPolicy(ctx, op.Role.TableID, op.Role.PolicyID)
	if err != nil {
		return err
	}
	for inx, r := range policy.RoleNames {
		if r == op.Role.RoleName {
			policy.RoleNames = append(policy.RoleNames[:inx], policy.RoleNames[inx+1:]...)
			return nil
		}
	}
	return errors.AssertionFailedf(
		"role %q does not exist in policy %d on table %d",
		op.Role.RoleName, op.Role.PolicyID, op.Role.TableID)
}

func (i *immediateVisitor) SetPolicyWithCheckExpression(
	ctx context.Context, op scop.SetPolicyWithCheckExpression,
) error {
	policy, err := i.checkOutPolicy(ctx, op.TableID, op.PolicyID)
	if err != nil {
		return err
	}
	policy.WithCheckExpr = op.Expr
	return nil
}

func (i *immediateVisitor) SetPolicyUsingExpression(
	ctx context.Context, op scop.SetPolicyUsingExpression,
) error {
	policy, err := i.checkOutPolicy(ctx, op.TableID, op.PolicyID)
	if err != nil {
		return err
	}
	policy.UsingExpr = op.Expr
	return nil
}

func (i *immediateVisitor) SetPolicyForwardReferences(
	ctx context.Context, op scop.SetPolicyForwardReferences,
) error {
	policy, err := i.checkOutPolicy(ctx, op.Deps.TableID, op.Deps.PolicyID)
	if err != nil {
		return err
	}
	policy.DependsOnTypes = op.Deps.UsesTypeIDs
	policy.DependsOnRelations = op.Deps.UsesRelationIDs
	policy.DependsOnFunctions = op.Deps.UsesFunctionIDs
	return nil
}
