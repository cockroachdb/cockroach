// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

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
