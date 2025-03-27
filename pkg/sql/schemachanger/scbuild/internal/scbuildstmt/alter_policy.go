// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// AlterPolicy implements ALTER POLICY.
func AlterPolicy(b BuildCtx, n *tree.AlterPolicy) {
	b.IncrementSchemaChangeAlterCounter("policy")

	tableElems := b.ResolveTable(n.TableName, ResolveParams{
		RequireOwnership: true,
	})
	panicIfSchemaChangeIsDisallowed(tableElems, n)
	tbl := tableElems.FilterTable().MustGetOneElement()

	// Alter of a policy implies that it must exist.
	policyElems := b.ResolvePolicy(tbl.TableID, n.PolicyName, ResolveParams{})
	policy := policyElems.FilterPolicy().MustGetOneElement()

	validateExprsForCmd(policy.Command, &n.Exprs)

	if n.NewPolicyName != "" {
		oldPolicyName := policyElems.FilterPolicyName().MustGetOneElement()
		b.Drop(oldPolicyName)
		b.Add(&scpb.PolicyName{
			TableID:  tbl.TableID,
			PolicyID: policy.PolicyID,
			Name:     string(n.NewPolicyName),
		})
	}
	if len(n.Roles) > 0 {
		upsertRoleElements(b, n.Roles, tbl.TableID, policy.PolicyID, policyElems)
	}
	if n.Exprs.Using != nil || n.Exprs.WithCheck != nil {
		upsertPolicyExpressions(b, n.TableName.ToTableName(), n.Exprs, tbl.TableID,
			policy.PolicyID, policyElems)
	}
	b.LogEventForExistingTarget(policy)
}
