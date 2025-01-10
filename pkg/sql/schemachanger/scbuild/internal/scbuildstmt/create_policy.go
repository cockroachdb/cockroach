// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CreatePolicy implements CREATE POLICY.
func CreatePolicy(b BuildCtx, n *tree.CreatePolicy) {
	failIfRLSIsNotEnabled(b)
	b.IncrementSchemaChangeCreateCounter("policy")

	tableElems := b.ResolveTable(n.TableName, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	})
	panicIfSchemaChangeIsDisallowed(tableElems, n)
	tbl := tableElems.FilterTable().MustGetOneElement()

	// Resolve the policy name to make sure one doesn't already exist
	policyElems := b.ResolvePolicy(tbl.TableID, n.PolicyName, ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   privilege.CREATE,
	})
	policyElems.FilterPolicyName().ForEachTarget(func(target scpb.TargetStatus, e *scpb.PolicyName) {
		if target == scpb.ToPublic {
			panic(pgerror.Newf(pgcode.DuplicateObject, "policy with name %q already exists on table %q",
				n.PolicyName, n.TableName.String()))
		}
	})

	policyID := b.NextTablePolicyID(tbl.TableID)
	b.Add(&scpb.Policy{
		TableID:  tbl.TableID,
		PolicyID: policyID,
	})
	b.Add(&scpb.PolicyName{
		TableID:  tbl.TableID,
		PolicyID: policyID,
		Name:     string(n.PolicyName),
	})
}
