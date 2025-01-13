// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
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
		Type:     convertPolicyType(n.Type),
		Command:  convertPolicyCommand(n.Cmd),
	})
	b.Add(&scpb.PolicyName{
		TableID:  tbl.TableID,
		PolicyID: policyID,
		Name:     string(n.PolicyName),
	})
}

// convertPolicyType will convert from a tree.PolicyType to a catpb.PolicyType
func convertPolicyType(in tree.PolicyType) catpb.PolicyType {
	switch in {
	case tree.PolicyTypeDefault, tree.PolicyTypePermissive:
		return catpb.PolicyType_PERMISSIVE
	case tree.PolicyTypeRestrictive:
		return catpb.PolicyType_RESTRICTIVE
	default:
		panic(errors.AssertionFailedf("cannot convert tree.PolicyType: %v", in))
	}
}

// convertPolicyCommand will convert from a tree.PolicyCommand to a catpb.PolicyCommand
func convertPolicyCommand(in tree.PolicyCommand) catpb.PolicyCommand {
	switch in {
	case tree.PolicyCommandDefault, tree.PolicyCommandAll:
		return catpb.PolicyCommand_ALL
	case tree.PolicyCommandSelect:
		return catpb.PolicyCommand_SELECT
	case tree.PolicyCommandInsert:
		return catpb.PolicyCommand_INSERT
	case tree.PolicyCommandUpdate:
		return catpb.PolicyCommand_UPDATE
	case tree.PolicyCommandDelete:
		return catpb.PolicyCommand_DELETE
	default:
		panic(errors.AssertionFailedf("cannot convert tree.PolicyCommand: %v", in))
	}
}
