// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// CreatePolicy handles the CREATE POLICY statement.
func (tc *Catalog) CreatePolicy(n *tree.CreatePolicy) {
	ctx := context.Background()
	tableName := n.TableName.ToTableName()
	ds, _, err := tc.ResolveDataSource(ctx, cat.Flags{}, &tableName)
	if err != nil {
		panic(err)
	}
	ts, isTable := ds.(*Table)
	if !isTable {
		panic(errors.New("policies can only be added to a table"))
	}

	foundPolicy, _, _ := ts.findPolicyByName(n.PolicyName)
	if foundPolicy != nil {
		panic(errors.Newf(`policy %q already exists on table %q`, n.PolicyName, ts.Name()))
	}

	policy := cat.Policy{Name: n.PolicyName, ID: ts.nextPolicyID}
	switch n.Cmd {
	case tree.PolicyCommandAll, tree.PolicyCommandDefault:
		policy.Command = catpb.PolicyCommand_ALL
	case tree.PolicyCommandSelect:
		policy.Command = catpb.PolicyCommand_SELECT
	case tree.PolicyCommandInsert:
		policy.Command = catpb.PolicyCommand_INSERT
	case tree.PolicyCommandUpdate:
		policy.Command = catpb.PolicyCommand_UPDATE
	case tree.PolicyCommandDelete:
		policy.Command = catpb.PolicyCommand_DELETE
	default:
		panic(errors.Newf("unknown policy command: %v", n.Cmd))
	}
	if n.Exprs.Using != nil {
		policy.UsingExpr = tree.Serialize(n.Exprs.Using)
	}
	if n.Exprs.WithCheck != nil {
		policy.WithCheckExpr = tree.Serialize(n.Exprs.WithCheck)
	}
	roleNames := make([]string, len(n.Roles))
	for i := range n.Roles {
		roleNames[i] = n.Roles[i].Name
	}
	policy.InitRoles(roleNames)

	// Add the policy to the appropriate slice based on its type. Default to
	// permissive if no type is specified.
	switch n.Type {
	case tree.PolicyTypePermissive, tree.PolicyTypeDefault:
		ts.policies.Permissive = append(ts.policies.Permissive, policy)
	default:
		ts.policies.Restrictive = append(ts.policies.Restrictive, policy)
	}
	ts.nextPolicyID++
}
