// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DropPolicy handles the DROP POLICY statement.
func (tc *Catalog) DropPolicy(n *tree.DropPolicy) {
	ctx := context.Background()
	tableName := n.TableName.ToTableName()
	ds, _, err := tc.ResolveDataSource(ctx, cat.Flags{}, &tableName)
	if err != nil {
		panic(err)
	}

	ts, ok := ds.(*Table)
	if !ok {
		panic(errors.New("policies can only be dropped from a table"))
	}

	policy, policyType, inx := ts.findPolicyByName(n.PolicyName)
	if policy == nil {
		if n.IfExists {
			return
		}
		panic(errors.Newf("cannot find policy %q on table %q", n.PolicyName, ts.Name))
	}
	switch policyType {
	case tree.PolicyTypeRestrictive:
		ts.policies.Restrictive = append(ts.policies.Restrictive[:inx], ts.policies.Restrictive[inx+1:]...)
	default:
		ts.policies.Permissive = append(ts.policies.Permissive[:inx], ts.policies.Permissive[inx+1:]...)
	}
}
