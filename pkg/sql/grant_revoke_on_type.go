// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// GrantOnType grants usage privileges to a set of users.
func (p *planner) GrantOnType(ctx context.Context, n *tree.GrantOnType) (planNode, error) {
	// TODO(richardjcai): Add telemetry

	if err := privilege.ValidateTypePrivileges(n.Privileges); err != nil {
		return nil, err
	}

	return &changePrivilegesNode{
		desiredprivs: n.Privileges,
		grantees:     n.Grantees,
		targets:      n.Targets,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
			privDesc.Grant(grantee, n.Privileges)
		},
	}, nil
}

// RevokeOnType grants usage privileges to a set of users.
func (p *planner) RevokeOnType(ctx context.Context, n *tree.RevokeOnType) (planNode, error) {
	// TODO(richardjcai): Add telemetry

	if err := privilege.ValidateTypePrivileges(n.Privileges); err != nil {
		return nil, err
	}

	return &changePrivilegesNode{
		desiredprivs: n.Privileges,
		grantees:     n.Grantees,
		targets:      n.Targets,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
			privDesc.Revoke(grantee, n.Privileges)
		},
	}, nil
}
