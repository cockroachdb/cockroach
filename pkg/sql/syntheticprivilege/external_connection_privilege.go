// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syntheticprivilege

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// ExternalConnectionPrivilege represents privileges on external connection
// objects stored in `system.external_connections`.
type ExternalConnectionPrivilege struct {
	ConnectionName string `priv:"ConnectionName"`
}

var _ catalog.PrivilegeObject = &ExternalConnectionPrivilege{}

// GetPath implements the Object interface.
func (e *ExternalConnectionPrivilege) GetPath() string {
	return fmt.Sprintf("/externalconn/%s", e.ConnectionName)
}

// GetPrivilegeDescriptor implements the PrivilegeObject interface.
func (e *ExternalConnectionPrivilege) GetPrivilegeDescriptor(
	ctx context.Context, planner eval.Planner,
) (*catpb.PrivilegeDescriptor, error) {
	if planner.IsActive(ctx, clusterversion.SystemPrivilegesTable) {
		return planner.SynthesizePrivilegeDescriptor(ctx, e.GetName(), e.GetPath(),
			e.GetObjectType())
	}
	return catpb.NewPrivilegeDescriptor(
		username.PublicRoleName(), privilege.List{privilege.USAGE}, privilege.List{}, username.NodeUserName(),
	), nil
}

// GetObjectType implements the PrivilegeObject interface.
func (e *ExternalConnectionPrivilege) GetObjectType() privilege.ObjectType {
	return privilege.ExternalConnection
}

// GetName implements the PrivilegeObject interface.
func (e *ExternalConnectionPrivilege) GetName() string {
	return e.ConnectionName
}
