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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// VirtualTablePrivilege represents privileges on virtual tables such as
// crdb_internal or pg_catalog tables.
type VirtualTablePrivilege struct {
	ID descpb.ID `priv:"ID"`
}

// VirtualTablePrivilegeType represents the object type for
// VirtualTablePrivilege.
const VirtualTablePrivilegeType = "VirtualTable"

// ToString implements the SyntheticPrivilegeObject interface.
func (p *VirtualTablePrivilege) ToString() string {
	return fmt.Sprintf("/vtable/%d", p.ID)
}

// PrivilegeObjectType implements the SyntheticPrivilegeObject interface.
func (p *VirtualTablePrivilege) PrivilegeObjectType() privilege.ObjectType {
	return privilege.VirtualTable
}

// GetPrivilegeDescriptor implements the PrivilegeObject interface.
func (p *VirtualTablePrivilege) GetPrivilegeDescriptor(
	ctx context.Context, planner eval.Planner,
) (*catpb.PrivilegeDescriptor, error) {
	if planner.IsActive(ctx, clusterversion.SystemPrivilegesTable) {
		return synthesizePrivilegeDescriptorFromSystemPrivilegesTable(ctx, planner, p)
	}
	return catpb.NewPrivilegeDescriptor(
		username.PublicRoleName(), privilege.List{privilege.SELECT}, privilege.List{}, username.NodeUserName(),
	), nil
}

// GetObjectTypeName implements the PrivilegeObject interface.
func (p *VirtualTablePrivilege) GetObjectTypeName() string {
	return VirtualTablePrivilegeType
}

// GetObjectType implements the PrivilegeObject interface.
func (p *VirtualTablePrivilege) GetObjectType() privilege.ObjectType {
	return privilege.VirtualTable
}

// GetName implements the PrivilegeObject interface.
func (p *VirtualTablePrivilege) GetName() string {
	// TODO(richardjcai): Make this get the actual table name.
	// This isn't super important since this is only used in error messages.
	return fmt.Sprintf("virtual table %d", p.ID)
}
