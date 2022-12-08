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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// VirtualTablePrivilege represents privileges on virtual tables such as
// crdb_internal or pg_catalog tables.
type VirtualTablePrivilege struct {
	SchemaName string `priv:"SchemaName"`
	TableName  string `priv:"TableName"`
}

// VirtualTablePrivilegeType represents the object type for
// VirtualTablePrivilege.
const VirtualTablePrivilegeType = "VirtualTable"

var _ Object = &VirtualTablePrivilege{}

// VirtualTablePathPrefix is the prefix used for virtual table privileges in system.privileges.
const VirtualTablePathPrefix = "vtable"

// GetPath implements the Object interface.
func (p *VirtualTablePrivilege) GetPath() string {
	return fmt.Sprintf("/%s/%s/%s", VirtualTablePathPrefix, p.SchemaName, p.TableName)
}

// GetFallbackPrivileges implements the Object interface.
func (e *VirtualTablePrivilege) GetFallbackPrivileges() *catpb.PrivilegeDescriptor {
	return catpb.NewPrivilegeDescriptor(
		username.PublicRoleName(),
		privilege.List{privilege.SELECT},
		privilege.List{},
		username.NodeUserName(),
	)
}

// GetObjectType implements the Object interface.
func (p *VirtualTablePrivilege) GetObjectType() privilege.ObjectType {
	return privilege.VirtualTable
}

// GetName implements the Object interface.
func (p *VirtualTablePrivilege) GetName() string {
	return fmt.Sprintf("%s.%s", p.SchemaName, p.TableName)
}
