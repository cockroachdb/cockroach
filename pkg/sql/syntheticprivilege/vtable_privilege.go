// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// GetObjectTypeString implements the Object interface.
func (p *VirtualTablePrivilege) GetObjectTypeString() string {
	return string(privilege.VirtualTable)
}

// GetName implements the Object interface.
func (p *VirtualTablePrivilege) GetName() string {
	return fmt.Sprintf("%s.%s", p.SchemaName, p.TableName)
}
