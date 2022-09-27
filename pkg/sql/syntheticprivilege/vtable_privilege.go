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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// VirtualTablePrivilege represents privileges on virtual tables such as
// crdb_internal or pg_catalog tables.
type VirtualTablePrivilege struct {
	SchemaName          string `priv:"SchemaName"`
	TableName           string `priv:"TableName"`
	PrivilegeDescriptor *catpb.PrivilegeDescriptor
}

var _ catalog.PrivilegeObject = &VirtualTablePrivilege{}
var _ Object = &VirtualTablePrivilege{}

// VirtualTablePrivilegeType represents the object type for
// VirtualTablePrivilege.
const VirtualTablePrivilegeType = "VirtualTable"

// InitVirtualTablePrivilege creates a new VirtualTablePrivilege.
func InitVirtualTablePrivilege(
	schemaName, tableName string, privDesc *catpb.PrivilegeDescriptor,
) *VirtualTablePrivilege {
	return &VirtualTablePrivilege{
		SchemaName:          schemaName,
		TableName:           tableName,
		PrivilegeDescriptor: privDesc,
	}
}

// CreateVirtualTablePrivilegePath creates a privilege path for a virtual table
// given the schema and table name.
func CreateVirtualTablePrivilegePath(schemaName, tableName string) string {
	return fmt.Sprintf("/vtable/%s/%s", schemaName, tableName)
}

// GetPath implements the Object interface.
func (p *VirtualTablePrivilege) GetPath() string {
	return CreateVirtualTablePrivilegePath(p.SchemaName, p.TableName)
}

// GetPrivileges implements the PrivilegeObject interface.
func (p *VirtualTablePrivilege) GetPrivileges() *catpb.PrivilegeDescriptor {
	return p.PrivilegeDescriptor
}

// GetObjectType implements the PrivilegeObject interface.
func (p *VirtualTablePrivilege) GetObjectType() privilege.ObjectType {
	return privilege.VirtualTable
}

// GetName implements the PrivilegeObject interface.
func (p *VirtualTablePrivilege) GetName() string {
	return fmt.Sprintf("%s.%s", p.SchemaName, p.TableName)
}

// EqualExcludingPrivilegeDescriptor implements the Object interface.
func (p *VirtualTablePrivilege) EqualExcludingPrivilegeDescriptor(other Object) bool {
	if p.GetObjectType() != other.GetObjectType() {
		return false
	}

	otherVtablePrivilege := other.(*VirtualTablePrivilege)
	return p.SchemaName == otherVtablePrivilege.SchemaName &&
		p.TableName == otherVtablePrivilege.TableName &&
		p.GetPath() == otherVtablePrivilege.GetPath()
}
