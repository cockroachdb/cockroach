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

// ExternalConnectionPrivilege represents privileges on external connection
// objects stored in `system.external_connections`.
type ExternalConnectionPrivilege struct {
	ConnectionName      string `priv:"ConnectionName"`
	PrivilegeDescriptor *catpb.PrivilegeDescriptor
}

// InitExternalConnectionPrivilege creates a new ExternalConnectionPrivilege.
func InitExternalConnectionPrivilege(
	name string, privDesc *catpb.PrivilegeDescriptor,
) *ExternalConnectionPrivilege {
	return &ExternalConnectionPrivilege{
		ConnectionName:      name,
		PrivilegeDescriptor: privDesc,
	}
}

var _ catalog.PrivilegeObject = &ExternalConnectionPrivilege{}
var _ Object = &ExternalConnectionPrivilege{}

// CreateExternalConnectionPrivilegePath creates an privilege path for an external
// connection given the name of the external connection.
func CreateExternalConnectionPrivilegePath(name string) string {
	return fmt.Sprintf("/externalconn/%s", name)
}

// GetPath implements the Object interface.
func (e *ExternalConnectionPrivilege) GetPath() string {
	return CreateExternalConnectionPrivilegePath(e.ConnectionName)
}

// GetPrivileges implements the PrivilegeObject interface.
func (e *ExternalConnectionPrivilege) GetPrivileges() *catpb.PrivilegeDescriptor {
	return e.PrivilegeDescriptor
}

// GetObjectType implements the PrivilegeObject interface.
func (e *ExternalConnectionPrivilege) GetObjectType() privilege.ObjectType {
	return privilege.ExternalConnection
}

// GetName implements the PrivilegeObject interface.
func (e *ExternalConnectionPrivilege) GetName() string {
	return e.ConnectionName
}

// EqualExcludingPrivilegeDescriptor implements the Object interface.
func (e *ExternalConnectionPrivilege) EqualExcludingPrivilegeDescriptor(other Object) bool {
	if e.GetObjectType() != other.GetObjectType() {
		return false
	}

	otherEcPrivilege := other.(*ExternalConnectionPrivilege)
	return e.GetPath() == otherEcPrivilege.GetPath() && e.ConnectionName == otherEcPrivilege.ConnectionName
}
