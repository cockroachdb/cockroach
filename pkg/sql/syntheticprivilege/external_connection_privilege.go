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

// ExternalConnectionPrivilege represents privileges on external connection
// objects stored in `system.external_connections`.
type ExternalConnectionPrivilege struct {
	ConnectionName string `priv:"ConnectionName"`
}

var _ Object = &ExternalConnectionPrivilege{}

// GetPath implements the Object interface.
func (e *ExternalConnectionPrivilege) GetPath() string {
	return fmt.Sprintf("/externalconn/%s", e.ConnectionName)
}

// GetFallbackPrivileges implements the Object interface.
func (e *ExternalConnectionPrivilege) GetFallbackPrivileges() *catpb.PrivilegeDescriptor {
	return catpb.NewPrivilegeDescriptor(
		username.PublicRoleName(),
		privilege.List{privilege.USAGE},
		privilege.List{},
		username.NodeUserName(),
	)
}

// GetObjectType implements the Object interface.
func (e *ExternalConnectionPrivilege) GetObjectType() privilege.ObjectType {
	return privilege.ExternalConnection
}

// GetObjectTypeString implements the Object interface.
func (e *ExternalConnectionPrivilege) GetObjectTypeString() string {
	return string(privilege.ExternalConnection)
}

// GetName implements the Object interface.
func (e *ExternalConnectionPrivilege) GetName() string {
	return e.ConnectionName
}
