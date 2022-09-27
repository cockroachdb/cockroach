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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// GlobalPrivilege represents privileges granted via
// GRANT SYSTEM [privilege...] TO [roles...].
// These privileges are "global", for example, MODIFYCLUSTERSETTING which lets
// the role modify cluster settings within the cluster.
type GlobalPrivilege struct {
	PrivilegeDescriptor *catpb.PrivilegeDescriptor
}

var _ catalog.PrivilegeObject = &GlobalPrivilege{}
var _ Object = &GlobalPrivilege{}

// GlobalPrivilegeObjectType represents the object type for
// GlobalPrivilege.
const GlobalPrivilegeObjectType = "Global"

// GetPath implements the Object interface.
func (p *GlobalPrivilege) GetPath() string {
	return "/global/"
}

// InitGlobalPrivilege creates a new GlobalPrivilege.
func InitGlobalPrivilege(privDesc *catpb.PrivilegeDescriptor) *GlobalPrivilege {
	return &GlobalPrivilege{
		PrivilegeDescriptor: privDesc,
	}
}

// GlobalPrivilegeObject is one of one since it is global.
// We can use a const to identify it.
var GlobalPrivilegeObject = &GlobalPrivilege{}

// GetPrivileges implements the PrivilegeObject interface.
func (p *GlobalPrivilege) GetPrivileges() *catpb.PrivilegeDescriptor {
	return p.PrivilegeDescriptor
}

// GetObjectType implements the PrivilegeObject interface.
func (p *GlobalPrivilege) GetObjectType() privilege.ObjectType {
	return privilege.Global
}

// GetName implements the PrivilegeObject interface.
func (p *GlobalPrivilege) GetName() string {
	// TODO(richardjcai): Turn this into a const map somewhere.
	// GetName can return none since SystemCluster is not named and is 1 of 1.
	return ""
}

// EqualExcludingPrivilegeDescriptor implements the Object interface.
func (p *GlobalPrivilege) EqualExcludingPrivilegeDescriptor(other Object) bool {
	if p.GetObjectType() != other.GetObjectType() {
		return false
	}

	otherGlobalPrivilege := other.(*GlobalPrivilege)
	return p.GetPath() == otherGlobalPrivilege.GetPath() && p.GetName() == otherGlobalPrivilege.GetName()
}
