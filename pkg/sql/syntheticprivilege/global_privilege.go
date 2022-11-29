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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/errors"
)

// GlobalPrivilege represents privileges granted via
// GRANT SYSTEM [privilege...] TO [roles...].
// These privileges are "global", for example, MODIFYCLUSTERSETTING which lets
// the role modify cluster settings within the cluster.
type GlobalPrivilege struct{}

// GlobalPrivilegeObjectType represents the object type for
// GlobalPrivilege.
const GlobalPrivilegeObjectType = "Global"

var _ Object = &GlobalPrivilege{}

// GetPath implements the Object interface.
func (p *GlobalPrivilege) GetPath() string {
	return "/global/"
}

// SystemPrivilegesTableVersionGate implements the Object interface.
func (p *GlobalPrivilege) SystemPrivilegesTableVersionGate() clusterversion.Key {
	return clusterversion.TODOPreV22_1
}

// GlobalPrivilegeObject is one of one since it is global.
// We can use a const to identify it.
var GlobalPrivilegeObject = &GlobalPrivilege{}

// GetFallbackPrivileges implements the Object interface.
func (e *GlobalPrivilege) GetFallbackPrivileges() *catpb.PrivilegeDescriptor {
	// We should always be retrieving global privileges from the
	// system.privileges table.
	panic(errors.AssertionFailedf("not implemented"))
}

// GetObjectType implements the Object interface.
func (p *GlobalPrivilege) GetObjectType() privilege.ObjectType {
	return privilege.Global
}

// GetName implements the Object interface.
func (p *GlobalPrivilege) GetName() string {
	// TODO(richardjcai): Turn this into a const map somewhere.
	// GetName can return none since SystemCluster is not named and is 1 of 1.
	return ""
}

// ID implements the cat.Object interface.
func (p *GlobalPrivilege) ID() cat.StableID {
	return cat.StableID(descpb.InvalidID)
}

// PostgresDescriptorID implements the cat.Object interface.
func (p *GlobalPrivilege) PostgresDescriptorID() catid.DescID {
	return descpb.InvalidID
}

// Equals implements the cat.Object interface.
func (p *GlobalPrivilege) Equals(otherObject cat.Object) bool {
	return p.ID() == otherObject.ID()
}
