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

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// GlobalPrivilege represents privileges granted via
// GRANT SYSTEM [privilege...] TO [roles...].
// These privileges are "global", for example, MODIFYCLUSTERSETTING which lets
// the role modify cluster settings within the cluster.
type GlobalPrivilege struct{}

// GlobalPrivilegeObjectType represents the object type for
// GlobalPrivilege.
const GlobalPrivilegeObjectType = "Global"

// ToString implements the SyntheticPrivilegeObject interface.
func (p *GlobalPrivilege) ToString() string {
	return "/global/"
}

// PrivilegeObjectType implements the SyntheticPrivilegeObject interface.
func (p *GlobalPrivilege) PrivilegeObjectType() privilege.ObjectType {
	return privilege.Global
}

// GlobalPrivilegeObject is one of one since it is global.
// We can use a const to identify it.
var GlobalPrivilegeObject = &GlobalPrivilege{}

// GetPrivilegeDescriptor implements the PrivilegeObject interface.
func (p *GlobalPrivilege) GetPrivilegeDescriptor(
	ctx context.Context, planner eval.Planner,
) (*catpb.PrivilegeDescriptor, error) {
	return synthesizePrivilegeDescriptorFromSystemPrivilegesTable(ctx, planner, p)
}

// GetObjectType implements the PrivilegeObject interface.
func (p *GlobalPrivilege) GetObjectType() string {
	// TODO(richardjcai): Turn this into a const map somewhere.
	return GlobalPrivilegeObjectType
}

// GetName implements the PrivilegeObject interface.
func (p *GlobalPrivilege) GetName() string {
	// TODO(richardjcai): Turn this into a const map somewhere.
	// GetName can return none since SystemCluster is not named and is 1 of 1.
	return ""
}

func synthesizePrivilegeDescriptorFromSystemPrivilegesTable(
	ctx context.Context,
	planner eval.Planner,
	systemTablePrivilegeObject catalog.SyntheticPrivilegeObject,
) (*catpb.PrivilegeDescriptor, error) {
	query := fmt.Sprintf(
		`SELECT username, privileges FROM system.%s WHERE path='%s'`,
		catconstants.SystemPrivilegeTableName,
		systemTablePrivilegeObject.ToString())

	it, err := planner.QueryIteratorEx(ctx, `get-system-privileges`,
		sessiondata.InternalExecutorOverride{
			User: username.RootUserName(),
		}, query)
	if err != nil {
		return nil, err
	}

	privileges := catpb.PrivilegeDescriptor{}
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		user := tree.MustBeDString(it.Cur()[0])
		arr := tree.MustBeDArray(it.Cur()[1])
		var privilegeStrings []string
		for _, elem := range arr.Array {
			privilegeStrings = append(privilegeStrings, string(tree.MustBeDString(elem)))
		}
		privs, err := privilege.ListFromStrings(privilegeStrings)
		if err != nil {
			return nil, err
		}
		privileges.Grant(
			username.MakeSQLUsernameFromPreNormalizedString(string(user)),
			privs,
			false,
		)
	}

	privilegeObjectType := systemTablePrivilegeObject.PrivilegeObjectType()
	// We use InvalidID to skip checks on the root/admin roles having
	// privileges.
	if err := privileges.Validate(descpb.InvalidID, privilegeObjectType, systemTablePrivilegeObject.GetName(), privilege.GetValidPrivilegesForObject(privilegeObjectType)); err != nil {
		return nil, err
	}

	return &privileges, nil
}
