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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/privilegeobject"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

// GetPath implements the SyntheticPrivilegeObject interface.
func (p *GlobalPrivilege) GetPath() string {
	return "/global/"
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
func (p *GlobalPrivilege) GetObjectType() privilege.ObjectType {
	return privilege.Global
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
	systemTablePrivilegeObject privilegeobject.SyntheticPrivilegeObject,
) (privileges *catpb.PrivilegeDescriptor, retErr error) {
	query := fmt.Sprintf(
		`SELECT username, privileges, grant_options FROM system.%s WHERE path='%s'`,
		catconstants.SystemPrivilegeTableName,
		systemTablePrivilegeObject.GetPath())

	it, err := planner.QueryIteratorEx(ctx, `get-system-privileges`,
		sessiondata.NodeUserSessionDataOverride, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	privileges = &catpb.PrivilegeDescriptor{}
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		user := tree.MustBeDString(it.Cur()[0])
		privArr := tree.MustBeDArray(it.Cur()[1])
		var privilegeStrings []string
		for _, elem := range privArr.Array {
			privilegeStrings = append(privilegeStrings, string(tree.MustBeDString(elem)))
		}

		grantOptionArr := tree.MustBeDArray(it.Cur()[2])
		var grantOptionStrings []string
		for _, elem := range grantOptionArr.Array {
			grantOptionStrings = append(grantOptionStrings, string(tree.MustBeDString(elem)))
		}
		privs, err := privilege.ListFromStrings(privilegeStrings)
		if err != nil {
			return nil, err
		}
		grantOptions, err := privilege.ListFromStrings(grantOptionStrings)
		if err != nil {
			return nil, err
		}
		privsWithGrantOption := privilege.ListFromBitField(
			privs.ToBitField()&grantOptions.ToBitField(),
			systemTablePrivilegeObject.GetObjectType(),
		)
		privsWithoutGrantOption := privilege.ListFromBitField(
			privs.ToBitField()&^privsWithGrantOption.ToBitField(),
			systemTablePrivilegeObject.GetObjectType(),
		)
		privileges.Grant(
			username.MakeSQLUsernameFromPreNormalizedString(string(user)),
			privsWithGrantOption,
			true, /* withGrantOption */
		)
		privileges.Grant(
			username.MakeSQLUsernameFromPreNormalizedString(string(user)),
			privsWithoutGrantOption,
			false, /* withGrantOption */
		)
	}

	// To avoid having to insert a row for public for each virtual
	// table into system.privileges, we assume that if there is
	// NO entry for public in the PrivilegeDescriptor, Public has
	// grant. If there is an empty row for Public, then public
	// does not have grant.
	if systemTablePrivilegeObject.GetObjectType() == privilege.VirtualTable {
		if _, found := privileges.FindUser(username.PublicRoleName()); !found {
			privileges.Grant(username.PublicRoleName(), privilege.List{privilege.SELECT}, false)
		}
	}

	privilegeObjectType := systemTablePrivilegeObject.GetObjectType()
	// We use InvalidID to skip checks on the root/admin roles having
	// privileges.
	if err := privileges.Validate(descpb.InvalidID, privilegeObjectType, systemTablePrivilegeObject.GetName(), privilege.GetValidPrivilegesForObject(privilegeObjectType)); err != nil {
		return nil, err
	}

	return privileges, nil
}
