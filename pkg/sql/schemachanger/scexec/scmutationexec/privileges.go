// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (i *immediateVisitor) UpdateOwner(ctx context.Context, op scop.UpdateOwner) error {
	desc, err := i.checkOutDescriptor(ctx, op.Owner.DescriptorID)
	if err != nil {
		return err
	}
	privs := desc.GetPrivileges()
	privs.SetOwner(username.SQLUsernameProto(op.Owner.Owner).Decode())
	return nil
}

func (i *immediateVisitor) UpdateUserPrivileges(
	ctx context.Context, op scop.UpdateUserPrivileges,
) error {
	desc, err := i.checkOutDescriptor(ctx, op.Privileges.DescriptorID)
	if err != nil {
		return err
	}

	privs := desc.GetPrivileges()
	user := username.SQLUsernameProto(op.Privileges.UserName).Decode()
	privs.RemoveUser(user)
	privList := privilege.ListFromBitField(op.Privileges.Privileges, desc.GetObjectType())
	grantList := privilege.ListFromBitField(op.Privileges.WithGrantOption, desc.GetObjectType())
	for _, priv := range privList {
		privs.Grant(
			user,
			privilege.List{priv},
			grantList.Contains(priv),
		)
	}
	return nil
}
