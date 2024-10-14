// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	privList, err := privilege.ListFromBitField(op.Privileges.Privileges, desc.GetObjectType())
	if err != nil {
		return err
	}
	grantList, err := privilege.ListFromBitField(op.Privileges.WithGrantOption, desc.GetObjectType())
	if err != nil {
		return err
	}
	for _, priv := range privList {
		privs.Grant(
			user,
			privilege.List{priv},
			grantList.Contains(priv),
		)
	}
	return nil
}
