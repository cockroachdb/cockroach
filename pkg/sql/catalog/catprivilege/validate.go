// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catprivilege

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// Validate validates a privilege descriptor.
func Validate(
	p catpb.PrivilegeDescriptor, objectNameKey catalog.NameKey, objectType privilege.ObjectType,
) error {
	return p.Validate(
		objectNameKey.GetParentID(),
		objectType,
		objectNameKey.GetName(),
		allowedSuperuserPrivileges(objectNameKey),
	)
}

// ValidateSuperuserPrivileges validates superuser privileges.
func ValidateSuperuserPrivileges(
	p catpb.PrivilegeDescriptor, objectNameKey catalog.NameKey, objectType privilege.ObjectType,
) error {
	return p.ValidateSuperuserPrivileges(
		objectNameKey.GetParentID(),
		objectType,
		objectNameKey.GetName(),
		allowedSuperuserPrivileges(objectNameKey),
	)
}

// ValidateDefaultPrivileges validates default privileges.
func ValidateDefaultPrivileges(p catpb.DefaultPrivilegeDescriptor) error {
	return p.Validate()
}

func allowedSuperuserPrivileges(objectNameKey catalog.NameKey) privilege.List {
	privs := SystemSuperuserPrivileges(objectNameKey)
	if privs != nil {
		return privs
	}
	return catpb.DefaultSuperuserPrivileges
}
