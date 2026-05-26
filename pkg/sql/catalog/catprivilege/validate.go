// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catprivilege

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
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

// ValidateSyntheticPrivilegeObject validates a Object.
func ValidateSyntheticPrivilegeObject(systemPrivilegeObject syntheticprivilege.Object) error {
	out, err := syntheticprivilege.Parse(systemPrivilegeObject.GetPath())
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(out, systemPrivilegeObject) {
		return errors.Newf("system privilege object is invalid, expected %v, got %v", out, systemPrivilegeObject)
	}

	return nil
}
