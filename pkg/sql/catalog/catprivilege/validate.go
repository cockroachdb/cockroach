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
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	// Cluster restores move certain system tables to a higher ID to prevent
	// conflicts with non-system descriptors that are going to be restored. The
	// newly created tables in the system database will be given ReadWrite
	// privileges.
	//
	// TODO(adityamaru,dt): Remove once we fix the handling of dynamic system
	// table IDs during restore.
	if objectNameKey.GetParentID() == keys.SystemDatabaseID &&
		strings.Contains(objectNameKey.GetName(), RestoreCopySystemTablePrefix) {
		return privilege.ReadWriteData
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
