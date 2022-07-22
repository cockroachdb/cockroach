// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privilege

import "github.com/cockroachdb/errors"

// TargetObjectType represents the type of object that is
// having its default privileges altered.
type TargetObjectType uint32

var targetObjectToPrivilegeObject = map[TargetObjectType]ObjectType{
	Tables:    Table,
	Sequences: Sequence,
	Schemas:   Schema,
	Types:     Type,
	Functions: Function,
}

// ToObjectType returns the privilege.ObjectType corresponding to
// the TargetObjectType.
func (t TargetObjectType) ToObjectType() ObjectType {
	return targetObjectToPrivilegeObject[t]
}

// The numbers are explicitly assigned since the DefaultPrivilegesPerObject
// map defined in the DefaultPrivilegesPerRole proto requires the key value
// for the object type to remain unchanged.
const (
	Tables    TargetObjectType = 1
	Sequences TargetObjectType = 2
	Types     TargetObjectType = 3
	Schemas   TargetObjectType = 4
	Functions TargetObjectType = 5
)

// GetTargetObjectTypes returns a slice of all the
// AlterDefaultPrivilegesTargetObjects.
func GetTargetObjectTypes() []TargetObjectType {
	return []TargetObjectType{
		Tables,
		Sequences,
		Types,
		Schemas,
		Functions,
	}
}

// String makes TargetObjectType a fmt.Stringer.
func (t TargetObjectType) String() string {
	switch t {
	case Tables:
		return "tables"
	case Sequences:
		return "sequences"
	case Types:
		return "types"
	case Schemas:
		return "schemas"
	case Functions:
		return "functions"
	default:
		panic(errors.AssertionFailedf("unknown TargetObjectType value: %d", t))
	}
}
