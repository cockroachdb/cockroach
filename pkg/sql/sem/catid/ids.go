// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package catid is a low-level package exporting ID types.
package catid

import (
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/lib/pq/oid"
)

// DescID is a custom type for {Database,Table}Descriptor IDs.
type DescID uint32

// InvalidDescID is the uninitialised descriptor id.
const InvalidDescID DescID = 0

// SafeValue implements the redact.SafeValue interface.
func (DescID) SafeValue() {}

// TypeIDToOID converts a type descriptor ID into a type OID.
func TypeIDToOID(id DescID) oid.Oid {
	return idToUserDefinedOID(id)
}

// FuncIDToOID converts a function descriptor ID into a function OID.
func FuncIDToOID(id DescID) oid.Oid {
	return idToUserDefinedOID(id)
}

func idToUserDefinedOID(id DescID) oid.Oid {
	return oid.Oid(id) + oidext.CockroachPredefinedOIDMax
}

// UserDefinedOIDToID converts the OID of a user-defined type or function
// to a descriptor ID. Returns zero if the OID is not user-defined.
func UserDefinedOIDToID(oid oid.Oid) DescID {
	if !IsOIDUserDefined(oid) {
		return InvalidDescID
	}
	return DescID(oid) - oidext.CockroachPredefinedOIDMax
}

// IsOIDUserDefined returns true if oid is greater than
// CockroachPredefinedOIDMax, otherwise false.
func IsOIDUserDefined(oid oid.Oid) bool {
	return DescID(oid) > oidext.CockroachPredefinedOIDMax
}

// ColumnID is a custom type for Column IDs.
type ColumnID uint32

// SafeValue implements the redact.SafeValue interface.
func (ColumnID) SafeValue() {}

// FamilyID is a custom type for ColumnFamilyDescriptor IDs.
type FamilyID uint32

// SafeValue implements the redact.SafeValue interface.
func (FamilyID) SafeValue() {}

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID uint32

// SafeValue implements the redact.SafeValue interface.
func (IndexID) SafeValue() {}

// ConstraintID is a custom type for TableDescriptor constraint IDs.
type ConstraintID uint32

// SafeValue implements the redact.SafeValue interface.
func (ConstraintID) SafeValue() {}

// TriggerID is a custom type for TableDescriptor trigger IDs.
type TriggerID uint32

// SafeValue implements the redact.SafeValue interface.
func (TriggerID) SafeValue() {}

// PGAttributeNum is a custom type for Column's logical order.
type PGAttributeNum uint32

// SafeValue implements the redact.SafeValue interface.
func (PGAttributeNum) SafeValue() {}

// RoleID is a custom type for a role id.
type RoleID uint32

// PolicyID is a custom type for TableDescriptor policy IDs.
type PolicyID uint32

// SafeValue implements the redact.SafeValue interface.
func (PolicyID) SafeValue() {}
