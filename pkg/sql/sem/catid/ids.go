// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	return oid.Oid(id) + oidext.CockroachPredefinedOIDMax
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

// ConstraintID is a custom type for TableDeascriptor constraint IDs.
type ConstraintID uint32

// SafeValue implements the redact.SafeValue interface.
func (ConstraintID) SafeValue() {}

// PGAttributeNum is a custom type for Column's logical order.
type PGAttributeNum uint32

// SafeValue implements the redact.SafeValue interface.
func (PGAttributeNum) SafeValue() {}
