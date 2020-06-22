// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Similar to Postgres, we also expose hidden system columns on tables.
// These system columns are not explicitly part of the TableDescriptor,
// and instead are constructs that are manipulated purely during planning.
// As of now, these system columns are able to be generated from the set
// of KV's that make up a row, and are produced by the row and cfetcher.
// Each system column is given a kind, and associated with a particular
// "ID offset" from a TableDescriptor's NextColumnID field. This is done
// so that each system column can have a unique column ID from all other
// columns present in the table. As of now, we support the following system
// columns:
// * MVCC Timestamp: contains a representation of the row's HLC timestamp.

// MVCCTimestampColumnName is the name of the MVCC timestamp system column.
const MVCCTimestampColumnName = "mvcc_timestamp"

// MVCCTimestampColumnType is the type of the MVCC timestamp system column.
var MVCCTimestampColumnType = types.Decimal

// NewMVCCTimestampColumnDesc creates a column descriptor for the MVCC timestamp
// system column. The returned descriptor still needs an ID to be set.
func NewMVCCTimestampColumnDesc() ColumnDescriptor {
	return ColumnDescriptor{
		Name:             MVCCTimestampColumnName,
		Type:             MVCCTimestampColumnType,
		Hidden:           true,
		Nullable:         true,
		SystemColumnKind: SystemColumnKind_MVCCTIMESTAMP,
	}
}

// IsColIDSystemColumn returns whether a column ID refers to a system column.
func IsColIDSystemColumn(desc *TableDescriptor, colID ColumnID) bool {
	return colID >= desc.NextColumnID
}

// GetSystemColumnDescriptorFromID returns a column descriptor corresponding
// to the system column referred to by the input column ID.
func GetSystemColumnDescriptorFromID(
	desc *TableDescriptor, colID ColumnID,
) (*ColumnDescriptor, error) {
	switch off := colID - desc.NextColumnID; off {
	case 0:
		colDesc := NewMVCCTimestampColumnDesc()
		colDesc.ID = desc.NextColumnID
		return &colDesc, nil
	default:
		return nil, errors.AssertionFailedf("unsupported system column ID offset %d", off)
	}
}

// GetSystemColumnKindFromColumnID returns the kind of system column that colID
// refers to.
func GetSystemColumnKindFromColumnID(desc *TableDescriptor, colID ColumnID) SystemColumnKind {
	if colID < desc.NextColumnID {
		return SystemColumnKind_NONE
	}
	switch colID - desc.NextColumnID {
	case 0:
		return SystemColumnKind_MVCCTIMESTAMP
	default:
		return SystemColumnKind_NONE
	}
}

// GetSystemColumnIDByKind returns the column ID of the desired system column.
func GetSystemColumnIDByKind(desc *TableDescriptor, kind SystemColumnKind) (ColumnID, error) {
	switch kind {
	case SystemColumnKind_MVCCTIMESTAMP:
		return desc.NextColumnID, nil
	default:
		return 0, errors.Newf("invalid system column kind %s", kind.String())
	}
}

// GetSystemColumnTypeForKind returns the types.T of the input system column.
func GetSystemColumnTypeForKind(kind SystemColumnKind) *types.T {
	switch kind {
	case SystemColumnKind_MVCCTIMESTAMP:
		return MVCCTimestampColumnType
	default:
		return nil
	}
}

// IsSystemColumnName returns whether or not a name is a reserved system
// column name.
func IsSystemColumnName(name string) bool {
	switch name {
	case MVCCTimestampColumnName:
		return true
	default:
		return false
	}
}
