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
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Similar to Postgres, we also expose hidden system columns on tables.
// These system columns are not explicitly part of the TableDescriptor,
// and instead are constructs that are manipulated purely during planning.
// As of now, these system columns are able to be generated from the set
// of KV's that make up a row, and are produced by the row and cfetcher.
// Each system column is given a kind, and associated with a particular
// column ID that is counting down from math.MaxUint32. This is done so that
// each system column ID won't conflict with existing column ID's and also
// will be stable across all changes to the table.
// * MVCC Timestamp: contains a representation of the row's HLC timestamp.

// MVCCTimestampColumnName is the name of the MVCC timestamp system column.
const MVCCTimestampColumnName = "crdb_internal_mvcc_timestamp"

// MVCCTimestampColumnType is the type of the MVCC timestamp system column.
var MVCCTimestampColumnType = types.Decimal

// MVCCTimestampColumnID is the ColumnID of the MVCC timesatmp column. Future
// system columns will have ID's that decrement from this value.
const MVCCTimestampColumnID = math.MaxUint32

// NewMVCCTimestampColumnDesc creates a column descriptor for the MVCC timestamp
// system column.
func NewMVCCTimestampColumnDesc() *ColumnDescriptor {
	return &ColumnDescriptor{
		Name:             MVCCTimestampColumnName,
		Type:             MVCCTimestampColumnType,
		Hidden:           true,
		Nullable:         true,
		SystemColumnKind: SystemColumnKind_MVCCTIMESTAMP,
		ID:               MVCCTimestampColumnID,
	}
}

// IsColIDSystemColumn returns whether a column ID refers to a system column.
func IsColIDSystemColumn(colID ColumnID) bool {
	switch colID {
	case MVCCTimestampColumnID:
		return true
	default:
		return false
	}
}

// GetSystemColumnDescriptorFromID returns a column descriptor corresponding
// to the system column referred to by the input column ID.
func GetSystemColumnDescriptorFromID(colID ColumnID) (*ColumnDescriptor, error) {
	switch colID {
	case MVCCTimestampColumnID:
		return NewMVCCTimestampColumnDesc(), nil
	default:
		return nil, errors.AssertionFailedf("unsupported system column ID %d", colID)
	}
}

// GetSystemColumnKindFromColumnID returns the kind of system column that colID
// refers to.
func GetSystemColumnKindFromColumnID(colID ColumnID) SystemColumnKind {
	switch colID {
	case MVCCTimestampColumnID:
		return SystemColumnKind_MVCCTIMESTAMP
	default:
		return SystemColumnKind_NONE
	}
}

// GetSystemColumnIDByKind returns the column ID of the desired system column.
func GetSystemColumnIDByKind(kind SystemColumnKind) (ColumnID, error) {
	switch kind {
	case SystemColumnKind_MVCCTIMESTAMP:
		return MVCCTimestampColumnID, nil
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

// GetSystemColumnTypesAndDescriptors is a utility method to construct a set of
// types and column descriptors from an input list of system column kinds.
func GetSystemColumnTypesAndDescriptors(
	desc *TableDescriptor, kinds []SystemColumnKind,
) ([]*types.T, []ColumnDescriptor, error) {
	resTypes := make([]*types.T, len(kinds))
	resDescs := make([]ColumnDescriptor, len(kinds))
	for i, k := range kinds {
		resTypes[i] = GetSystemColumnTypeForKind(k)
		colID, err := GetSystemColumnIDByKind(k)
		if err != nil {
			return nil, nil, err
		}
		colDesc, err := GetSystemColumnDescriptorFromID(colID)
		if err != nil {
			return nil, nil, err
		}
		resDescs[i] = *colDesc
	}
	return resTypes, resDescs, nil
}
