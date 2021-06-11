// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
// * crdb_internal_mvcc_timestamp: contains a representation of the row's
//   HLC timestamp.
// * tableoid: A Postgres system column that contains the ID of the table
//   that a particular row came from.

// AllSystemColumnDescs contains all registered system columns.
var AllSystemColumnDescs = []descpb.ColumnDescriptor{
	MVCCTimestampColumnDesc,
	TableOIDColumnDesc,
}

// MVCCTimestampColumnID is the ColumnID of the MVCC timesatmp column. Future
// system columns will have ID's that decrement from this value.
const MVCCTimestampColumnID = math.MaxUint32

// TableOIDColumnID is the ID of the tableoid system column.
const TableOIDColumnID = MVCCTimestampColumnID - 1

// MVCCTimestampColumnDesc is a column descriptor for the MVCC system column.
var MVCCTimestampColumnDesc = descpb.ColumnDescriptor{
	Name:             MVCCTimestampColumnName,
	Type:             MVCCTimestampColumnType,
	Hidden:           true,
	Nullable:         true,
	SystemColumnKind: descpb.SystemColumnKind_MVCCTIMESTAMP,
	ID:               MVCCTimestampColumnID,
}

// MVCCTimestampColumnName is the name of the MVCC timestamp system column.
const MVCCTimestampColumnName = "crdb_internal_mvcc_timestamp"

// MVCCTimestampColumnType is the type of the MVCC timestamp system column.
var MVCCTimestampColumnType = types.Decimal

// TableOIDColumnDesc is a column descriptor for the tableoid column.
var TableOIDColumnDesc = descpb.ColumnDescriptor{
	Name:             TableOIDColumnName,
	Type:             types.Oid,
	Hidden:           true,
	Nullable:         true,
	SystemColumnKind: descpb.SystemColumnKind_TABLEOID,
	ID:               TableOIDColumnID,
}

// TableOIDColumnName is the name of the tableoid system column.
const TableOIDColumnName = "tableoid"

// IsColIDSystemColumn returns whether a column ID refers to a system column.
func IsColIDSystemColumn(colID descpb.ColumnID) bool {
	return GetSystemColumnKindFromColumnID(colID) != descpb.SystemColumnKind_NONE
}

// GetSystemColumnKindFromColumnID returns the kind of system column that colID
// refers to.
func GetSystemColumnKindFromColumnID(colID descpb.ColumnID) descpb.SystemColumnKind {
	for i := range AllSystemColumnDescs {
		if AllSystemColumnDescs[i].ID == colID {
			return AllSystemColumnDescs[i].SystemColumnKind
		}
	}
	return descpb.SystemColumnKind_NONE
}

// IsSystemColumnName returns whether or not a name is a reserved system
// column name.
func IsSystemColumnName(name string) bool {
	for i := range AllSystemColumnDescs {
		if AllSystemColumnDescs[i].Name == name {
			return true
		}
	}
	return false
}
