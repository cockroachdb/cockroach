// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colinfo

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
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

const (
	// MVCCTimestampColumnID is the ColumnID of the MVCC timestamp column.
	MVCCTimestampColumnID descpb.ColumnID = math.MaxUint32 - iota

	// TableOIDColumnID is the ID of the tableoid system column.
	TableOIDColumnID

	numSystemColumns = iota
)

func init() {
	if len(AllSystemColumnDescs) != numSystemColumns {
		panic("need to update system column IDs or descriptors")
	}
	if catalog.NumSystemColumns != numSystemColumns {
		panic("need to update catalog.NumSystemColumns")
	}
	if catalog.SmallestSystemColumnColumnID != math.MaxUint32-numSystemColumns+1 {
		panic("need to update catalog.SmallestSystemColumnColumnID")
	}
	for _, desc := range AllSystemColumnDescs {
		if desc.SystemColumnKind != GetSystemColumnKindFromColumnID(desc.ID) {
			panic("system column ID ordering must match SystemColumnKind value ordering")
		}
	}
}

// AllSystemColumnDescs contains all registered system columns.
var AllSystemColumnDescs = []descpb.ColumnDescriptor{
	MVCCTimestampColumnDesc,
	TableOIDColumnDesc,
}

// MVCCTimestampColumnDesc is a column descriptor for the MVCC system column.
var MVCCTimestampColumnDesc = descpb.ColumnDescriptor{
	Name:             MVCCTimestampColumnName,
	Type:             MVCCTimestampColumnType,
	Hidden:           true,
	Nullable:         true,
	SystemColumnKind: catpb.SystemColumnKind_MVCCTIMESTAMP,
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
	SystemColumnKind: catpb.SystemColumnKind_TABLEOID,
	ID:               TableOIDColumnID,
}

// TableOIDColumnName is the name of the tableoid system column.
const TableOIDColumnName = "tableoid"

// IsColIDSystemColumn returns whether a column ID refers to a system column.
func IsColIDSystemColumn(colID descpb.ColumnID) bool {
	return colID > math.MaxUint32-numSystemColumns
}

// GetSystemColumnKindFromColumnID returns the kind of system column that colID
// refers to.
func GetSystemColumnKindFromColumnID(colID descpb.ColumnID) catpb.SystemColumnKind {
	i := math.MaxUint32 - uint32(colID)
	if i >= numSystemColumns {
		return catpb.SystemColumnKind_NONE
	}
	return catpb.SystemColumnKind_NONE + 1 + catpb.SystemColumnKind(i)
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
