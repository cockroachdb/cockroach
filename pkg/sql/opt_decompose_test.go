// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func testTableDesc(
	t *testing.T, intermediate func(desc *MutableTableDescriptor),
) *sqlbase.ImmutableTableDescriptor {
	mut := sqlbase.NewMutableCreatedTableDescriptor(sqlbase.TableDescriptor{
		Name:     "test",
		ID:       1001,
		ParentID: 1000,
		Columns: []sqlbase.ColumnDescriptor{
			{Name: "a", Type: *types.Int},
			{Name: "b", Type: *types.Int},
			{Name: "c", Type: *types.Bool},
			{Name: "d", Type: *types.Bool},
			{Name: "e", Type: *types.Bool},
			{Name: "f", Type: *types.Bool},
			{Name: "g", Type: *types.Bool},
			{Name: "h", Type: *types.Float},
			{Name: "i", Type: *types.String},
			{Name: "j", Type: *types.Int},
			{Name: "k", Type: *types.Bytes},
			{Name: "l", Type: *types.Decimal},
			{Name: "m", Type: *types.Decimal},
			{Name: "n", Type: *types.Date},
			{Name: "o", Type: *types.Timestamp},
			{Name: "p", Type: *types.TimestampTZ},
			{Name: "q", Type: *types.Int, Nullable: true},
			{Name: "r", Type: *types.Uuid},
			{Name: "s", Type: *types.INet},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			Name: "primary", Unique: true, ColumnNames: []string{"a"},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		},
		Privileges:    sqlbase.NewDefaultPrivilegeDescriptor(),
		FormatVersion: sqlbase.FamilyFormatVersion,
	})
	intermediate(mut)
	if err := mut.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	return sqlbase.NewImmutableTableDescriptor(mut.TableDescriptor)
}
