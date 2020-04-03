// Copyright 2020 The Cockroach Authors.
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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestAddLogicalColumnID writes a TableDescriptor to disk without
// LogicalColumnIDs set and reads it back expecting the LogicalColumnID
// to be updated on read and equal to ColumnID.
func TestAddLogicalColumnID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		name  string
		query string
	}{
		{"t1", "CREATE TABLE t1 (x INT);"},
		{"t2", "CREATE TABLE t2 (x INT, y INT);"},
	}
	for i := range tests {
		if _, err := sqlDB.Exec(tests[i].query); err != nil {
			t.Fatal(err)
		}

		removeLogicalColumnIDs := func(tDesc *sqlbase.TableDescriptor) {
			for i := range tDesc.Columns {
				tDesc.Columns[i].LogicalColumnID = 0
			}
		}

		desc := sqlbase.GetTableDescriptor(kvDB, "defaultdb", tests[i].name)

		err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := txn.NewBatch()
			removeLogicalColumnIDs(desc)
			if err := writeDescToBatch(ctx, false, s.ClusterSettings(), b, desc.ID, desc); err != nil {
				return err
			}
			return txn.Run(ctx, b)
		})

		if err != nil {
			t.Fatal(err)
		}

		desc = sqlbase.GetTableDescriptor(kvDB, "defaultdb", desc.Name)

		for i := range desc.Columns {
			if desc.Columns[i].LogicalColumnID != desc.Columns[i].ID {
				t.Fatalf("Expected LogicalColumnID to be %d, got %d.",
					desc.Columns[i].ID, desc.Columns[i].LogicalColumnID)
			}
		}
	}
}
