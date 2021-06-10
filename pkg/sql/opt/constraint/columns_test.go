// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestColumns_RemapColumns(t *testing.T) {
	var md opt.Metadata
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE tab (a INT PRIMARY KEY, b INT, c INT, d INT);")
	if err != nil {
		t.Fatal(err)
	}
	tn := tree.NewUnqualifiedTableName("tab")
	tab := catalog.Table(tn)

	from := md.AddTable(tab, &tree.TableName{})
	to := md.AddTable(tab, &tree.TableName{})

	var originalColumns Columns
	originalColumns.Init([]opt.OrderingColumn{
		opt.MakeOrderingColumn(from.ColumnID(0), false /* descending */),
		opt.MakeOrderingColumn(from.ColumnID(2), true /* descending */),
		opt.MakeOrderingColumn(from.ColumnID(3), false /* descending */),
	})

	remappedColumns := originalColumns.RemapColumns(from, to)

	expected := "/1/-3/4"
	if originalColumns.String() != expected {
		t.Errorf("\noriginal Columns were changed: %s", originalColumns.String())
	}

	expected = "/7/-9/10"
	if remappedColumns.String() != expected {
		t.Errorf("\nexpected: %s\nactual: %s\n", expected, remappedColumns.String())
	}
}
