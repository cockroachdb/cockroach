// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestInputStatCollector verifies that an InputStatCollector correctly collects
// stats from an input.
func TestInputStatCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numRows = 100

	isc := execinfra.NewInputStatCollector(
		distsqlutils.NewRowBuffer(sqlbase.OneIntCol, sqlbase.MakeIntRows(numRows, 1), distsqlutils.RowBufferArgs{}),
	)
	for row, meta := isc.Next(); row != nil || meta != nil; row, meta = isc.Next() {
	}
	if isc.NumRows != numRows {
		t.Fatalf("counted %d rows but expected %d", isc.NumRows, numRows)
	}
}
