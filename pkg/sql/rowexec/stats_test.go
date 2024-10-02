// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestInputStatCollector verifies that an inputStatCollector correctly collects
// stats from an input.
func TestInputStatCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numRows = 100

	isc := newInputStatCollector(
		distsqlutils.NewRowBuffer(types.OneIntCol, randgen.MakeIntRows(numRows, 1), distsqlutils.RowBufferArgs{}),
	)
	for row, meta := isc.Next(); row != nil || meta != nil; row, meta = isc.Next() {
	}
	if isc.stats.NumTuples.Value() != numRows {
		t.Fatalf("counted %s rows but expected %d", isc.stats.NumTuples, numRows)
	}
}
