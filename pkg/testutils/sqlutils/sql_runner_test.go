// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that the RowsToStrMatrix doesn't swallow errors.
func TestRowsToStrMatrixError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// We'll run a query that only fails after returning some rows, so that the
	// error is discovered by RowsToStrMatrix below.
	rows, err := db.Query(
		"select case x when 5 then crdb_internal.force_error('00000', 'testing error') else x end from generate_series(1,5) as v(x);")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlutils.RowsToStrMatrix(rows); !testutils.IsError(err, "testing error") {
		t.Fatalf("expected 'testing error', got: %v", err)
	}
}
