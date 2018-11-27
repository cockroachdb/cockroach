// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	defer s.Stopper().Stop(context.TODO())

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
