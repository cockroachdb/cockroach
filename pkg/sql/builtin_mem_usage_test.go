// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// TestAggregatesMonitorMemory verifies that the aggregates incrementally
// record their memory usage as they build up their result.
func TestAggregatesMonitorMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create a table with a modest number of long strings, with the
	// intention of using them to exhaust the distsql workmem budget (which
	// is reduced below).
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TABLE d.t (a STRING)
`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO d.t VALUES (repeat('a', 50000))`); err != nil {
			t.Fatal(err)
		}
	}
	// We expect that the aggregated value will use about 5MB, so set the
	// workmem limit to be slightly less.
	if _, err := sqlDB.Exec(`SET distsql_workmem = '4MiB'`); err != nil {
		t.Fatal(err)
	}

	for _, statement := range []string{
		// By avoiding printing the aggregate results we prevent anything
		// besides the aggregate itself from being able to catch the large
		// memory usage.
		// TODO(yuzefovich): for now, all of the test queries are commented out
		// because they don't trigger the "memory budget exceeded" error due to
		// #100548. Uncomment them once that issue is resolved.
		// `SELECT length(concat_agg(a)) FROM d.t`,
		// `SELECT array_length(array_agg(a), 1) FROM d.t`,
		// `SELECT json_typeof(json_agg(a)) FROM d.t`,
	} {
		_, err := sqlDB.Exec(statement)
		if pqErr := (*pq.Error)(nil); !errors.As(err, &pqErr) || pgcode.MakeCode(string(pqErr.Code)) != pgcode.OutOfMemory {
			t.Fatalf("Expected \"%s\" to consume too much memory", statement)
		}
	}
}
