// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestShowLastQueryStatistics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, sqlConn, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlConn.Exec("CREATE TABLE t(a INT)"); err != nil {
		t.Fatal(err)
	}

	rows, err := sqlConn.Query("SHOW LAST QUERY STATISTICS")
	if err != nil {
		t.Fatalf("show last query statistics failed: %v", err)
	}
	defer rows.Close()

	var parseLatency string
	var planLatency string
	var execLatency string
	var serviceLatency string

	rows.Next()
	if err := rows.Scan(&parseLatency, &planLatency, &execLatency, &serviceLatency); err != nil {
		t.Fatalf("unexpected error while reading last query statistics: %v", err)
	}

	parseInterval, err := tree.ParseDInterval(parseLatency)
	if err != nil {
		t.Fatal(err)
	}
	planInterval, err := tree.ParseDInterval(planLatency)
	if err != nil {
		t.Fatal(err)
	}
	execInterval, err := tree.ParseDInterval(execLatency)
	if err != nil {
		t.Fatal(err)
	}
	serviceInterval, err := tree.ParseDInterval(serviceLatency)
	if err != nil {
		t.Fatal(err)
	}

	if parseInterval.AsFloat64() <= 0 || parseInterval.AsFloat64() > 0.001 {
		t.Fatalf("unexpected parse latency: %v", parseInterval.AsFloat64())
	}

	if planInterval.AsFloat64() <= 0 || planInterval.AsFloat64() > 0.001 {
		t.Fatalf("unexpected plan latency: %v", planInterval.AsFloat64())
	}

	if serviceInterval.AsFloat64() <= 0 || serviceInterval.AsFloat64() > 0.1 {
		t.Fatalf("unexpected service latency: %v", serviceInterval.AsFloat64())
	}

	if execInterval.AsFloat64() <= 0 || execInterval.AsFloat64() > 0.1 {
		t.Fatalf("unexpected execution latency: %v", execInterval.AsFloat64())
	}

	if rows.Next() {
		t.Fatalf("unexpected number of rows returned by last query statistics: %v", err)
	}
}
