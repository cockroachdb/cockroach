// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package application_api_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestMetricsMetadata ensures that the server's recorder return metrics and
// that each metric has a Name, Help, Unit, and DisplayUnit defined.
func TestMetricsMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.(*server.TestServer)

	metricsMetadata := s.MetricsRecorder().GetMetricsMetadata()

	if len(metricsMetadata) < 200 {
		t.Fatal("s.recorder.GetMetricsMetadata() failed sanity check; didn't return enough metrics.")
	}

	for _, v := range metricsMetadata {
		if v.Name == "" {
			t.Fatal("metric missing name.")
		}
		if v.Help == "" {
			t.Fatalf("%s missing Help.", v.Name)
		}
		if v.Measurement == "" {
			t.Fatalf("%s missing Measurement.", v.Name)
		}
		if v.Unit == 0 {
			t.Fatalf("%s missing Unit.", v.Name)
		}
	}
}

// TestStatusVars verifies that prometheus metrics are available via the
// /_status/vars and /_status/load endpoints.
func TestStatusVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String()); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sql_bytesout counter\nsql_bytesout")) {
		t.Errorf("expected sql_bytesout, got: %s", body)
	}
	if body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"load").String()); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sys_cpu_user_ns gauge\nsys_cpu_user_ns")) {
		t.Errorf("expected sys_cpu_user_ns, got: %s", body)
	}
}

// TestStatusVarsTxnMetrics verifies that the metrics from the /_status/vars
// endpoint for txns and the special cockroach_restart savepoint are correct.
func TestStatusVarsTxnMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer db.Close()
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec("BEGIN;" +
		"SAVEPOINT cockroach_restart;" +
		"SELECT 1;" +
		"RELEASE SAVEPOINT cockroach_restart;" +
		"ROLLBACK;"); err != nil {
		t.Fatal(err)
	}

	body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte("sql_txn_begin_count{node_id=\"1\"} 1")) {
		t.Errorf("expected `sql_txn_begin_count{node_id=\"1\"} 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_restart_savepoint_count{node_id=\"1\"} 1")) {
		t.Errorf("expected `sql_restart_savepoint_count{node_id=\"1\"} 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_restart_savepoint_release_count{node_id=\"1\"} 1")) {
		t.Errorf("expected `sql_restart_savepoint_release_count{node_id=\"1\"} 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_txn_commit_count{node_id=\"1\"} 1")) {
		t.Errorf("expected `sql_txn_commit_count{node_id=\"1\"} 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_txn_rollback_count{node_id=\"1\"} 0")) {
		t.Errorf("expected `sql_txn_rollback_count{node_id=\"1\"} 0`, got: %s", body)
	}
}

func TestSpanStatsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(context.Background())

	httpClient, err := ts.GetAdminHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	var response roachpb.SpanStatsResponse
	span := roachpb.Span{
		Key:    roachpb.RKeyMin.AsRawKey(),
		EndKey: roachpb.RKeyMax.AsRawKey(),
	}
	request := roachpb.SpanStatsRequest{
		NodeID: "1",
		Spans:  []roachpb.Span{span},
	}

	url := ts.AdminURL().WithPath(apiconstants.StatusPrefix + "span").String()
	if err := httputil.PostJSON(httpClient, url, &request, &response); err != nil {
		t.Fatal(err)
	}
	initialRanges, err := ts.ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}
	responseSpanStats := response.SpanToStats[span.String()]
	if a, e := int(responseSpanStats.RangeCount), initialRanges; a != e {
		t.Errorf("expected %d ranges, found %d", e, a)
	}
}
