// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestHealthTelemetry confirms that hits on some status endpoints increment
// feature telemetry counters.
func TestHealthTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	rows, err := db.Query("SELECT * FROM crdb_internal.feature_usage WHERE feature_name LIKE 'monitoring%' AND usage_count > 0;")
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}

	initialCounts := make(map[string]int)
	for rows.Next() {
		var featureName string
		var usageCount int

		if err := rows.Scan(&featureName, &usageCount); err != nil {
			t.Fatal(err)
		}

		initialCounts[featureName] = usageCount
	}

	var details serverpb.DetailsResponse
	if err := serverutils.GetJSONProto(s, "/health", &details); err != nil {
		t.Fatal(err)
	}
	if _, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String()); err != nil {
		t.Fatal(err)
	}

	expectedCounts := map[string]int{
		"monitoring.prometheus.vars": 1,
		"monitoring.health.details":  1,
	}

	rows2, err := db.Query("SELECT feature_name, usage_count FROM crdb_internal.feature_usage WHERE feature_name LIKE 'monitoring%' AND usage_count > 0;")
	defer func() {
		if err := rows2.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}

	for rows2.Next() {
		var featureName string
		var usageCount int

		if err := rows2.Scan(&featureName, &usageCount); err != nil {
			t.Fatal(err)
		}

		usageCount -= initialCounts[featureName]
		if count, ok := expectedCounts[featureName]; ok {
			if count != usageCount {
				t.Fatalf("expected %d count for feature %s, got %d", count, featureName, usageCount)
			}
			delete(expectedCounts, featureName)
		}
	}

	if len(expectedCounts) > 0 {
		t.Fatalf("%d expected telemetry counters not emitted", len(expectedCounts))
	}
}

func TestDiagnosticsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer s.Stopper().Stop(context.Background())

	var resp diagnosticspb.DiagnosticReport
	if err := srvtestutils.GetStatusJSONProto(s, "diagnostics/local", &resp); err != nil {
		t.Fatal(err)
	}

	// The endpoint just serializes result of getReportingInfo() which is already
	// tested elsewhere, so simply verify that we have a non-empty reply.
	if expected, actual := s.NodeID(), resp.Node.NodeID; expected != actual {
		t.Fatalf("expected %v got %v", expected, actual)
	}
}
