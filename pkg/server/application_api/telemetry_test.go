// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestThrottlingMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	testtime := timeutil.Now()

	s := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				LicenseTestingKnobs: &license.TestingKnobs{
					OverrideStartTime: &testtime,
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	sys := s.SystemLayer(1)

	for i := range s.NodeIDs() {
		s.SystemLayer(i).DiagnosticsReporter().(*diagnostics.Reporter).LastSuccessfulTelemetryPing.Store(testtime.Unix())
		s.ApplicationLayer(i).DiagnosticsReporter().(*diagnostics.Reporter).LastSuccessfulTelemetryPing.Store(testtime.Unix())
	}

	for _, tc := range []struct {
		name                        string
		license                     *licenseccl.License
		lastSuccessfulTelemetryPing int64
		expected                    serverpb.GetThrottlingMetadataResponse
	}{
		{
			name:    "no license",
			license: nil,
			expected: serverpb.GetThrottlingMetadataResponse{
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add(7 * 24 * time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{},
			},
		},
		{
			name: "free license",
			license: &licenseccl.License{
				Type:              licenseccl.License_Free,
				ValidUntilUnixSec: testtime.Add(1 * time.Hour).Unix(),
			},
			lastSuccessfulTelemetryPing: testtime.Unix(),
			expected: serverpb.GetThrottlingMetadataResponse{
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add((30 * 24 * time.Hour) + time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{},
				HasTelemetryDeadline:         true,
				LastTelemetryReceivedSeconds: testtime.Unix(),
				TelemetryDeadlineSeconds:     testtime.Unix() + 604800,
			},
		},
		{
			name: "trial license",
			license: &licenseccl.License{
				Type:              licenseccl.License_Trial,
				ValidUntilUnixSec: testtime.Add(1 * time.Hour).Unix(),
			},
			lastSuccessfulTelemetryPing: testtime.Unix(),
			expected: serverpb.GetThrottlingMetadataResponse{
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add((7 * 24 * time.Hour) + time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{},
				HasTelemetryDeadline:         true,
				LastTelemetryReceivedSeconds: testtime.Unix(),
				TelemetryDeadlineSeconds:     testtime.Unix() + 604800,
			},
		},
		{
			name: "enterprise license",
			license: &licenseccl.License{
				Type: licenseccl.License_Enterprise,
			},
			expected: serverpb.GetThrottlingMetadataResponse{
				NodeIdsWithTelemetryProblems: []string{},
			},
		},
		{
			name: "evaluation license",
			license: &licenseccl.License{
				Type:              licenseccl.License_Evaluation,
				ValidUntilUnixSec: testtime.Add(1 * time.Hour).Unix(),
			},
			lastSuccessfulTelemetryPing: testtime.Unix(),
			expected: serverpb.GetThrottlingMetadataResponse{
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add((30 * 24 * time.Hour) + time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{},
				HasTelemetryDeadline:         false,
			},
		},
		{
			name: "free license with missing telemetry",
			license: &licenseccl.License{
				Type:              licenseccl.License_Free,
				ValidUntilUnixSec: testtime.Add(1 * time.Hour).Unix(),
			},
			lastSuccessfulTelemetryPing: testtime.Add(-2 * 24 * time.Hour).Unix(),
			expected: serverpb.GetThrottlingMetadataResponse{
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add((30 * 24 * time.Hour) + time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{"1", "2", "3"},
				HasTelemetryDeadline:         true,
				LastTelemetryReceivedSeconds: testtime.Add(-2 * 24 * time.Hour).Unix(),
				TelemetryDeadlineSeconds:     testtime.Add(5 * 24 * time.Hour).Unix(), // 7 days from last ping.
			},
		},
		{
			name: "free license expired",
			license: &licenseccl.License{
				Type:              licenseccl.License_Free,
				ValidUntilUnixSec: testtime.Add(-1 * time.Hour).Unix(),
			},
			lastSuccessfulTelemetryPing: testtime.Unix(),
			expected: serverpb.GetThrottlingMetadataResponse{
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add((30 * 24 * time.Hour) - time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{},
				HasTelemetryDeadline:         true,
				LastTelemetryReceivedSeconds: testtime.Unix(),
				TelemetryDeadlineSeconds:     testtime.Add(7 * 24 * time.Hour).Unix(), // 7 days from last ping.
			},
		},
		{
			name: "free license expired and grace period over",
			license: &licenseccl.License{
				Type:              licenseccl.License_Free,
				ValidUntilUnixSec: testtime.Add(-40 * 24 * time.Hour).Unix(),
			},
			lastSuccessfulTelemetryPing: testtime.Unix(),
			expected: serverpb.GetThrottlingMetadataResponse{
				Throttled:                    true,
				ThrottleExplanation:          fmt.Sprintf("License expired on %s. The maximum number of concurrently open transactions has been reached.", timeutil.Unix(testtime.Add(-40*24*time.Hour).Unix(), 0)),
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add(-10 * 24 * time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{},
				HasTelemetryDeadline:         true,
				LastTelemetryReceivedSeconds: testtime.Unix(),
				TelemetryDeadlineSeconds:     testtime.Unix() + 604800,
			},
		},
		{
			name: "free license telemetry missing and telemetry deadline over",
			license: &licenseccl.License{
				Type:              licenseccl.License_Free,
				ValidUntilUnixSec: testtime.Add(10 * 24 * time.Hour).Unix(),
			},
			lastSuccessfulTelemetryPing: testtime.Add(-8 * 24 * time.Hour).Unix(),
			expected: serverpb.GetThrottlingMetadataResponse{
				Throttled:                    true,
				ThrottleExplanation:          "The maximum number of concurrently open transactions has been reached because the license requires diagnostic reporting, but none has been received by Cockroach Labs.",
				HasGracePeriod:               true,
				GracePeriodEndSeconds:        testtime.Add(40 * 24 * time.Hour).Unix(),
				NodeIdsWithTelemetryProblems: []string{"1", "2", "3"},
				HasTelemetryDeadline:         true,
				LastTelemetryReceivedSeconds: testtime.Add(-8 * 24 * time.Hour).Unix(),
				TelemetryDeadlineSeconds:     testtime.Add(-8*24*time.Hour).Unix() + 604800,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.license != nil {
				lic, err := tc.license.Encode()
				require.NoError(t, err)
				_, err = sys.SQLConn(t).Exec(
					fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", lic),
				)
				require.NoError(t, err)
			}

			if tc.lastSuccessfulTelemetryPing != 0 {
				for i := range s.NodeIDs() {
					s.SystemLayer(i).DiagnosticsReporter().(*diagnostics.Reporter).LastSuccessfulTelemetryPing.Store(
						tc.lastSuccessfulTelemetryPing,
					)
					s.ApplicationLayer(i).DiagnosticsReporter().(*diagnostics.Reporter).LastSuccessfulTelemetryPing.Store(
						tc.lastSuccessfulTelemetryPing,
					)
				}
			}

			testutils.SucceedsSoon(t, func() error {
				var resp serverpb.GetThrottlingMetadataResponse
				if err := srvtestutils.GetStatusJSONProto(sys, "throttling", &resp); err != nil {
					t.Fatal(err)
				}

				sort.Strings(resp.NodeIdsWithTelemetryProblems)
				if !assert.ObjectsAreEqual(tc.expected, resp) {
					return errors.Newf("not equal: %v and %v", tc.expected, resp)
				}
				return nil
			})
		})
	}

}
