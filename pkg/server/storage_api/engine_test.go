// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// TestStatusEngineStatsJson ensures that the output response for the engine
// stats contains the required fields.
func TestStatusEngineStatsJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(110020),

		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	t.Logf("using admin URL %s", s.AdminURL())

	var engineStats serverpb.EngineStatsResponse
	// Using SucceedsSoon because we have seen in the wild that
	// occasionally requests don't go through with error "transport:
	// error while dialing: connection interrupted (did the remote node
	// shut down or are there networking issues?)"
	testutils.SucceedsSoon(t, func() error {
		return srvtestutils.GetStatusJSONProto(s, "enginestats/local", &engineStats)
	})

	formattedStats := debug.FormatLSMStats(engineStats.StatsByStoreId)
	re := regexp.MustCompile(`^(Store \d+:(.|\n)+?)+$`)
	if !re.MatchString(formattedStats) {
		t.Fatal(errors.Errorf("expected engine metrics to be correctly formatted, got:\n %s", formattedStats))
	}
}
