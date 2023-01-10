// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	io "io"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // for tenant functionality
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTenantsStorageMetrics ensures that tenant storage metrics are properly
// set upon split. There's two interesting cases:
//
//  1. The common case where the RHS and LHS of the split are co-located
//  2. The rare case where the RHS of the split has already been removed from
//     the store by the time the LHS applies the split.
//
// This test at time of writing only deals with ensuring that 1) is covered.
// TODO(ajwerner): add a test for 2).
func TestTenantsStorageMetricsOnSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})

	ctx := context.Background()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	defer store.Stopper().Stop(ctx)

	tenantID := serverutils.TestTenantID()
	codec := keys.MakeSQLCodec(tenantID)

	tenantPrefix := codec.TenantPrefix()
	_, _, err = s.SplitRange(tenantPrefix)
	require.NoError(t, err)

	splitKey := codec.TablePrefix(42)
	_, _, err = s.SplitRange(splitKey)
	require.NoError(t, err)

	require.NoError(t, db.Put(ctx, codec.TablePrefix(41), "bazbax"))
	require.NoError(t, db.Put(ctx, splitKey, "foobar"))
	require.NoError(t, db.DelRangeUsingTombstone(ctx, splitKey.Next(), splitKey.Next().Next()))

	// We want it to be the case that the MVCC stats for the individual ranges
	// of our tenant line up with the metrics for the tenant.
	check := func() error {
		var aggregateStats enginepb.MVCCStats
		var seen int
		store.VisitReplicas(func(replica *kvserver.Replica) (wantMore bool) {
			ri := replica.State(ctx)
			if ri.TenantID != tenantID.ToUint64() {
				return true
			}
			seen++
			aggregateStats.Add(*ri.Stats)
			return true
		})
		ex := metric.MakePrometheusExporter()
		scrape := func(ex *metric.PrometheusExporter) {
			ex.ScrapeRegistry(store.Registry(), true /* includeChildMetrics */)
		}
		var in bytes.Buffer
		if err := ex.ScrapeAndPrintAsText(&in, scrape); err != nil {
			t.Fatalf("failed to print prometheus data: %v", err)
		}
		if seen != 2 {
			return errors.Errorf("expected to see two replicas for our tenant, saw %d", seen)
		}
		sc := bufio.NewScanner(&in)
		re := regexp.MustCompile(`^(\w+)\{.*,tenant_id="` + tenantID.String() + `"\} (\d+)`)
		metricsToVal := map[string]int64{
			"gcbytesage":    aggregateStats.GCBytesAge,
			"intentage":     aggregateStats.IntentAge,
			"livebytes":     aggregateStats.LiveBytes,
			"livecount":     aggregateStats.LiveCount,
			"keybytes":      aggregateStats.KeyBytes,
			"keycount":      aggregateStats.KeyCount,
			"valbytes":      aggregateStats.ValBytes,
			"valcount":      aggregateStats.ValCount,
			"rangekeybytes": aggregateStats.RangeKeyBytes,
			"rangekeycount": aggregateStats.RangeKeyCount,
			"rangevalbytes": aggregateStats.RangeValBytes,
			"rangevalcount": aggregateStats.RangeValCount,
			"intentbytes":   aggregateStats.IntentBytes,
			"intentcount":   aggregateStats.IntentCount,
			"sysbytes":      aggregateStats.SysBytes,
			"syscount":      aggregateStats.SysCount,
		}
		for sc.Scan() {
			matches := re.FindAllStringSubmatch(sc.Text(), 1)
			if matches == nil {
				continue
			}
			metric, valStr := matches[0][1], matches[0][2]
			exp, ok := metricsToVal[matches[0][1]]
			if !ok {
				continue
			}
			val, err := strconv.ParseInt(valStr, 10, 64)
			require.NoError(t, err)
			if exp != val {
				return errors.Errorf("value for %s %d != %d", metric, val, exp)
			}
			delete(metricsToVal, metric)
		}
		if len(metricsToVal) != 0 {
			return errors.Errorf("did not see all metrics, still expect %v", metricsToVal)
		}
		return nil
	}
	testutils.SucceedsSoon(t, check)
}

// TestTenantRateLimiter ensures that the rate limiter is hooked up properly
// and report the correct metrics.
func TestTenantRateLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test utilizes manual time to make the rate-limiting calculations more
	// obvious. This timesource is not used inside the actual database.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TenantRateKnobs: tenantrate.TestingKnobs{
					TimeSource: timeSource,
				},
			},
		},
	})
	ctx := context.Background()
	tenantID := serverutils.TestTenantID()
	ts, err := s.StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
		TestingKnobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// Disable the span reconciler because it performs tenant KV requests
				// that interfere with our operation counts below.
				ManagerDisableJobCreation: true,
			},
		},
	})
	require.NoError(t, err)

	defer s.Stopper().Stop(ctx)

	// Set a small rate limit so the test doesn't take a long time.
	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, `SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = 200`)

	codec := keys.MakeSQLCodec(tenantID)

	tenantPrefix := codec.TenantPrefix()
	_, _, err = s.SplitRange(tenantPrefix)
	require.NoError(t, err)
	tablePrefix := codec.TablePrefix(42)
	tablePrefix = tablePrefix[:len(tablePrefix):len(tablePrefix)] // appends realloc
	mkKey := func() roachpb.Key {
		return encoding.EncodeUUIDValue(tablePrefix, 1, uuid.MakeV4())
	}

	timeSource.Advance(time.Second)
	// Ensure that the qps rate limit does not affect the system tenant even for
	// the tenant range.
	cfg := tenantrate.ConfigFromSettings(&s.ClusterSettings().SV)

	// We don't know the exact size of the write, but we can set lower and upper
	// bounds.
	writeCostLower := cfg.WriteBatchUnits + cfg.WriteRequestUnits
	writeCostUpper := cfg.WriteBatchUnits + cfg.WriteRequestUnits + float64(32)*cfg.WriteUnitsPerByte
	tolerance := 10.0 // leave space for a couple of other background requests
	// burstWrites is a number of writes that don't exceed the burst limit.
	burstWrites := int((cfg.Burst - tolerance) / writeCostUpper)
	// tooManyWrites is a number of writes which definitely exceed the burst
	// limit.
	tooManyWrites := int(cfg.Burst/writeCostLower) + 2

	// Make sure that writes to the system tenant don't block, even if we
	// definitely exceed the burst rate.
	for i := 0; i < tooManyWrites; i++ {
		require.NoError(t, db.Put(ctx, mkKey(), 0))
	}
	timeSource.Advance(time.Second)
	// Now ensure that in the same instant the write QPS limit does affect the
	// tenant. First issue requests that can happen without blocking.
	for i := 0; i < burstWrites; i++ {
		require.NoError(t, ts.DB().Put(ctx, mkKey(), 0))
	}
	// Attempt to issue another request, make sure that it gets blocked by
	// observing a timer.
	errCh := make(chan error, 1)
	go func() {
		// Issue enough requests so that one has to block.
		for i := burstWrites; i < tooManyWrites; i++ {
			if err := ts.DB().Put(ctx, mkKey(), 0); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	testutils.SucceedsSoon(t, func() error {
		timers := timeSource.Timers()
		if len(timers) != 1 {
			return errors.Errorf("seeing %d timers: %v", len(timers), timers)
		}
		return nil
	})

	// Create some tooling to read and verify metrics off of the prometheus
	// endpoint.
	runner.Exec(t, `SET CLUSTER SETTING server.child_metrics.enabled = true`)
	httpClient, err := s.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	getMetrics := func() string {
		resp, err := httpClient.Get(s.AdminURL() + "/_status/vars")
		require.NoError(t, err)
		read, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return string(read)
	}

	// Allow the blocked request to proceed.
	timeSource.Advance(time.Second)
	require.NoError(t, <-errCh)

	// Ensure that the metric for the admitted requests reflects the number of
	// admitted requests.
	// TODO(radu): this is fragile because a background write could sneak in and
	// the count wouldn't match exactly.
	m := getMetrics()
	lines := strings.Split(m, "\n")
	tenantMetricStr := fmt.Sprintf(`kv_tenant_rate_limit_write_requests_admitted{store="1",tenant_id="%d"}`, tenantID.ToUint64())
	re := regexp.MustCompile(tenantMetricStr + ` (\d*)`)
	for _, line := range lines {
		match := re.FindStringSubmatch(line)
		if match != nil {
			admittedMetricVal, err := strconv.Atoi(match[1])
			require.NoError(t, err)
			require.GreaterOrEqual(t, admittedMetricVal, tooManyWrites)
			// Allow a tolerance for other requests performed while starting the
			// tenant server.
			require.Less(t, admittedMetricVal, tooManyWrites+300)
			break
		}
	}
}
