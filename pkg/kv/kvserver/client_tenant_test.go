// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	io "io"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // for tenant functionality
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
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
			id, _ := replica.TenantID() // now initialized
			if id != tenantID {
				return true
			}
			seen++
			aggregateStats.Add(replica.GetMVCCStats())
			return true
		})
		ex := metric.MakePrometheusExporter()
		scrape := func(ex *metric.PrometheusExporter) {
			ex.ScrapeRegistry(store.Registry(), true /* includeChildMetrics */)
		}
		var in bytes.Buffer
		if err := ex.ScrapeAndPrintAsText(&in, expfmt.FmtText, scrape); err != nil {
			t.Fatalf("failed to print prometheus data: %v", err)
		}
		if seen != 2 {
			return errors.Errorf("expected to see two replicas for our tenant, saw %d", seen)
		}
		sc := bufio.NewScanner(&in)
		re := regexp.MustCompile(`^(\w+)\{.*,tenant_id="` + tenantID.String() + `"\} (\d+)`)
		metricsToVal := map[string]int64{
			"gcbytesage":    aggregateStats.GCBytesAge,
			"intentage":     aggregateStats.LockAge,
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
			"lockbytes":     aggregateStats.LockBytes,
			"lockcount":     aggregateStats.LockCount,
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

	// This test shouldn't take forever. If we're going to fail, better to
	// do it in minutes than in an hour.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// We're testing the server-side tenant rate limiter, but there is also a tenant-side one.
	// That one actually throttles too in this test (making it take 10+s) unless we work around that.
	ctx = multitenant.WithTenantCostControlExemption(ctx)

	var numAcquired atomic.Int32
	acqFunc := func(
		ctx context.Context, poolName string, r quotapool.Request, start time.Time,
	) {
		numAcquired.Add(1)
	}
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TenantRateKnobs: tenantrate.TestingKnobs{
					QuotaPoolOptions: []quotapool.Option{
						quotapool.WithTimeSource(timeSource),
						quotapool.OnAcquisition(acqFunc),
					},
				},
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{SkipJobBootstrap: true},
			JobsTestingKnobs: &jobs.TestingKnobs{
				DisableAdoptions: true,
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipJobMetricsPollingJobBootstrap: true,
			},
		},
	})
	tenantID := serverutils.TestTenantID()
	ts, err := s.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
		TestingKnobs: base.TestingKnobs{
			JobsTestingKnobs: &jobs.TestingKnobs{
				DisableAdoptions: true,
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs: true,
			},
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
	// bounds for a "small" (up to 32 bytes) request.

	// If we put at least that many bytes into a single put, it should consume at
	// least one percent of the configured burst, meaning if we send 100 such requests
	// we should expect blocking. (We may block earlier due to background writes
	// sneaking in).
	//
	// The below is based on the equation
	//
	//   cost = WriteBatchUnits + WriteRequestUnits + bytes * WriteUnitsPerByte.
	//
	// and solving for the case in which cost is a percent of Burst.
	numBytesForAtLeastOnePercentOfBurst := int((cfg.Burst/100 - cfg.WriteBatchUnits - cfg.WriteRequestUnits) / cfg.WriteUnitsPerByte)
	require.NotZero(t, numBytesForAtLeastOnePercentOfBurst)
	t.Logf("bytes for one percent of burst: %d", numBytesForAtLeastOnePercentOfBurst)
	atLeastOnePercentValue := strings.Repeat("x", numBytesForAtLeastOnePercentOfBurst)

	// Spawn a helper that will detect if the rate limiter is blocking us.
	// This prevents cases where the test would stall and fail opaquely instead
	// of making it clear that writes on the main goroutine blocked unexpectedly.
	watcher := func(ctx context.Context, t *testing.T, msg string) (_ context.Context, cancel func()) {
		t.Helper()
		t.Logf("testing: %v", msg)
		ctx, cancel = context.WithCancel(ctx)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					t.Logf("total acquisitions: %d", numAcquired.Load())
				}
				if !assert.Len(t, timeSource.Timers(), 0, msg) {
					cancel()
				}
			}
		}()
		return ctx, cancel
	}

	// Make sure that writes to the system tenant don't block, even if we
	// definitely exceed the burst rate.
	{
		ctx, cancel := watcher(ctx, t, "system tenant should not be rate limited")
		defer cancel()

		for i := 0; i < 100; i++ {
			require.NoError(t, db.Put(ctx, mkKey(), atLeastOnePercentValue))
		}
		cancel()
	}
	timeSource.Advance(time.Second)
	// Now ensure that in the same instant the write QPS limit does affect the
	// tenant. First issue a handful of small requests, which should not block
	// as we can send ~100 larger requests before running out of burst budget.
	//
	// In the past, this test was trying to get very close to cfg.Burst without
	// blocking, but that is quite brittle because there are dozens of background
	// writes that sneak in during an average test run. So we are now intentionally
	// staying very far from running out of burst.
	{
		ctx, cancel := watcher(ctx, t, "first writes should not experience blocking")
		defer cancel()

		for i := 0; i < 5; i++ {
			require.NoError(t, ts.DB().Put(ctx, mkKey(), 0))
		}
		cancel()
	}
	// Now intentionally break through the burst and make sure this blocks.
	// We observe blocking by noticing that a timer was created via the custom
	// timeSource which we only handed to the tenant rate limiter.
	t.Logf("testing: requests should eventually block")
	errCh := make(chan error, 1)
	// doneCh is closed once we've verified blocking and have unblocked.
	// This prevents the additional writes from potentially running into
	// blocking again.
	doneCh := make(chan struct{})
	go func() {
		// Issue enough requests so that one has to block.
		ev := log.Every(100 * time.Millisecond)
		for i := 0; i < 100; i++ {
			if ev.ShouldLog() {
				t.Logf("put %d", i+1)
			}
			if err := ts.DB().Put(ctx, mkKey(), atLeastOnePercentValue); err != nil {
				errCh <- err
				return
			}
			select {
			default:
			case <-doneCh:
				errCh <- nil
				return
			}
		}
		t.Error("never blocked")
		errCh <- errors.New("never blocked")
	}()

	testutils.SucceedsSoon(t, func() error {
		if len(timeSource.Timers()) == 0 {
			return errors.Errorf("not seeing any timers")
		}
		return nil
	})
	t.Log("blocking confirmed")

	// Allow the blocked request to proceed.
	close(doneCh) // close first so that goroutine terminates once unblocked
	timeSource.Advance(time.Second)
	require.NoError(t, <-errCh)

	t.Log("checking metrics")
	// Create some tooling to read and verify metrics off of the prometheus
	// endpoint.
	runner.Exec(t, `SET CLUSTER SETTING server.child_metrics.enabled = true`)
	httpClient, err := s.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	getMetrics := func() string {
		resp, err := httpClient.Get(s.AdminURL().WithPath("/_status/vars").String())
		require.NoError(t, err)
		read, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return string(read)
	}

	// Ensure that the metric for the admitted requests reflects the number of
	// admitted requests. We run only shallow checks here due to background writes.
	m := getMetrics()
	lines := strings.Split(m, "\n")
	tenantMetricStr := fmt.Sprintf(`kv_tenant_rate_limit_write_requests_admitted{store="1",node_id="1",tenant_id="%d"}`, tenantID.ToUint64())
	re := regexp.MustCompile(tenantMetricStr + ` (\d*)`)
	var matched bool
	for _, line := range lines {
		match := re.FindStringSubmatch(line)
		if match != nil {
			matched = true
			admittedMetricVal, err := strconv.Atoi(match[1])
			require.NoError(t, err)
			// The background chatter alone tends to put north of 100 writes in.
			// But let's be conservative and not rely on that. Our blocking writes should
			// get at least 50 in (we know 100 result in blocking). We can't and shouldn't
			// add tighter checks here because they're a nightmare to debug should they fail.
			require.GreaterOrEqual(t, admittedMetricVal, 50)
			break
		}
	}
	require.True(t, matched, "did not match %s:\n\n%s", tenantMetricStr, m)
}

// Test that KV requests made by a tenant get a context annotated with the tenant ID.
func TestTenantCtx(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const magicKey = "424242"
	tenantID := serverutils.TestTenantID()

	testutils.RunTrueAndFalse(t, "shared-process tenant", func(t *testing.T, sharedProcess bool) {
		getErr := make(chan error)
		pushErr := make(chan error)
		s := serverutils.StartServerOnly(t, base.TestServerArgs{
			// Disable the default test tenant since we're going to create our own.
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
						// We'll recognize a GetRequest and a PushRequest, check that the
						// context looks as expected, and signal their channels.

						tenID, isTenantRequest := roachpb.ClientTenantFromContext(ctx)
						keyRecognized := strings.Contains(ba.Requests[0].GetInner().Header().Key.String(), magicKey)
						if !keyRecognized {
							return nil
						}

						var getReq *kvpb.GetRequest
						var pushReq *kvpb.PushTxnRequest
						if isSingleGet := ba.IsSingleRequest() && ba.Requests[0].GetInner().Method() == kvpb.Get; isSingleGet {
							getReq = ba.Requests[0].GetInner().(*kvpb.GetRequest)
						}
						if isSinglePushTxn := ba.IsSingleRequest() && ba.Requests[0].GetInner().Method() == kvpb.PushTxn; isSinglePushTxn {
							pushReq = ba.Requests[0].GetInner().(*kvpb.PushTxnRequest)
						}

						switch {
						case getReq != nil:
							var err error
							if !isTenantRequest || tenID != tenantID {
								err = errors.Newf("expected Get to run as the expected tenant (%d), but it isn't. tenant request: %t, tenantID: %d",
									tenantID, isTenantRequest, tenID)
							}
							getErr <- err
							return nil
						case pushReq != nil:
							// Check that the Push request no longer has the txn request; RPCs
							// done by KV do not identify the tenant.
							var err error
							if isTenantRequest {
								err = errors.Newf("got unexpected tenant in push: %d", tenID)
							}
							pushErr <- err
							return nil
						default:
							// Unrecognized requests pass through.
							return nil
						}
					},
				},
			},
		})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)

		var tsql *gosql.DB
		if sharedProcess {
			var err error
			_, tsql, err = s.TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
				TenantName: "test",
				TenantID:   tenantID,
			})
			require.NoError(t, err)
		} else {
			_, tsql = serverutils.StartTenant(t, s, base.TestTenantArgs{
				TenantName: "test",
				TenantID:   tenantID,
			})
			defer tsql.Close()
		}

		_, err := tsql.Exec("create table t (x int primary key)")
		require.NoError(t, err)
		tx1, err := tsql.BeginTx(ctx, nil /* opts */)
		require.NoError(t, err)
		_, err = tx1.Exec("insert into t(x) values ($1)", magicKey)
		require.NoError(t, err)

		var tx2 *gosql.Tx
		var tx2C = make(chan struct{})
		go func() {
			var err error
			tx2, err = tsql.BeginTx(ctx, nil /* opts */)
			assert.NoError(t, err)
			_, err = tx2.Exec("select * from t where x = $1", magicKey)
			assert.NoError(t, err)
			close(tx2C)
		}()

		// Wait for tx2 goroutine to send the PushTxn request, and then roll back tx1
		// to unblock tx2.
		select {
		case err := <-getErr:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for Get")
		}
		select {
		case err := <-pushErr:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for PushTxn")
		}
		_ = tx1.Rollback()
		// Wait for tx2 to be unblocked.
		<-tx2C
		_ = tx2.Rollback()
	})
}
