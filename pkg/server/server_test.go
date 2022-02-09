// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSelfBootstrap verifies operation when no bootstrap hosts have
// been specified.
func TestSelfBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, err := serverutils.StartServerRaw(base.TestServerArgs{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.Background())

	if s.RPCContext().ClusterID.Get() == uuid.Nil {
		t.Error("cluster ID failed to be set on the RPC context")
	}
}

func TestPanicRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, err := serverutils.StartServerRaw(base.TestServerArgs{})
	require.NoError(t, err)
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	// Enable a test-only endpoint that induces a panic.
	ts.http.mux.Handle("/panic", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("induced panic for testing")
	}))

	// Create a request.
	req, err := http.NewRequest(http.MethodGet, ts.AdminURL()+"/panic", nil /* body */)
	require.NoError(t, err)

	// Create a ResponseRecorder to record the response.
	rr := httptest.NewRecorder()
	require.NotPanics(t, func() {
		ts.http.baseHandler(rr, req)
	})

	// Check that the status code is correct.
	require.Equal(t, http.StatusInternalServerError, rr.Code)

	// Check that the panic has been reported.
	entries, err := log.FetchEntriesFromFiles(
		0, /* startTimestamp */
		math.MaxInt64,
		10000, /* maxEntries */
		regexp.MustCompile("a panic has occurred!"),
		log.WithMarkedSensitiveData)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "no log entries matching the regexp")
}

// TestHealthCheck runs a basic sanity check on the health checker.
func TestHealthCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cfg := zonepb.DefaultZoneConfig()
	cfg.NumReplicas = proto.Int32(1)
	s, err := serverutils.StartServerRaw(base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &TestingKnobs{
				DefaultZoneConfigOverride: &cfg,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()

	recorder := s.(*TestServer).Server.recorder

	{
		summary := *recorder.GenerateNodeStatus(ctx)
		result := recorder.CheckHealth(ctx, summary)
		if len(result.Alerts) != 0 {
			t.Fatal(result)
		}
	}

	store, err := s.(*TestServer).Server.node.stores.GetStore(1)
	if err != nil {
		t.Fatal(err)
	}

	store.Metrics().UnavailableRangeCount.Inc(100)

	{
		summary := *recorder.GenerateNodeStatus(ctx)
		result := recorder.CheckHealth(ctx, summary)
		expAlerts := []statuspb.HealthAlert{
			{StoreID: 1, Category: statuspb.HealthAlert_METRICS, Description: "ranges.unavailable", Value: 100.0},
		}
		if !reflect.DeepEqual(expAlerts, result.Alerts) {
			t.Fatalf("expected %+v, got %+v", expAlerts, result.Alerts)
		}
	}
}

// TestEngineTelemetry tests that the server increments a telemetry counter on
// start that denotes engine type.
func TestEngineTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	rows, err := db.Query("SELECT * FROM crdb_internal.feature_usage WHERE feature_name LIKE 'storage.engine.%' AND usage_count > 0;")
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if count < 1 {
		t.Fatal("expected engine type telemetry counter to be emiitted")
	}
}

// TestServerStartClock tests that a server's clock is not pushed out of thin
// air. This used to happen - the simple act of starting was causing a server's
// clock to be pushed because we were introducing bogus future timestamps into
// our system.
func TestServerStartClock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set a high max-offset so that, if the server's clock is pushed by
	// MaxOffset, we don't hide that under the latency of the Start operation
	// which would allow the physical clock to catch up to the pushed one.
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				MaxOffset: time.Second,
			},
		},
	}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Run a command so that we are sure to touch the timestamp cache. This is
	// actually not needed because other commands run during server
	// initialization, but we cannot guarantee that's going to stay that way.
	get := &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("a")},
	}
	if _, err := kv.SendWrapped(
		context.Background(), s.DB().NonTransactionalSender(), get,
	); err != nil {
		t.Fatal(err)
	}

	now := s.Clock().Now()
	physicalNow := s.Clock().PhysicalNow()
	serverClockWasPushed := now.WallTime > physicalNow
	if serverClockWasPushed {
		t.Fatalf("time: server %s vs actual %d", now, physicalNow)
	}
}

// TestPlainHTTPServer verifies that we can serve plain http and talk to it.
// This is controlled by -cert=""
func TestPlainHTTPServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// The default context uses embedded certs.
		Insecure: true,
	})
	defer s.Stopper().Stop(context.Background())

	// First, make sure that the TestServer's built-in client interface
	// still works in insecure mode.
	var data serverpb.JSONResponse
	testutils.SucceedsSoon(t, func() error {
		return getStatusJSONProto(s, "metrics/local", &data)
	})

	// Now make a couple of direct requests using both http and https.
	// They won't succeed because we're not jumping through
	// authentication hoops, but they verify that the server is using
	// the correct protocol.
	url := s.AdminURL()
	if !strings.HasPrefix(url, "http://") {
		t.Fatalf("expected insecure admin url to start with http://, but got %s", url)
	}
	if resp, err := httputil.Get(context.Background(), url); err != nil {
		t.Error(err)
	} else {
		func() {
			defer resp.Body.Close()
			if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
				t.Error(err)
			}
		}()
	}

	// Attempting to connect to the insecure server with HTTPS doesn't work.
	secureURL := strings.Replace(url, "http://", "https://", 1)
	resp, err := httputil.Get(context.Background(), secureURL)
	if !testutils.IsError(err, "http: server gave HTTP response to HTTPS client") {
		t.Error(err)
	}
	if err == nil {
		resp.Body.Close()
	}
}

func TestSecureHTTPRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	httpClient, err := s.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	// Avoid automatically following redirects.
	httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	origURL := "http://" + ts.Cfg.HTTPAddr
	expURL := url.URL{Scheme: "https", Host: ts.Cfg.HTTPAddr, Path: "/"}

	if resp, err := httpClient.Get(origURL); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusTemporaryRedirect {
			t.Errorf("expected status code %d; got %d", http.StatusTemporaryRedirect, resp.StatusCode)
		}
		if redirectURL, err := resp.Location(); err != nil {
			t.Error(err)
		} else if a, e := redirectURL.String(), expURL.String(); a != e {
			t.Errorf("expected location %s; got %s", e, a)
		}
	}

	if resp, err := httpClient.Post(origURL, "text/plain; charset=utf-8", nil); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusTemporaryRedirect {
			t.Errorf("expected status code %d; got %d", http.StatusTemporaryRedirect, resp.StatusCode)
		}
		if redirectURL, err := resp.Location(); err != nil {
			t.Error(err)
		} else if a, e := redirectURL.String(), expURL.String(); a != e {
			t.Errorf("expected location %s; got %s", e, a)
		}
	}
}

// TestAcceptEncoding hits the server while explicitly disabling
// decompression on a custom client's Transport and setting it
// conditionally via the request's Accept-Encoding headers.
func TestAcceptEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	client, err := s.GetAdminAuthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		acceptEncoding string
		newReader      func(io.Reader) io.Reader
	}{
		{"",
			func(b io.Reader) io.Reader {
				return b
			},
		},
		{httputil.GzipEncoding,
			func(b io.Reader) io.Reader {
				r, err := gzip.NewReader(b)
				if err != nil {
					t.Fatalf("could not create new gzip reader: %s", err)
				}
				return r
			},
		},
	}
	for _, d := range testData {
		func() {
			req, err := http.NewRequest("GET", s.AdminURL()+statusPrefix+"metrics/local", nil)
			if err != nil {
				t.Fatalf("could not create request: %s", err)
			}
			if d.acceptEncoding != "" {
				req.Header.Set(httputil.AcceptEncodingHeader, d.acceptEncoding)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("could not make request to %s: %s", req.URL, err)
			}
			defer resp.Body.Close()
			if ce := resp.Header.Get(httputil.ContentEncodingHeader); ce != d.acceptEncoding {
				t.Fatalf("unexpected content encoding: '%s' != '%s'", ce, d.acceptEncoding)
			}
			r := d.newReader(resp.Body)
			var data serverpb.JSONResponse
			if err := jsonpb.Unmarshal(r, &data); err != nil {
				t.Error(err)
			}
		}()
	}
}

// TestSystemConfigGossip tests that system config gossip works in the mixed
// version state. After the 22.1 release is finalized, system config gossip
// will no longer occur.
//
// TODO(ajwerner): Delete this test in 22.2.
func TestSystemConfigGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryMinSupportedVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false,
	)
	serverArgs := base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
			},
			Server: &TestingKnobs{
				BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
		},
	}
	s, _, kvDB := serverutils.StartServer(t, serverArgs)
	defer s.Stopper().Stop(ctx)
	ts := s.(*TestServer)

	key := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, descpb.ID(keys.MaxSystemConfigDescID+1))
	valAt := func(i int) *descpb.Descriptor {
		return dbdesc.NewInitial(
			descpb.ID(i), "foo", security.AdminRoleName(),
		).DescriptorProto()
	}

	// Register a callback for gossip updates.
	resultChan := ts.Gossip().DeprecatedRegisterSystemConfigChannel()

	// The span gets gossiped when it first shows up.
	select {
	case <-resultChan:

	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive gossip message")
	}

	// Write a system key with the transaction marked as having a Gossip trigger.
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.DeprecatedSetSystemConfigTrigger(true /* forSystemTenant */); err != nil {
			return err
		}
		return txn.Put(ctx, key, valAt(2))
	}); err != nil {
		t.Fatal(err)
	}

	// This has to be wrapped in a SucceedSoon because system migrations on the
	// testserver's startup can trigger system config updates without the key we
	// wrote.
	testutils.SucceedsSoon(t, func() error {
		// New system config received.
		var systemConfig *config.SystemConfig
		select {
		case <-resultChan:
			systemConfig = ts.gossip.DeprecatedGetSystemConfig()

		case <-time.After(500 * time.Millisecond):
			return errors.Errorf("did not receive gossip message")
		}

		// Now check the new config.
		var val *roachpb.Value
		for _, kv := range systemConfig.Values {
			if bytes.Equal(key, kv.Key) {
				val = &kv.Value
				break
			}
		}
		if val == nil {
			return errors.Errorf("key not found in gossiped info")
		}

		// Make sure the returned value is valAt(2).
		var got descpb.Descriptor
		if err := val.GetProto(&got); err != nil {
			return err
		}

		_, expected, _, _ := descpb.FromDescriptor(valAt(2))
		_, db, _, _ := descpb.FromDescriptor(&got)
		if db == nil {
			panic(errors.Errorf("found nil database: %v", got))
		}
		if !reflect.DeepEqual(*db, *expected) {
			panic(errors.Errorf("mismatch: expected %+v, got %+v", *expected, *db))
		}
		return nil
	})
}

func TestListenerFileCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	s, err := serverutils.StartServerRaw(base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.Background())

	files, err := filepath.Glob(filepath.Join(dir, "cockroach.*"))
	if err != nil {
		t.Fatal(err)
	}

	li := listenerInfo{
		listenRPC:    s.RPCAddr(),
		advertiseRPC: s.ServingRPCAddr(),
		listenSQL:    s.SQLAddr(),
		advertiseSQL: s.ServingSQLAddr(),
		listenHTTP:   s.HTTPAddr(),
	}
	expectedFiles := li.Iter()

	for _, file := range files {
		base := filepath.Base(file)
		expVal, ok := expectedFiles[base]
		if !ok {
			t.Fatalf("unexpected file %s", file)
		}
		delete(expectedFiles, base)

		data, err := ioutil.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		addr := string(data)

		if addr != expVal {
			t.Fatalf("expected %s %s to match host %s", base, addr, expVal)
		}
	}

	for f := range expectedFiles {
		t.Errorf("never saw expected file %s", f)
	}
}

func TestClusterIDMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engines := make([]storage.Engine, 2)
	for i := range engines {
		e := storage.NewDefaultInMemForTesting()
		defer e.Close()

		sIdent := roachpb.StoreIdent{
			ClusterID: uuid.MakeV4(),
			NodeID:    1,
			StoreID:   roachpb.StoreID(i + 1),
		}
		if err := storage.MVCCPutProto(
			context.Background(), e, nil, keys.StoreIdentKey(), hlc.Timestamp{}, nil, &sIdent); err != nil {

			t.Fatal(err)
		}
		engines[i] = e
	}

	_, err := inspectEngines(
		context.Background(), engines, roachpb.Version{}, roachpb.Version{})
	expected := "conflicting store ClusterIDs"
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected %s error, got %v", expected, err)
	}
}

func TestEnsureInitialWallTimeMonotonicity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name              string
		prevHLCUpperBound int64
		clockStartTime    int64
		checkPersist      bool
	}{
		{
			name:              "lower upper bound time",
			prevHLCUpperBound: 100,
			clockStartTime:    1000,
			checkPersist:      true,
		},
		{
			name:              "higher upper bound time",
			prevHLCUpperBound: 10000,
			clockStartTime:    1000,
			checkPersist:      true,
		},
		{
			name:              "significantly higher upper bound time",
			prevHLCUpperBound: int64(3 * time.Hour),
			clockStartTime:    int64(1 * time.Hour),
			checkPersist:      true,
		},
		{
			name:              "equal upper bound time",
			prevHLCUpperBound: int64(time.Hour),
			clockStartTime:    int64(time.Hour),
			checkPersist:      true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)

			const maxOffset = 500 * time.Millisecond
			m := hlc.NewManualClock(test.clockStartTime)
			c := hlc.NewClock(m.UnixNano, maxOffset)

			sleepUntilFn := func(ctx context.Context, t hlc.Timestamp) error {
				delta := t.WallTime - c.Now().WallTime
				if delta > 0 {
					m.Increment(delta)
				}
				return nil
			}

			wallTime1 := c.Now().WallTime
			if test.clockStartTime < test.prevHLCUpperBound {
				a.True(
					wallTime1 < test.prevHLCUpperBound,
					fmt.Sprintf(
						"expected wall time %d < prev upper bound %d",
						wallTime1,
						test.prevHLCUpperBound,
					),
				)
			}

			ensureClockMonotonicity(
				context.Background(),
				c,
				c.PhysicalTime(),
				test.prevHLCUpperBound,
				sleepUntilFn,
			)

			wallTime2 := c.Now().WallTime
			// After ensuring monotonicity, wall time should be greater than
			// persisted upper bound
			a.True(
				wallTime2 > test.prevHLCUpperBound,
				fmt.Sprintf(
					"expected wall time %d > prev upper bound %d",
					wallTime2,
					test.prevHLCUpperBound,
				),
			)
		})
	}
}

func TestPersistHLCUpperBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var fatal bool
	defer log.ResetExitFunc()
	log.SetExitFunc(true /* hideStack */, func(r exit.Code) {
		defer log.Flush()
		if r == exit.FatalError() {
			fatal = true
		}
	})

	testCases := []struct {
		name            string
		persistInterval time.Duration
	}{
		{
			name:            "persist default delta",
			persistInterval: 200 * time.Millisecond,
		},
		{
			name:            "persist 100ms delta",
			persistInterval: 50 * time.Millisecond,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)
			m := hlc.NewManualClock(int64(1))
			c := hlc.NewClock(m.UnixNano, time.Nanosecond)

			var persistErr error
			var persistedUpperBound int64
			var tickerDur time.Duration
			persistUpperBoundFn := func(i int64) error {
				if persistErr != nil {
					return persistErr
				}
				persistedUpperBound = i
				return nil
			}

			tickerCh := make(chan time.Time)
			tickProcessedCh := make(chan struct{})
			persistHLCUpperBoundIntervalCh := make(chan time.Duration, 1)
			stopCh := make(chan struct{}, 1)
			defer close(persistHLCUpperBoundIntervalCh)

			go periodicallyPersistHLCUpperBound(
				c,
				persistHLCUpperBoundIntervalCh,
				persistUpperBoundFn,
				func(d time.Duration) *time.Ticker {
					ticker := time.NewTicker(d)
					ticker.Stop()
					ticker.C = tickerCh
					tickerDur = d
					return ticker
				},
				stopCh,
				func() {
					tickProcessedCh <- struct{}{}
				},
			)

			fatal = false
			// persist an upper bound
			m.Increment(100)
			wallTime3 := c.Now().WallTime
			persistHLCUpperBoundIntervalCh <- test.persistInterval
			<-tickProcessedCh

			a.True(
				test.persistInterval == tickerDur,
				fmt.Sprintf(
					"expected persist interval %d = ticker duration %d",
					test.persistInterval,
					tickerDur,
				),
			)

			// Updating persistInterval should have triggered a persist
			firstPersist := persistedUpperBound
			a.True(
				persistedUpperBound > wallTime3,
				fmt.Sprintf(
					"expected persisted wall time %d > wall time %d",
					persistedUpperBound,
					wallTime3,
				),
			)
			// ensure that in memory value and persisted value are same
			a.Equal(c.WallTimeUpperBound(), persistedUpperBound)

			// Increment clock by 100 and tick the timer.
			// A persist should have happened
			m.Increment(100)
			tickerCh <- timeutil.Now()
			<-tickProcessedCh
			secondPersist := persistedUpperBound
			a.True(
				secondPersist == firstPersist+100,
				fmt.Sprintf(
					"expected persisted wall time %d to be 100 more than earlier persisted value %d",
					secondPersist,
					firstPersist,
				),
			)
			a.Equal(c.WallTimeUpperBound(), persistedUpperBound)
			a.False(fatal)
			fatal = false

			// After disabling persistHLCUpperBound, a value of 0 should be persisted
			persistHLCUpperBoundIntervalCh <- 0
			<-tickProcessedCh
			a.Equal(
				int64(0),
				c.WallTimeUpperBound(),
			)
			a.Equal(int64(0), persistedUpperBound)
			a.Equal(int64(0), c.WallTimeUpperBound())
			a.False(fatal)
			fatal = false

			persistHLCUpperBoundIntervalCh <- test.persistInterval
			<-tickProcessedCh
			m.Increment(100)
			tickerCh <- timeutil.Now()
			<-tickProcessedCh
			// If persisting fails, a fatal error is expected
			persistErr = errors.New("test err")
			fatal = false
			tickerCh <- timeutil.Now()
			<-tickProcessedCh
			a.True(fatal)
		})
	}
}

func TestServeIndexHTML(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const htmlTemplate = `<!DOCTYPE html>
<html>
	<head>
		<title>Cockroach Console</title>
		<meta charset="UTF-8">
		<link href="favicon.ico" rel="shortcut icon">
	</head>
	<body>
		<div id="react-layout"></div>

		<script>
			window.dataFromServer = %s;
		</script>

		<script src="protos.dll.js" type="text/javascript"></script>
		<script src="vendor.dll.js" type="text/javascript"></script>
		<script src="bundle.js" type="text/javascript"></script>
	</body>
</html>
`

	ctx := context.Background()

	linkInFakeUI := func() {
		ui.HaveUI = true
	}
	unlinkFakeUI := func() {
		ui.HaveUI = false
	}

	t.Run("Insecure mode", func(t *testing.T) {
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Insecure: true,
			// This test server argument has the same effect as setting the environment variable
			// `COCKROACH_EXPERIMENTAL_REQUIRE_WEB_SESSION` to false, or not setting it.
			// In test servers, web sessions are required by default.
			DisableWebSessionAuthentication: true,
		})
		defer s.Stopper().Stop(ctx)
		tsrv := s.(*TestServer)

		client, err := tsrv.GetHTTPClient()
		require.NoError(t, err)

		t.Run("short build", func(t *testing.T) {
			resp, err := client.Get(s.AdminURL())
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, 200, resp.StatusCode)

			respBytes, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			respString := string(respBytes)
			expected := fmt.Sprintf(`<!DOCTYPE html>
<title>CockroachDB</title>
Binary built without web UI.
<hr>
<em>%s</em>`,
				build.GetInfo().Short())
			require.Equal(t, expected, respString)
		})

		t.Run("non-short build", func(t *testing.T) {
			linkInFakeUI()
			defer unlinkFakeUI()
			resp, err := client.Get(s.AdminURL())
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, 200, resp.StatusCode)

			respBytes, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			respString := string(respBytes)
			expected := fmt.Sprintf(
				htmlTemplate,
				fmt.Sprintf(
					`{"ExperimentalUseLogin":false,"LoginEnabled":false,"LoggedInUser":null,"Tag":"%s","Version":"%s","NodeID":"%d","OIDCAutoLogin":false,"OIDCLoginEnabled":false,"OIDCButtonText":""}`,
					build.GetInfo().Tag,
					build.BinaryVersionPrefix(),
					1,
				),
			)
			require.Equal(t, expected, respString)
		})
	})

	t.Run("Secure mode", func(t *testing.T) {
		linkInFakeUI()
		defer unlinkFakeUI()
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		tsrv := s.(*TestServer)

		loggedInClient, err := tsrv.GetAdminAuthenticatedHTTPClient()
		require.NoError(t, err)
		loggedOutClient, err := tsrv.GetHTTPClient()
		require.NoError(t, err)

		cases := []struct {
			client http.Client
			json   string
		}{
			{
				loggedInClient,
				fmt.Sprintf(
					`{"ExperimentalUseLogin":true,"LoginEnabled":true,"LoggedInUser":"authentic_user","Tag":"%s","Version":"%s","NodeID":"%d","OIDCAutoLogin":false,"OIDCLoginEnabled":false,"OIDCButtonText":""}`,
					build.GetInfo().Tag,
					build.BinaryVersionPrefix(),
					1,
				),
			},
			{
				loggedOutClient,
				fmt.Sprintf(
					`{"ExperimentalUseLogin":true,"LoginEnabled":true,"LoggedInUser":null,"Tag":"%s","Version":"%s","NodeID":"%d","OIDCAutoLogin":false,"OIDCLoginEnabled":false,"OIDCButtonText":""}`,
					build.GetInfo().Tag,
					build.BinaryVersionPrefix(),
					1,
				),
			},
		}

		for i, testCase := range cases {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				req, err := http.NewRequestWithContext(ctx, "GET", s.AdminURL(), nil)
				require.NoError(t, err)

				resp, err := testCase.client.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()
				require.Equal(t, 200, resp.StatusCode)

				respBytes, err := ioutil.ReadAll(resp.Body)
				require.NoError(t, err)

				respString := string(respBytes)
				expected := fmt.Sprintf(htmlTemplate, testCase.json)
				require.Equal(t, expected, respString)
			})
		}
	})
}

func TestGWRuntimeMarshalProto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	r, err := http.NewRequest(http.MethodGet, "nope://unused", strings.NewReader(""))
	require.NoError(t, err)
	// Regression test against:
	// https://github.com/cockroachdb/cockroach/issues/49842
	runtime.DefaultHTTPError(
		ctx,
		runtime.NewServeMux(),
		&protoutil.ProtoPb{}, // calls XXX_size
		&httptest.ResponseRecorder{},
		r,
		errors.New("boom"),
	)
}

func TestDecommissionNodeStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)
	decomNodeID := tc.Server(2).NodeID()

	// Make sure node status entries have been created.
	for i := 0; i < tc.NumServers(); i++ {
		srv := tc.Server(i)
		entry, err := srv.DB().Get(ctx, keys.NodeStatusKey(srv.NodeID()))
		require.NoError(t, err)
		require.NotNil(t, entry.Value, "node status entry not found for node %d", srv.NodeID())
	}

	// Decommission the node.
	srv := tc.Server(0)
	require.NoError(t, srv.Decommission(
		ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{decomNodeID}))
	require.NoError(t, srv.Decommission(
		ctx, livenesspb.MembershipStatus_DECOMMISSIONED, []roachpb.NodeID{decomNodeID}))

	// The node status entry should now have been cleaned up.
	entry, err := srv.DB().Get(ctx, keys.NodeStatusKey(decomNodeID))
	require.NoError(t, err)
	require.Nil(t, entry.Value, "found stale node status entry for node %d", decomNodeID)
}

func TestSQLDecommissioned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
		ServerArgs: base.TestServerArgs{
			Insecure: true, // to set up a simple SQL client
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Decommission server 1 and wait for it to lose cluster access.
	srv := tc.Server(0)
	decomSrv := tc.Server(1)
	for _, status := range []livenesspb.MembershipStatus{
		livenesspb.MembershipStatus_DECOMMISSIONING, livenesspb.MembershipStatus_DECOMMISSIONED,
	} {
		require.NoError(t, srv.Decommission(ctx, status, []roachpb.NodeID{decomSrv.NodeID()}))
	}

	require.Eventually(t, func() bool {
		_, err := decomSrv.DB().Scan(ctx, keys.MinKey, keys.MaxKey, 0)
		s, ok := status.FromError(errors.UnwrapAll(err))
		return ok && s.Code() == codes.PermissionDenied
	}, 10*time.Second, 100*time.Millisecond, "timed out waiting for server to lose cluster access")

	// Tests SQL access. We're mostly concerned with these operations returning
	// fast rather than hanging indefinitely due to internal retries. We both
	// try the internal executor, to check that the internal structured error is
	// propagated correctly, and also from a client to make sure the error is
	// propagated all the way.
	require.Eventually(t, func() bool {
		sqlExec := decomSrv.InternalExecutor().(*sql.InternalExecutor)

		datums, err := sqlExec.QueryBuffered(ctx, "", nil, "SELECT 1")
		require.NoError(t, err)
		require.Equal(t, []tree.Datums{{tree.NewDInt(1)}}, datums)

		_, err = sqlExec.QueryBuffered(ctx, "", nil, "SELECT * FROM crdb_internal.tables")
		if err == nil {
			return false
		}
		s, ok := status.FromError(errors.UnwrapAll(err))
		require.True(t, ok, "expected gRPC status error, got %T: %s", err, err)
		require.Equal(t, codes.PermissionDenied, s.Code())

		sqlClient, err := serverutils.OpenDBConnE(decomSrv.ServingSQLAddr(), "", true, tc.Stopper())
		require.NoError(t, err)

		var result int
		err = sqlClient.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 1, result)

		_, err = sqlClient.Query("SELECT * FROM crdb_internal.tables")
		return err != nil
	}, 10*time.Second, 100*time.Millisecond, "timed out waiting for queries to error")
}

func TestAssertEnginesEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	eng, err := storage.Open(ctx, storage.InMemory())
	require.NoError(t, err)
	defer eng.Close()

	require.NoError(t, assertEnginesEmpty([]storage.Engine{eng}))

	require.NoError(t, storage.MVCCPutProto(ctx, eng, nil, keys.StoreClusterVersionKey(),
		hlc.Timestamp{}, nil, &roachpb.Version{Major: 21, Minor: 1, Internal: 122}))
	require.NoError(t, assertEnginesEmpty([]storage.Engine{eng}))

	batch := eng.NewBatch()
	key := storage.MVCCKey{
		Key:       []byte{0xde, 0xad, 0xbe, 0xef},
		Timestamp: hlc.Timestamp{WallTime: 100},
	}
	require.NoError(t, batch.PutMVCC(key, []byte("foo")))
	require.NoError(t, batch.Commit(false))
	require.Error(t, assertEnginesEmpty([]storage.Engine{eng}))
}

func Test_makeFakeNodeStatuses(t *testing.T) {
	defer leaktest.AfterTest(t)()

	n1Desc := roachpb.NodeDescriptor{NodeID: 1}
	n9Desc := roachpb.NodeDescriptor{NodeID: 9}
	tests := []struct {
		name       string
		mapping    map[roachpb.StoreID]roachpb.NodeID
		storesSeen map[string]struct{}

		exp    []statuspb.NodeStatus
		expErr string
	}{
		{
			name:       "store-missing-in-mapping",
			mapping:    map[roachpb.StoreID]roachpb.NodeID{1: 1},
			storesSeen: map[string]struct{}{"1": {}, "2": {}},
			expErr:     `need to map the remaining stores`,
		},
		{
			name:       "store-only-in-mapping",
			mapping:    map[roachpb.StoreID]roachpb.NodeID{1: 1, 3: 9},
			storesSeen: map[string]struct{}{"9": {}},
			expErr:     `s1 supplied in input mapping, but no timeseries found for it`,
		},
		{
			name:       "success",
			mapping:    map[roachpb.StoreID]roachpb.NodeID{1: 1, 3: 9},
			storesSeen: map[string]struct{}{"1": {}, "3": {}},
			exp: []statuspb.NodeStatus{
				{Desc: n1Desc, StoreStatuses: []statuspb.StoreStatus{{Desc: roachpb.StoreDescriptor{
					StoreID: 1,
					Node:    n1Desc,
				}}}},
				{Desc: n9Desc, StoreStatuses: []statuspb.StoreStatus{{Desc: roachpb.StoreDescriptor{
					StoreID: 3,
					Node:    n9Desc,
				}}}},
			},
		},
		{
			name: "success-multi-store",
			// n1 has [s1,s12], n3 has [s3,s6].
			mapping:    map[roachpb.StoreID]roachpb.NodeID{1: 1, 3: 9, 12: 1, 6: 9},
			storesSeen: map[string]struct{}{"1": {}, "3": {}, "6": {}, "12": {}},
			exp: []statuspb.NodeStatus{
				{Desc: n1Desc, StoreStatuses: []statuspb.StoreStatus{
					{Desc: roachpb.StoreDescriptor{StoreID: 1, Node: n1Desc}},
					{Desc: roachpb.StoreDescriptor{StoreID: 12, Node: n1Desc}},
				}},
				{Desc: n9Desc, StoreStatuses: []statuspb.StoreStatus{
					{Desc: roachpb.StoreDescriptor{StoreID: 3, Node: n9Desc}},
					{Desc: roachpb.StoreDescriptor{StoreID: 6, Node: n9Desc}},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := makeFakeNodeStatuses(tt.mapping)
			if err == nil {
				err = checkFakeStatuses(result, tt.storesSeen)
			}
			if err != nil {
				result = nil
			}
			require.Equal(t, tt.exp, result)
			require.True(t, testutils.IsError(err, tt.expErr), "%+v didn't match expectation %s", err, tt.expErr)
		})
	}
}
