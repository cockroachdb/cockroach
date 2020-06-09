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
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
)

// TestSelfBootstrap verifies operation when no bootstrap hosts have
// been specified.
func TestSelfBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, err := serverutils.StartServerRaw(base.TestServerArgs{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.Background())

	if s.RPCContext().ClusterID.Get() == uuid.Nil {
		t.Error("cluster ID failed to be set on the RPC context")
	}
}

// TestHealthCheck runs a basic sanity check on the health checker.
func TestHealthCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
			t.Error(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}

	// Attempting to connect to the insecure server with HTTPS doesn't work.
	secureURL := strings.Replace(url, "http://", "https://", 1)
	if _, err := httputil.Get(context.Background(), secureURL); !testutils.IsError(err, "http: server gave HTTP response to HTTPS client") {
		t.Error(err)
	}
}

func TestSecureHTTPRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	}
}

// TestMultiRangeScanDeleteRange tests that commands which access multiple
// ranges are carried out properly.
func TestMultiRangeScanDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.(*TestServer)
	tds := db.NonTransactionalSender()

	if err := ts.node.storeCfg.DB.AdminSplit(ctx, "m", hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
	writes := []roachpb.Key{roachpb.Key("a"), roachpb.Key("z")}
	get := &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{Key: writes[0]},
	}
	get.EndKey = writes[len(writes)-1]
	if _, err := kv.SendWrapped(ctx, tds, get); err == nil {
		t.Errorf("able to call Get with a key range: %v", get)
	}
	var delTS hlc.Timestamp
	for i, k := range writes {
		put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
		if _, err := kv.SendWrapped(ctx, tds, put); err != nil {
			t.Fatal(err)
		}
		scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next(), false)
		reply, err := kv.SendWrapped(ctx, tds, scan)
		if err != nil {
			t.Fatal(err)
		}
		sr := reply.(*roachpb.ScanResponse)
		if sr.Txn != nil {
			// This was the other way around at some point in the past.
			// Same below for Delete, etc.
			t.Errorf("expected no transaction in response header")
		}
		if rows := sr.Rows; len(rows) != i+1 {
			t.Fatalf("expected %d rows, but got %d", i+1, len(rows))
		}
	}

	del := &roachpb.DeleteRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    writes[0],
			EndKey: writes[len(writes)-1].Next(),
		},
		ReturnKeys: true,
	}
	reply, err := kv.SendWrappedWith(ctx, tds, roachpb.Header{Timestamp: delTS}, del)
	if err != nil {
		t.Fatal(err)
	}
	dr := reply.(*roachpb.DeleteRangeResponse)
	if dr.Txn != nil {
		t.Errorf("expected no transaction in response header")
	}
	if !reflect.DeepEqual(dr.Keys, writes) {
		t.Errorf("expected %d keys to be deleted, but got %d instead", writes, dr.Keys)
	}

	now := s.Clock().Now()
	txnProto := roachpb.MakeTransaction("MyTxn", nil, 0, now, 0)
	txn := kv.NewTxnFromProto(ctx, db, s.NodeID(), now, kv.RootTxn, &txnProto)

	scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next(), false)
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txnProto}
	ba.Add(scan)
	br, pErr := txn.Send(ctx, ba)
	if pErr != nil {
		t.Fatal(err)
	}
	replyTxn := br.Txn
	if replyTxn == nil || replyTxn.Name != "MyTxn" {
		t.Errorf("wanted Txn to persist, but it changed to %v", txn)
	}
	sr := br.Responses[0].GetInner().(*roachpb.ScanResponse)
	if rows := sr.Rows; len(rows) > 0 {
		t.Fatalf("scan after delete returned rows: %v", rows)
	}
}

// TestMultiRangeScanWithPagination tests that specifying MaxSpanResultKeys
// and/or TargetBytes to break up result sets works properly, even across
// ranges.
func TestMultiRangeScanWithPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		splitKeys []roachpb.Key
		keys      []roachpb.Key
	}{
		{[]roachpb.Key{roachpb.Key("m")},
			[]roachpb.Key{roachpb.Key("a"), roachpb.Key("z")}},
		{[]roachpb.Key{roachpb.Key("h"), roachpb.Key("q")},
			[]roachpb.Key{roachpb.Key("b"), roachpb.Key("f"), roachpb.Key("k"),
				roachpb.Key("r"), roachpb.Key("w"), roachpb.Key("y")}},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			ts := s.(*TestServer)
			tds := db.NonTransactionalSender()

			for _, sk := range tc.splitKeys {
				if err := ts.node.storeCfg.DB.AdminSplit(ctx, sk, hlc.MaxTimestamp /* expirationTime */); err != nil {
					t.Fatal(err)
				}
			}

			for _, k := range tc.keys {
				put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
				if _, err := kv.SendWrapped(ctx, tds, put); err != nil {
					t.Fatal(err)
				}
			}

			// The maximum TargetBytes to use in this test. We use the bytes in
			// all kvs in this test case as a ceiling. Nothing interesting
			// happens above this.
			var maxTargetBytes int64
			{
				scan := roachpb.NewScan(tc.keys[0], tc.keys[len(tc.keys)-1].Next(), false)
				resp, pErr := kv.SendWrapped(ctx, tds, scan)
				require.Nil(t, pErr)
				maxTargetBytes = resp.Header().NumBytes
			}

			testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
				// Iterate through MaxSpanRequestKeys=1..n and TargetBytes=1..m
				// and (where n and m are chosen to reveal the full result set
				// in one page). At each(*) combination, paginate both the
				// forward and reverse scan and make sure we get the right
				// result.
				//
				// (*) we don't increase the limits when there's only one page,
				// but short circuit to something more interesting instead.
				msrq := int64(1)
				for targetBytes := int64(1); ; targetBytes++ {
					var numPages int
					t.Run(fmt.Sprintf("targetBytes=%d,maxSpanRequestKeys=%d", targetBytes, msrq), func(t *testing.T) {
						req := func(span roachpb.Span) roachpb.Request {
							if reverse {
								return roachpb.NewReverseScan(span.Key, span.EndKey, false)
							}
							return roachpb.NewScan(span.Key, span.EndKey, false)
						}
						// Paginate.
						resumeSpan := &roachpb.Span{Key: tc.keys[0], EndKey: tc.keys[len(tc.keys)-1].Next()}
						var keys []roachpb.Key
						for {
							numPages++
							scan := req(*resumeSpan)
							var ba roachpb.BatchRequest
							ba.Add(scan)
							ba.Header.TargetBytes = targetBytes
							ba.Header.MaxSpanRequestKeys = msrq
							br, pErr := tds.Send(ctx, ba)
							require.Nil(t, pErr)
							var rows []roachpb.KeyValue
							if reverse {
								rows = br.Responses[0].GetReverseScan().Rows
							} else {
								rows = br.Responses[0].GetScan().Rows
							}
							for _, kv := range rows {
								keys = append(keys, kv.Key)
							}
							resumeSpan = br.Responses[0].GetInner().Header().ResumeSpan
							t.Logf("page #%d: scan %v -> keys (after) %v resume %v", scan.Header().Span(), numPages, keys, resumeSpan)
							if resumeSpan == nil {
								// Done with this pagination.
								break
							}
						}
						if reverse {
							for i, n := 0, len(keys); i < n-i-1; i++ {
								keys[i], keys[n-i-1] = keys[n-i-1], keys[i]
							}
						}
						require.Equal(t, tc.keys, keys)
						if targetBytes == 1 || msrq < int64(len(tc.keys)) {
							// Definitely more than one page in this case.
							require.Less(t, 1, numPages)
						}
						if targetBytes >= maxTargetBytes && msrq >= int64(len(tc.keys)) {
							// Definitely one page if limits are larger than result set.
							require.Equal(t, 1, numPages)
						}
					})
					if targetBytes >= maxTargetBytes || numPages == 1 {
						if msrq >= int64(len(tc.keys)) {
							return
						}
						targetBytes = 0
						msrq++
					}
				}
			})
		})
	}
}

func TestSystemConfigGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.(*TestServer)

	key := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, keys.MaxReservedDescID)
	valAt := func(i int) *sqlbase.Descriptor {
		return sqlbase.NewInitialDatabaseDescriptor(
			sqlbase.ID(i), "foo",
		).DescriptorProto()
	}

	// Register a callback for gossip updates.
	resultChan := ts.Gossip().RegisterSystemConfigChannel()

	// The span gets gossiped when it first shows up.
	select {
	case <-resultChan:

	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive gossip message")
	}

	// Write a system key with the transaction marked as having a Gossip trigger.
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
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
			systemConfig = ts.gossip.GetSystemConfig()

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
		var got sqlbase.Descriptor
		if err := val.GetProto(&got); err != nil {
			return err
		}

		expected := valAt(2).GetDatabase()
		db := got.GetDatabase()
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
		advertiseRPC: s.ServingRPCAddr(),
		listenHTTP:   s.HTTPAddr(),
		listenRPC:    s.RPCAddr(),
		listenSQL:    s.SQLAddr(),
		advertiseSQL: s.ServingSQLAddr(),
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

	engines := make([]storage.Engine, 2)
	for i := range engines {
		e := storage.NewDefaultInMem()
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

			sleepUntilFn := func(until int64, currentTime func() int64) {
				delta := until - currentTime()
				if delta > 0 {
					m.Increment(delta)
				}
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

	var fatal bool
	defer log.ResetExitFunc()
	log.SetExitFunc(true /* hideStack */, func(r int) {
		defer log.Flush()
		if r != 0 {
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

	linkInFakeUI := func() {
		ui.Asset = func(string) (_ []byte, _ error) { return }
		ui.AssetDir = func(name string) (_ []string, _ error) { return }
		ui.AssetInfo = func(name string) (_ os.FileInfo, _ error) { return }
	}
	unlinkFakeUI := func() {
		ui.Asset = nil
		ui.AssetDir = nil
		ui.AssetInfo = nil
	}

	t.Run("Insecure mode", func(t *testing.T) {
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Insecure: true,
			// This test server argument has the same effect as setting the environment variable
			// `COCKROACH_EXPERIMENTAL_REQUIRE_WEB_SESSION` to false, or not setting it.
			// In test servers, web sessions are required by default.
			DisableWebSessionAuthentication: true,
		})
		defer s.Stopper().Stop(context.Background())
		tsrv := s.(*TestServer)

		client, err := tsrv.GetHTTPClient()
		if err != nil {
			t.Fatal(err)
		}

		t.Run("short build", func(t *testing.T) {
			resp, err := client.Get(s.AdminURL())
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected status code 200; got %d", resp.StatusCode)
			}
			respBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			respString := string(respBytes)
			expected := fmt.Sprintf(`<!DOCTYPE html>
<title>CockroachDB</title>
Binary built without web UI.
<hr>
<em>%s</em>`,
				build.GetInfo().Short())
			if respString != expected {
				t.Fatalf("expected %s; got %s", expected, respString)
			}
		})

		t.Run("non-short build", func(t *testing.T) {
			linkInFakeUI()
			defer unlinkFakeUI()
			resp, err := client.Get(s.AdminURL())
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected status code 200; got %d", resp.StatusCode)
			}
			respBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			respString := string(respBytes)
			expected := fmt.Sprintf(
				htmlTemplate,
				fmt.Sprintf(
					`{"ExperimentalUseLogin":false,"LoginEnabled":false,"LoggedInUser":null,"Tag":"%s","Version":"%s","NodeID":"%d"}`,
					build.GetInfo().Tag,
					build.VersionPrefix(),
					1,
				),
			)
			if respString != expected {
				t.Fatalf("expected %s; got %s", expected, respString)
			}
		})
	})

	t.Run("Secure mode", func(t *testing.T) {
		linkInFakeUI()
		defer unlinkFakeUI()
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.Background())
		tsrv := s.(*TestServer)

		loggedInClient, err := tsrv.GetAdminAuthenticatedHTTPClient()
		if err != nil {
			t.Fatal(err)
		}
		loggedOutClient, err := tsrv.GetHTTPClient()
		if err != nil {
			t.Fatal(err)
		}

		cases := []struct {
			client http.Client
			json   string
		}{
			{
				loggedInClient,
				fmt.Sprintf(
					`{"ExperimentalUseLogin":true,"LoginEnabled":true,"LoggedInUser":"authentic_user","Tag":"%s","Version":"%s","NodeID":"%d"}`,
					build.GetInfo().Tag,
					build.VersionPrefix(),
					1,
				),
			},
			{
				loggedOutClient,
				fmt.Sprintf(
					`{"ExperimentalUseLogin":true,"LoginEnabled":true,"LoggedInUser":null,"Tag":"%s","Version":"%s","NodeID":"%d"}`,
					build.GetInfo().Tag,
					build.VersionPrefix(),
					1,
				),
			},
		}

		for _, testCase := range cases {
			resp, err := testCase.client.Get(s.AdminURL())
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected status code 200; got %d", resp.StatusCode)
			}
			respBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			respString := string(respBytes)
			expected := fmt.Sprintf(htmlTemplate, testCase.json)
			if respString != expected {
				t.Fatalf("expected %s; got %s", expected, respString)
			}
		}
	})
}

func TestGWRuntimeMarshalProto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	// Regression test against:
	// https://github.com/cockroachdb/cockroach/issues/49842
	runtime.DefaultHTTPError(
		ctx,
		runtime.NewServeMux(),
		&protoutil.ProtoPb{}, // calls XXX_size
		&httptest.ResponseRecorder{},
		nil, /* request */
		errors.New("boom"),
	)
}
