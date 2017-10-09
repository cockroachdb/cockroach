// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/pkg/errors"
)

var nodeTestBaseContext = testutils.NewNodeTestBaseContext()

// TestSelfBootstrap verifies operation when no bootstrap hosts have
// been specified.
func TestSelfBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, err := serverutils.StartServerRaw(base.TestServerArgs{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.TODO())
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
			Store: &storage.StoreTestingKnobs{
				MaxOffset: time.Second,
			},
		},
	}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// Run a command so that we are sure to touch the timestamp cache. This is
	// actually not needed because other commands run during server
	// initialization, but we cannot guarantee that's going to stay that way.
	get := &roachpb.GetRequest{
		Span: roachpb.Span{Key: roachpb.Key("a")},
	}
	if _, err := client.SendWrapped(
		context.Background(), s.KVClient().(*client.DB).GetSender(), get,
	); err != nil {
		t.Fatal(err)
	}

	now := s.Clock().Now()
	// We rely on s.Clock() having been initialized from hlc.UnixNano(), which is a
	// bit fragile.
	physicalNow := hlc.UnixNano()
	serverClockWasPushed := (now.Logical > 0) || (now.WallTime > physicalNow)
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
	defer s.Stopper().Stop(context.TODO())

	var data serverpb.JSONResponse
	testutils.SucceedsSoon(t, func() error {
		return getStatusJSONProto(s, "metrics/local", &data)
	})

	ctx := s.RPCContext()
	ctx.Insecure = false
	if err := getStatusJSONProto(s, "metrics/local", &data); !testutils.IsError(err, "http: server gave HTTP response to HTTPS client") {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestSecureHTTPRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
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
		if resp.StatusCode != http.StatusPermanentRedirect {
			t.Errorf("expected status code %d; got %d", http.StatusPermanentRedirect, resp.StatusCode)
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
		if resp.StatusCode != http.StatusPermanentRedirect {
			t.Errorf("expected status code %d; got %d", http.StatusPermanentRedirect, resp.StatusCode)
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
	defer s.Stopper().Stop(context.TODO())
	client, err := s.GetAuthenticatedHTTPClient()
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
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)
	tds := db.GetSender()

	if err := ts.node.storeCfg.DB.AdminSplit(context.TODO(), "m", "m"); err != nil {
		t.Fatal(err)
	}
	writes := []roachpb.Key{roachpb.Key("a"), roachpb.Key("z")}
	get := &roachpb.GetRequest{
		Span: roachpb.Span{Key: writes[0]},
	}
	get.EndKey = writes[len(writes)-1]
	if _, err := client.SendWrapped(context.Background(), tds, get); err == nil {
		t.Errorf("able to call Get with a key range: %v", get)
	}
	var delTS hlc.Timestamp
	for i, k := range writes {
		put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
		if _, err := client.SendWrapped(context.Background(), tds, put); err != nil {
			t.Fatal(err)
		}
		scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next())
		reply, err := client.SendWrapped(context.Background(), tds, scan)
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
		Span: roachpb.Span{
			Key:    writes[0],
			EndKey: roachpb.Key(writes[len(writes)-1]).Next(),
		},
		ReturnKeys: true,
	}
	reply, err := client.SendWrappedWith(context.Background(), tds, roachpb.Header{Timestamp: delTS}, del)
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

	scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next())
	txn := roachpb.MakeTransaction("MyTxn", nil, 0, 0, s.Clock().Now(), 0)
	reply, err = client.SendWrappedWith(context.Background(), tds, roachpb.Header{Txn: &txn}, scan)
	if err != nil {
		t.Fatal(err)
	}
	sr := reply.(*roachpb.ScanResponse)
	if txn := sr.Txn; txn == nil || txn.Name != "MyTxn" {
		t.Errorf("wanted Txn to persist, but it changed to %v", txn)
	}
	if rows := sr.Rows; len(rows) > 0 {
		t.Fatalf("scan after delete returned rows: %v", rows)
	}
}

// TestMultiRangeScanWithMaxResults tests that commands which access multiple
// ranges with MaxResults parameter are carried out properly.
func TestMultiRangeScanWithMaxResults(t *testing.T) {
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

	for i, tc := range testCases {
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.TODO())
		ts := s.(*TestServer)
		tds := db.GetSender()

		for _, sk := range tc.splitKeys {
			if err := ts.node.storeCfg.DB.AdminSplit(context.TODO(), sk, sk); err != nil {
				t.Fatal(err)
			}
		}

		for _, k := range tc.keys {
			put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
			if _, err := client.SendWrapped(context.Background(), tds, put); err != nil {
				t.Fatal(err)
			}
		}

		// Try every possible ScanRequest startKey.
		for start := 0; start < len(tc.keys); start++ {
			// Try every possible maxResults, from 1 to beyond the size of key array.
			for maxResults := 1; maxResults <= len(tc.keys)-start+1; maxResults++ {
				scan := roachpb.NewScan(tc.keys[start], tc.keys[len(tc.keys)-1].Next())
				reply, err := client.SendWrappedWith(
					context.Background(), tds, roachpb.Header{MaxSpanRequestKeys: int64(maxResults)}, scan,
				)
				if err != nil {
					t.Fatal(err)
				}
				rows := reply.(*roachpb.ScanResponse).Rows
				if start+maxResults <= len(tc.keys) && len(rows) != maxResults {
					t.Errorf("%d: start=%s: expected %d rows, but got %d", i, tc.keys[start], maxResults, len(rows))
				} else if start+maxResults == len(tc.keys)+1 && len(rows) != maxResults-1 {
					t.Errorf("%d: expected %d rows, but got %d", i, maxResults-1, len(rows))
				}
			}
		}
	}
}

func TestSystemConfigGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#12351")

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)
	ctx := context.TODO()

	key := sqlbase.MakeDescMetadataKey(keys.MaxReservedDescID)
	valAt := func(i int) *sqlbase.DatabaseDescriptor {
		return &sqlbase.DatabaseDescriptor{Name: "foo", ID: sqlbase.ID(i)}
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
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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
		var systemConfig config.SystemConfig
		select {
		case <-resultChan:
			systemConfig, _ = ts.gossip.GetSystemConfig()

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
		got := new(sqlbase.DatabaseDescriptor)
		if err := val.GetProto(got); err != nil {
			return err
		}
		if expected := valAt(2); !reflect.DeepEqual(got, expected) {
			return errors.Errorf("mismatch: expected %+v, got %+v", *expected, *got)
		}
		return nil
	})
}

func TestOfficializeAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := net.DefaultResolver.LookupHost(context.TODO(), host)
	if err != nil {
		t.Fatal(err)
	}

	for _, network := range []string{"tcp", "tcp4", "tcp6"} {
		t.Run(fmt.Sprintf("network=%s", network), func(t *testing.T) {
			for _, tc := range []struct {
				cfgAddr, lnAddr, expAddr string
			}{
				{"localhost:0", "127.0.0.1:1234", "localhost:1234"},
				{"localhost:1234", "127.0.0.1:2345", "localhost:1234"},
				{":1234", net.JoinHostPort(addrs[0], "2345"), net.JoinHostPort(host, "1234")},
				{":0", net.JoinHostPort(addrs[0], "2345"), net.JoinHostPort(host, "2345")},
			} {
				t.Run(tc.cfgAddr, func(t *testing.T) {
					lnAddr := util.NewUnresolvedAddr(network, tc.lnAddr)

					if unresolvedAddr, err := officialAddr(context.TODO(), tc.cfgAddr, lnAddr, os.Hostname); err != nil {
						t.Fatal(err)
					} else if retAddrString := unresolvedAddr.String(); retAddrString != tc.expAddr {
						t.Errorf("officialAddr(%s, %s) was %s; expected %s", tc.cfgAddr, tc.lnAddr, retAddrString, tc.expAddr)
					}
				})
			}
		})
	}

	osHostnameError := errors.New("some error")

	t.Run("osHostnameError", func(t *testing.T) {
		if _, err := officialAddr(
			context.TODO(),
			":0",
			util.NewUnresolvedAddr("tcp", "0.0.0.0:1234"),
			func() (string, error) { return "", osHostnameError },
		); errors.Cause(err) != osHostnameError {
			t.Fatalf("unexpected error %v", err)
		}
	})

	t.Run("LookupHostError", func(t *testing.T) {
		if _, err := officialAddr(
			context.TODO(),
			"notarealhost.local.:0",
			util.NewUnresolvedAddr("tcp", "0.0.0.0:1234"),
			os.Hostname,
		); !testutils.IsError(err, "lookup notarealhost.local.(?: on .+)?: no such host") {
			// On Linux but not on macOS, the error returned from
			// (*net.Resolver).LookupHost reports the DNS server used; permit
			// both.
			t.Fatalf("unexpected error %v", err)
		}
	})
}

func TestListenURLFileCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	file, err := ioutil.TempFile(os.TempDir(), t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := serverutils.StartServerRaw(base.TestServerArgs{
		ListeningURLFile: file.Name(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.TODO())
	defer func() {
		if err := os.Remove(file.Name()); err != nil {
			t.Error(err)
		}
	}()

	data, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	u, err := url.Parse(string(data))
	if err != nil {
		t.Fatal(err)
	}

	if s.ServingAddr() != u.Host {
		t.Fatalf("expected URL %s to match host %s", u, s.ServingAddr())
	}
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
	defer s.Stopper().Stop(context.TODO())

	files, err := filepath.Glob(filepath.Join(dir, "cockroach.*"))
	if err != nil {
		t.Fatal(err)
	}

	li := listenerInfo{
		advertise: s.ServingAddr(),
		http:      s.HTTPAddr(),
		listen:    s.Addr(),
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

func TestHeartbeatCallbackForDecommissioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)
	nodeLiveness := ts.nodeLiveness

	for {
		// Morally this loop only runs once. However, early in the boot
		// sequence, the own liveness record may not yet exist. This
		// has not been observed in this test, but was the root cause
		// of #16656, so consider this part of its documentation.
		liveness, err := nodeLiveness.Self()
		if err != nil {
			if errors.Cause(err) == storage.ErrNoLivenessRecord {
				continue
			}
			t.Fatal(err)
		}
		if liveness.Decommissioning {
			t.Fatal("Decommissioning set already")
		}
		if liveness.Draining {
			t.Fatal("Draining set already")
		}
		break
	}
	if _, err := nodeLiveness.SetDecommissioning(context.Background(), ts.nodeIDContainer.Get(), true); err != nil {
		t.Fatal(err)
	}

	// Node should realize it is decommissioning after next heartbeat update.
	testutils.SucceedsSoon(t, func() error {
		nodeLiveness.PauseHeartbeat(false) // trigger immediate heartbeat
		if liveness, err := nodeLiveness.Self(); err != nil {
			// Record must exist at this point, so any error is fatal now.
			t.Fatal(err)
		} else if !liveness.Decommissioning {
			return errors.Errorf("not decommissioning")
		} else if !liveness.Draining {
			return errors.Errorf("not draining")
		}
		return nil
	})
}

// TestCleanupTempDirs verifies that on server startup, abandoned temporary
// directories in the record file are removed.
func TestCleanupTempDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	storeDir, storeDirCleanup := testutils.TempDir(t)
	defer storeDirCleanup()

	// Create temporary directories and add them to the record file.
	tempDir1, tempDir1Cleanup := tempStorageDir(t, storeDir)
	defer tempDir1Cleanup()
	tempDir2, tempDir2Cleanup := tempStorageDir(t, storeDir)
	defer tempDir2Cleanup()
	if err := recordTempDir(storeDir, tempDir1); err != nil {
		t.Fatal(err)
	}
	if err := recordTempDir(storeDir, tempDir2); err != nil {
		t.Fatal(err)
	}

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{Path: storeDir}},
	})
	defer s.Stopper().Stop(context.TODO())

	// Verify that the temporary directories are removed after startup.
	for i, tempDir := range []string{tempDir1, tempDir2} {
		_, err := os.Stat(tempDir)
		// os.Stat returns a nil err if the file exists, so we need to check
		// the NOT of this condition instead of os.IsExist() (which returns
		// false and misses this error).
		if !os.IsNotExist(err) {
			if err == nil {
				t.Fatalf("temporary directory %d: %s not cleaned up after stopping server", i, tempDir)
			} else {
				// Unexpected error.
				t.Fatal(err)
			}
		}
	}
}

// TestTempStorage verifies that on server startup:
// 1. A temporary directory is created.
// 2. If store is on disk, The path of the temporary directory is recorded in
// the store's path.
// On server shutdown:
// 3. The temporary directory is removed.
func TestTempStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	storeDir, storeDirCleanup := testutils.TempDir(t)
	defer storeDirCleanup()
	for _, tc := range []struct {
		name      string
		storeSpec base.StoreSpec
	}{
		{
			name:      "OnDiskStore",
			storeSpec: base.StoreSpec{Path: storeDir},
		},
		{
			name:      "InMemStore",
			storeSpec: base.StoreSpec{InMemory: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tempDir, tempDirCleanup := testutils.TempDir(t)
			defer tempDirCleanup()

			// This will be cleaned up by tempDirCleanup.
			tempStorage := base.TempStorageConfigFromEnv(
				context.TODO(), tc.storeSpec, tempDir, base.DefaultTempStorageMaxSizeBytes)

			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
				StoreSpecs:        []base.StoreSpec{tc.storeSpec},
				TempStorageConfig: tempStorage,
			})

			// 1. Verify temporary directory is created. Since
			// starting a server generates an arbitrary temporary
			// directory under tempDir, we need to retrieve that
			// directory and check it exists.
			files, err := ioutil.ReadDir(tempStorage.ParentDir)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 {
				t.Fatalf("expected only one temporary file to be created during server startup: found %d files", len(files))
			}
			tempPath := filepath.Join(tempStorage.ParentDir, files[0].Name())

			// 2. (If on-disk store) Verify that the temporary
			// directory's path is recorded in the temporary
			// directory record file under the store's path.
			if !tc.storeSpec.InMemory {
				expected := append([]byte(tempPath), '\n')
				actual, err := ioutil.ReadFile(filepath.Join(storeDir, tempStorageDirsRecordFilename))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(expected, actual) {
					t.Fatalf("record file not properly appended to.\nexpected: %s\nactual %s", expected, actual)
				}
			}

			s.Stopper().Stop(context.TODO())

			// 3. verify that the temporary directory is removed.
			_, err = os.Stat(tempPath)
			// os.Stat returns a nil err if the file exists, so we
			// need to check the NOT of this condition instead of
			// os.IsExist() (which returns false and misses this
			// error).
			if !os.IsNotExist(err) {
				if err == nil {
					t.Fatalf("temporary directory %s not cleaned up after stopping server", tempPath)
				} else {
					// Unexpected error.
					t.Fatal(err)
				}
			}

			// 4. (If on-disk store) Verify that the removed
			// temporary directory's path is also removed
			// from the record file.
			if !tc.storeSpec.InMemory {
				contents, err := ioutil.ReadFile(filepath.Join(storeDir, tempStorageDirsRecordFilename))
				if err != nil {
					t.Fatal(err)
				}
				if len(contents) > 0 {
					t.Fatalf("temporary directory path not removed from record file.\ncontents: %s", contents)
				}

			}
		})
	}
}

func tempStorageDir(t *testing.T, dir string) (string, func()) {
	tempStoragePath, err := ioutil.TempDir(dir, "temp-engine-storage")
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(tempStoragePath); err != nil {
			t.Fatal(err)
		}
	}
	return tempStoragePath, cleanup
}
