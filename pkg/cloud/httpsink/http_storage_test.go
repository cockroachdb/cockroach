// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package httpsink

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func TestPutHttp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	testSettings := cluster.MakeTestingClusterSettings()

	const badHeadResponse = "bad-head-response"
	user := security.RootUserName()
	ctx := context.Background()

	makeServer := func() (*url.URL, func() int, func()) {
		var files int
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			localfile := filepath.Join(tmp, filepath.Base(r.URL.Path))
			switch r.Method {
			case "PUT":
				f, err := os.Create(localfile)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				defer f.Close()
				if _, err := io.Copy(f, r.Body); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				files++
				w.WriteHeader(201)
			case "GET", "HEAD":
				if filepath.Base(localfile) == badHeadResponse {
					http.Error(w, "HEAD not implemented", 500)
					return
				}
				http.ServeFile(w, r, localfile)
			case "DELETE":
				if err := os.Remove(localfile); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				w.WriteHeader(204)
			default:
				http.Error(w, "unsupported method "+r.Method, 400)
			}
		}))

		u := testSettings.MakeUpdater()
		if err := u.Set(ctx, "cloudstorage.http.custom_ca", settings.EncodedValue{
			Value: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})),
			Type:  "s",
		}); err != nil {
			t.Fatal(err)
		}

		cleanup := func() {
			srv.Close()
			if err := u.Set(ctx, "cloudstorage.http.custom_ca", settings.EncodedValue{
				Value: "",
				Type:  "s",
			}); err != nil {
				t.Fatal(err)
			}
		}

		t.Logf("Mock HTTP Storage %q", srv.URL)
		uri, err := url.Parse(srv.URL)
		if err != nil {
			srv.Close()
			t.Fatal(err)
		}
		uri.Path = filepath.Join(uri.Path, "testing")
		return uri, func() int { return files }, cleanup
	}

	t.Run("singleHost", func(t *testing.T) {
		srv, files, cleanup := makeServer()
		defer cleanup()
		cloudtestutils.CheckExportStore(t, srv.String(), false, user, nil, nil, testSettings)
		if expected, actual := 14, files(); expected != actual {
			t.Fatalf("expected %d files to be written to single http store, got %d", expected, actual)
		}
	})

	t.Run("multiHost", func(t *testing.T) {
		srv1, files1, cleanup1 := makeServer()
		defer cleanup1()
		srv2, files2, cleanup2 := makeServer()
		defer cleanup2()
		srv3, files3, cleanup3 := makeServer()
		defer cleanup3()

		combined := *srv1
		combined.Host = strings.Join([]string{srv1.Host, srv2.Host, srv3.Host}, ",")

		cloudtestutils.CheckExportStore(t, combined.String(), true, user, nil, nil, testSettings)
		if expected, actual := 3, files1(); expected != actual {
			t.Fatalf("expected %d files written to http host 1, got %d", expected, actual)
		}
		if expected, actual := 4, files2(); expected != actual {
			t.Fatalf("expected %d files written to http host 2, got %d", expected, actual)
		}
		if expected, actual := 4, files3(); expected != actual {
			t.Fatalf("expected %d files written to http host 3, got %d", expected, actual)
		}
	})

	// Ensure that servers that error on HEAD are handled gracefully.
	t.Run("bad-head-response", func(t *testing.T) {
		ctx := context.Background()

		srv, _, cleanup := makeServer()
		defer cleanup()

		conf, err := cloud.ExternalStorageConfFromURI(srv.String(), user)
		if err != nil {
			t.Fatal(err)
		}
		s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{},
			testSettings, blobs.TestEmptyBlobClientFactory, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		const file = "file"
		var content = []byte("contents")
		if err := cloud.WriteFile(ctx, s, file, bytes.NewReader(content)); err != nil {
			t.Fatal(err)
		}
		if err := cloud.WriteFile(ctx, s, badHeadResponse, bytes.NewReader(content)); err != nil {
			t.Fatal(err)
		}
		if sz, err := s.Size(ctx, file); err != nil {
			t.Fatal(err)
		} else if sz != int64(len(content)) {
			t.Fatalf("expected %d, got %d", len(content), sz)
		}
		if sz, err := s.Size(ctx, badHeadResponse); !testutils.IsError(err, "500 Internal Server Error") {
			t.Fatalf("unexpected error: %v", err)
		} else if sz != 0 {
			t.Fatalf("expected 0 size, got %d", sz)
		}
	})
}

func rangeStart(r string) (int, error) {
	if len(r) == 0 {
		return 0, nil
	}
	r = strings.TrimPrefix(r, "bytes=")

	return strconv.Atoi(r[:strings.IndexByte(r, '-')])
}

func TestHttpGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	data := []byte("to serve, or not to serve.  c'est la question")

	defer func(opts retry.Options) {
		cloud.HTTPRetryOptions = opts
	}(cloud.HTTPRetryOptions)

	cloud.HTTPRetryOptions.InitialBackoff = 1 * time.Microsecond
	cloud.HTTPRetryOptions.MaxBackoff = 10 * time.Millisecond
	cloud.HTTPRetryOptions.MaxRetries = 25

	testSettings := cluster.MakeTestingClusterSettings()

	for _, tc := range []int{1, 2, 5, 16, 32, len(data) - 1, len(data)} {
		t.Run(fmt.Sprintf("read-%d", tc), func(t *testing.T) {
			limit := tc
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				start, err := rangeStart(r.Header.Get("Range"))
				if start < 0 || start >= len(data) {
					t.Errorf("invalid start offset %d in range header %s",
						start, r.Header.Get("Range"))
				}
				end := start + limit
				if end > len(data) {
					end = len(data)
				}

				w.Header().Add("Accept-Ranges", "bytes")
				w.Header().Add("Content-Length", strconv.Itoa(len(data)-start))

				if start > 0 {
					w.Header().Add(
						"Content-Range",
						fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				}

				if err == nil {
					_, err = w.Write(data[start:end])
				}
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))

			// Start antagonist function that aggressively closes client connections.
			ctx, cancelAntagonist := context.WithCancel(context.Background())
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				opts := retry.Options{
					InitialBackoff: 500 * time.Microsecond,
					MaxBackoff:     5 * time.Millisecond,
				}
				for attempt := retry.StartWithCtx(ctx, opts); attempt.Next(); {
					s.CloseClientConnections()
				}
				return nil
			})

			conf := roachpb.ExternalStorage{HttpPath: roachpb.ExternalStorage_Http{BaseUri: s.URL}}
			store, err := MakeHTTPStorage(ctx, cloud.ExternalStorageContext{Settings: testSettings}, conf)
			require.NoError(t, err)

			var file ioctx.ReadCloserCtx

			// Cleanup.
			defer func() {
				s.Close()
				if store != nil {
					require.NoError(t, store.Close())
				}
				if file != nil {
					require.NoError(t, file.Close(ctx))
				}
				cancelAntagonist()
				_ = g.Wait()
			}()

			// Read the file and verify results.
			file, err = store.ReadFile(ctx, "/something")
			require.NoError(t, err)

			b, err := ioctx.ReadAll(ctx, file)
			require.NoError(t, err)
			require.EqualValues(t, data, b)
		})
	}
}

func TestHttpGetWithCancelledContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer s.Close()
	testSettings := cluster.MakeTestingClusterSettings()

	conf := roachpb.ExternalStorage{HttpPath: roachpb.ExternalStorage_Http{BaseUri: s.URL}}
	store, err := MakeHTTPStorage(context.Background(), cloud.ExternalStorageContext{Settings: testSettings}, conf)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = store.ReadFile(ctx, "/something")
	require.Error(t, context.Canceled, err)
}

func TestCanDisableHttp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	conf := base.ExternalIODirConfig{
		DisableHTTP: true,
	}
	testSettings := cluster.MakeTestingClusterSettings()

	s, err := cloud.MakeExternalStorage(
		context.Background(),
		roachpb.ExternalStorage{Provider: roachpb.ExternalStorageProvider_http},
		conf, testSettings, blobs.TestEmptyBlobClientFactory, nil, nil)
	require.Nil(t, s)
	require.Error(t, err)
}

func TestCanDisableOutbound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	conf := base.ExternalIODirConfig{
		DisableOutbound: true,
	}
	testSettings := cluster.MakeTestingClusterSettings()

	for _, provider := range []roachpb.ExternalStorageProvider{
		roachpb.ExternalStorageProvider_http,
		roachpb.ExternalStorageProvider_s3,
		roachpb.ExternalStorageProvider_gs,
		roachpb.ExternalStorageProvider_nodelocal,
	} {
		s, err := cloud.MakeExternalStorage(
			context.Background(),
			roachpb.ExternalStorage{Provider: provider},
			conf, testSettings, blobs.TestEmptyBlobClientFactory, nil, nil)
		require.Nil(t, s)
		require.Error(t, err)
	}
}

func TestExternalStorageCanUseHTTPProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf("proxied-%s", r.URL)))
	}))
	defer proxy.Close()

	testSettings := cluster.MakeTestingClusterSettings()

	// Normally, we would set proxy via HTTP_PROXY environment variable.
	// However, if we run multiple tests in this package, and earlier tests
	// happen to create an http client, then the DefaultTransport will have
	// been been initialized with an empty Proxy.  So, set proxy directly.
	http.DefaultTransport.(*http.Transport).Proxy = func(_ *http.Request) (*url.URL, error) {
		return url.Parse(proxy.URL)
	}
	defer func() {
		http.DefaultTransport.(*http.Transport).Proxy = nil
	}()

	conf, err := cloud.ExternalStorageConfFromURI("http://my-server", security.RootUserName())
	require.NoError(t, err)
	s, err := cloud.MakeExternalStorage(
		context.Background(), conf, base.ExternalIODirConfig{}, testSettings, nil,
		nil, nil)
	require.NoError(t, err)
	stream, err := s.ReadFile(context.Background(), "file")
	require.NoError(t, err)
	defer stream.Close(ctx)
	data, err := ioctx.ReadAll(ctx, stream)
	require.NoError(t, err)

	require.EqualValues(t, "proxied-http://my-server/file", string(data))
}

type alwaysRefuseConnectionDialer struct {
	net.Dialer
}

func (d *alwaysRefuseConnectionDialer) DialContext(
	_ context.Context, _, _ string,
) (net.Conn, error) {
	return nil, cloudtestutils.EConnRefused
}

func TestExhaustRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testSettings := cluster.MakeTestingClusterSettings()

	// Override DialContext implementation in http transport.
	dialer := &alwaysRefuseConnectionDialer{}

	// Override transport to return antagonistic connection.
	transport := http.DefaultTransport.(*http.Transport)
	transport.DialContext =
		func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		}

	defer func() {
		transport.DialContext = nil
	}()

	// Override retry options to retry faster.
	defer func(opts retry.Options) {
		cloud.HTTPRetryOptions = opts
	}(cloud.HTTPRetryOptions)

	cloud.HTTPRetryOptions.InitialBackoff = 1 * time.Microsecond
	cloud.HTTPRetryOptions.MaxBackoff = 10 * time.Millisecond
	cloud.HTTPRetryOptions.MaxRetries = 10

	conf := roachpb.ExternalStorage{HttpPath: roachpb.ExternalStorage_Http{BaseUri: "http://does.not.matter"}}
	store, err := MakeHTTPStorage(context.Background(), cloud.ExternalStorageContext{Settings: testSettings}, conf)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	_, err = store.ReadFile(context.Background(), "/something")
	require.Error(t, err)
}
