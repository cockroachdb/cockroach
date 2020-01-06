// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func TestPutHttp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	const badHeadResponse = "bad-head-response"

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
		if err := u.Set(
			cloudstorageHTTPCASetting,
			string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})),
			"s",
		); err != nil {
			t.Fatal(err)
		}

		cleanup := func() {
			srv.Close()
			if err := u.Set(cloudstorageHTTPCASetting, "", "s"); err != nil {
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
		testExportStore(t, srv.String(), false)
		if expected, actual := 13, files(); expected != actual {
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

		testExportStore(t, combined.String(), true)
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
		ctx := context.TODO()

		srv, _, cleanup := makeServer()
		defer cleanup()

		conf, err := ExternalStorageConfFromURI(srv.String())
		if err != nil {
			t.Fatal(err)
		}
		s, err := MakeExternalStorage(ctx, conf, testSettings, blobs.TestEmptyBlobClientFactory)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		const file = "file"
		var content = []byte("contents")
		if err := s.WriteFile(ctx, file, bytes.NewReader(content)); err != nil {
			t.Fatal(err)
		}
		if err := s.WriteFile(ctx, badHeadResponse, bytes.NewReader(content)); err != nil {
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

	httpRetryOptions.InitialBackoff = 1 * time.Microsecond
	httpRetryOptions.MaxBackoff = 10 * time.Millisecond
	httpRetryOptions.MaxRetries = 100

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
					MaxBackoff:     1 * time.Millisecond,
				}
				for attempt := retry.StartWithCtx(ctx, opts); attempt.Next(); {
					s.CloseClientConnections()
				}
				return nil
			})

			store, err := makeHTTPStorage(s.URL, testSettings)
			require.NoError(t, err)

			var file io.ReadCloser

			// Cleanup.
			defer func() {
				s.Close()
				if store != nil {
					require.NoError(t, store.Close())
				}
				if file != nil {
					require.NoError(t, file.Close())
				}
				cancelAntagonist()
				_ = g.Wait()
			}()

			// Read the file and verify results.
			file, err = store.ReadFile(ctx, "/something")
			require.NoError(t, err)

			b, err := ioutil.ReadAll(file)
			require.NoError(t, err)
			require.EqualValues(t, data, b)
		})
	}
}

func TestHttpGetWithCancelledContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer s.Close()

	store, err := makeHTTPStorage(s.URL, testSettings)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = store.ReadFile(ctx, "/something")
	require.Error(t, context.Canceled, err)
}
