// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"golang.org/x/oauth2/google"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func appendPath(t *testing.T, s, add string) string {
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = path.Join(u.Path, add)
	return u.String()
}

var testSettings *cluster.Settings

func init() {
	testSettings = cluster.MakeTestingClusterSettings()
	up := testSettings.MakeUpdater()
	if err := up.Set(cloudstorageGSDefaultKey, os.Getenv("GS_JSONKEY"), gcsDefault.Typ()); err != nil {
		panic(err)
	}
}

func storeFromURI(ctx context.Context, t *testing.T, uri string) ExportStorage {
	conf, err := ExportStorageConfFromURI(uri)
	if err != nil {
		t.Fatal(err)
	}
	// Setup a sink for the given args.
	s, err := MakeExportStorage(ctx, conf, testSettings)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testExportStore(t *testing.T, storeURI string, skipSingleFile bool) {
	ctx := context.TODO()

	conf, err := ExportStorageConfFromURI(storeURI)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	s, err := MakeExportStorage(ctx, conf, testSettings)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if readConf := s.Conf(); readConf != conf {
		t.Fatalf("conf does not roundtrip: started with %+v, got back %+v", conf, readConf)
	}

	t.Run("simple round trip", func(t *testing.T) {
		sampleName := "somebytes"
		sampleBytes := "hello world"

		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("%s-%d", sampleName, i)
			payload := []byte(strings.Repeat(sampleBytes, i))
			if err := s.WriteFile(ctx, name, bytes.NewReader(payload)); err != nil {
				t.Fatal(err)
			}

			if sz, err := s.Size(ctx, name); err != nil {
				t.Error(err)
			} else if sz != int64(len(payload)) {
				t.Errorf("size mismatch, got %d, expected %d", sz, len(payload))
			}

			r, err := s.ReadFile(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			res, err := ioutil.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(res, payload) {
				t.Fatalf("got %v expected %v", res, payload)
			}
			if err := s.Delete(ctx, name); err != nil {
				t.Fatal(err)
			}
		}
	})

	// The azure driver makes us chunk files that are greater than 4mb, so make
	// sure that files larger than that work on all the providers.
	t.Run("8mb-tempfile", func(t *testing.T) {
		const size = 1024 * 1024 * 8 // 8MiB
		testingContent := make([]byte, size)
		if _, err := rand.Read(testingContent); err != nil {
			t.Fatal(err)
		}
		testingFilename := "testing-123"

		// Write some random data (random so it doesn't compress).
		if err := s.WriteFile(ctx, testingFilename, bytes.NewReader(testingContent)); err != nil {
			t.Fatal(err)
		}

		// Attempt to read (or fetch) it back.
		res, err := s.ReadFile(ctx, testingFilename)
		if err != nil {
			t.Fatalf("Could not get reader for %s: %+v", testingFilename, err)
		}
		defer res.Close()
		content, err := ioutil.ReadAll(res)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(content, testingContent) {
			t.Fatalf("wrong content")
		}
		if err := s.Delete(ctx, testingFilename); err != nil {
			t.Fatal(err)
		}
	})
	if skipSingleFile {
		return
	}
	t.Run("read-single-file-by-uri", func(t *testing.T) {
		const testingFilename = "A"
		if err := s.WriteFile(ctx, testingFilename, bytes.NewReader([]byte("aaa"))); err != nil {
			t.Fatal(err)
		}
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename))
		defer singleFile.Close()

		res, err := singleFile.ReadFile(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close()
		content, err := ioutil.ReadAll(res)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(content, []byte("aaa")) {
			t.Fatalf("wrong content")
		}
		if err := s.Delete(ctx, testingFilename); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("write-single-file-by-uri", func(t *testing.T) {
		const testingFilename = "B"
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename))
		defer singleFile.Close()

		if err := singleFile.WriteFile(ctx, "", bytes.NewReader([]byte("bbb"))); err != nil {
			t.Fatal(err)
		}

		res, err := s.ReadFile(ctx, testingFilename)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close()
		content, err := ioutil.ReadAll(res)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(content, []byte("bbb")) {
			t.Fatalf("wrong content")
		}
		if err := s.Delete(ctx, testingFilename); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testSettings.ExternalIODir = p
	dest, err := MakeLocalStorageURI(p)
	if err != nil {
		t.Fatal(err)
	}

	testExportStore(t, dest, false)
}

func TestLocalIOLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	const allowed = "/allowed"
	testSettings.ExternalIODir = allowed

	for dest, expected := range map[string]string{allowed: "", "/../../blah": "not allowed"} {
		u := fmt.Sprintf("nodelocal://%s", dest)

		conf, err := ExportStorageConfFromURI(u)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := MakeExportStorage(ctx, conf, testSettings); !testutils.IsError(err, expected) {
			t.Fatal(err)
		}
	}
}

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
		uri.Path = path.Join(uri.Path, "testing")
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

		conf, err := ExportStorageConfFromURI(srv.String())
		if err != nil {
			t.Fatal(err)
		}
		s, err := MakeExportStorage(ctx, conf, testSettings)
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

func TestPutS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		t.Skip("No AWS credentials")
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	testExportStore(t,
		fmt.Sprintf(
			"s3://%s/%s?%s=%s&%s=%s",
			bucket, "backup-test",
			S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
			S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
		),
		false,
	)
}

func TestPutS3Endpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_S3_ENDPOINT":        S3EndpointParam,
		"AWS_S3_ENDPOINT_KEY":    S3AccessKeyParam,
		"AWS_S3_ENDPOINT_REGION": S3RegionParam,
		"AWS_S3_ENDPOINT_SECRET": S3SecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			t.Skipf("%s env var must be set", env)
		}
		q.Add(param, v)
	}

	bucket := os.Getenv("AWS_S3_ENDPOINT_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_ENDPOINT_BUCKET env var must be set")
	}

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	testExportStore(t, u.String(), false)
}

func TestPutGoogleCloud(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		t.Skip("GS_BUCKET env var must be set")
	}

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	t.Run("empty", func(t *testing.T) {
		testExportStore(t, fmt.Sprintf("gs://%s/%s", bucket, "backup-test-empty"), false)
	})
	t.Run("default", func(t *testing.T) {
		testExportStore(t, fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-default", AuthParam, authParamDefault), false)
	})
	t.Run("implicit", func(t *testing.T) {
		// Only test these if they exist.
		if _, err := google.FindDefaultCredentials(context.TODO()); err != nil {
			t.Skip(err)
		}
		testExportStore(t, fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-implicit", AuthParam, authParamImplicit), false)
	})
}

func TestPutAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	if accountName == "" || accountKey == "" {
		t.Skip("AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY env vars must be set")
	}
	bucket := os.Getenv("AZURE_CONTAINER")
	if bucket == "" {
		t.Skip("AZURE_CONTAINER env var must be set")
	}

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	testExportStore(t,
		fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
			bucket, "backup-test",
			AzureAccountNameParam, url.QueryEscape(accountName),
			AzureAccountKeyParam, url.QueryEscape(accountKey),
		),
		false,
	)
}
