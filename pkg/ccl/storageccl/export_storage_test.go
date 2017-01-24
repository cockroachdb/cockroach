// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package storageccl

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"golang.org/x/net/context"

	"io"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/rlmcpherson/s3gof3r"
)

// TODO(dt): remove when calling code lands and these have real users.
var _, _ = S3AccessKeyParam, S3SecretParam
var _, _ = ExportStorageFromURI, ExportStorageConfFromURI

func testExportToTarget(t *testing.T, args roachpb.ExportStorage) {
	testingContent := []byte("hello world")
	testingFilename := "testing-123"
	ctx := context.TODO()
	// Setup a sink for the given args.
	s, err := MakeExportStorage(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if conf := s.Conf(); conf != args {
		t.Fatalf("got %+v expected %+v", conf, args)
	}

	w, err := s.PutFile(ctx, testingFilename)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Write something to the local file specified by our sink.
	t.Logf("writing %q to %s", string(testingContent), w.LocalFile())
	tmp, err := os.Create(w.LocalFile())
	if err != nil {
		t.Fatal(err)
	}
	defer tmp.Close()
	if _, err := tmp.Write(testingContent); err != nil {
		t.Fatal(err)
	}

	// Let the sink finish its work (e.g. upload to storage).
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	t.Logf("verifying content of %s is %q", testingFilename, string(testingContent))
	// Attempt to read (or fetch) the result.
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
		t.Fatalf("Wrong content. Expected %v, got %v", testingContent, content)
	}
	if err := s.Delete(ctx, testingFilename); err != nil {
		t.Fatal(err)
	}
}

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t, 0)
	defer cleanupFn()

	testExportToTarget(t, roachpb.ExportStorage{
		Provider:  roachpb.ExportStorageProvider_LocalFile,
		LocalFile: roachpb.ExportStorage_LocalFilePath{Path: p},
	})
}

func TestPutHttp(t *testing.T) {
	tmp, dirCleanup := testutils.TempDir(t, 0)
	defer dirCleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		localfile := filepath.Join(tmp, filepath.Base(r.URL.Path))
		switch r.Method {
		case "PUT":
			f, err := os.Create(localfile)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			defer f.Close()
			defer r.Body.Close()
			if _, err := io.Copy(f, r.Body); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
		case "GET":
			http.ServeFile(w, r, localfile)
		case "DELETE":
			if err := os.Remove(localfile); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
		default:
			http.Error(w, "unsupported method "+r.Method, 400)
		}
	}))
	defer srv.Close()
	t.Logf("Mock HTTP Storage %q", srv.URL)
	testExportToTarget(t, roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_Http,
		HttpPath: roachpb.ExportStorage_Http{BaseUri: srv.URL + "/"},
	})
}

func TestPutS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s3Keys, err := s3gof3r.EnvKeys()
	if err != nil {
		s3Keys, err = s3gof3r.InstanceKeys()
		if err != nil {
			t.Skip("No AWS keys instance or env keys")
		}
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	testExportToTarget(t, roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_S3,
		S3Config: &roachpb.ExportStorage_S3{
			Bucket:    bucket,
			Prefix:    "backup-test",
			AccessKey: s3Keys.AccessKey,
			Secret:    s3Keys.SecretKey,
		},
	})
}

func TestPutGoogleCloud(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		t.Skip("GS_BUCKET env var must be set")
	}

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	testExportToTarget(t, roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_GoogleCloud,
		GoogleCloudConfig: &roachpb.ExportStorage_GCS{
			Bucket: bucket,
			Prefix: "backup-test"},
	})
}
