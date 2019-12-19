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
	"crypto/rand"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
)

func appendPath(t *testing.T, s, add string) string {
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = filepath.Join(u.Path, add)
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

func storeFromURI(
	ctx context.Context, t *testing.T, uri string, clientFactory blobs.BlobClientFactory,
) ExternalStorage {
	conf, err := ExternalStorageConfFromURI(uri)
	if err != nil {
		t.Fatal(err)
	}
	// Setup a sink for the given args.
	s, err := MakeExternalStorage(ctx, conf, testSettings, clientFactory)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testExportStore(t *testing.T, storeURI string, skipSingleFile bool) {
	ctx := context.TODO()

	conf, err := ExternalStorageConfFromURI(storeURI)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := MakeExternalStorage(ctx, conf, testSettings, clientFactory)
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
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename), clientFactory)
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
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename), clientFactory)
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

func testListFiles(t *testing.T, storeURI string) {
	ctx := context.TODO()
	dataLetterFiles := []string{"file/letters/dataA.csv", "file/letters/dataB.csv", "file/letters/dataC.csv"}
	dataNumberFiles := []string{"file/numbers/data1.csv", "file/numbers/data2.csv", "file/numbers/data3.csv"}
	letterFiles := []string{"file/abc/A.csv", "file/abc/B.csv", "file/abc/C.csv"}
	fileNames := append(dataLetterFiles, dataNumberFiles...)
	fileNames = append(fileNames, letterFiles...)
	sort.Strings(fileNames)

	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	for _, fileName := range fileNames {
		file := storeFromURI(ctx, t, storeURI, clientFactory)
		if err := file.WriteFile(ctx, fileName, bytes.NewReader([]byte("bbb"))); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}

	uri, _ := url.Parse(storeURI)
	expectedBaseURI := fmt.Sprintf("%s://%s%s", uri.Scheme, uri.Host, uri.Path)

	for _, tc := range []struct {
		name       string
		URI        string
		resultList []string
	}{
		{
			"list-all-csv",
			appendPath(t, storeURI, "file/*/*.csv"),
			fileNames,
		},
		{
			"list-letter-csv",
			appendPath(t, storeURI, "file/abc/?.csv"),
			letterFiles,
		},
		{
			"list-data-num-csv",
			appendPath(t, storeURI, "file/numbers/data[0-9].csv"),
			dataNumberFiles,
		},
		{
			"wildcard-bucket-and-filename",
			appendPath(t, storeURI, "*/numbers/*.csv"),
			dataNumberFiles,
		},
		{
			"list-all-csv-skip-dir",
			// filepath.Glob() assumes that / is the separator, and enforces that it's there.
			// So this pattern would not actually match anything.
			appendPath(t, storeURI, "file/*.csv"),
			[]string{},
		},
		{
			"list-no-matches",
			appendPath(t, storeURI, "file/letters/dataD.csv"),
			[]string{},
		},
		{
			"list-escaped-star",
			appendPath(t, storeURI, "file/*/\\*.csv"),
			[]string{},
		},
		{
			"list-escaped-range",
			appendPath(t, storeURI, "file/*/data\\[0-9\\].csv"),
			[]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := storeFromURI(ctx, t, tc.URI, clientFactory)
			filesList, err := s.ListFiles(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if len(filesList) != len(tc.resultList) {
				t.Fatal(`listed incorrect number of files`, filesList)
			}
			for i, result := range filesList {
				if result != fmt.Sprintf("%s/%s", expectedBaseURI, tc.resultList[i]) {
					t.Fatal(`resulting list is incorrect`, expectedBaseURI, filesList)
				}
			}
		})
	}

	for _, fileName := range fileNames {
		file := storeFromURI(ctx, t, storeURI, clientFactory)
		if err := file.Delete(ctx, fileName); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
}

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testSettings.ExternalIODir = p
	dest := MakeLocalStorageURI(p)

	testExportStore(t, dest, false)
	testListFiles(t, fmt.Sprintf("nodelocal:///%s", "listing-test"))
}

func TestLocalIOLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	const allowed = "/allowed"
	testSettings.ExternalIODir = allowed

	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	for dest, expected := range map[string]string{allowed: "", "/../../blah": "not allowed"} {
		u := fmt.Sprintf("nodelocal://%s", dest)
		e, err := ExternalStorageFromURI(ctx, u, testSettings, clientFactory)
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.ListFiles(ctx)
		if !testutils.IsError(err, expected) {
			t.Fatal(err)
		}
	}

	for host, expectErr := range map[string]bool{"": false, "1": false, "0": false, "blah": true} {
		u := fmt.Sprintf("nodelocal://%s/path/to/file", host)

		var expected string
		if expectErr {
			expected = "host component of nodelocal URI must be a node ID"
		}
		if _, err := ExternalStorageConfFromURI(u); !testutils.IsError(err, expected) {
			t.Fatalf("%q: expected error %q, got %v", u, expected, err)
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

func TestPutS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		t.Skip("No AWS credentials")
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	ctx := context.TODO()
	t.Run("auth-empty-no-cred", func(t *testing.T) {
		_, err := ExternalStorageFromURI(
			ctx, fmt.Sprintf("s3://%s/%s", bucket, "backup-test-default"),
			testSettings, blobs.TestEmptyBlobClientFactory,
		)
		require.EqualError(t, err, fmt.Sprintf(
			`%s is set to '%s', but %s is not set`,
			AuthParam,
			authParamSpecified,
			S3AccessKeyParam,
		))
	})
	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			t.Skip(err)
		}

		testExportStore(
			t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s",
				bucket, "backup-test-default",
				AuthParam, authParamImplicit,
			),
			false,
		)
	})

	t.Run("auth-specified", func(t *testing.T) {
		testExportStore(t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s&%s=%s",
				bucket, "backup-test",
				S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
				S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
			),
			false,
		)
		testListFiles(t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s&%s=%s",
				bucket, "listing-test",
				S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
				S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
			),
		)
	})
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

	t.Run("empty", func(t *testing.T) {
		testExportStore(t, fmt.Sprintf("gs://%s/%s", bucket, "backup-test-empty"), false)
	})
	t.Run("default", func(t *testing.T) {
		testExportStore(t,
			fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-default", AuthParam, authParamDefault),
			false,
		)
	})
	t.Run("specified", func(t *testing.T) {
		credentials := os.Getenv("GS_JSONKEY")
		if credentials == "" {
			t.Skip("GS_JSONKEY env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
		testExportStore(t,
			fmt.Sprintf("gs://%s/%s?%s=%s&%s=%s",
				bucket,
				"backup-test-specified",
				AuthParam,
				authParamSpecified,
				CredentialsParam,
				url.QueryEscape(encoded),
			),
			false,
		)
		testListFiles(t,
			fmt.Sprintf("gs://%s/%s/%s?%s=%s&%s=%s",
				bucket,
				"backup-test-specified",
				"listing-test",
				AuthParam,
				authParamSpecified,
				CredentialsParam,
				url.QueryEscape(encoded),
			),
		)
	})
	t.Run("implicit", func(t *testing.T) {
		// Only test these if they exist.
		if _, err := google.FindDefaultCredentials(context.TODO()); err != nil {
			t.Skip(err)
		}
		testExportStore(t,
			fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-implicit", AuthParam, authParamImplicit),
			false,
		)
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

	testExportStore(t,
		fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
			bucket, "backup-test",
			AzureAccountNameParam, url.QueryEscape(accountName),
			AzureAccountKeyParam, url.QueryEscape(accountKey),
		),
		false,
	)
	testListFiles(
		t,
		fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
			bucket, "listing-test",
			AzureAccountNameParam, url.QueryEscape(accountName),
			AzureAccountKeyParam, url.QueryEscape(accountKey),
		),
	)
}

func TestWorkloadStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	settings := cluster.MakeTestingClusterSettings()

	rows, payloadBytes, ranges := 4, 12, 1
	gen := bank.FromConfig(rows, rows, payloadBytes, ranges)
	bankTable := gen.Tables()[0]
	bankURL := func(extraParams ...map[string]string) *url.URL {
		params := url.Values{`version`: []string{gen.Meta().Version}}
		flags := gen.(workload.Flagser).Flags()
		flags.VisitAll(func(f *pflag.Flag) {
			if flags.Meta[f.Name].RuntimeOnly {
				return
			}
			params[f.Name] = append(params[f.Name], f.Value.String())
		})
		for _, p := range extraParams {
			for key, value := range p {
				params.Add(key, value)
			}
		}
		return &url.URL{
			Scheme:   `workload`,
			Path:     `/` + filepath.Join(`csv`, gen.Meta().Name, bankTable.Name),
			RawQuery: params.Encode(),
		}
	}

	ctx := context.Background()

	{
		s, err := ExternalStorageFromURI(
			ctx, bankURL().String(), settings, blobs.TestEmptyBlobClientFactory,
		)
		require.NoError(t, err)
		r, err := s.ReadFile(ctx, ``)
		require.NoError(t, err)
		bytes, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, strings.TrimSpace(`
0,0,initial-dTqn
1,0,initial-Pkyk
2,0,initial-eJkM
3,0,initial-TlNb
		`), strings.TrimSpace(string(bytes)))
	}

	{
		params := map[string]string{
			`row-start`: `1`, `row-end`: `3`, `payload-bytes`: `14`, `batch-size`: `1`}
		s, err := ExternalStorageFromURI(
			ctx, bankURL(params).String(), settings, blobs.TestEmptyBlobClientFactory,
		)
		require.NoError(t, err)
		r, err := s.ReadFile(ctx, ``)
		require.NoError(t, err)
		bytes, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, strings.TrimSpace(`
1,0,initial-vOpikz
2,0,initial-qMvoPe
		`), strings.TrimSpace(string(bytes)))
	}

	_, err := ExternalStorageFromURI(
		ctx, `workload:///nope`, settings, blobs.TestEmptyBlobClientFactory,
	)
	require.EqualError(t, err, `path must be of the form /<format>/<generator>/<table>: /nope`)
	_, err = ExternalStorageFromURI(
		ctx, `workload:///fmt/bank/bank?version=`, settings, blobs.TestEmptyBlobClientFactory,
	)
	require.EqualError(t, err, `unsupported format: fmt`)
	_, err = ExternalStorageFromURI(
		ctx, `workload:///csv/nope/nope?version=`, settings, blobs.TestEmptyBlobClientFactory,
	)
	require.EqualError(t, err, `unknown generator: nope`)
	_, err = ExternalStorageFromURI(
		ctx, `workload:///csv/bank/bank`, settings, blobs.TestEmptyBlobClientFactory,
	)
	require.EqualError(t, err, `parameter version is required`)
	_, err = ExternalStorageFromURI(
		ctx, `workload:///csv/bank/bank?version=`, settings, blobs.TestEmptyBlobClientFactory,
	)
	require.EqualError(t, err, `expected bank version "" but got "1.0.0"`)
	_, err = ExternalStorageFromURI(
		ctx, `workload:///csv/bank/bank?version=nope`, settings, blobs.TestEmptyBlobClientFactory,
	)
	require.EqualError(t, err, `expected bank version "nope" but got "1.0.0"`)
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

			defer s.Close()
			store, err := makeHTTPStorage(s.URL, testSettings)
			require.NoError(t, err)

			defer store.Close()
			f, err := store.ReadFile(context.Background(), "/something")
			require.NoError(t, err)
			defer f.Close()
			b, err := ioutil.ReadAll(f)
			require.NoError(t, err)
			require.EqualValues(t, data, b)
		})
	}

}
