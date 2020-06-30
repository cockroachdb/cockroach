// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudimpltests

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/errors"
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
	if err := up.Set(cloudimpl.CloudstorageGSDefaultKey, os.Getenv("GS_JSONKEY"), cloudimpl.GcsDefault.Typ()); err != nil {
		panic(err)
	}
}

func storeFromURI(
	ctx context.Context,
	t *testing.T,
	uri string,
	clientFactory blobs.BlobClientFactory,
	user string,
	ie *sql.InternalExecutor,
	kvDB *kv.DB,
) cloud.ExternalStorage {
	conf, err := cloudimpl.ExternalStorageConfFromURI(uri, user)
	if err != nil {
		t.Fatal(err)
	}
	// Setup a sink for the given args.
	s, err := cloudimpl.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		clientFactory, ie, kvDB)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testExportStore(
	t *testing.T,
	storeURI string,
	skipSingleFile bool,
	user string,
	ie *sql.InternalExecutor,
	kvDB *kv.DB,
) {
	testExportStoreWithExternalIOConfig(t, base.ExternalIODirConfig{}, storeURI, user,
		skipSingleFile, ie, kvDB)
}

func testExportStoreWithExternalIOConfig(
	t *testing.T,
	ioConf base.ExternalIODirConfig,
	storeURI, user string,
	skipSingleFile bool,
	ie *sql.InternalExecutor,
	kvDB *kv.DB,
) {
	ctx := context.Background()

	conf, err := cloudimpl.ExternalStorageConfFromURI(storeURI, user)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := cloudimpl.MakeExternalStorage(ctx, conf, ioConf, testSettings, clientFactory, ie, kvDB)
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

			require.NoError(t, s.Delete(ctx, name))
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
		require.NoError(t, s.Delete(ctx, testingFilename))
	})
	if skipSingleFile {
		return
	}
	t.Run("read-single-file-by-uri", func(t *testing.T) {
		const testingFilename = "A"
		if err := s.WriteFile(ctx, testingFilename, bytes.NewReader([]byte("aaa"))); err != nil {
			t.Fatal(err)
		}
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename), clientFactory,
			user, ie, kvDB)
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
		require.NoError(t, s.Delete(ctx, testingFilename))
	})
	t.Run("write-single-file-by-uri", func(t *testing.T) {
		const testingFilename = "B"
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename), clientFactory,
			user, ie, kvDB)
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

		require.NoError(t, s.Delete(ctx, testingFilename))
	})
	// This test ensures that the ReadFile method of the ExternalStorage interface
	// raises a sentinel error indicating that a requested bucket/key/file/object
	// (based on the storage system) could not be found.
	t.Run("file-does-not-exist", func(t *testing.T) {
		const testingFilename = "A"
		if err := s.WriteFile(ctx, testingFilename, bytes.NewReader([]byte("aaa"))); err != nil {
			t.Fatal(err)
		}
		singleFile := storeFromURI(ctx, t, storeURI, clientFactory, user, ie, kvDB)
		defer singleFile.Close()

		// Read a valid file.
		res, err := singleFile.ReadFile(ctx, testingFilename)
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

		// Attempt to read a file which does not exist.
		_, err = singleFile.ReadFile(ctx, "file_does_not_exist")
		require.Error(t, err)
		require.True(t, errors.Is(err, cloudimpl.ErrFileDoesNotExist), "Expected a file does not exist error but returned %s")
		require.NoError(t, s.Delete(ctx, testingFilename))
	})
}

// RunListFilesTest tests the ListFiles() interface method for the ExternalStorage
// specified by storeURI.
func testListFiles(t *testing.T, storeURI, user string, ie *sql.InternalExecutor, kvDB *kv.DB) {
	ctx := context.Background()
	dataLetterFiles := []string{"file/letters/dataA.csv", "file/letters/dataB.csv", "file/letters/dataC.csv"}
	dataNumberFiles := []string{"file/numbers/data1.csv", "file/numbers/data2.csv", "file/numbers/data3.csv"}
	letterFiles := []string{"file/abc/A.csv", "file/abc/B.csv", "file/abc/C.csv"}
	fileNames := append(dataLetterFiles, dataNumberFiles...)
	fileNames = append(fileNames, letterFiles...)
	sort.Strings(fileNames)

	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	for _, fileName := range fileNames {
		file := storeFromURI(ctx, t, storeURI, clientFactory, user, ie, kvDB)
		if err := file.WriteFile(ctx, fileName, bytes.NewReader([]byte("bbb"))); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}

	uri, _ := url.Parse(storeURI)

	abs := func(in []string) []string {
		out := make([]string, len(in))
		for i := range in {
			u := *uri
			u.Path = u.Path + "/" + in[i]
			out[i] = u.String()
		}
		return out
	}

	t.Run("ListFiles", func(t *testing.T) {

		for _, tc := range []struct {
			name       string
			URI        string
			suffix     string
			resultList []string
		}{
			{
				"list-all-csv",
				appendPath(t, storeURI, "file/*/*.csv"),
				"",
				abs(fileNames),
			},
			{
				"list-letter-csv",
				appendPath(t, storeURI, "file/abc/?.csv"),
				"",
				abs(letterFiles),
			},
			{
				"list-letter-csv-rel-file-suffix",
				appendPath(t, storeURI, "file"),
				"abc/?.csv",
				[]string{"abc/A.csv", "abc/B.csv", "abc/C.csv"},
			},
			{
				"list-letter-csv-rel-abc-suffix",
				appendPath(t, storeURI, "file/abc"),
				"?.csv",
				[]string{"A.csv", "B.csv", "C.csv"},
			},
			{
				"list-letter-csv-dotdot",
				appendPath(t, storeURI, "file/abc/xzy/../?.csv"),
				"",
				abs(letterFiles),
			},
			{
				"list-abc-csv-suffix",
				appendPath(t, storeURI, "file"),
				"abc/?.csv",
				[]string{"abc/A.csv", "abc/B.csv", "abc/C.csv"},
			},
			{
				"list-letter-csv-dotdot-suffix",
				appendPath(t, storeURI, "file/abc/xzy"),
				"../../?.csv",
				nil,
			},
			{
				"list-data-num-csv",
				appendPath(t, storeURI, "file/numbers/data[0-9].csv"),
				"",
				abs(dataNumberFiles),
			},
			{
				"wildcard-bucket-and-filename",
				appendPath(t, storeURI, "*/numbers/*.csv"),
				"",
				abs(dataNumberFiles),
			},
			{
				"wildcard-bucket-and-filename-suffix",
				appendPath(t, storeURI, ""),
				"*/numbers/*.csv",
				[]string{"file/numbers/data1.csv", "file/numbers/data2.csv", "file/numbers/data3.csv"},
			},
			{
				"list-all-csv-skip-dir",
				// filepath.Glob() assumes that / is the separator, and enforces that it's there.
				// So this pattern would not actually match anything.
				appendPath(t, storeURI, "file/*.csv"),
				"",
				[]string{},
			},
			{
				"list-no-matches",
				appendPath(t, storeURI, "file/letters/dataD.csv"),
				"",
				[]string{},
			},
			{
				"list-escaped-star",
				appendPath(t, storeURI, "file/*/\\*.csv"),
				"",
				[]string{},
			},
			{
				"list-escaped-star-suffix",
				appendPath(t, storeURI, "file"),
				"*/\\*.csv",
				[]string{},
			},
			{
				"list-escaped-range",
				appendPath(t, storeURI, "file/*/data\\[0-9\\].csv"),
				"",
				[]string{},
			},
			{
				"list-escaped-range-suffix",
				appendPath(t, storeURI, "file"),
				"*/data\\[0-9\\].csv",
				[]string{},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := storeFromURI(ctx, t, tc.URI, clientFactory, user, ie, kvDB)
				filesList, err := s.ListFiles(ctx, tc.suffix)
				if err != nil {
					t.Fatal(err)
				}

				if len(filesList) != len(tc.resultList) {
					t.Fatal(`listed incorrect number of files`, filesList)
				}
				for i, got := range filesList {
					if expected := tc.resultList[i]; got != expected {
						t.Fatal(`resulting list is incorrect. got: `, got, `expected: `, expected, "\n", filesList)
					}
				}
			})
		}
	})

	for _, fileName := range fileNames {
		file := storeFromURI(ctx, t, storeURI, clientFactory, user, ie, kvDB)
		if err := file.Delete(ctx, fileName); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
}

func TestPutGoogleCloud(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		t.Skip("GS_BUCKET env var must be set")
	}

	user := security.RootUser

	t.Run("empty", func(t *testing.T) {
		testExportStore(t, fmt.Sprintf("gs://%s/%s", bucket, "backup-test-empty"),
			false, user, nil, nil)
	})
	t.Run("default", func(t *testing.T) {
		testExportStore(t, fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-default", cloudimpl.AuthParam,
			cloudimpl.AuthParamDefault), false, user, nil, nil)
	})
	t.Run("specified", func(t *testing.T) {
		credentials := os.Getenv("GS_JSONKEY")
		if credentials == "" {
			t.Skip("GS_JSONKEY env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
		testExportStore(t, fmt.Sprintf("gs://%s/%s?%s=%s&%s=%s",
			bucket,
			"backup-test-specified",
			cloudimpl.AuthParam,
			cloudimpl.AuthParamSpecified,
			cloudimpl.CredentialsParam,
			url.QueryEscape(encoded),
		), false, user, nil, nil)
		testListFiles(t,
			fmt.Sprintf("gs://%s/%s/%s?%s=%s&%s=%s",
				bucket,
				"backup-test-specified",
				"listing-test",
				cloudimpl.AuthParam,
				cloudimpl.AuthParamSpecified,
				cloudimpl.CredentialsParam,
				url.QueryEscape(encoded),
			),
			security.RootUser, nil, nil,
		)
	})
	t.Run("implicit", func(t *testing.T) {
		// Only test these if they exist.
		if _, err := google.FindDefaultCredentials(context.Background()); err != nil {
			t.Skip(err)
		}
		testExportStore(t, fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-implicit",
			cloudimpl.AuthParam, cloudimpl.AuthParamImplicit), false, user, nil, nil)
	})
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
	user := security.RootUser

	{
		s, err := cloudimpl.ExternalStorageFromURI(ctx, bankURL().String(), base.ExternalIODirConfig{},
			settings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
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
		s, err := cloudimpl.ExternalStorageFromURI(ctx, bankURL(params).String(), base.ExternalIODirConfig{},
			settings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
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

	_, err := cloudimpl.ExternalStorageFromURI(ctx, `workload:///nope`, base.ExternalIODirConfig{}, settings,
		blobs.TestEmptyBlobClientFactory, user, nil, nil)
	require.EqualError(t, err, `path must be of the form /<format>/<generator>/<table>: /nope`)
	_, err = cloudimpl.ExternalStorageFromURI(ctx, `workload:///fmt/bank/bank?version=`,
		base.ExternalIODirConfig{}, settings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
	require.EqualError(t, err, `unsupported format: fmt`)
	_, err = cloudimpl.ExternalStorageFromURI(ctx, `workload:///csv/nope/nope?version=`,
		base.ExternalIODirConfig{}, settings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
	require.EqualError(t, err, `unknown generator: nope`)
	_, err = cloudimpl.ExternalStorageFromURI(ctx, `workload:///csv/bank/bank`, base.ExternalIODirConfig{},
		settings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
	require.EqualError(t, err, `parameter version is required`)
	_, err = cloudimpl.ExternalStorageFromURI(ctx, `workload:///csv/bank/bank?version=`,
		base.ExternalIODirConfig{}, settings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
	require.EqualError(t, err, `expected bank version "" but got "1.0.0"`)
	_, err = cloudimpl.ExternalStorageFromURI(ctx, `workload:///csv/bank/bank?version=nope`,
		base.ExternalIODirConfig{}, settings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
	require.EqualError(t, err, `expected bank version "nope" but got "1.0.0"`)
}
