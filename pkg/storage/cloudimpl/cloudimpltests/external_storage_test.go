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
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
)

var econnrefused = &net.OpError{Err: &os.SyscallError{
	Syscall: "test",
	Err:     sysutil.ECONNREFUSED,
}}

var econnreset = &net.OpError{Err: &os.SyscallError{
	Syscall: "test",
	Err:     sysutil.ECONNRESET,
}}

type antagonisticDialer struct {
	net.Dialer
	rnd               *rand.Rand
	numRepeatFailures *int
}

type antagonisticConn struct {
	net.Conn
	rnd               *rand.Rand
	numRepeatFailures *int
}

func (d *antagonisticDialer) DialContext(
	ctx context.Context, network, addr string,
) (net.Conn, error) {
	if network == "tcp" {
		// The maximum number of injected errors should always be less than the maximum retry attempts in delayedRetry.
		if *d.numRepeatFailures < cloudimpl.MaxDelayedRetryAttempts-1 && d.rnd.Int()%2 == 0 {
			*(d.numRepeatFailures)++
			return nil, econnrefused
		}
		c, err := d.Dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		return &antagonisticConn{Conn: c, rnd: d.rnd, numRepeatFailures: d.numRepeatFailures}, nil
	}
	return d.Dialer.DialContext(ctx, network, addr)
}

func (c *antagonisticConn) Read(b []byte) (int, error) {
	// The maximum number of injected errors should always be less
	// than the maximum retry attempts in delayedRetry.
	if *c.numRepeatFailures < cloudimpl.MaxDelayedRetryAttempts-1 && c.rnd.Int()%2 == 0 {
		*(c.numRepeatFailures)++
		return 0, econnreset
	}
	return c.Conn.Read(b[:64])
}

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
	user security.SQLUsername,
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
	user security.SQLUsername,
	ie *sql.InternalExecutor,
	kvDB *kv.DB,
) {
	testExportStoreWithExternalIOConfig(t, base.ExternalIODirConfig{}, storeURI, user,
		skipSingleFile, ie, kvDB)
}

func testExportStoreWithExternalIOConfig(
	t *testing.T,
	ioConf base.ExternalIODirConfig,
	storeURI string,
	user security.SQLUsername,
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

	rng, _ := randutil.NewPseudoRand()

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
	t.Run("exceeds-4mb-chunk", func(t *testing.T) {
		const size = 1024 * 1024 * 5
		testingContent := randutil.RandBytes(rng, size)
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

		testingFilename += "-stream"

		w, err := s.Writer(ctx, testingFilename)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(w, bytes.NewReader(testingContent)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Attempt to read (or fetch) it back.
		streamRes, err := s.ReadFile(ctx, testingFilename)
		if err != nil {
			t.Fatalf("Could not get reader for %s: %+v", testingFilename, err)
		}
		defer res.Close()
		streamContent, err := ioutil.ReadAll(streamRes)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(streamContent, testingContent) {
			t.Fatalf("wrong content")
		}

		t.Run("rand-readats", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				t.Run("", func(t *testing.T) {
					byteReader := bytes.NewReader(testingContent)
					offset, length := rng.Int63n(size), rng.Intn(32*1024)
					t.Logf("read %d of file at %d", length, offset)
					reader, size, err := s.ReadFileAt(ctx, testingFilename, offset)
					require.NoError(t, err)
					defer reader.Close()
					require.Equal(t, int64(len(testingContent)), size)
					expected, got := make([]byte, length), make([]byte, length)
					_, err = byteReader.Seek(offset, io.SeekStart)
					require.NoError(t, err)

					expectedN, expectedErr := io.ReadFull(byteReader, expected)
					gotN, gotErr := io.ReadFull(reader, got)
					require.Equal(t, expectedErr != nil, gotErr != nil, "%+v vs %+v", expectedErr, gotErr)
					require.Equal(t, expectedN, gotN)
					require.Equal(t, expected, got)
				})
			}
		})

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
func testListFiles(
	t *testing.T, storeURI string, user security.SQLUsername, ie *sql.InternalExecutor, kvDB *kv.DB,
) {
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
			if u.Scheme == "userfile" && u.Host == "" {
				composedTableName := tree.Name(cloudimpl.DefaultQualifiedNamePrefix + user.Normalized())
				u.Host = cloudimpl.DefaultQualifiedNamespace +
					// Escape special identifiers as needed.
					composedTableName.String()
			}
			u.Path = u.Path + "/" + in[i]
			out[i] = u.String()
		}
		return out
	}

	t.Run("ListFiles", func(t *testing.T) {
		var noResults []string = nil

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
				noResults,
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
				noResults,
			},
			{
				"list-no-matches",
				appendPath(t, storeURI, "file/letters/dataD.csv"),
				"",
				noResults,
			},
			{
				"list-escaped-star",
				appendPath(t, storeURI, "file/*/\\*.csv"),
				"",
				noResults,
			},
			{
				"list-escaped-star-suffix",
				appendPath(t, storeURI, "file"),
				"*/\\*.csv",
				noResults,
			},
			{
				"list-escaped-range",
				appendPath(t, storeURI, "file/*/data\\[0-9\\].csv"),
				"",
				noResults,
			},
			{
				"list-escaped-range-suffix",
				appendPath(t, storeURI, "file"),
				"*/data\\[0-9\\].csv",
				noResults,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := storeFromURI(ctx, t, tc.URI, clientFactory, user, ie, kvDB)
				filesList, err := s.ListFiles(ctx, tc.suffix)
				require.NoError(t, err)
				require.Equal(t, filesList, tc.resultList)
			})
		}
	})

	foreach := func(in []string, fn func(string) string) []string {
		out := make([]string, len(in))
		for i := range in {
			out[i] = fn(in[i])
		}
		return out
	}

	t.Run("List", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			uri       string
			prefix    string
			delimiter string
			expected  []string
		}{
			{
				"root",
				storeURI,
				"",
				"",
				foreach(fileNames, func(s string) string { return "/" + s }),
			},
			{
				"file-slash-numbers-slash",
				storeURI,
				"file/numbers/",
				"",
				[]string{"data1.csv", "data2.csv", "data3.csv"},
			},
			{
				"root-slash",
				storeURI,
				"/",
				"",
				foreach(fileNames, func(s string) string { return s }),
			},
			{
				"file",
				storeURI,
				"file",
				"",
				foreach(fileNames, func(s string) string { return strings.TrimPrefix(s, "file") }),
			},
			{
				"file-slash",
				storeURI,
				"file/",
				"",
				foreach(fileNames, func(s string) string { return strings.TrimPrefix(s, "file/") }),
			},
			{
				"slash-f",
				storeURI,
				"/f",
				"",
				foreach(fileNames, func(s string) string { return strings.TrimPrefix(s, "f") }),
			},
			{
				"nothing",
				storeURI,
				"nothing",
				"",
				nil,
			},
			{
				"delim-slash-file-slash",
				storeURI,
				"file/",
				"/",
				[]string{"abc/", "letters/", "numbers/"},
			},
			{
				"delim-data",
				storeURI,
				"",
				"data",
				[]string{"/file/abc/A.csv", "/file/abc/B.csv", "/file/abc/C.csv", "/file/letters/data", "/file/numbers/data"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := storeFromURI(ctx, t, tc.uri, clientFactory, user, ie, kvDB)
				var actual []string
				require.NoError(t, s.List(ctx, tc.prefix, tc.delimiter, func(f string) error {
					actual = append(actual, f)
					return nil
				}))
				sort.Strings(actual)
				require.Equal(t, tc.expected, actual)
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
		skip.IgnoreLint(t, "GS_BUCKET env var must be set")
	}

	user := security.RootUserName()

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
			skip.IgnoreLint(t, "GS_JSONKEY env var must be set")
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
			security.RootUserName(), nil, nil,
		)
	})
	t.Run("implicit", func(t *testing.T) {
		// Only test these if they exist.
		if _, err := google.FindDefaultCredentials(context.Background()); err != nil {
			skip.IgnoreLint(t, err)
		}
		testExportStore(t, fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-implicit",
			cloudimpl.AuthParam, cloudimpl.AuthParamImplicit), false, user, nil, nil)
		testListFiles(t,
			fmt.Sprintf("gs://%s/%s/%s?%s=%s",
				bucket,
				"backup-test-implicit",
				"listing-test",
				cloudimpl.AuthParam,
				cloudimpl.AuthParamImplicit,
			),
			security.RootUserName(), nil, nil,
		)
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
	user := security.RootUserName()

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

func uploadData(
	t *testing.T, rnd *rand.Rand, dest roachpb.ExternalStorage, basename string,
) ([]byte, func()) {
	data := randutil.RandBytes(rnd, 16<<20)
	ctx := context.Background()

	s, err := cloudimpl.MakeExternalStorage(
		ctx, dest, base.ExternalIODirConfig{}, testSettings,
		nil, nil, nil)
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.WriteFile(ctx, basename, bytes.NewReader(data)))
	return data, func() {
		_ = s.Delete(ctx, basename)
	}
}

func testAntagonisticRead(t *testing.T, conf roachpb.ExternalStorage) {
	rnd, _ := randutil.NewPseudoRand()

	const basename = "test-antagonistic-read"
	data, cleanup := uploadData(t, rnd, conf, basename)
	defer cleanup()

	// Try reading the data while injecting errors.
	failures := 0
	// Override DialContext implementation in http transport.
	dialer := &antagonisticDialer{rnd: rnd, numRepeatFailures: &failures}

	// Override transport to return antagonistic connection.
	transport := http.DefaultTransport.(*http.Transport)
	transport.DialContext =
		func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		}
	transport.DisableKeepAlives = true

	defer func() {
		transport.DialContext = nil
		transport.DisableKeepAlives = false
	}()

	ctx := context.Background()
	s, err := cloudimpl.MakeExternalStorage(
		ctx, conf, base.ExternalIODirConfig{}, testSettings,
		nil, nil, nil)
	require.NoError(t, err)
	defer s.Close()

	stream, err := s.ReadFile(ctx, basename)
	require.NoError(t, err)
	defer stream.Close()
	read, err := ioutil.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, data, read)
}
