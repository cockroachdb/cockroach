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
	gosql "database/sql"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
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
	ie sqlutil.InternalExecutor,
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
	ie sqlutil.InternalExecutor,
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
	ie sqlutil.InternalExecutor,
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
		require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "Expected a file does not exist error but returned %s")
		require.NoError(t, s.Delete(ctx, testingFilename))
	})
}

// RunListFilesTest tests the ListFiles() interface method for the ExternalStorage
// specified by storeURI.
func testListFiles(
	t *testing.T,
	storeURI string,
	user security.SQLUsername,
	ie sqlutil.InternalExecutor,
	kvDB *kv.DB,
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
	})
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

func makeUserfile(
	ctx context.Context,
	t *testing.T,
	s serverutils.TestServerInterface,
	sqlDB *gosql.DB,
	kvDB *kv.DB,
) cloud.ExternalStorage {
	qualifiedTableName := "defaultdb.public.user_file_table_test"

	dest := cloudimpl.MakeUserFileStorageURI(qualifiedTableName, "")
	ie := s.InternalExecutor().(sqlutil.InternalExecutor)

	// Create a user and grant them privileges on defaultdb.
	user1 := security.MakeSQLUsernameFromPreNormalizedString("foo")
	require.NoError(t, createUserGrantAllPrivieleges(user1, "defaultdb", sqlDB))

	// Create a userfile connection as user1.
	fileTableSystem, err := cloudimpl.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user1, ie, kvDB)
	require.NoError(t, err)

	return fileTableSystem
}

func makeNodelocal(ctx context.Context, t *testing.T, kvDB *kv.DB) cloud.ExternalStorage {
	user := security.RootUserName()

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	store := storeFromURI(ctx, t, "nodelocal://self/base", clientFactory,
		user, nil /* ie */, kvDB)
	return store
}

// TestFileExistence checks that the appropriate error is returned when trying
// to read a file that doesn't exist.
func TestFileExistence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Setup a server.
	params, _ := tests.CreateTestServerParams()

	tmp, cleanupTmp := testutils.TempDir(t)
	defer cleanupTmp()
	testSettings.ExternalIODir = tmp

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create external storages to tests.
	// TODO: Add more here (e.g. s3, azure, gcs).
	uf := makeUserfile(ctx, t, s, sqlDB, kvDB)
	defer uf.Close()
	nl := makeNodelocal(ctx, t, kvDB)
	defer nl.Close()

	externalStorages := []cloud.ExternalStorage{uf, nl}

	for _, es := range externalStorages {
		// Try reading a file that doesn't exist and assert we get the error the
		// interface claims it provides.
		_, err := es.ReadFile(ctx, "this-doesnt-exist")
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			t.Fatalf("expected a file does not exist error, got %+v", err)
		}

		_, _, err = es.ReadFileAt(ctx, "this-doesnt-exist", 0 /* offset */)
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			t.Fatalf("expected a file does not exist error, got %+v", err)
		}

		// Create a file that exists and assert that we don't get the error.
		filename := "exists"
		require.NoError(t, es.WriteFile(ctx, filename, bytes.NewReader([]byte("data"))))

		_, err = es.ReadFile(ctx, filename)
		require.NoError(t, err)
		_, _, err = es.ReadFileAt(ctx, filename, 0 /* offset */)
		require.NoError(t, err)
	}
}
