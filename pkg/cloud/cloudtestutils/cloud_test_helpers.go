// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudtestutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// EConnRefused is the conn refused err used by the antagonisticDialer.
var EConnRefused = &net.OpError{Err: &os.SyscallError{
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
		if *d.numRepeatFailures < cloud.MaxDelayedRetryAttempts-1 && d.rnd.Int()%2 == 0 {
			*(d.numRepeatFailures)++
			return nil, EConnRefused
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
	if *c.numRepeatFailures < cloud.MaxDelayedRetryAttempts-1 && c.rnd.Int()%2 == 0 {
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

func storeFromURI(
	ctx context.Context,
	t *testing.T,
	uri string,
	clientFactory blobs.BlobClientFactory,
	user username.SQLUsername,
	db isql.DB,
	testSettings *cluster.Settings,
) cloud.ExternalStorage {
	conf, err := cloud.ExternalStorageConfFromURI(uri, user)
	if err != nil {
		t.Fatal(err)
	}
	// Setup a sink for the given args.
	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		clientFactory, db, nil, cloud.NilMetrics)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type StoreInfo struct {
	URI  string
	User username.SQLUsername

	// Fields below are optional.

	TestSettings  *cluster.Settings
	DB            isql.DB
	ExternalIODir string
}

func (info StoreInfo) testSettings() *cluster.Settings {
	if info.TestSettings != nil {
		return info.TestSettings
	}
	return cluster.MakeTestingClusterSettings()
}

// CheckExportStore runs an array of tests against a store.
func CheckExportStore(t *testing.T, info StoreInfo) {
	checkExportStore(t, info, false /* skipSingleFile */)
}

// CheckExportStoreSkipSingleFile runs an array of tests against a store,
// skipping single file tests.
func CheckExportStoreSkipSingleFile(t *testing.T, info StoreInfo) {
	checkExportStore(t, info, true /* skipSingleFile */)
}

// CheckExportStore runs an array of tests against a store.
func checkExportStore(t *testing.T, info StoreInfo, skipSingleFile bool) {
	ioConf := base.ExternalIODirConfig{}
	ctx := context.Background()
	testSettings := info.testSettings()

	conf, err := cloud.ExternalStorageConfFromURI(info.URI, info.User)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(info.ExternalIODir)
	s, err := cloud.MakeExternalStorage(ctx, conf, ioConf, testSettings, clientFactory, info.DB, nil, cloud.NilMetrics)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if readConf := s.Conf(); readConf != conf {
		t.Fatalf("conf does not roundtrip: started with %+v, got back %+v", conf, readConf)
	}

	rng, _ := randutil.NewTestRand()

	t.Run("simple round trip", func(t *testing.T) {
		sampleName := "somebytes"
		sampleBytes := "hello world"

		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("%s-%d", sampleName, i)
			payload := []byte(strings.Repeat(sampleBytes, i))
			if err := cloud.WriteFile(ctx, s, name, bytes.NewReader(payload)); err != nil {
				t.Fatal(err)
			}

			if sz, err := s.Size(ctx, name); err != nil {
				t.Error(err)
			} else if sz != int64(len(payload)) {
				t.Errorf("size mismatch, got %d, expected %d", sz, len(payload))
			}

			r, _, err := s.ReadFile(ctx, name, cloud.ReadOptions{NoFileSize: true})
			if err != nil {
				t.Fatal(err)
			}
			//nolint:deferloop TODO(#137605)
			defer r.Close(ctx)

			res, err := ioctx.ReadAll(ctx, r)
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
		if err := cloud.WriteFile(ctx, s, testingFilename, bytes.NewReader(testingContent)); err != nil {
			t.Fatal(err)
		}

		// Attempt to read (or fetch) it back.
		res, _, err := s.ReadFile(ctx, testingFilename, cloud.ReadOptions{NoFileSize: true})
		if err != nil {
			t.Fatalf("Could not get reader for %s: %+v", testingFilename, err)
		}
		defer res.Close(ctx)
		content, err := ioctx.ReadAll(ctx, res)
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
					offset := rng.Int63n(size)
					length := 1 + rng.Int63n(32*1024)
					t.Logf("read %d of file at %d", length, offset)
					reader, size, err := s.ReadFile(ctx, testingFilename, cloud.ReadOptions{
						Offset:     offset,
						LengthHint: length,
					})
					require.NoError(t, err)
					defer reader.Close(ctx)
					require.Equal(t, int64(len(testingContent)), size)

					expectedByteReader := io.LimitReader(bytes.NewReader(testingContent[offset:]), length)

					expected := make([]byte, length)
					expectedN, expectedErr := io.ReadFull(expectedByteReader, expected)
					got := make([]byte, length)
					gotN, gotErr := io.ReadFull(ioctx.ReaderCtxAdapter(ctx, reader), got)
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
		if err := cloud.WriteFile(ctx, s, testingFilename, bytes.NewReader([]byte("aaa"))); err != nil {
			t.Fatal(err)
		}
		singleFile := storeFromURI(ctx, t, appendPath(t, info.URI, testingFilename), clientFactory,
			info.User, info.DB, testSettings)
		defer singleFile.Close()

		res, _, err := singleFile.ReadFile(ctx, "", cloud.ReadOptions{NoFileSize: true})
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close(ctx)
		content, err := ioctx.ReadAll(ctx, res)
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
		singleFile := storeFromURI(ctx, t, appendPath(t, info.URI, testingFilename), clientFactory,
			info.User, info.DB, testSettings)
		defer singleFile.Close()

		if err := cloud.WriteFile(ctx, singleFile, "", bytes.NewReader([]byte("bbb"))); err != nil {
			t.Fatal(err)
		}

		res, _, err := s.ReadFile(ctx, testingFilename, cloud.ReadOptions{NoFileSize: true})
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close(ctx)
		content, err := ioctx.ReadAll(ctx, res)
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
		if err := cloud.WriteFile(ctx, s, testingFilename, bytes.NewReader([]byte("aaa"))); err != nil {
			t.Fatal(err)
		}
		singleFile := storeFromURI(ctx, t, info.URI, clientFactory, info.User, info.DB, testSettings)
		defer singleFile.Close()

		// Read a valid file.
		res, _, err := s.ReadFile(ctx, testingFilename, cloud.ReadOptions{NoFileSize: true})
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close(ctx)
		content, err := ioctx.ReadAll(ctx, res)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(content, []byte("aaa")) {
			t.Fatalf("wrong content")
		}

		// Attempt to read a file which does not exist.
		_, _, err = s.ReadFile(ctx, "file does not exist", cloud.ReadOptions{NoFileSize: true})
		require.Error(t, err)
		require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "Expected a file does not exist error but returned %s")

		_, _, err = singleFile.ReadFile(ctx, "file_does_not_exist", cloud.ReadOptions{Offset: 24})
		require.Error(t, err)
		require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "Expected a file does not exist error but returned %s")

		require.NoError(t, s.Delete(ctx, testingFilename))
		// Deleting a file that does not exist is okay. This behavior is somewhat
		// forced by the behavior of S3. In non-versioned S3 buckets, S3 does not
		// return an error or metadata indicating the object did not exist before
		// the delete. So if we want consistent behavior across all cloud.Storage
		// interfaces, and we don't want to read before we delete an S3 object, we
		// need to treat this as a non-error.
		require.NoError(t, s.Delete(ctx, testingFilename))
	})
}

// CheckListFiles tests the ListFiles() interface method for the ExternalStorage
// specified by storeURI.
func CheckListFiles(t *testing.T, info StoreInfo) {
	CheckListFilesCanonical(t, info, "" /* canonical */)
}

// CheckListFilesCanonical is like CheckListFiles but takes a canonical prefix
// that it should expect to see on returned listings, instead of storeURI (e.g.
// if storeURI automatically expands).
func CheckListFilesCanonical(t *testing.T, info StoreInfo, canonical string) {
	ctx := context.Background()
	testSettings := info.testSettings()
	dataLetterFiles := []string{"file/letters/dataA.csv", "file/letters/dataB.csv", "file/letters/dataC.csv"}
	dataNumberFiles := []string{"file/numbers/data1.csv", "file/numbers/data2.csv", "file/numbers/data3.csv"}
	letterFiles := []string{"file/abc/A.csv", "file/abc/B.csv", "file/abc/C.csv"}
	fileNames := append(dataLetterFiles, dataNumberFiles...)
	fileNames = append(fileNames, letterFiles...)
	sort.Strings(fileNames)

	clientFactory := blobs.TestBlobServiceClient(info.ExternalIODir)
	for _, fileName := range fileNames {
		file := storeFromURI(ctx, t, info.URI, clientFactory, info.User, info.DB, testSettings)
		if err := cloud.WriteFile(ctx, file, fileName, bytes.NewReader([]byte("bbb"))); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}

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
				info.URI,
				"",
				"",
				foreach(fileNames, func(s string) string { return "/" + s }),
			},
			{
				"file-slash-numbers-slash",
				info.URI,
				"file/numbers/",
				"",
				[]string{"data1.csv", "data2.csv", "data3.csv"},
			},
			{
				"root-slash",
				info.URI,
				"/",
				"",
				foreach(fileNames, func(s string) string { return s }),
			},
			{
				"file",
				info.URI,
				"file",
				"",
				foreach(fileNames, func(s string) string { return strings.TrimPrefix(s, "file") }),
			},
			{
				"file-slash",
				info.URI,
				"file/",
				"",
				foreach(fileNames, func(s string) string { return strings.TrimPrefix(s, "file/") }),
			},
			{
				"slash-f",
				info.URI,
				"/f",
				"",
				foreach(fileNames, func(s string) string { return strings.TrimPrefix(s, "f") }),
			},
			{
				"nothing",
				info.URI,
				"nothing",
				"",
				nil,
			},
			{
				"delim-slash-file-slash",
				info.URI,
				"file/",
				"/",
				[]string{"abc/", "letters/", "numbers/"},
			},
			{
				"delim-data",
				info.URI,
				"",
				"data",
				[]string{"/file/abc/A.csv", "/file/abc/B.csv", "/file/abc/C.csv", "/file/letters/data", "/file/numbers/data"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := storeFromURI(ctx, t, tc.uri, clientFactory, info.User, info.DB, testSettings)
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
		file := storeFromURI(ctx, t, info.URI, clientFactory, info.User, info.DB, testSettings)
		if err := file.Delete(ctx, fileName); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
}

func uploadData(
	t *testing.T,
	testSettings *cluster.Settings,
	rnd *rand.Rand,
	dest cloudpb.ExternalStorage,
	basename string,
) ([]byte, func()) {
	data := randutil.RandBytes(rnd, 16<<20)
	ctx := context.Background()

	s, err := cloud.MakeExternalStorage(ctx, dest, base.ExternalIODirConfig{}, testSettings,
		nil, /* blobClientFactory */
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	require.NoError(t, cloud.WriteFile(ctx, s, basename, bytes.NewReader(data)))
	return data, func() {
		defer s.Close()
		_ = s.Delete(ctx, basename)
	}
}

// CheckAntagonisticRead checks an external storage is able to perform reads if
// the HTTP client's dialer artificially degrades its connections.
func CheckAntagonisticRead(
	t *testing.T, conf cloudpb.ExternalStorage, testSettings *cluster.Settings,
) {
	rnd, _ := randutil.NewTestRand()

	basename := fmt.Sprintf("test-antagonistic-read-%d", NewTestID())
	data, cleanup := uploadData(t, testSettings, rnd, conf, basename)
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
	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		nil, /* blobClientFactory */
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	defer s.Close()

	stream, _, err := s.ReadFile(ctx, basename, cloud.ReadOptions{NoFileSize: true})
	require.NoError(t, err)
	defer stream.Close(ctx)
	read, err := ioctx.ReadAll(ctx, stream)
	require.NoError(t, err)
	require.Equal(t, data, read)
}

// CheckNoPermission checks that we do not have permission to list the external
// storage at storeURI.
func CheckNoPermission(t *testing.T, info StoreInfo) {
	ioConf := base.ExternalIODirConfig{}
	ctx := context.Background()

	conf, err := cloud.ExternalStorageConfFromURI(info.URI, info.User)
	if err != nil {
		t.Fatal(err)
	}

	testSettings := info.testSettings()
	clientFactory := blobs.TestBlobServiceClient(info.ExternalIODir)
	s, err := cloud.MakeExternalStorage(
		ctx, conf, ioConf, testSettings, clientFactory, info.DB, nil, cloud.NilMetrics,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.List(ctx, "", "", nil)
	if err == nil {
		t.Fatalf("expected error when listing %s with no permissions", info.URI)
	}

	require.Regexp(t, "(failed|unable) to list", err)
}

// IsImplicitAuthConfigured returns true if the `GOOGLE_APPLICATION_CREDENTIALS`
// environment variable is set. This env variable points to the `keys.json` file
// that is used for implicit authentication.
func IsImplicitAuthConfigured() bool {
	credentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	return credentials != ""
}

func NewTestID() uint64 {
	rng, _ := randutil.NewTestRand()
	return rng.Uint64()
}

func RequireSuccessfulKMS(ctx context.Context, t *testing.T, uri string) {
	data := []byte("test123")
	k, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{
		Settings:         cluster.MakeTestingClusterSettings(),
		ExternalIOConfig: &base.ExternalIODirConfig{},
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, k.Close())
	}()

	encData, err := k.Encrypt(ctx, data)
	require.NoError(t, err)

	decData, err := k.Decrypt(ctx, encData)
	require.NoError(t, err)

	require.Equal(t, data, decData)
}

func RequireKMSInaccessibleErrorContaining(
	ctx context.Context, t *testing.T, uri string, errRE string,
) {
	data := []byte("test123")
	k, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{
		Settings:         cluster.MakeTestingClusterSettings(),
		ExternalIOConfig: &base.ExternalIODirConfig{},
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, k.Close())
	}()

	_, err = k.Encrypt(ctx, data)
	require.True(t, cloud.IsKMSInaccessible(err), "error not kms inaccessible")
	if !testutils.IsError(err, errRE) {
		t.Fatalf("expected error '%s', got: %s", errRE, err)
	}

	_, err = k.Decrypt(ctx, data)
	require.True(t, cloud.IsKMSInaccessible(err), "error not kms inaccessible")
	if !testutils.IsError(err, errRE) {
		t.Fatalf("expected error '%s', got: %s", errRE, err)
	}
}
