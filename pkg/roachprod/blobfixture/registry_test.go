// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobfixture

import (
	"context"
	"io"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func newLogger(t *testing.T) *logger.Logger {
	cfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := cfg.NewLogger("")
	require.NoError(t, err)
	return l
}

func newTestRegistry(t *testing.T, uri string) *Registry {
	dir, cleanup := testutils.TempDir(t)
	t.Cleanup(cleanup)

	url, err := url.Parse(uri)
	require.NoError(t, err)

	cfg := cloudpb.ExternalStorage{}
	cfg.LocalFileConfig.Path = dir

	return &Registry{
		storage: nodelocal.TestingMakeNodelocalStorage(dir, cluster.MakeTestingClusterSettings(), cfg),
		uri:     *url,
		clock:   func() time.Time { return timeutil.Now() },
	}
}

func TestFixtureRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type fixture struct {
		kind           string
		createdAt      time.Time
		readyAt        time.Time
		isLatestOfKind bool
		survivesGC     bool
	}

	start := timeutil.Now()
	makeTime := func(days float32) time.Time {
		return start.Add(time.Duration(days*24) * time.Hour)
	}

	fixtures := []fixture{
		{
			// This fixture was created 3 days ago, but is not ready yet, so it will
			// be garbage collected.
			kind:           "kind-leaked",
			createdAt:      makeTime(-3),
			survivesGC:     false,
			isLatestOfKind: false,
		},
		{
			// This fixture is not ready yet, but it was created less than 2 days
			// ago, so it will not be garbage collected.
			kind:           "kind-creating",
			createdAt:      makeTime(-1),
			survivesGC:     true,
			isLatestOfKind: false,
		},
		{
			// This fixture is older than 2 days, but it is the latest of its kind,
			// so it will not be garbage collected.
			kind:           "kind-singleton",
			createdAt:      makeTime(-10),
			readyAt:        makeTime(-5),
			survivesGC:     true,
			isLatestOfKind: true,
		},
		{
			// This fixture was obsolete for more than two days, so it will be
			// deleted.
			kind:           "kind-multiple",
			createdAt:      makeTime(-10),
			readyAt:        makeTime(-9),
			survivesGC:     false,
			isLatestOfKind: false,
		},
		{
			// This fixture was not obsolete for more than one day, so it will not be
			// deleted.
			kind:           "kind-multiple",
			createdAt:      makeTime(-5),
			readyAt:        makeTime(-4),
			survivesGC:     true,
			isLatestOfKind: false,
		},
		{
			// This fixture was recently created, so its predecessor will not be deleted.
			kind:           "kind-multiple",
			createdAt:      makeTime(-2),
			readyAt:        makeTime(-0.5),
			survivesGC:     true,
			isLatestOfKind: true,
		},
		{
			// This is the most recent fixture of its kind, but its not ready yet, so
			// its not the latest and its not eligible for GC.
			kind:           "kind-multiple",
			createdAt:      makeTime(-1),
			survivesGC:     true,
			isLatestOfKind: false,
		},
	}

	type fixturesCreated struct {
		fixture  fixture
		metadata FixtureMetadata
	}

	var created []fixturesCreated

	var now time.Time
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	cfg := cloudpb.ExternalStorage{}
	cfg.LocalFileConfig.Path = dir

	registry := newTestRegistry(t, "nodelocal://1/roachtest/v25.1")
	registry.clock = func() time.Time { return now }

	l := newLogger(t)

	ctx := context.Background()
	for _, f := range fixtures {
		now = f.createdAt

		handle, err := registry.Create(ctx, f.kind, l)
		require.NoError(t, err)

		metadata := handle.Metadata()

		writer, err := registry.storage.Writer(ctx, path.Join(metadata.DataPath, "sentinel"))
		require.NoError(t, err)

		_, err = writer.Write([]byte(metadata.CreatedAt.String()))
		require.NoError(t, err)

		require.NoError(t, writer.Close())

		if !f.readyAt.IsZero() {
			now = f.readyAt
			require.NoError(t, handle.SetReadyAt(ctx))
		}

		created = append(created, fixturesCreated{
			fixture:  f,
			metadata: metadata,
		})
	}

	now = makeTime(0)

	require.NoError(t, registry.GC(ctx, l))

	for _, c := range created {
		metadata, err := registry.GetLatest(ctx, c.fixture.kind)
		if c.fixture.isLatestOfKind {
			require.NoError(t, err)
			require.Equal(t, c.metadata.DataPath, metadata.DataPath)
		} else {
			if err != nil {
				require.ErrorContains(t, err, "no fixtures found for kind")
			}
			require.NotEqual(t, c.metadata.DataPath, metadata.DataPath)
		}

		reader, _, err := registry.storage.ReadFile(ctx, path.Join(c.metadata.DataPath, "sentinel"), cloud.ReadOptions{})
		if err == nil {
			require.NoError(t, reader.Close(ctx))
		}

		if c.fixture.survivesGC {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, cloud.ErrFileDoesNotExist, "fixture %s", c.metadata.DataPath)
		}
	}
}

func TestFixtureRegistryURI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	registry := newTestRegistry(t, "nodelocal://1/roachprod/v25.1")
	// The time is always 2024-06-01 12:23
	registry.clock = func() time.Time { return time.Date(2024, 6, 1, 12, 23, 0, 0, time.UTC) }

	handle, err := registry.Create(context.Background(), "test-kind", newLogger(t))
	require.NoError(t, err)

	meta := handle.Metadata()

	dataUri := registry.URI(meta.DataPath)
	require.Equal(t,
		"nodelocal://1/roachprod/v25.1/test-kind/20240601-1223",
		dataUri.String())

	metaUri := registry.URI(meta.MetadataPath)
	require.Equal(t,
		"nodelocal://1/roachprod/v25.1/metadata/test-kind/20240601-1223",
		metaUri.String())
}

// setupTestFile creates a test file with given content in the registry
func setupTestFile(t *testing.T, registry *Registry, path string, content []byte) {
	t.Helper()
	ctx := context.Background()
	writer, err := registry.storage.Writer(ctx, path)
	require.NoError(t, err)
	_, err = writer.Write(content)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

// checkFileContent verifies file content matches expected or doesn't exist
func checkFileContent(t *testing.T, registry *Registry, path string, expectedContent []byte) {
	t.Helper()
	ctx := context.Background()
	content, err := registry.maybeReadFile(ctx, path)
	if expectedContent == nil {
		require.Nil(t, content)
		return
	}
	require.NoError(t, err)
	require.Equal(t, expectedContent, content)
}

func TestRegistryHelpers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	registry := newTestRegistry(t, "nodelocal://1/roachtest/v25.1")
	ctx := context.Background()

	testData := []byte("test content")
	testPath := "test/file.txt"

	// Test reading non-existent file
	content, err := registry.maybeReadFile(ctx, "nonexistent")
	require.NoError(t, err)
	require.Nil(t, content)

	// Test reading existing file
	setupTestFile(t, registry, testPath, testData)
	checkFileContent(t, registry, testPath, testData)
}
