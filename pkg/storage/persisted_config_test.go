// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestMarshallUnmarshall(t *testing.T) {
	expected := storagepb.NodeConfig{
		Stores: []storagepb.StoreSpec{
			{
				Path: "foo",
				// NB: nil vs empty arrays are not handled correctly by the json
				// encoder. See https://github.com/golang/protobuf/issues/1348.
				// This shouldn't cause any problems in practice, but after
				// encoding and decoding a nil field becomes a non-nil
				// attribute.
				Attributes: roachpb.Attributes{Attrs: []string{}},
			},
		},
	}

	b, err := toJson(expected, true)
	require.NoError(t, err)
	actual, err := fromJson(b)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestLoadNodeStoreConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tempDir := t.TempDir()

	t.Run("file does not exist", func(t *testing.T) {
		config, exists, err := LoadNodeStoreConfig(tempDir)
		require.NoError(t, err)
		require.False(t, exists)
		require.Equal(t, storagepb.NodeConfig{}, config)
	})

	t.Run("file exists and is valid", func(t *testing.T) {
		expected := storagepb.NodeConfig{
			Stores: []storagepb.StoreSpec{
				{
					Path: "foo",
				},
			},
		}
		filename := filepath.Join(tempDir, PersistedConfigFilename)
		b, err := toJson(expected, true)
		require.NoError(t, err)
		err = os.WriteFile(filename, b, 0644)
		require.NoError(t, err)

		config, exists, err := LoadNodeStoreConfig(tempDir)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, expected, config)
	})

	t.Run("file exists and is invalid", func(t *testing.T) {
		filename := filepath.Join(tempDir, PersistedConfigFilename)
		err := os.WriteFile(filename, []byte("invalid json"), 0644)
		require.NoError(t, err)

		config, exists, err := LoadNodeStoreConfig(tempDir)
		require.Error(t, err)
		require.True(t, exists)
		require.Equal(t, storagepb.NodeConfig{}, config)
	})
}

func TestPersistNodeStoreConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tempDir := t.TempDir()

	expected := storagepb.NodeConfig{
		Stores: []storagepb.StoreSpec{
			{
				Path: "foo",
			},
		},
	}
	// Mock the Pebble struct and its configuration
	env, err := fs.InitEnv(ctx, vfs.NewMem(), tempDir, fs.EnvConfig{}, nil /* statsCollector */)
	require.NoError(t, err)
	eng := &Pebble{
		cfg: engineConfig{
			env: env,
		},
	}
	//	require.NoError(t, eng.cfg.env.MkdirAll(tempDir, 0755))
	require.NoError(t, eng.PersistNodeStoreConfig(ctx, expected))

	loaded, found, err := eng.LoadNodeStoreConfig()
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, expected, loaded)
}
