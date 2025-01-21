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

	"github.com/cockroachdb/cockroach/pkg/storage/configpb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestLoadNodeStoreConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tempDir := t.TempDir()

	t.Run("file does not exist", func(t *testing.T) {
		config, exists, err := LoadNodeStoreConfig(ctx, tempDir)
		require.NoError(t, err)
		require.False(t, exists)
		require.Equal(t, configpb.Storage{}, config)
	})

	t.Run("file exists and is valid", func(t *testing.T) {
		expected := configpb.Storage{
			Stores: []configpb.Store{
				{
					Path: "foo",
				},
			},
		}
		filename := filepath.Join(tempDir, PersistedConfigFilename)
		b, err := expected.ToJson(true)
		require.NoError(t, err)
		err = os.WriteFile(filename, b, 0644)
		require.NoError(t, err)

		config, exists, err := LoadNodeStoreConfig(ctx, tempDir)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, expected, config)
	})

	t.Run("file exists and is invalid", func(t *testing.T) {
		filename := filepath.Join(tempDir, PersistedConfigFilename)
		err := os.WriteFile(filename, []byte("invalid json"), 0644)
		require.NoError(t, err)

		config, exists, err := LoadNodeStoreConfig(ctx, tempDir)
		require.Error(t, err)
		require.True(t, exists)
		require.Equal(t, configpb.Storage{}, config)
	})
}

func TestPersistNodeStoreConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tempDir := t.TempDir()

	expected := configpb.Storage{
		Stores: []configpb.Store{
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

	loaded, found, err := eng.LoadNodeStoreConfig(ctx)
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, expected, loaded)
}
