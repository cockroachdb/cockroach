// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestStickyVFS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx       = context.Background()
		storeSize = int64(512 << 20) /* 512 MiB */
		settings  = cluster.MakeTestingClusterSettings()
		registry  = fs.NewStickyRegistry()
	)

	spec1 := base.StoreSpec{
		Type:        storageconfig.InMemoryStore,
		StickyVFSID: "engine1",
		Size:        storageconfig.BytesSize(storeSize),
	}
	fs1 := registry.Get(spec1.StickyVFSID)
	env, err := fs.InitEnvFromStoreSpec(ctx, spec1, fs.EnvConfig{
		RW:      fs.ReadWrite,
		Version: settings.Version,
	}, registry, nil /* statsCollector */)
	require.NoError(t, err)
	engine1, err := storage.Open(ctx, env, settings)
	require.NoError(t, err)
	fs2 := registry.Get(spec1.StickyVFSID)
	require.Equal(t, fs1, fs2)
	require.False(t, engine1.Closed())
	engine1.Close()

	// Refetching the engine should give back a different engine with the same
	// underlying fs.
	fs3 := registry.Get(spec1.StickyVFSID)
	env, err = fs.InitEnvFromStoreSpec(ctx, spec1, fs.EnvConfig{
		RW:      fs.ReadWrite,
		Version: settings.Version,
	}, registry, nil /* statsCollector */)
	require.NoError(t, err)
	engine2, err := storage.Open(ctx, env, settings)
	require.NoError(t, err)
	require.NotEqual(t, engine1, engine2)
	require.Equal(t, fs1, fs3)
	require.True(t, engine1.Closed())
	require.False(t, engine2.Closed())
	engine2.Close()

	for _, engine := range []storage.Engine{engine1, engine2} {
		require.True(t, engine.Closed())
	}
}
