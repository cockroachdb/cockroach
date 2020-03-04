// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestStickyEngines(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	engineType := enginepb.EngineTypeRocksDB
	attrs := roachpb.Attributes{}
	cacheSize := int64(1 << 20)

	engine1, err := getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.False(t, engine1.Closed())

	engine2, err := getOrCreateStickyInMemEngine(ctx, "engine2", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.False(t, engine2.Closed())

	// Regetting the engine whilst it is not closed will fail.
	_, err = getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.EqualError(t, err, "sticky engine engine1 has not been closed")

	// Close the engine, which allows it to be refetched.
	engine1.Close()
	require.True(t, engine1.Closed())
	require.False(t, engine1.(*stickyInMemEngine).Engine.Closed())

	// Refetching the engine should give back the same engine.
	engine1Refetched, err := getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.Equal(t, engine1, engine1Refetched)
	require.False(t, engine1.Closed())

	// Closing an engine that does not exist will error.
	err = CloseStickyInMemEngine("engine3")
	require.EqualError(t, err, "sticky in-mem engine engine3 does not exist")

	// Cleaning up the engine should result in a new engine.
	err = CloseStickyInMemEngine("engine1")
	require.NoError(t, err)
	require.True(t, engine1.Closed())
	require.True(t, engine1.(*stickyInMemEngine).Engine.Closed())

	newEngine1, err := getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.NotEqual(t, engine1, newEngine1)

	// Cleaning up everything asserts everything is closed.
	CloseAllStickyInMemEngines()
	require.Len(t, stickyInMemEnginesRegistry.entries, 0)
	for _, engine := range []storage.Engine{engine1, newEngine1, engine2} {
		require.True(t, engine.Closed())
		require.True(t, engine.(*stickyInMemEngine).Engine.Closed())
	}
}
