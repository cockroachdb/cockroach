// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCachedSettingsStoreAndLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var testSettings []roachpb.KeyValue
	for i := 0; i < 5; i++ {
		testKey := fmt.Sprintf("key_%d", i)
		testVal := fmt.Sprintf("val_%d", i)
		testSettings = append(testSettings, roachpb.KeyValue{
			Key:   []byte(testKey),
			Value: roachpb.MakeValueFromString(testVal),
		})
	}

	ctx := context.Background()
	engineType := enginepb.EngineTypeDefault
	attrs := roachpb.Attributes{}
	cacheSize := int64(1 << 20)
	engine := storage.NewInMem(ctx, engineType, attrs, cacheSize)
	defer engine.Close()

	require.NoError(t, storeCachedSettingsKVs(ctx, engine, testSettings))

	actualSettings, err := loadCachedSettingsKVs(ctx, engine)
	require.NoError(t, err)
	require.Equal(t, testSettings, actualSettings)
}
