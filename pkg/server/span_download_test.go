// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestSpanDownloadLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	accessor := s.ExternalStorageAccessor().(*cloud.ExternalStorageAccessor)
	eng := storage.NewDefaultInMemForTesting(storage.RemoteStorageFactory(accessor))
	_, err := eng.IngestExternalFiles(ctx, []pebble.ExternalFile{
		{
			Locator:         "http://example.com/",
			ObjName:         "test",
			Size:            100,
			SmallestUserKey: roachpb.Key("a"),
			LargestUserKey:  roachpb.Key("d"),
			HasPointKey:     true,
		},
	})
	require.NoError(t, err)
	//log.Infof(ctx, "ingested %v", stats)
	//require.NoError(t, eng.Download(ctx, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}))
}
