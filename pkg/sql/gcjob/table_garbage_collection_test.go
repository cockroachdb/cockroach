// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDeleteAllSpanData checks that deleteAllSpanData properly deletes data
// from the provided span.
func TestDeleteAllSpanData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	require.NoError(t, kvDB.Run(context.Background(), b))

	deleteSpan := roachpb.RSpan{Key: roachpb.RKey("aa"), EndKey: roachpb.RKey("bb")}
	distSender := s.DistSenderI().(*kvcoord.DistSender)
	require.NoError(t, deleteAllSpanData(ctx, kvDB, distSender, deleteSpan))

	kvs, err := kvDB.Scan(ctx, "aa", "bb", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(kvs))
}
