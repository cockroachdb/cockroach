// Copyright 2021 The Cockroach Authors.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestUpdateSpanConfigSeenByAllStores tests that a span config update is
// observed by all stores on a given node.
func TestUpdateSpanConfigSeenByAllStores(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, numStores := range []int{1, 3} {
		t.Run(fmt.Sprintf("num-stores=%d", numStores), func(t *testing.T) {
			var storeSpecs []base.StoreSpec
			for i := 0; i < numStores; i++ {
				storeSpecs = append(storeSpecs, base.StoreSpec{InMemory: true})
			}

			waitForAllUpdatesCh := make(chan struct{})
			var seenCount int32 = 0
			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
				StoreSpecs: storeSpecs,
				Knobs: base.TestingKnobs{
					SpanConfigManager: &spanconfigmanager.TestingKnobs{
						DisableJobCreation: true,
					},

					Store: &kvserver.StoreTestingKnobs{
						SpanConfigUpdateInterceptor: func(_ spanconfig.Update) {
							if atomic.AddInt32(&seenCount, 1) == int32(numStores) {
								close(waitForAllUpdatesCh)
							}
						},
					},
				},
			})
			defer s.Stopper().Stop(context.Background())

			ctx := context.Background()
			nameSpaceTableStart := s.ExecutorConfig().(sql.ExecutorConfig).Codec.TablePrefix(keys.NamespaceTableID)
			nameSpaceTableSpan := roachpb.Span{
				Key:    nameSpaceTableStart,
				EndKey: nameSpaceTableStart.PrefixEnd(),
			}
			update := []roachpb.SpanConfigEntry{
				{
					Span:   nameSpaceTableSpan,
					Config: roachpb.SpanConfig{},
				},
			}

			require.NoError(t, s.Node().(spanconfig.Accessor).UpdateSpanConfigEntries(ctx, update, nil))

			select {
			case <-waitForAllUpdatesCh:
			case <-time.After(5 * time.Second):
				t.Errorf("test timed out")
			}
		})
	}
}
