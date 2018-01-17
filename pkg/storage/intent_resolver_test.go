// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestPushTransactionsWithNonPendingIntent verifies that maybePushTransactions
// returns an error when a non-pending intent is passed.
func TestPushTransactionsWithNonPendingIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	testCases := [][]roachpb.Intent{
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.ABORTED}},
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.COMMITTED}},
	}
	for _, intents := range testCases {
		if _, pErr := tc.store.intentResolver.maybePushTransactions(
			context.Background(), intents, roachpb.Header{}, roachpb.PUSH_TOUCH, true,
		); !testutils.IsPError(pErr, "unexpected (ABORTED|COMMITTED) intent") {
			t.Errorf("expected error on aborted/resolved intent, but got %s", pErr)
		}
		if cnt := len(tc.store.intentResolver.mu.inFlightPushes); cnt != 0 {
			t.Errorf("expected no inflight pushe refcount map entries, found %d", cnt)
		}
	}
}
