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
//
// Author: Kenji Kaneda (kenji.kaneda@gmail.com)

package storage

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestPushTransactionsWithNonPendingIntent verifies that maybePushTransactions
// returns an error when a non-pending intent is passed.
func TestPushTransactionsWithNonPendingIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	intents := []roachpb.Intent{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.ABORTED}}
	if _, pErr := tc.store.intentResolver.maybePushTransactions(
		tc.rng.context(context.Background()), intents, roachpb.Header{}, roachpb.PUSH_TOUCH, true); !testutils.IsPError(pErr, "unexpected aborted/resolved intent") {
		t.Errorf("expected error on aborted/resolved intent, but got %s", pErr)
	}
}
