// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package storage

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDeclareKeysResolveIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	txnMeta := enginepb.TxnMeta{}
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("a"),
	}
	for _, test := range []struct {
		status   roachpb.TransactionStatus
		poison   roachpb.PoisonType
		expSpans []string
	}{
		{
			status:   roachpb.ABORTED,
			poison:   roachpb.PoisonType_Noop,
			expSpans: []string{"1 0: {b-c}"},
		},
		{
			status:   roachpb.ABORTED,
			poison:   roachpb.PoisonType_Clear,
			expSpans: []string{"1 0: {b-c}", `1 1: /{Local/RangeID/99/r/AbortCache/"00000000-0000-0000-0000-000000000000"-Min}`},
		},
		{
			status:   roachpb.ABORTED,
			poison:   roachpb.PoisonType_Do,
			expSpans: []string{"1 0: {b-c}", `1 1: /{Local/RangeID/99/r/AbortCache/"00000000-0000-0000-0000-000000000000"-Min}`},
		},
		{
			status:   roachpb.COMMITTED,
			poison:   roachpb.PoisonType_Noop,
			expSpans: []string{"1 0: {b-c}"},
		},
		{
			status:   roachpb.COMMITTED,
			poison:   roachpb.PoisonType_Clear,
			expSpans: []string{"1 0: {b-c}", `1 1: /{Local/RangeID/99/r/AbortCache/"00000000-0000-0000-0000-000000000000"-Min}`},
		},
		{
			status:   roachpb.COMMITTED,
			poison:   roachpb.PoisonType_Do,
			expSpans: []string{"1 0: {b-c}"},
		},
	} {
		t.Run("", func(t *testing.T) {
			ri := roachpb.ResolveIntentRequest{
				IntentTxn: txnMeta,
				Status:    test.status,
				Poison:    test.poison,
			}
			ri.Key = roachpb.Key("b")
			ri.EndKey = roachpb.Key("c")

			var spans SpanSet
			var h roachpb.Header
			h.RangeID = desc.RangeID
			declareKeysResolveIntent(desc, h, &ri, &spans)
			exp := strings.Join(test.expSpans, "\n")
			if s := strings.TrimSpace(spans.String()); s != exp {
				t.Errorf("expected %s, got %s", exp, s)
			}
		})
	}
}
