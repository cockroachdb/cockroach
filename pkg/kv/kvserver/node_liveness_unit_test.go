// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestShouldReplaceLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()

	l := func(epo int64, expiration hlc.Timestamp, draining, decom bool) kvserverpb.Liveness {
		return kvserverpb.Liveness{
			Epoch:           epo,
			Expiration:      hlc.LegacyTimestamp(expiration),
			Draining:        draining,
			Decommissioning: decom,
		}
	}
	const (
		no  = false
		yes = true
	)
	now := hlc.Timestamp{WallTime: 12345}

	for _, test := range []struct {
		old, new kvserverpb.Liveness
		exp      bool
	}{
		{
			// Epoch update only.
			kvserverpb.Liveness{},
			l(1, hlc.Timestamp{}, false, false),
			yes,
		},
		{
			// No Epoch update, but Expiration update.
			l(1, now, false, false),
			l(1, now.Add(0, 1), false, false),
			yes,
		},
		{
			// No update.
			l(1, now, false, false),
			l(1, now, false, false),
			no,
		},
		{
			// Only Decommissioning changes.
			l(1, now, false, false),
			l(1, now, false, true),
			yes,
		},
		{
			// Only Draining changes.
			l(1, now, false, false),
			l(1, now, true, false),
			yes,
		},
		{
			// Decommissioning changes, but Epoch moves backwards.
			l(10, now, true, true),
			l(9, now, true, false),
			no,
		},
		{
			// Draining changes, but Expiration moves backwards..
			l(10, now, false, false),
			l(10, now.Add(-1, 0), true, false),
			no,
		},
	} {
		t.Run("", func(t *testing.T) {
			if act := shouldReplaceLiveness(test.old, test.new); act != test.exp {
				t.Errorf("unexpected update: %+v", test)
			}
		})
	}
}
