// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package liveness

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func TestShouldReplaceLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	toMembershipStatus := func(membership string) livenesspb.MembershipStatus {
		switch membership {
		case "active":
			return livenesspb.MembershipStatus_ACTIVE
		case "decommissioning":
			return livenesspb.MembershipStatus_DECOMMISSIONING
		case "decommissioned":
			return livenesspb.MembershipStatus_DECOMMISSIONED
		default:
			err := fmt.Sprintf("unexpected membership: %s", membership)
			panic(err)
		}
	}

	l := func(epo int64, expiration hlc.Timestamp, draining bool, membership string) Record {
		liveness := livenesspb.Liveness{
			Epoch:      epo,
			Expiration: expiration.ToLegacyTimestamp(),
			Draining:   draining,
			Membership: toMembershipStatus(membership),
		}
		raw, err := protoutil.Marshal(&liveness)
		if err != nil {
			t.Fatal(err)
		}
		return Record{
			Liveness: liveness,
			raw:      raw,
		}
	}
	const (
		no  = false
		yes = true
	)
	now := hlc.Timestamp{WallTime: 12345}

	for _, test := range []struct {
		old, new Record
		exp      bool
	}{
		{
			// Epoch update only.
			l(1, hlc.Timestamp{}, false, "active"),
			l(2, hlc.Timestamp{}, false, "active"),
			yes,
		},
		{
			// No Epoch update, but Expiration update.
			l(1, now, false, "active"),
			l(1, now.Add(0, 1), false, "active"),
			yes,
		},
		{
			// No update.
			l(1, now, false, "active"),
			l(1, now, false, "active"),
			no,
		},
		{
			// Only Decommissioning changes.
			l(1, now, false, "active"),
			l(1, now, false, "decommissioning"),
			yes,
		},
		{
			// Only Decommissioning changes.
			l(1, now, false, "decommissioned"),
			l(1, now, false, "decommissioning"),
			yes,
		},
		{
			// Only Draining changes.
			l(1, now, false, "active"),
			l(1, now, true, "active"),
			yes,
		},
		{
			// Decommissioning changes, but Epoch moves backwards.
			l(10, now, true, "decommissioning"),
			l(9, now, true, "active"),
			no,
		},
		{
			// Draining changes, but Expiration moves backwards.
			l(10, now, false, "active"),
			l(10, now.Add(-1, 0), true, "active"),
			no,
		},
		{
			// Only raw encoding changes.
			l(1, now, false, "active"),
			func() Record {
				r := l(1, now, false, "active")
				r.raw = append(r.raw, []byte("different")...)
				return r
			}(),
			yes,
		},
	} {
		t.Run("", func(t *testing.T) {
			if act := shouldReplaceLiveness(context.Background(), test.old, test.new); act != test.exp {
				t.Errorf("unexpected update: %+v", test)
			}
		})
	}
}
