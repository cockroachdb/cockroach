package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/tracker"
)

func TestUpdateRaftStatusActivity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		prs        []tracker.Progress
		replicas   []roachpb.ReplicaDescriptor
		lastUpdate LastUpdateTimesMap
		now        time.Time

		exp []tracker.Progress
	}

	now := timeutil.Now()

	inactivityThreashold := time.Second

	tcs := []testCase{
		// No data, no crash.
		{},
		// No knowledge = no update.
		{prs: []tracker.Progress{{RecentActive: true}}, exp: []tracker.Progress{{RecentActive: true}}},
		{prs: []tracker.Progress{{RecentActive: false}}, exp: []tracker.Progress{{RecentActive: false}}},
		// See replica in descriptor but then don't find it in the map. Assumes the follower is not
		// active.
		{
			replicas: []roachpb.ReplicaDescriptor{{ReplicaID: 1}},
			prs:      []tracker.Progress{{RecentActive: true}},
			exp:      []tracker.Progress{{RecentActive: false}},
		},
		// Three replicas in descriptor. The first one responded recently, the second didn't,
		// the third did but it doesn't have a Progress.
		{
			replicas: []roachpb.ReplicaDescriptor{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}},
			prs:      []tracker.Progress{{RecentActive: false}, {RecentActive: true}},
			lastUpdate: map[roachpb.ReplicaID]time.Time{
				1: now.Add(-1 * inactivityThreashold / 2),
				2: now.Add(-1 - inactivityThreashold),
				3: now,
			},
			now: now,

			exp: []tracker.Progress{{RecentActive: true}, {RecentActive: false}},
		},
	}

	ctx := context.Background()

	for _, tc := range tcs {
		t.Run("", func(t *testing.T) {
			prs := make(map[uint64]tracker.Progress)
			for i, pr := range tc.prs {
				prs[uint64(i+1)] = pr
			}
			expPRs := make(map[uint64]tracker.Progress)
			for i, pr := range tc.exp {
				expPRs[uint64(i+1)] = pr
			}
			updateRaftProgressFromActivity(ctx, prs, tc.replicas,
				func(replicaID roachpb.ReplicaID) bool {
					return tc.lastUpdate.IsFollowerActiveSince(ctx, replicaID, tc.now, inactivityThreashold)
				},
			)

			assert.Equal(t, expPRs, prs)
		})
	}
}
