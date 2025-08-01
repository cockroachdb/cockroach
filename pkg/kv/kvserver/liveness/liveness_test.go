// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestLivenessRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	liveness := livenesspb.Liveness{
		NodeID:     roachpb.NodeID(13),
		Epoch:      3,
		Expiration: hlc.Timestamp{WallTime: 12345}.ToLegacyTimestamp(),
		Draining:   true,
		Membership: livenesspb.MembershipStatus_ACTIVE,
	}

	require.EqualValues(t,
		"liveness(nid:13 epo:3 exp:0.000012345,0 drain:true membership:active)",
		redact.Sprintf("%+v", liveness).Redact())
}

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
			if act := livenessChanged(test.old, test.new); act != test.exp {
				t.Errorf("unexpected update: %+v", test)
			}
		})
	}
}

func TestNodeLivenessLivenessStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(100000, 100000)))
	cache := NewCache(&mockGossip{}, clock, cluster.MakeTestingClusterSettings(), nil)
	now := clock.Now()
	threshold := TimeUntilNodeDead.Default()
	suspectThreshold := TimeAfterNodeSuspect.Default()

	for _, tc := range []struct {
		name           string
		liveness       livenesspb.Liveness
		descriptor     UpdateInfo
		expectedAlive  bool
		expectedStatus livenesspb.NodeLivenessStatus
	}{
		// Assume that we just haven't received a gossip update if we have a
		// liveness record but no descriptor.
		{
			name: "No descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expired descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DEAD,
		},
		{
			name: "Unavailable recent descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUnavailableTime: now.AddDuration(-1 * time.Second), lastUpdateTime: now.AddDuration(-1 * time.Second)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Unavailable descriptor a long time ago",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUnavailableTime: now.AddDuration(-suspectThreshold), lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Unavailable and Expired descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUnavailableTime: now.AddDuration(-suspectThreshold), lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DEAD,
		},
		// Valid status.
		{
			name: "Valid 5 min",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(5 * time.Minute).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expires just slightly in the future",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(time.Nanosecond).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expired both",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Max bound of expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).AddDuration(time.Nanosecond).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Dead",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DEAD,
		},
		{
			name: "Decommissioning",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(time.Second).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		{
			name: "Decommissioning + expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		{
			name: "Decommissioning + live",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(time.Second).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONED,
				Draining:   false,
			},
			// Despite having marked the node as fully decommissioned, through
			// this NodeLivenessStatus API we still surface the node as
			// "Decommissioning". See #50707 for more details.
			descriptor: UpdateInfo{lastUpdateTime: now},
			// Despite having marked the node as fully decommissioned, through
			// this NodeLivenessStatus API we still surface the node as
			// "Decommissioning". See #50707 for more details.
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		{
			name: "Decommissioned + expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONED,
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		{
			name: "Draining",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(5 * time.Minute).ToLegacyTimestamp(),
				Draining:   true,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_DRAINING,
		},
		{
			name: "Decommissioning that is unavailable",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   false,
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Draining that is unavailable",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   true,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// TODO(baptist): Remove this when LivenessStatus is removed. This is here
			// to make sure we preserve existing behavior. Both the old and new
			// mechanisms should return the same result.
			// Use the set node descriptor.
			if (tc.descriptor != UpdateInfo{}) {
				cache.mu.lastNodeUpdate[1] = tc.descriptor
			}

			nv := cache.convertToNodeVitality(tc.liveness)
			require.Equal(t, tc.expectedAlive, nv.IsLive(livenesspb.Rebalance))
			require.Equal(t, tc.expectedStatus, nv.LivenessStatus())
		})
	}
}

type mockGossip struct {
	m map[string]gossip.Callback
}

func (g *mockGossip) RegisterCallback(
	pattern string, method gossip.Callback, opts ...gossip.CallbackOption,
) func() {
	if g.m == nil {
		g.m = map[string]gossip.Callback{}
	}
	g.m[pattern] = method
	return func() {
		delete(g.m, pattern)
	}
}

func (g *mockGossip) GetNodeID() roachpb.NodeID {
	return 1
}

func mockRecord(nodeID roachpb.NodeID) Record {
	return Record{Liveness: livenesspb.Liveness{NodeID: nodeID}}
}

type mockStorage struct{}

func (s *mockStorage) Get(ctx context.Context, nodeID roachpb.NodeID) (Record, error) {
	return mockRecord(nodeID), nil
}

func (s *mockStorage) Update(
	ctx context.Context, update LivenessUpdate, handleCondFailed func(actual Record) error,
) (Record, error) {
	return Record{Liveness: update.newLiveness}, nil
}

func (s *mockStorage) Create(ctx context.Context, nodeID roachpb.NodeID) error {
	return nil
}

func (s *mockStorage) Scan(ctx context.Context) ([]Record, error) {
	return []Record{mockRecord(2), mockRecord(3)}, nil
}

func TestNodeLivenessIsLiveCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	st := cluster.MakeTestingClusterSettings()
	mc := timeutil.NewManualTime(timeutil.Unix(1, 0))
	g := &mockGossip{}
	s := &mockStorage{}
	clock := hlc.NewClockForTesting(mc)
	nl := NewNodeLiveness(NodeLivenessOptions{
		AmbientCtx:              log.MakeTestingAmbientCtxWithNewTracer(),
		Stopper:                 stopper,
		Cache:                   NewCache(g, clock, st, nil),
		Clock:                   clock,
		Storage:                 s,
		LivenessThreshold:       24 * time.Hour,
		RenewalDuration:         23 * time.Hour,
		HistogramWindowInterval: base.DefaultHistogramWindowInterval(),
	})

	require.Len(t, g.m, 2)

	update := func(nodeID roachpb.NodeID) {
		fn := g.m[livenessRegex]
		k := gossip.MakeNodeLivenessKey(nodeID)
		exp := nl.clock.Now().ToLegacyTimestamp()
		exp.WallTime++ // stays valid until we bump `mc`
		lv := livenesspb.Liveness{
			NodeID:     nodeID,
			Epoch:      1,
			Expiration: exp,
			Membership: livenesspb.MembershipStatus_ACTIVE,
		}
		var v roachpb.Value
		require.NoError(t, v.SetProto(&lv))
		fn(k, v)
	}

	m := struct {
		syncutil.Mutex
		m map[string]int
	}{m: map[string]int{}}

	getMap := func() map[string]int {
		m.Lock()
		defer m.Unlock()
		mm := map[string]int{}
		for k, v := range m.m {
			mm[k] = v
		}
		return mm
	}

	const (
		preStartPreGossip   = "pre-start-pre-gossip"
		preStartPostGossip  = "pre-start-post-gossip"
		postStartPreGossip  = "post-start-pre-gossip"
		postStartPostGossip = "post-start-post-gossip"
	)

	reg := func(name string) {
		nl.RegisterCallback(func(liveness livenesspb.Liveness) {
			if liveness.NodeID == 1 {
				// Ignore calls that come from the liveness heartbeating itself,
				// since these are on a goroutine and we just want a deterministic
				// test here.
				return
			}
			m.Lock()
			defer m.Unlock()
			m.m[name]++
		})
	}

	reg(preStartPreGossip)
	// n2 becomes live right after (n1's) Liveness is instantiated, before it's started.
	// This should invoke the callback that is already registered. There is no concurrency
	// yet so we don't have to lock `m`.
	update(2)
	require.Equal(t, map[string]int{
		preStartPreGossip: 1,
	}, getMap())

	reg(preStartPostGossip)
	nl.Start(ctx)
	reg(postStartPreGossip)
	update(3)
	reg(postStartPostGossip)

	// Each callback gets called twice (once for n2 once for n3), regardless of when
	// it was registered.
	require.Equal(t, map[string]int{
		preStartPreGossip:   2,
		preStartPostGossip:  2,
		postStartPreGossip:  2,
		postStartPostGossip: 2,
	}, getMap())

	// Additional gossip updates don't trigger more callbacks unless node
	// is non-live in the meantime.
	update(2)
	update(3)
	require.Equal(t, map[string]int{
		preStartPreGossip:   2,
		preStartPostGossip:  2,
		postStartPreGossip:  2,
		postStartPostGossip: 2,
	}, getMap())

	mc.Advance(time.Second) // n2 and n3 are now non-live

	require.Equal(t, map[string]int{
		preStartPreGossip:   2,
		preStartPostGossip:  2,
		postStartPreGossip:  2,
		postStartPostGossip: 2,
	}, getMap())

	// Heartbeat from 3 triggers callbacks because old liveness
	// is now non-live whereas the new one is.
	update(3)
	require.Equal(t, map[string]int{
		preStartPreGossip:   3,
		preStartPostGossip:  3,
		postStartPreGossip:  3,
		postStartPostGossip: 3,
	}, getMap())
}
