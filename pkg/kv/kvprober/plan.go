// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvprober

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

// Plan is a decision on what range to probe, including info needed by kvprober
// to execute on the plan, such as the range's start key.
type Plan struct {
	RangeID  roachpb.RangeID
	StartKey roachpb.RKey
}

// Planner abstracts deciding on ranges to probe. It is public to integration
// test from kvprober_test.
type Planner interface {
	// Plan returns a plan for the prober to execute on. Executing on the
	// plan repeatedly should lead to an even distribution of probes over ranges,
	// at least in the limit.
	Plan(ctx context.Context) (Plan, error)
}

// meta2Planner is a planner that scans meta2 to make plans. A plan is a
// decision on what range to probe, including info needed by kvprober to
// execute on that plan, such as the range's start key.
type meta2Planner struct {
	db       *kv.DB
	settings *cluster.Settings
	// cursor points to a key in meta2 at which scanning should resume when new
	// plans are needed.
	cursor roachpb.Key
	// meta2Planner makes plans of size planNProbesAtATime as per below. Plans
	// not yet passed to kvprober.go via the Plan interface are stored here.
	plans []Plan
}

// PlanNProbesAtATime controls how big of a plan is made, when planning is done.
// The production planner determines ranges to probe in batches of size
// PlanNProbesAtATime. Setting this very high leads to scanning a lot of meta2
// at once, which may cause high resource usage. Setting this very low leads
// to an overly deterministic probe plan, which may not be desirable, as all
// CRDB nodes running the prober could probe the same ranges at the same time
// (though there is jitter in kvprober.go to avoid this). See the docs above
// Plan for more.
var PlanNProbesAtATime = settings.RegisterIntSetting(
	"kv.prober.planner.n_probes_at_a_time",
	"the number of probes to plan out at once",
	100)

func newMeta2Planner(db *kv.DB, settings *cluster.Settings) *meta2Planner {
	return &meta2Planner{
		db:       db,
		settings: settings,
		cursor:   keys.Meta2Prefix,
	}
}

// Beyond even distribution of probes over the "rangespace", we have
// two design goals for our approach to probabilistically selecting a place in
// the keyspace to probe:
//
// 1. That the approach is efficient enough. Resource requirements shouldn't
//    scale with the number of ranges in the cluster, for example.
// 2. That the approach is available enough in times of outage that the
//    prober is able to generate useful signal when we need it most.
//
// How do we do it? The first option we considered was to probe
// crdb_internal.ranges_no_leases. We reject that approach in favor of making a
// plan by scanning meta2 via *kv.DB. This approach shouldn't have the
// performance issues that querying crdb_internal.ranges_no_leases does, e.g.
// at plan time only a portion of meta2 is scanned. It also depends on just
// meta2, unlike querying crdb_internal.ranges_no_leases (which pulls table
// descriptors so as to list table names).
//
// Note that though we scan meta2 here, we also randomize the order of
// ranges in the plan. This is avoid all nodes probing the same ranges at
// the same time. Jitter is also added to the sleep between probe time
// to de-synchronize different nodes' probe loops.
//
// What about resource usage?
//
// The first thing to note is that due to the
// kv.prober.planner.n_probes_at_a_time cluster setting, the resource usage
// should not scale up as the number of ranges in the cluster grows.
//
// Memory:
// - The meta2Planner struct's mem usage scales with
//   size(the Plan struct) * the kv.prober.planner.n_probes_at_a_time cluster
//   setting.
// - The Plan function's mem usage scales with
//   size(KV pairs holding range descriptors) * the
//   kv.prober.planner.n_probes_at_a_time cluster setting.
//
// CPU:
// - Again scales with the the kv.prober.planner.n_probes_at_a_time cluster
//   setting. Note that we sort a slice of size
//   kv.prober.planner.n_probes_at_a_time. If the setting is set to a high
//   number, we pay a higher CPU cost less often; if it's set to a low number,
//   we pay a smaller CPU cost more often.
func (p *meta2Planner) Plan(ctx context.Context) (Plan, error) {
	if len(p.plans) == 0 {
		kvs, cursor, err := getNMeta2KVs(
			ctx, p.db, PlanNProbesAtATime.Get(&p.settings.SV), p.cursor)
		if err != nil {
			return Plan{}, errors.Wrapf(err, "failed to get meta2 rows")
		}
		p.cursor = cursor

		plans, err := meta2KVsToPlans(kvs)
		if err != nil {
			return Plan{}, errors.Wrapf(err, "failed to make plans from meta2 rows")
		}

		// This plus jitter added to the sleep time means probes on all nodes
		// shouldn't hit same ranges at the same time.
		rand.Shuffle(len(plans), func(i, j int) {
			plans[i], plans[j] = plans[j], plans[i]
		})

		p.plans = plans
	}

	pl := p.plans[0]
	p.plans = p.plans[1:]
	return pl, nil
}

type dbScan interface {
	Scan(ctx context.Context, begin, end interface{}, maxRows int64) ([]kv.KeyValue, error)
}

func getNMeta2KVs(
	ctx context.Context, db dbScan, n int64, cursor roachpb.Key,
) ([]kv.KeyValue, roachpb.Key, error) {
	var kvs []kv.KeyValue
	for n > 0 {
		newkvs, err := db.Scan(ctx, cursor, keys.Meta2KeyMax.Next(), n /*maxRows*/)
		if err != nil {
			return nil, nil, err
		}

		n = n - int64(len(newkvs))
		kvs = append(kvs, newkvs...)
		cursor = kvs[len(kvs)-1].Key.Next()
		// If at end of meta2, wrap around to the beginning.
		if cursor.Equal(keys.Meta2KeyMax.Next()) {
			cursor = keys.Meta2Prefix
		}
	}
	return kvs, cursor, nil
}

func meta2KVsToPlans(kvs []kv.KeyValue) ([]Plan, error) {
	plans := make([]Plan, len(kvs))

	var rangeDesc roachpb.RangeDescriptor
	for i, kv := range kvs {
		if err := kv.ValueProto(&rangeDesc); err != nil {
			return nil, err
		}
		plans[i] = Plan{
			RangeID:  rangeDesc.RangeID,
			StartKey: rangeDesc.StartKey,
		}
		// It appears r1's start key (/Min) can't be queried. The prober gets
		// back this error if it's attempted: "attempted access to empty key"
		if rangeDesc.RangeID == 1 {
			plans[i].StartKey = plans[i].StartKey.Next()
		}
	}

	return plans, nil
}
