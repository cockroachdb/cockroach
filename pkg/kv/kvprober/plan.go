package kvprober

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type Plan struct{
	RangeID roachpb.RangeID
	StartKey roachpb.RKey
}

// TODO(josh): This is public to integration test from kvprober_test.
type Planner interface {
	// Plan returns a plan for the prober to execute on. Executing on the
	// plan repeatedly should lead to an even distribution of probes over ranges.
	Plan(ctx context.Context) (Plan, error)
}

// A Planner that scans meta2 to make plans.
type meta2Planner struct {
	db *kv.DB
	cursor roachpb.Key
	plans []Plan
	planNProbesAtATime int
}

func newMeta2Planner(db *kv.DB, planNProbesAtATime int) *meta2Planner {
	return &meta2Planner{
		db:                 db,
		cursor:             keys.Meta2Prefix,
		planNProbesAtATime: planNProbesAtATime,
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
func (p *meta2Planner) Plan(ctx context.Context) (Plan, error) {
	  if len(p.plans) == 0 {
			kvs, err := p.db.Scan(ctx, p.cursor, keys.Meta2KeyMax.Next(), int64(p.planNProbesAtATime) /*maxRows*/)
			if err != nil {
				return Plan{}, err
			}

			p.plans = make([]Plan, len(kvs))
			var rangeDesc roachpb.RangeDescriptor
			for i, kv := range kvs {
				if err := kv.ValueProto(&rangeDesc); err != nil {
					return Plan{}, err
				}
				p.plans[i] = Plan{
					RangeID:  rangeDesc.RangeID,
					StartKey: rangeDesc.StartKey,
				}
				// It appears r1's start key (/Min) can't be queried. The prober gets
				// back this error if it's attempted: "attempted access to empty key"
				if rangeDesc.RangeID == 1 {
					p.plans[i].StartKey = p.plans[i].StartKey.Next()
				}
			}

			// This plus jitter added to the sleep time means probes on all nodes
			// shouldn't hit same ranges at the same time.
			rand.Shuffle(len(p.plans), func(i, j int) {
				p.plans[i], p.plans[j] = p.plans[j], p.plans[i]
			})

			p.cursor = kvs[len(kvs)-1].Key.Next()
			// If less than planNProbesAtATime kv pairs were returned, then at end
			// of meta2. So wrap around to beginning.
			// TODO(josh): Plan plaNProbesAtATime even in case of hitting the end of
			// meta2.
			if len(kvs) < p.planNProbesAtATime {
				p.cursor = keys.Meta2Prefix
			}
		}

	pl := p.plans[0]
	p.plans = p.plans[1:]
	return pl, nil
}
