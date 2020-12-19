package prober

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// TODO(josh): Beyond correctness, we have three design goals for our
// approach to probabilistically selecting a place in the keyspace to probe:
// 1. That the approach is efficient enough (shouldn't scale with number of
//    ranges in the cluster).
// 2. That the approach is available enough in times of outage that the
//    prober is able to generate useful signal when we need it most.
// 3. That the approach doesn't unnecessarily make quiescent ranges active.

const (
	mode = queryMeta2

	// Make a plan by querying crdb_internal.ranges_no_leases. This is straight
	// forward but not performant. There are multiple problems:
	// 1. The whole virtual table is materialized in memory regardless of details
	//    of the query (e.g. LIMIT 1).
	// 2. ORDER BY random() LIMIT 1 means loading the whole table even if 1 was
	//    fixed.
	//
	// Probing based on these plans will make quiescent ranges active.
	queryCRDBInternal = iota
	// Make a plan by scanning meta2 via *kv.DB. This approach shouldn't have the
	// performance issues that queryCRDBInternal does, e.g. at plan time only
	// a portion of meta2 is scanned. It also depends on just meta2, unlike
	// queryCRDBInternal (which pulls descriptors so as to list DB & table names).
	//
	// Probing based on these plans will make quiescent ranges active.
	queryMeta2
	// Make a plan by querying the rangecache. Raphael suggested this but I
	// haven't implemented it or even looked much at rangecache.
	queryRangeCache
)

const planNProbesAtATime = 10

type Plan []ProbePlan

type ProbePlan struct{
	RangeID roachpb.RangeID
	StartKey string
	EndKey string
}

// TODO(josh): I've made this public in order to integration test from package
// prober_test that repeated calls to it make a plan covering all ranges
// in the cluster. I've written this test against serverutils.StartSever
// & server.TestServerFactory. I can't write a test like this from package
// prober without introducing a circular dependency IIUC. I dislike that this
// function is now public and wonder if reviewers have thoughts on how to avoid
// given the test utilities being depended on.

func (p *KVProber) MakePlan(ctx context.Context) (Plan, error) {
	switch mode {
	case queryMeta2:
		// Our goal is to probe the whole keyspace. This can be trivially achieved by
		// scanning meta2 and sending probes to ranges in the order they
		// appear in meta2. We scan meta2 here but we also randomize the order of
		// ranges in the plan. This is avoid all nodes probing the same ranges at
		// the same time. Note that jitter is also added to the sleep between probe time
		// to de-synchronize different nodes' probe loops.
		//
		// The CPU & mem spend of the prober should not scale with the number of ranges
		// in the cluster. To achieve this, planNProbesAtATime probes are planned at a
		// time.
		kvs, err := p.db.Scan(ctx, p.indexIntoMeta2, keys.Meta2KeyMax, planNProbesAtATime /*maxRows*/)
		if err != nil {
			return nil, err
		}

		plan := make(Plan, len(kvs))
		var rangeDesc roachpb.RangeDescriptor
		for i, kv := range kvs {
			if err := kv.ValueProto(&rangeDesc); err != nil {
				return nil, err
			}
			sk := tree.DBytes(rangeDesc.StartKey)
			ek := tree.DBytes(rangeDesc.EndKey)
			plan[i] = ProbePlan{
				RangeID:  rangeDesc.RangeID,
				StartKey: sk.String(),
				EndKey: ek.String(),
			}
		}

		// This plus jitter added to the sleep time means probes on all nodes
		// shouldn't hit same ranges at the same time.
		// TODO(josh): Implement jitter in kvprober.go.
		rand.Shuffle(len(plan), func(i, j int) {
			plan[i], plan[j] = plan[j], plan[i]
		})

		p.indexIntoMeta2 = kvs[len(kvs)-1].Key.Next()
		// TODO(josh): Wrap so as to always plan planNProbesAtATime probes at a time.
		if len(kvs) < planNProbesAtATime {
			p.indexIntoMeta2 = keys.Meta2Prefix.Next()
		}

		return plan, nil
	case queryCRDBInternal:
		row, err := p.internalExecutor.QueryRowEx(
			ctx, "kvprober", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			// TODO(josh): Probing for range ID 1 leads to following error:
			// start key in [/Min,/System/NodeLiveness) must be greater than LocalMax
			// I don't understand the situation exactly. Is filtering out range ID
			// 1 sufficient to avoid the problem?
			"SELECT * FROM crdb_internal.ranges_no_leases WHERE range_id != 1 ORDER BY random() LIMIT 1;",
		)
		if err != nil {
			return nil, err
		}

		rangeID := tree.MustBeDInt(row[0])
		startKey := tree.MustBeDBytes(row[1])
		endKey := tree.MustBeDBytes(row[3])

		return Plan{{
			RangeID: roachpb.RangeID(rangeID),
			StartKey: startKey.String(),
			EndKey: endKey.String(),
		}}, nil
	case queryRangeCache:
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("invalid mode")
	}
}
