// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// workPool keeps track of what tests still need to run and facilitates
// selecting the next test to run.
type workPool struct {
	// count is the total number of times each test has to run. It is constant.
	// Not to be confused with the count inside mu.tests, which tracks remaining
	// runs.
	count int
	mu    struct {
		syncutil.Mutex
		// tests with remaining run count.
		tests []testWithCount
	}
}

func newWorkPool(tests []registry.TestSpec, count int) *workPool {
	p := &workPool{count: count}
	for _, spec := range tests {
		p.mu.tests = append(p.mu.tests, testWithCount{spec: spec, count: count})
	}
	return p
}

// testToRunRes represents the return value of getTestToRun. It provides
// information about what test to run (if any) and what cluster to use for it.
type testToRunRes struct {
	// noWork is set if the work pool was empty and thus no test was selected. No
	// other fields are set.
	noWork bool
	// spec is the selected test.
	spec registry.TestSpec
	// runNum is run number. 1 if --count was not used.
	runNum int

	// canReuseCluster is true if the selected test can reuse the cluster passed
	// to testToRun(). Will be false if noWork is set.
	canReuseCluster bool
	// alloc is set if canReuseCluster is false (and noWork is not set). It
	// represents the resources to use for creating a new cluster (matching spec).
	// The alloc needs to be transferred to the cluster that is created, or
	// otherwise Release()d.
	alloc *quotapool.IntAlloc
}

func (p *workPool) workRemaining() []testWithCount {
	p.mu.Lock()
	defer p.mu.Unlock()
	res := make([]testWithCount, len(p.mu.tests))
	copy(res, p.mu.tests)
	return res
}

// getTestToRun selects a test. It optionally takes a cluster and will try to
// select a test that can reuse that cluster. If it succeeds, then
// testToRunRes.canReuseCluster will be set. Otherwise, the cluster is destroyed
// so its resources are released, and the result will contain a quota alloc to
// be used by the caller for creating a new cluster.
//
// If a new cluster needs to be created, the call blocks until enough resources
// are taken out of qp.
//
// If there are no more tests to run, c will be destroyed and the result will
// have noWork set.
func (p *workPool) getTestToRun(
	ctx context.Context,
	c *clusterImpl,
	qp *quotapool.IntPool,
	cr *clusterRegistry,
	onDestroy func(),
	l *logger.Logger,
) (testToRunRes, error) {
	// If we've been given a cluster, see if we can reuse it.
	if c != nil {
		ttr := p.selectTestForCluster(ctx, c.spec, cr)
		if ttr.noWork {
			// We failed to find a test that can take advantage of this cluster. So
			// we're going to release is, which will deallocate its resources, and
			// then we'll look for a test below.
			l.PrintfCtx(ctx,
				"No tests that can reuse cluster %s found (or there are no further tests to run). "+
					"Destroying.", c)
			c.Destroy(ctx, closeLogger, l)
			onDestroy()
		} else {
			return ttr, nil
		}
	}

	return p.selectTest(ctx, qp)
}

// selectTestForCluster selects a test to run on a cluster with a given spec.
//
// Among tests that match the spec, we do the following:
// - If the cluster is already tagged, we only look at tests with the same tag.
// - Otherwise, we'll choose in the following order of preference:
// 1) tests that leave the cluster usable by anybody afterwards
// 2) tests that leave the cluster usable by some other tests
// 	2.1) within this OnlyTagged<foo> category, we'll prefer the tag with the
// 			 fewest existing clusters.
// 3) tests that leave the cluster unusable by anybody
//
// Within each of the categories, we'll give preference to tests with fewer
// runs.
//
// cr is used for its information about how many clusters with a given tag currently exist.
func (p *workPool) selectTestForCluster(
	ctx context.Context, s spec.ClusterSpec, cr *clusterRegistry,
) testToRunRes {
	p.mu.Lock()
	defer p.mu.Unlock()
	testsWithCounts := p.findCompatibleTestsLocked(s)

	if len(testsWithCounts) == 0 {
		return testToRunRes{noWork: true}
	}

	tag := ""
	if p, ok := s.ReusePolicy.(spec.ReusePolicyTagged); ok {
		tag = p.Tag
	}
	// Find the best test to run.
	candidateScore := 0
	var candidate testWithCount
	for _, tc := range testsWithCounts {
		score := scoreTestAgainstCluster(tc, tag, cr)
		if score > candidateScore {
			candidateScore = score
			candidate = tc
		}
	}

	p.decTestLocked(ctx, candidate.spec.Name)
	runNum := p.count - candidate.count + 1
	return testToRunRes{
		spec:            candidate.spec,
		runNum:          runNum,
		canReuseCluster: true,
	}
}

// selectTest selects a test to run based on the available resources. If there are
// no resources available to run any test, it blocks until enough resources become available.
//
// If multiple tests are eligible to run, one with the most runs left is chosen.
// TODO(andrei): We could be smarter in guessing what kind of cluster is best to
// allocate.
func (p *workPool) selectTest(ctx context.Context, qp *quotapool.IntPool) (testToRunRes, error) {
	var ttr testToRunRes
	alloc, err := qp.AcquireFunc(ctx, func(ctx context.Context, pi quotapool.PoolInfo) (uint64, error) {
		p.mu.Lock()
		defer p.mu.Unlock()

		if len(p.mu.tests) == 0 {
			ttr = testToRunRes{
				noWork: true,
			}
			return 0, nil
		}

		candidateIdx := -1
		candidateCount := 0
		smallestTest := math.MaxInt64
		for i, t := range p.mu.tests {
			cpu := t.spec.Cluster.NodeCount * t.spec.Cluster.CPUs
			if cpu < smallestTest {
				smallestTest = cpu
			}
			if uint64(cpu) > pi.Available {
				continue
			}
			if t.count > candidateCount {
				candidateIdx = i
				candidateCount = t.count
			}
		}

		if candidateIdx == -1 {
			if uint64(smallestTest) > pi.Capacity {
				return 0, fmt.Errorf("not enough CPU quota to run any of the remaining tests")
			}

			return 0, quotapool.ErrNotEnoughQuota
		}

		tc := p.mu.tests[candidateIdx]
		runNum := p.count - tc.count + 1
		p.decTestLocked(ctx, tc.spec.Name)
		ttr = testToRunRes{
			spec:            tc.spec,
			runNum:          runNum,
			canReuseCluster: false,
		}
		cpu := tc.spec.Cluster.NodeCount * tc.spec.Cluster.CPUs
		return uint64(cpu), nil
	})
	if err != nil {
		return testToRunRes{}, err
	}
	ttr.alloc = alloc
	return ttr, nil
}

// scoreTestAgainstCluster scores the suitability of running a test against a
// cluster currently tagged with tag (empty if cluster is not tagged).
//
// cr is used for its information about how many clusters with a given tag
// currently exist.
func scoreTestAgainstCluster(tc testWithCount, tag string, cr *clusterRegistry) int {
	t := tc.spec
	testPolicy := t.Cluster.ReusePolicy
	if tag != "" && testPolicy != (spec.ReusePolicyTagged{Tag: tag}) {
		log.Fatalf(context.TODO(),
			"incompatible test and cluster. Cluster tag: %s. Test policy: %+v",
			tag, t.Cluster.ReusePolicy)
	}
	score := 0
	if _, ok := testPolicy.(spec.ReusePolicyAny); ok {
		score = 1000000
	} else if _, ok := testPolicy.(spec.ReusePolicyTagged); ok {
		score = 500000
		if tag == "" {
			// We have an untagged cluster and a tagged test. Within this category of
			// tests, we prefer the tags with the fewest existing clusters.
			score -= 1000 * cr.countForTag(tag)
		}
	} else { // NoReuse policy
		score = 0
	}

	// We prefer tests that have run fewer times (so, that have more runs left).
	score += tc.count

	return score
}

// findCompatibleTestsLocked returns a list of tests compatible with a cluster spec.
func (p *workPool) findCompatibleTestsLocked(clusterSpec spec.ClusterSpec) []testWithCount {
	if _, ok := clusterSpec.ReusePolicy.(spec.ReusePolicyNone); ok {
		panic("can't search for tests compatible with a ReuseNone policy")
	}
	var tests []testWithCount
	for _, tc := range p.mu.tests {
		if spec.ClustersCompatible(clusterSpec, tc.spec.Cluster) {
			tests = append(tests, tc)
		}
	}
	return tests
}

// decTestLocked decrements a test's remaining count and removes it
// from the workPool if it was exhausted.
func (p *workPool) decTestLocked(ctx context.Context, name string) {
	idx := -1
	for idx = range p.mu.tests {
		if p.mu.tests[idx].spec.Name == name {
			break
		}
	}
	if idx == -1 {
		log.Fatalf(ctx, "failed to find test: %s", name)
	}
	tc := &p.mu.tests[idx]
	tc.count--
	if tc.count == 0 {
		// We've selected the last run for a test. Take that test out of the pool.
		p.mu.tests = append(p.mu.tests[:idx], p.mu.tests[idx+1:]...)
	}
}
