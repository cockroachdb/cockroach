// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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
	// runCount is the total number of runs. 1 if --count was not used.
	runCount int
	// runNum is run number. 1 if --count was not used.
	runNum int

	// canReuseCluster is true if the selected test can reuse the cluster passed
	// to testToRun(). Will be false if noWork is set.
	canReuseCluster bool
}

func (p *workPool) workRemaining() []testWithCount {
	p.mu.Lock()
	defer p.mu.Unlock()
	res := make([]testWithCount, len(p.mu.tests))
	copy(res, p.mu.tests)
	return res
}

// selectTestForCluster selects a test to run on a cluster with a given spec.
//
// Among tests that match the spec, we do the following:
//   - If the cluster is already tagged, we only look at tests with the same tag.
//   - Otherwise, we'll choose in the following order of preference:
//     1) tests that leave the cluster usable by anybody afterwards
//     2) tests that leave the cluster usable by some other tests
//     2.1) within this OnlyTagged<foo> category, we'll prefer the tag with the
//     fewest existing clusters.
//     3) tests that leave the cluster unusable by anybody
//
// Within each of the categories, we'll give preference to tests with fewer
// runs.
//
// cr is used for its information about how many clusters with a given tag currently exist.
func (p *workPool) selectTestForCluster(
	ctx context.Context, l *logger.Logger, s spec.ClusterSpec, cr *clusterRegistry, cloud spec.Cloud,
) testToRunRes {
	p.mu.Lock()
	defer p.mu.Unlock()
	testsWithCounts := p.findCompatibleTestsLocked(s, cloud)

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
		score := scoreTestAgainstCluster(l, tc, tag, cr)
		if score > candidateScore {
			candidateScore = score
			candidate = tc
		}
	}

	p.decTestLocked(ctx, l, candidate.spec.Name)

	runNum := p.count - candidate.count + 1
	return testToRunRes{
		spec:            candidate.spec,
		runCount:        p.count,
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
//
// ensures:  !testToRunRes.noWork || error == nil
func (p *workPool) selectTest(
	ctx context.Context, qp *quotapool.IntPool, l *logger.Logger,
) (testToRunRes, *quotapool.IntAlloc, error) {
	logTimer := time.AfterFunc(5*time.Second, func() {
		l.PrintfCtx(ctx, "Waiting for CPU quota to select a new test...")
	})

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
		smallestTestCPU := math.MaxInt64
		smallestTestIdx := -1
		for i, t := range p.mu.tests {
			cpu := t.spec.Cluster.TotalCPUs()
			if cpu < smallestTestCPU {
				smallestTestCPU = cpu
				smallestTestIdx = i
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
			if uint64(smallestTestCPU) > pi.Capacity {
				return 0, fmt.Errorf(
					"not enough CPU quota to run any of the remaining tests; smallest test %s requires %d CPUs but capacity is %d",
					p.mu.tests[smallestTestIdx].spec.Name, smallestTestCPU, pi.Capacity)
			}

			return 0, quotapool.ErrNotEnoughQuota
		}

		tc := p.mu.tests[candidateIdx]
		runNum := p.count - tc.count + 1
		p.decTestLocked(ctx, l, tc.spec.Name)
		ttr = testToRunRes{
			spec:            tc.spec,
			runCount:        p.count,
			runNum:          runNum,
			canReuseCluster: false,
		}
		cpu := tc.spec.Cluster.TotalCPUs()
		return uint64(cpu), nil
	})

	// Cancel the log message.
	logTimer.Stop()

	if err != nil {
		l.PrintfCtx(ctx, "Error acquiring quota: %v", err)
		return testToRunRes{}, nil, err
	}
	if alloc.Acquired() > 0 {
		l.PrintfCtx(ctx, "Acquired quota for %s CPUs", alloc.String())
	}
	return ttr, alloc, nil
}

// scoreTestAgainstCluster scores the suitability of running a test against a
// cluster currently tagged with tag (empty if cluster is not tagged).
//
// cr is used for its information about how many clusters with a given tag
// currently exist.
func scoreTestAgainstCluster(
	l *logger.Logger, tc testWithCount, tag string, cr *clusterRegistry,
) int {
	t := tc.spec
	testPolicy := t.Cluster.ReusePolicy
	if tag != "" && testPolicy != (spec.ReusePolicyTagged{Tag: tag}) {
		logFatalfCtx(context.Background(), l,
			"incompatible test and cluster. Cluster tag: %s. Test policy: %+v",
			tag, t.Cluster.ReusePolicy,
		)
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
func (p *workPool) findCompatibleTestsLocked(
	clusterSpec spec.ClusterSpec, cloud spec.Cloud,
) []testWithCount {
	if _, ok := clusterSpec.ReusePolicy.(spec.ReusePolicyNone); ok {
		// Cluster cannot be reused, so no tests are compatible.
		return nil
	}
	var tests []testWithCount
	for _, tc := range p.mu.tests {
		if spec.ClustersCompatible(clusterSpec, tc.spec.Cluster, cloud) {
			tests = append(tests, tc)
		}
	}
	return tests
}

// decTestLocked decrements a test's remaining count and removes it
// from the workPool if it was exhausted.
func (p *workPool) decTestLocked(ctx context.Context, l *logger.Logger, name string) {
	idx := -1
	for idx = range p.mu.tests {
		if p.mu.tests[idx].spec.Name == name {
			break
		}
	}
	if idx == -1 {
		logFatalfCtx(ctx, l, "failed to find test: %s", name)
	}
	tc := &p.mu.tests[idx]
	tc.count--
	if tc.count == 0 {
		// We've selected the last run for a test. Take that test out of the pool.
		p.mu.tests = append(p.mu.tests[:idx], p.mu.tests[idx+1:]...)
	}
}
