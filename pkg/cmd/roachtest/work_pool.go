// Copyright 2019 The Cockroach Authors.
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

package main

import (
	"context"

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

func newWorkPool(tests []testSpec, count int) *workPool {
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
	spec testSpec
	// runNum is run number. 1 if --count was not used.
	runNum int

	// canReuseCluster is true if the selected test can reuse the cluster passed
	// to testToRun(). Will be false if noWork is set.
	canReuseCluster bool
	// alloc is set if canReuseCluster is false. It represents the resources to
	// use for creating a new cluster (matching spec). The alloc needs to be
	// transferred to the cluster that is created, or otherwise Release()d.
	alloc quotaAlloc
}

// getTestToRun selects a test. It optionally takes a cluster and will try to
// select a test that can reuse that cluster. If it succeeds, then
// testToRunRes.canReuseCluster will be set. Otherwise, the cluster is destroyed
// so its resources are released, and the result will contain a quotaAlloc to be
// used by the caller for creating a new cluster.
//
// If a new cluster needs to be created, the call blocks until enough resources
// are taken out of qp.
func (p *workPool) getTestToRun(
	ctx context.Context,
	c *cluster,
	cs clusterSpec,
	qp *quotapool.IntPool,
	cr *clusterRegistry,
	l *logger,
) (testToRunRes, error) {
	// If we've been given a cluster, see if we can reuse it.
	if c != nil {
		ttr := p.selectTestForCluster(ctx, cs, cr)
		if ttr.noWork {
			// We failed to find a test that can take advantage of this cluster. So
			// we're going to release is, which will deallocate its resources, and
			// then we'll look for a test below.
			l.PrintfCtx(ctx, "Cluster (%s) cannot be reused. Destroying.\n", c)
			c.Destroy(ctx, closeLogger, l)
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
	ctx context.Context, spec clusterSpec, cr *clusterRegistry,
) testToRunRes {
	p.mu.Lock()
	defer p.mu.Unlock()
	testsWithCounts := p.findCompatibleTestsLocked(spec)

	if len(testsWithCounts) == 0 {
		return testToRunRes{noWork: true}
	}

	tag := ""
	if p, ok := spec.ReusePolicy.(reusePolicyTagged); ok {
		tag = p.tag
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

// quotaAlloc represents a cloud CPU allocation. Release() must be called on it
// to release the resources back to the pool from whence they came.
type quotaAlloc struct {
	qp    *quotapool.IntPool
	alloc int64
}

// Release releases the alloc's resources back to the resource pool.
func (qa quotaAlloc) Release() {
	qa.qp.Add(qa.alloc)
}

// selectTest selects a test to run based on the available resources. If there are
// no resources available to run any test, it blocks until enough resources become available.
//
// If multiple tests are eligible to run, one with the most runs left is chosen.
// TODO(andrei): We could be smarter in guessing what kind of cluster is best to
// allocate.
func (p *workPool) selectTest(ctx context.Context, qp *quotapool.IntPool) (testToRunRes, error) {
	var ttr testToRunRes
	alloc, err := qp.AcquireCb(ctx, func(ctx context.Context, availableCPUs int64) (bool, int64) {
		p.mu.Lock()
		defer p.mu.Unlock()

		if len(p.mu.tests) == 0 {
			ttr = testToRunRes{
				noWork: true,
			}
			return true, 0
		}

		candidateIdx := -1
		candidateCount := 0
		for i, t := range p.mu.tests {
			cpu := t.spec.Cluster.NodeCount * t.spec.Cluster.CPUs
			if int64(cpu) > availableCPUs {
				continue
			}
			if t.count > candidateCount {
				candidateIdx = i
				candidateCount = t.count
			}
		}

		if candidateIdx == -1 {
			return false, 0
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
		return true, int64(cpu)
	})
	if err != nil {
		return testToRunRes{}, err
	}
	ttr.alloc = quotaAlloc{qp: qp, alloc: alloc}
	return ttr, nil
}

// scoreTestAgainstCluster scores the suitability of running a test against a
// cluster currently tagged with tag.
//
// cr is used for its information about how many clusters with a given tag currently exist.
func scoreTestAgainstCluster(tc testWithCount, tag string, cr *clusterRegistry) int {
	t := tc.spec
	if tag != "" && t.Cluster.ReusePolicy != (reusePolicyTagged{tag: tag}) {
		log.Fatalf(context.TODO(),
			"incompatible test and cluster. Cluster tag: %s. Test policy: %+v",
			tag, t.Cluster.ReusePolicy)
	}
	score := 0
	if _, ok := t.Cluster.ReusePolicy.(reusePolicyAny); ok {
		score = 1000000
	} else if _, ok := t.Cluster.ReusePolicy.(reusePolicyTagged); ok {
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
func (p *workPool) findCompatibleTestsLocked(clusterSpec clusterSpec) []testWithCount {
	if _, ok := clusterSpec.ReusePolicy.(reusePolicyNone); ok {
		panic("can't search for tests compatible with a ReuseNone policy")
	}
	var tests []testWithCount
	for _, tc := range p.mu.tests {
		if clustersCompatible(clusterSpec, tc.spec.Cluster) {
			tests = append(tests, tc)
		}
	}
	return tests
}

func clustersCompatible(s1, s2 clusterSpec) bool {
	s1.Lifetime = 0
	s2.Lifetime = 0
	return s1 == s2
}
