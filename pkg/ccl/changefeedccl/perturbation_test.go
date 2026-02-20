// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"math"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestChangefeedPerturbation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")
	skip.UnderDeadlock(t, "takes too long under deadlock")

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rng, seed := randutil.NewTestRand()
		t.Logf("random seed: %d", seed)

		v, err := cdctest.RunPerturbation(f, s.DB, t.Name(), rng)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		for _, failure := range v.Failures() {
			t.Error(failure)
		}
	}

	// Tenant tests disabled because ALTER TABLE .. SPLIT is not
	// supported with cluster virtualization.
	cdcTest(t, testFn, feedTestNoTenants, withKnobsFn(func(knobs *base.TestingKnobs) {
		knobs.SQLEvalContext = &eval.TestingKnobs{
			ForceProductionValues: true,
		}
		knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	}))
	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}
