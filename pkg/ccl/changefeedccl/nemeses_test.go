// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"math"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestChangefeedNemeses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")

	testutils.RunValues(t, "nemeses_options=", cdctest.NemesesOptions, func(t *testing.T, nop cdctest.NemesesOption) {
		if nop.EnableSQLSmith == true {
			skip.WithIssue(t, 137125)
		}
		testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			rng, seed := randutil.NewPseudoRand()
			t.Logf("random seed: %d", seed)

			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			withLegacySchemaChanger := maybeDisableDeclarativeSchemaChangesForTest(t, sqlDB)

			v, err := cdctest.RunNemesis(f, s.DB, t.Name(), withLegacySchemaChanger, rng, nop)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			for _, failure := range v.Failures() {
				t.Error(failure)
			}
		}

		// Tenant tests disabled because ALTER TABLE .. SPLIT is not
		// supported with cluster virtualization:
		//
		// nemeses_test.go:39: pq: unimplemented: operation is unsupported inside virtual clusters
		//
		// TODO(knz): This seems incorrect, see issue #109417.
		cdcTest(t, testFn, feedTestNoTenants)
		log.FlushFiles()
		entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
			regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 {
			t.Fatalf("Found violation of CDC's guarantees: %v", entries)
		}
	})
}
