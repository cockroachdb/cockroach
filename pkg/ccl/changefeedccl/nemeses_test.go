// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	gosql "database/sql"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestChangefeedNemeses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(i time.Duration) { jobs.DefaultAdoptInterval = i }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond
	scope := log.Scope(t)
	defer scope.Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		// Disable eventPause for sinkless changefeeds because we currently do not
		// have "correct" pause and unpause mechanisms for changefeeds that aren't
		// based on the jobs infrastructure. Enabling it for sinkless might require
		// using "AS OF SYSTEM TIME" for sinkless changefeeds. See #41006 for more
		// details.
		omitPause := strings.Contains(t.Name(), "sinkless")
		// TODO(dan): Figure out why this doesn't work for poller. I suspect one of
		// the later cherry-picks will fix this, so waiting until we have those to
		// investigate.
		omitPause = omitPause || strings.Contains(t.Name(), "poller")
		// The cloudstorage sink has known changefeed invariant violations around
		// restarts. This cannot be fixed without changing the format of filenames
		// it uses, which was deemed too invasive to backport. Users of the
		// cloudstorage should upgrade to 19.2+.
		omitPause = omitPause || strings.Contains(t.Name(), "cloudstorage")
		v, err := cdctest.RunNemesis(f, db, omitPause)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		for _, failure := range v.Failures() {
			t.Error(failure)
		}
	}
	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1, regexp.MustCompile("cdc ux violation"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}
