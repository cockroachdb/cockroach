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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestChangefeedNemeses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(i time.Duration) { jobs.DefaultAdoptInterval = i }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		// TODO(aayush,dan): Ugly hack to disable `eventPause` in sinkless
		// feeds. See comment in `RunNemesis` for details.
		isSinkless := strings.Contains(t.Name(), "sinkless")
		v, err := cdctest.RunNemesis(f, db, isSinkless)
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
}
