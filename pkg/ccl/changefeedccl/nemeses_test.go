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
		// TODO(dan): Ugly hack to disable `eventPause` in sinkless feeds. See comment in
		// `RunNemesis` for details.
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
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}
