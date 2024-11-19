// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status_test

import (
	"context"
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var cmLogRe = regexp.MustCompile(`event_log\.go`)

// TestStructuredEventLogging tests that the runtime stats structured event was logged.
func TestStructuredEventLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				EnvironmentSampleInterval: 500 * time.Millisecond,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	testStartTs := timeutil.Now()

	// Wait longer than EnvironmentSampleInterval duration.
	time.Sleep(time.Second)

	// Ensure that the entry hits the OS so it can be read back below.
	log.FlushAllSync()

	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 10000, cmLogRe, log.WithMarkedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}

	foundEntry := false
	for _, e := range entries {
		if !strings.Contains(e.Message, "runtime_stats") {
			continue
		}
		foundEntry = true
		// TODO(knz): Remove this when crdb-v2 becomes the new format.
		e.Message = strings.TrimPrefix(e.Message, "Structured entry:")
		// crdb-v2 starts json with an equal sign.
		e.Message = strings.TrimPrefix(e.Message, "=")
		jsonPayload := []byte(e.Message)
		var ev eventpb.RuntimeStats
		if err := json.Unmarshal(jsonPayload, &ev); err != nil {
			t.Errorf("unmarshalling %q: %v", e.Message, err)
		}
	}
	if !foundEntry {
		t.Error("structured entry for runtime_stats not found in log")
	}
}
