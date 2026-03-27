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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

	// Poll for the runtime_stats log entry rather than sleeping a fixed
	// duration. Under CI load the sample timer can jitter significantly,
	// and with external process tenants the server startup delay can push
	// testStartTs past the first few timer firings.
	testutils.SucceedsWithin(t, func() error {
		log.FlushAllSync()

		entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
			math.MaxInt64, 10000, cmLogRe, log.WithMarkedSensitiveData)
		if err != nil {
			return err
		}

		for _, e := range entries {
			if !strings.Contains(e.Message, "runtime_stats") {
				continue
			}
			// TODO(knz): Remove this when crdb-v2 becomes the new format.
			e.Message = strings.TrimPrefix(e.Message, "Structured entry:")
			// crdb-v2 starts json with an equal sign.
			e.Message = strings.TrimPrefix(e.Message, "=")
			jsonPayload := []byte(e.Message)
			var ev eventpb.RuntimeStats
			if err := json.Unmarshal(jsonPayload, &ev); err != nil {
				return errors.Wrapf(err, "unmarshalling %q", e.Message)
			}
			return nil
		}
		return errors.New("structured entry for runtime_stats not found in log")
	}, 10*time.Second)
}
