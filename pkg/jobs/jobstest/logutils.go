// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobstest

import (
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// CheckEmittedEvents is a helper method used by IMPORT and RESTORE tests to
// ensure events are emitted deterministically.
func CheckEmittedEvents(
	t *testing.T,
	expectedStatus []string,
	startTime int64,
	jobID int64,
	expectedMessage, expectedJobType string,
) {
	// Check that the structured event was logged.
	testutils.SucceedsSoon(t, func() error {
		log.Flush()
		entries, err := log.FetchEntriesFromFiles(startTime,
			math.MaxInt64, 10000, cmLogRe, log.WithMarkedSensitiveData)
		if err != nil {
			t.Fatal(err)
		}
		foundEntry := false
		var matchingEntryIndex int
		for _, e := range entries {
			if !strings.Contains(e.Message, expectedMessage) {
				continue
			}
			foundEntry = true
			// TODO(knz): Remove this when crdb-v2 becomes the new format.
			e.Message = strings.TrimPrefix(e.Message, "Structured entry:")
			// crdb-v2 starts json with an equal sign.
			e.Message = strings.TrimPrefix(e.Message, "=")
			jsonPayload := []byte(e.Message)
			var ev eventpb.CommonJobEventDetails
			if err := json.Unmarshal(jsonPayload, &ev); err != nil {
				t.Errorf("unmarshalling %q: %v", e.Message, err)
			}
			require.Equal(t, expectedJobType, ev.JobType)
			require.Equal(t, jobID, ev.JobID)
			if matchingEntryIndex >= len(expectedStatus) {
				return errors.New("more events fround in log than expected")
			}
			require.Equal(t, expectedStatus[matchingEntryIndex], ev.Status)
			matchingEntryIndex++
		}
		if !foundEntry {
			return errors.New("structured entry for import not found in log")
		}
		return nil
	})
}

var cmLogRe = regexp.MustCompile(`event_log\.go`)
