// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

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
		for i, e := range entries {
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
			if i >= len(expectedStatus) {
				return errors.New("more events fround in log than expected")
			}
			require.Equal(t, expectedStatus[i], ev.Status)
		}
		if !foundEntry {
			return errors.New("structured entry for import not found in log")
		}
		return nil
	})
}

var cmLogRe = regexp.MustCompile(`event_log\.go`)
