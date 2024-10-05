// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package autoconfig_test

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/autoconfig"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecodeMarkers tests that the result of the encode
// functions can be processed by the decode functions.
func TestEncodeDecodeMarkers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, testEnv := range []autoconfig.EnvironmentID{"", "foo", "bar"} {
		for _, testTask := range []autoconfig.TaskID{0, 1, 10, math.MaxUint64} {
			t.Run(fmt.Sprintf("%s/%d", testEnv, testTask), func(t *testing.T) {
				taskRef := autoconfig.InfoKeyTaskRef{Environment: testEnv, Task: testTask}

				encodedStart := taskRef.EncodeStartMarkerKey()
				var tr autoconfig.InfoKeyTaskRef
				require.NoError(t, tr.DecodeStartMarkerKey(encodedStart))
				require.Equal(t, taskRef, tr)

				require.Error(t, tr.DecodeCompletionMarkerKey(encodedStart))

				encodedComplete := taskRef.EncodeCompletionMarkerKey()
				var tr2 autoconfig.InfoKeyTaskRef
				require.NoError(t, tr2.DecodeCompletionMarkerKey(encodedComplete))
				require.Equal(t, taskRef, tr2)

				require.Error(t, tr2.DecodeStartMarkerKey(encodedComplete))
			})
		}
	}
}

// TestPrefixes ensures that the prefix keys used for job info searches
// are properly encoded.
func TestPrefixes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, testEnv := range []autoconfig.EnvironmentID{"", "foo", "bar"} {
		t.Run(string(testEnv), func(t *testing.T) {
			tref := autoconfig.InfoKeyTaskRef{Environment: testEnv}

			prefix := autoconfig.InfoKeyStartPrefix(testEnv)
			marker := tref.EncodeStartMarkerKey()
			require.True(t, strings.HasPrefix(marker, prefix), "%q vs %q", marker, prefix)

			prefix = autoconfig.InfoKeyCompletionPrefix(testEnv)
			marker = tref.EncodeCompletionMarkerKey()
			require.True(t, strings.HasPrefix(marker, prefix), "%q vs %q", marker, prefix)
		})
	}
}

// TestMarkerOrdering tests that the ordering of the encoded markers
// is the same as the ordering of the task IDs.
func TestMarkerOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var taskRefs []autoconfig.InfoKeyTaskRef
	for _, testEnv := range []autoconfig.EnvironmentID{"", "bar", "foo"} {
		for i := 0; i < 10; i++ {
			taskRefs = append(taskRefs, autoconfig.InfoKeyTaskRef{Environment: testEnv, Task: autoconfig.TaskID(i)})
		}
	}

	// Copy taskRefs to taskRefsRandom.
	taskRefsRandom := make([]autoconfig.InfoKeyTaskRef, len(taskRefs))
	copy(taskRefsRandom, taskRefs)

	// Randomize the ordering of taskRefsRandom.
	r, _ := randutil.NewTestRand()
	for i := range taskRefsRandom {
		j := i + int(r.Int31n(int32(len(taskRefsRandom)-i)))
		taskRefsRandom[i], taskRefsRandom[j] = taskRefsRandom[j], taskRefsRandom[i]
	}

	// Get start marker encodings for all taskRefs.
	var startMarkers []string
	for _, taskRef := range taskRefsRandom {
		startMarkers = append(startMarkers, taskRef.EncodeStartMarkerKey())
	}
	// Sort the start markers. This should be the same as sorting the taskRefs.
	sort.Strings(startMarkers)

	// Decode the start markers.
	var decodedStartMarkers []autoconfig.InfoKeyTaskRef
	for _, startMarker := range startMarkers {
		var taskRef autoconfig.InfoKeyTaskRef
		require.NoError(t, taskRef.DecodeStartMarkerKey(startMarker))
		decodedStartMarkers = append(decodedStartMarkers, taskRef)
	}

	// Check that the decoded start markers are the same as the original taskRefs.
	require.Equal(t, taskRefs, decodedStartMarkers)

	// Get completion marker encodings for all taskRefs.
	var completionMarkers []string
	for _, taskRef := range taskRefsRandom {
		completionMarkers = append(completionMarkers, taskRef.EncodeCompletionMarkerKey())
	}
	// Sort the completion markers. This should be the same as sorting the taskRefs.
	sort.Strings(completionMarkers)

	// Decode the completion markers.
	var decodedCompletionMarkers []autoconfig.InfoKeyTaskRef
	for _, completionMarker := range completionMarkers {
		var taskRef autoconfig.InfoKeyTaskRef
		require.NoError(t, taskRef.DecodeCompletionMarkerKey(completionMarker))
		decodedCompletionMarkers = append(decodedCompletionMarkers, taskRef)
	}
	// Check that the decoded completion markers are the same as the original taskRefs.
	require.Equal(t, taskRefs, decodedCompletionMarkers)
}
