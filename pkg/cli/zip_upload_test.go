// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type uploadProfileReq struct {
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Duration  int64  `json:"duration"`
}

func setupZipDirWithProfiles(t *testing.T, inputs map[int][]uploadProfileReq) (string, func()) {
	t.Helper()

	// make sure that the debug directory name is unique. Or the tests will be flaky.
	debugDir := path.Join(os.TempDir(), fmt.Sprintf("debug-%s/", uuid.MakeV4().String()))

	for nodeID, nodeInputs := range inputs {
		// create a subdirectory for each node
		profDir := path.Join(debugDir, fmt.Sprintf("nodes/%d/", nodeID))
		require.NoError(t, os.MkdirAll(profDir, 0755))

		for _, i := range nodeInputs {
			p := &profile.Profile{
				TimeNanos:     time.Unix(i.Timestamp, 0).UnixNano(),
				DurationNanos: i.Duration,
				SampleType: []*profile.ValueType{
					{Type: i.Type},
				},
			}

			file, err := os.Create(
				path.Join(profDir, fmt.Sprintf("%s.pprof", i.Type)),
			)
			require.NoError(t, err)
			require.NoError(t, p.Write(file))
		}
	}

	return debugDir, func() {
		require.NoError(t, os.RemoveAll(debugDir))
	}
}

func TestUploadZipProfiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer testutils.TestingHook(&newUploadID, func() string {
		return "123"
	})()

	defer testutils.TestingHook(&doUploadProfileReq,
		func(req *http.Request) (*http.Response, error) {
			defer req.Body.Close()

			_, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
			reader := multipart.NewReader(req.Body, params["boundary"])

			// find the "event" part in the multipart request and copy it to the final output
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					break
				}

				if part.FormName() == "event" {
					var event profileUploadEvent
					require.NoError(t, json.NewDecoder(part).Decode(&event))

					if strings.Contains(event.Tags, "ERR") {
						// this is a test to simulate a client error
						return &http.Response{
							StatusCode: 400,
							Body:       io.NopCloser(strings.NewReader("'runtime' is a required field")),
						}, nil
					}

					// validate the timestamps outside the data-driven test framework
					// to keep the test deterministic.
					start, err := time.Parse(time.RFC3339Nano, event.Start)
					require.NoError(t, err)

					end, err := time.Parse(time.RFC3339Nano, event.End)
					require.NoError(t, err)

					require.Equal(t, time.Second*5, end.Sub(start))
					event.Start = ""
					event.End = ""

					// require.NoError(t, json.NewEncoder(&finaloutput).Encode(event))
					rawEvent, err := json.Marshal(event)
					require.NoError(t, err)

					// print the event so that it gets captured as a part of RunWithCapture
					fmt.Println(string(rawEvent))
				}
			}

			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader("200 OK")),
			}, nil
		},
	)()

	datadriven.RunTest(t, "testdata/upload/profiles", func(t *testing.T, d *datadriven.TestData) string {
		c := NewCLITest(TestCLIParams{})
		defer c.Cleanup()

		var finaloutput bytes.Buffer

		var testInput map[int][]uploadProfileReq
		require.NoError(t, json.Unmarshal([]byte(d.Input), &testInput))

		var tags string
		if d.HasArg("tags") {
			d.ScanArgs(t, "tags", &tags)
			tags = fmt.Sprintf("--tags=%s", tags)
		} else {
			debugZipUploadOpts.tags = nil
		}

		clusterNameArg := "--cluster=ABC"
		if d.HasArg("skip-cluster-name") {
			debugZipUploadOpts.clusterName = ""
			clusterNameArg = ""
		}

		debugDir, cleanup := setupZipDirWithProfiles(t, testInput)
		defer cleanup()

		stdout, err := c.RunWithCapture(
			fmt.Sprintf("debug zip upload %s --dd-api-key=dd-api-key %s %s", debugDir, tags, clusterNameArg),
		)
		require.NoError(t, err)

		// also write the STDOUT output to the finaloutput buffer. So, both the
		// API request made to Datadog and the STDOUT output are validated.
		_, err = finaloutput.WriteString(stdout)
		require.NoError(t, err)

		// sort the lines to avoid flakiness in the test
		lines := strings.Split(finaloutput.String(), "\n")
		sort.Strings(lines)

		// replace the debugDir with a constant string to avoid flakiness in the test
		return strings.ReplaceAll(strings.TrimSpace(strings.Join(lines, "\n")), debugDir, "debugDir")
	})
}

func TestAppendUserTags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tt := []struct {
		name       string
		systemTags []string
		userTags   []string
		expected   []string
	}{
		{
			name:       "no user tags",
			systemTags: []string{"tag1:val1", "tag2:val2"},
			expected:   []string{"tag1:val1", "tag2:val2"},
		},
		{
			name:     "no system tags",
			userTags: []string{"tag1:val1", "tag2:val2"},
			expected: []string{"tag1:val1", "tag2:val2"},
		},
		{
			name:       "different sets of tags",
			systemTags: []string{"A:1", "B:2"},
			userTags:   []string{"C:3", "D:4"},
			expected:   []string{"A:1", "B:2", "C:3", "D:4"},
		},
		{
			name:       "colliding tags",
			systemTags: []string{"A:1", "B:2"},
			userTags:   []string{"C:3", "A:4"},
			expected:   []string{"A:4", "B:2", "C:3"},
		},
		{
			name:       "non-key-value tags given by the user",
			systemTags: []string{"A:1", "B:2"},
			userTags:   []string{"C:3", "ABC"},
			expected:   []string{"A:1", "ABC", "B:2", "C:3"},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, appendUserTags(tc.systemTags, tc.userTags...))
		})
	}
}

func TestZipUploadArtifactTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, artifactType := range zipArtifactTypes {
		_, ok := uploadZipArtifactFuncs[artifactType]
		assert.True(
			t, ok, "Missing upload function for '%s'. Either add an upload function for this artifact type or remove it from the list.",
			artifactType,
		)
	}
}
