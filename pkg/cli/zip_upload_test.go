// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type zipUploadTestContents struct {
	Nodes map[int]struct {
		Profiles []uploadProfileReq `json:"profiles"`
		Logs     []uploadLogsReq    `json:"logs"`
	} `json:"nodes"`
}

type uploadLogsReq struct {
	Name  string   `json:"name"`
	Lines []string `json:"lines"`
}

type uploadProfileReq struct {
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Duration  int64  `json:"duration"`
}

func setupZipDir(t *testing.T, inputs zipUploadTestContents) (string, func()) {
	t.Helper()

	// make sure that the debug directory name is unique. Or the tests will be flaky.
	debugDir := path.Join(os.TempDir(), fmt.Sprintf("debug-%s/", uuid.MakeV4().String()))

	for nodeID, nodeInputs := range inputs.Nodes {
		// setup profiles
		profDir := path.Join(debugDir, fmt.Sprintf("nodes/%d/", nodeID))
		require.NoError(t, os.MkdirAll(profDir, 0755))

		for _, prof := range nodeInputs.Profiles {
			p := &profile.Profile{
				TimeNanos:     time.Unix(prof.Timestamp, 0).UnixNano(),
				DurationNanos: prof.Duration,
				SampleType: []*profile.ValueType{
					{Type: prof.Type},
				},
			}

			file, err := os.Create(
				path.Join(profDir, fmt.Sprintf("%s.pprof", prof.Type)),
			)
			require.NoError(t, err)
			require.NoError(t, p.Write(file))
			require.NoError(t, file.Close())
		}

		// setup logs
		logDir := path.Join(debugDir, fmt.Sprintf("nodes/%d/logs", nodeID))
		require.NoError(t, os.MkdirAll(logDir, 0755))

		for _, log := range nodeInputs.Logs {
			var logBuilder bytes.Buffer
			for _, line := range log.Lines {
				logBuilder.WriteString(line)
				logBuilder.WriteString("\n")
			}

			file, err := os.Create(
				path.Join(logDir, log.Name),
			)
			require.NoError(t, err)

			_, err = file.Write(logBuilder.Bytes())
			require.NoError(t, err)
			require.NoError(t, file.Close())
		}
	}

	return debugDir, func() {
		require.NoError(t, os.RemoveAll(debugDir))
	}
}

func TestUploadZipEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer testutils.TestingHook(&newUploadID, func(string) string {
		return "123"
	})()
	defer testutils.TestingHook(&newRandStr, func(l int, n bool) string {
		if n {
			return "123"
		}
		return "a1b2c3"
	})()

	origConc := debugZipUploadOpts.maxConcurrentUploads
	debugZipUploadOpts.maxConcurrentUploads = 2
	defer func() {
		debugZipUploadOpts.maxConcurrentUploads = origConc
	}()

	defer testutils.TestingHook(&doUploadReq,
		func(req *http.Request) ([]byte, error) {
			defer req.Body.Close()

			switch req.URL.Path {
			case "/v1/input":
				return uploadProfileHook(t, req)
			case "/api/v2/logs/config/archives":
				return setupDDArchiveHook(t, req)
			default:
				return nil, fmt.Errorf(
					"unexpected request is being made to datadog: %s", req.URL.Path,
				)
			}
		},
	)()

	defer testutils.TestingHook(&writeLogsToGCS, writeLogsToGCSHook)()

	datadriven.Walk(t, "testdata/upload", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			c := NewCLITest(TestCLIParams{})
			defer c.Cleanup()
			debugZipUploadOpts.include = nil

			var finaloutput bytes.Buffer

			var testInput zipUploadTestContents
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

			var includeFlag string // no include flag by default
			switch d.Cmd {
			case "upload-profiles":
				includeFlag = "--include=profiles"
			case "upload-logs":
				includeFlag = "--include=logs"
				if d.HasArg("log-format") {
					var logFormat string
					d.ScanArgs(t, "log-format", &logFormat)
					includeFlag = fmt.Sprintf("%s --log-format=%s", includeFlag, logFormat)
				}
			}

			debugDir, cleanup := setupZipDir(t, testInput)
			defer cleanup()

			stdout, err := c.RunWithCapture(fmt.Sprintf(
				"debug zip upload %s --dd-api-key=dd-api-key --dd-app-key=dd-app-key %s %s %s",
				debugDir, tags, clusterNameArg, includeFlag,
			))
			require.NoError(t, err)

			// also write the STDOUT output to the finaloutput buffer. So, both the
			// API request made to Datadog and the STDOUT output are validated.
			_, err = finaloutput.WriteString(stdout)
			require.NoError(t, err)

			lines := strings.Split(finaloutput.String(), "\n")
			sort.Strings(lines)

			// replace the debugDir with a constant string to avoid flakiness in the test
			return strings.ReplaceAll(strings.TrimSpace(strings.Join(lines, "\n")), debugDir, "debugDir")
		})
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

func uploadProfileHook(t *testing.T, req *http.Request) ([]byte, error) {
	t.Helper()

	_, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	reader := multipart.NewReader(req.Body, params["boundary"])

	// validate the headers
	require.Equal(t, "dd-api-key", req.Header.Get("DD-API-KEY"))

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
				return nil, fmt.Errorf("status: 400, body: 'runtime' is a required field")
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

	return []byte("200 OK"), nil
}

func setupDDArchiveHook(t *testing.T, req *http.Request) ([]byte, error) {
	t.Helper()

	// validate the headers
	require.Equal(t, "dd-api-key", req.Header.Get("DD-API-KEY"))
	require.Equal(t, "dd-app-key", req.Header.Get("DD-APPLICATION-KEY"))

	var body bytes.Buffer
	_, err := body.ReadFrom(req.Body)
	require.NoError(t, err)

	// print the request bidy so that it gets captured as a part of
	// RunWithCapture
	fmt.Println(body.String())
	return []byte("200 OK"), nil
}

func writeLogsToGCSHook(ctx context.Context, sig logUploadSig) error {
	out := strings.Builder{}
	out.WriteString(fmt.Sprintf("%s:\n", sig.key))
	out.WriteString(fmt.Sprintf("%s\n", string(sig.data)))

	// print the logs so that it gets captured as a part of RunWithCapture
	fmt.Println(out.String())
	return nil
}

func TestLogEntryToJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// set the maxConcurrentUploads to 1 to avoid flakiness in the test
	origConc := debugZipUploadOpts.maxConcurrentUploads
	debugZipUploadOpts.maxConcurrentUploads = 1
	defer func() {
		debugZipUploadOpts.maxConcurrentUploads = origConc
	}()

	raw, err := logEntryToJSON(logpb.Entry{
		Severity: logpb.Severity_INFO,
		Channel:  logpb.Channel_STORAGE,
		Time:     time.Date(2024, time.August, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
		Message:  "something happend",
	}, []string{})
	require.NoError(t, err)

	t.Log(string(raw))

	raw, err = logEntryToJSON(logpb.Entry{
		Severity: logpb.Severity_INFO,
		Channel:  logpb.Channel_STORAGE,
		Time:     time.Date(2024, time.August, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
		Message:  `{"foo": "bar"}`,
	}, []string{})
	require.NoError(t, err)

	t.Log(string(raw))
}

func TestHumanReadableSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tt := []struct {
		size     int
		expected string
	}{
		{size: 0, expected: "0 B"},
		{size: 1, expected: "1 B"},
		{size: 1024, expected: "1.0 KB"},
		{size: 1024 * 1024, expected: "1.0 MB"},
		{size: 1024 * 1024 * 1024, expected: "1.0 GB"},
		{size: 1024 * 1024 * 1024 * 1024, expected: "1.0 TB"},
		{size: 1024 * 1024 * 1024 * 1024 * 1024, expected: "1.0 PB"},
	}

	for _, tc := range tt {
		assert.Equal(t, tc.expected, humanReadableSize(tc.size))
	}
}
