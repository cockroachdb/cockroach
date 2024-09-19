// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestFrontierExecutionDetailFile is a unit test for
// constructSpanFrontierExecutionDetails. Refer to the method header for
// details.
func TestConstructFrontierExecutionDetailFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clearTimestamps := func(executionDetails []frontierExecutionDetails) []frontierExecutionDetails {
		res := make([]frontierExecutionDetails, len(executionDetails))
		for i, ed := range executionDetails {
			ed.frontierTS = hlc.Timestamp{}
			ed.behindBy = ""
			res[i] = ed
		}
		return res
	}

	for _, tc := range []struct {
		name            string
		partitionSpecs  execinfrapb.StreamIngestionPartitionSpecs
		frontierEntries execinfrapb.FrontierEntries
		expected        []frontierExecutionDetails
	}{
		{
			name: "matching spans",
			partitionSpecs: execinfrapb.StreamIngestionPartitionSpecs{
				Specs: []*execinfrapb.StreamIngestionPartitionSpec{
					{
						SrcInstanceID:  1,
						DestInstanceID: 2,
						Spans: []roachpb.Span{
							{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
						},
					},
				}},
			frontierEntries: execinfrapb.FrontierEntries{ResolvedSpans: []jobspb.ResolvedSpan{
				{
					Span:      roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			}},
			expected: []frontierExecutionDetails{
				{
					srcInstanceID:  1,
					destInstanceID: 2,
					span:           "{a-b}",
				}},
		},
		{
			name: "multi-partition",
			partitionSpecs: execinfrapb.StreamIngestionPartitionSpecs{
				Specs: []*execinfrapb.StreamIngestionPartitionSpec{
					{
						SrcInstanceID:  1,
						DestInstanceID: 2,
						Spans: []roachpb.Span{
							{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
						},
					},
					{
						SrcInstanceID:  1,
						DestInstanceID: 3,
						Spans: []roachpb.Span{
							{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
						},
					},
				}},
			frontierEntries: execinfrapb.FrontierEntries{ResolvedSpans: []jobspb.ResolvedSpan{
				{
					Span:      roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("a'")},
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					Span:      roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					Timestamp: hlc.Timestamp{WallTime: 2},
				},
			}},
			expected: []frontierExecutionDetails{
				{
					srcInstanceID:  1,
					destInstanceID: 2,
					span:           "a{-'}",
				},
				{
					srcInstanceID:  1,
					destInstanceID: 3,
					span:           "{b-c}",
				},
			},
		},
		{
			name: "merged frontier",
			partitionSpecs: execinfrapb.StreamIngestionPartitionSpecs{
				Specs: []*execinfrapb.StreamIngestionPartitionSpec{
					{
						SrcInstanceID:  1,
						DestInstanceID: 2,
						Spans: []roachpb.Span{
							{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
						},
					},
					{
						SrcInstanceID:  1,
						DestInstanceID: 3,
						Spans: []roachpb.Span{
							{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")},
						},
					},
				}},
			frontierEntries: execinfrapb.FrontierEntries{ResolvedSpans: []jobspb.ResolvedSpan{
				{
					Span:      roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")},
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			}},
			expected: []frontierExecutionDetails{
				{
					srcInstanceID:  1,
					destInstanceID: 2,
					span:           "{a-b}",
				},
				{
					srcInstanceID:  1,
					destInstanceID: 3,
					span:           "{b-d}",
				}},
		},
		{
			name: "no matching spans",
			partitionSpecs: execinfrapb.StreamIngestionPartitionSpecs{
				Specs: []*execinfrapb.StreamIngestionPartitionSpec{
					{
						SrcInstanceID:  1,
						DestInstanceID: 2,
						Spans: []roachpb.Span{
							{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
						},
					},
				},
			},
			frontierEntries: execinfrapb.FrontierEntries{ResolvedSpans: []jobspb.ResolvedSpan{
				{
					Span:      roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			}},
			expected: []frontierExecutionDetails{},
		},
		{
			name: "split frontier",
			partitionSpecs: execinfrapb.StreamIngestionPartitionSpecs{
				Specs: []*execinfrapb.StreamIngestionPartitionSpec{
					{
						SrcInstanceID:  1,
						DestInstanceID: 2,
						Spans:          []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}}}}},
			frontierEntries: execinfrapb.FrontierEntries{ResolvedSpans: []jobspb.ResolvedSpan{
				{
					Span:      roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					Span:      roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					Timestamp: hlc.Timestamp{WallTime: 2},
				},
			}},
			expected: []frontierExecutionDetails{
				{
					srcInstanceID:  1,
					destInstanceID: 2,
					span:           "{a-b}",
				},
				{
					srcInstanceID:  1,
					destInstanceID: 2,
					span:           "{b-c}",
				}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executionDetails, err := constructSpanFrontierExecutionDetails(tc.partitionSpecs, tc.frontierEntries)
			require.NoError(t, err)
			executionDetails = clearTimestamps(executionDetails)
			require.Equal(t, tc.expected, executionDetails)
		})
	}
}

func listExecutionDetails(
	t *testing.T, s serverutils.ApplicationLayerInterface, jobID jobspb.JobID,
) []string {
	t.Helper()

	client, err := s.GetAdminHTTPClient()
	require.NoError(t, err)

	url := s.AdminURL().WithPath(fmt.Sprintf("/_status/list_job_profiler_execution_details/%d", jobID)).String()
	req, err := http.NewRequest("GET", url, nil)
	require.NoError(t, err)

	req.Header.Set("Content-Type", httputil.ProtoContentType)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	edResp := serverpb.ListJobProfilerExecutionDetailsResponse{}
	require.NoError(t, protoutil.Unmarshal(body, &edResp))
	sort.Slice(edResp.Files, func(i, j int) bool {
		return edResp.Files[i] < edResp.Files[j]
	})
	return edResp.Files
}

func checkExecutionDetails(
	t *testing.T, s serverutils.ApplicationLayerInterface, jobID jobspb.JobID, filename string,
) ([]byte, error) {
	t.Helper()

	client, err := s.GetAdminHTTPClient()
	if err != nil {
		return nil, err
	}

	url := s.AdminURL().WithPath(fmt.Sprintf("/_status/job_profiler_execution_details/%d?%s", jobID, filename)).String()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", httputil.ProtoContentType)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)

	edResp := serverpb.GetJobProfilerExecutionDetailResponse{}
	if err := protoutil.Unmarshal(body, &edResp); err != nil {
		return nil, err
	}

	r := bytes.NewReader(edResp.Data)
	data, err := io.ReadAll(r)
	if err != nil {
		return data, err
	}
	if len(data) == 0 {
		return data, errors.New("no data returned")
	}
	return data, nil
}

func TestEndToEndFrontierExecutionDetailFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// First, let's persist some partitions specs.
	streamIngestionsSpecs := map[base.SQLInstanceID][]execinfrapb.StreamIngestionDataSpec{
		1: {{
			PartitionSpecs: map[string]execinfrapb.StreamIngestionPartitionSpec{
				"1": {
					SrcInstanceID:  2,
					DestInstanceID: 1,
					Spans: []roachpb.Span{
						{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					},
				},
			},
		}},
		2: {{
			PartitionSpecs: map[string]execinfrapb.StreamIngestionPartitionSpec{
				"1": {
					SrcInstanceID:  1,
					DestInstanceID: 2,
					Spans: []roachpb.Span{
						{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					},
				},
			},
		}},
		3: {{
			PartitionSpecs: map[string]execinfrapb.StreamIngestionPartitionSpec{
				"1": {
					SrcInstanceID:  3,
					DestInstanceID: 3,
					Spans: []roachpb.Span{
						{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")},
					},
				},
			},
		}},
	}

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()
	execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)

	ingestionJobID := jobspb.JobID(123)
	require.NoError(t, persistStreamIngestionPartitionSpecs(ctx, &execCfg,
		ingestionJobID, streamIngestionsSpecs))

	// Now, let's persist some frontier entries.
	frontierEntries := execinfrapb.FrontierEntries{ResolvedSpans: []jobspb.ResolvedSpan{
		{
			Span:      roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
		{
			Span:      roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
			Timestamp: hlc.Timestamp{WallTime: 2},
		},
		{
			Span:      roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("d'")},
			Timestamp: hlc.Timestamp{WallTime: 2},
		},
		{
			Span:      roachpb.Span{Key: roachpb.Key("d'"), EndKey: roachpb.Key("e")},
			Timestamp: hlc.Timestamp{WallTime: 0},
		},
	}}

	frontierBytes, err := protoutil.Marshal(&frontierEntries)
	require.NoError(t, err)
	require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return jobs.WriteChunkedFileToJobInfo(ctx, frontierEntriesFilename, frontierBytes, txn, ingestionJobID)
	}))
	require.NoError(t, generateSpanFrontierExecutionDetailFile(ctx, &execCfg, ingestionJobID, true /* skipBehindBy */))
	files := listExecutionDetails(t, ts, ingestionJobID)
	require.Len(t, files, 1)
	data, err := checkExecutionDetails(t, ts, ingestionJobID, files[0])
	require.NoError(t, err)
	require.NotEmpty(t, data)

	expectedData, err := os.ReadFile(datapathutils.TestDataPath(t, "expected_replication_frontier.txt"))
	require.NoError(t, err)
	require.Equal(t, expectedData, data)
}
