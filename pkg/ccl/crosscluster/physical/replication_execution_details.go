// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

type frontierExecutionDetails struct {
	srcInstanceID  base.SQLInstanceID
	destInstanceID base.SQLInstanceID
	span           string
	frontierTS     hlc.Timestamp
	behindBy       redact.SafeString
}

// constructSpanFrontierExecutionDetails constructs the frontierExecutionDetails
// using the initial partition specs that map spans to the src and dest
// instances, and a snapshot of the current state of the frontier.
//
// The shape of the spans tracked by the frontier can be different from the
// initial partitioned set of spans. To account for this, for each span in the
// initial partition set we want to output all the intersecting sub-spans in the
// frontier along with their timestamps.
//
// TODO (msbutler): consider pushing frontier construction to the caller, so we
// don't have two functions with long names.
func constructSpanFrontierExecutionDetails(
	partitionSpecs execinfrapb.StreamIngestionPartitionSpecs,
	frontierSpans execinfrapb.FrontierEntries,
) ([]frontierExecutionDetails, error) {
	f, err := span.MakeFrontier()
	if err != nil {
		return nil, err
	}
	for _, rs := range frontierSpans.ResolvedSpans {
		if err := f.AddSpansAt(rs.Timestamp, rs.Span); err != nil {
			return nil, err
		}
	}
	return constructSpanFrontierExecutionDetailsWithFrontier(partitionSpecs, f), nil
}

func constructSpanFrontierExecutionDetailsWithFrontier(
	partitionSpecs execinfrapb.StreamIngestionPartitionSpecs, f span.Frontier,
) []frontierExecutionDetails {
	now := timeutil.Now()
	res := make([]frontierExecutionDetails, 0)
	for _, spec := range partitionSpecs.Specs {
		for _, sp := range spec.Spans {
			f.SpanEntries(sp, func(r roachpb.Span, timestamp hlc.Timestamp) (done span.OpResult) {
				res = append(res, frontierExecutionDetails{
					srcInstanceID:  spec.SrcInstanceID,
					destInstanceID: spec.DestInstanceID,
					span:           r.String(),
					frontierTS:     timestamp,
					behindBy:       humanizeutil.Duration(now.Sub(timestamp.GoTime())),
				})
				return span.ContinueMatch
			})
		}

		// Sort res on the basis of srcInstanceID, destInstanceID.
		sort.Slice(res, func(i, j int) bool {
			if res[i].srcInstanceID != res[j].srcInstanceID {
				return res[i].srcInstanceID < res[j].srcInstanceID
			}
			if res[i].destInstanceID != res[j].destInstanceID {
				return res[i].destInstanceID < res[j].destInstanceID
			}
			return res[i].span < res[j].span
		})
	}

	return res
}

// generateSpanFrontierExecutionDetailFile generates and writes a file to the
// job_info table that captures the mapping from:
//
// # Src Instance	| Dest Instance	| Span | Frontier Timestamp	| Behind By
//
// This information is computed from information persisted by the
// stream ingestion resumer and frontier processor. Namely:
//
// - The StreamIngestionPartitionSpec of each partition providing a mapping from
// span to src and dest SQLInstanceID.
// - The snapshot of the frontier tracking how far each span has been replicated
// up to.
func generateSpanFrontierExecutionDetailFile(
	ctx context.Context, execCfg *sql.ExecutorConfig, ingestionJobID jobspb.JobID, skipBehindBy bool,
) error {
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var sb bytes.Buffer
		w := tabwriter.NewWriter(&sb, 0, 0, 1, ' ', tabwriter.TabIndent)

		// Read the StreamIngestionPartitionSpecs to get a mapping from spans to
		// their source and destination SQL instance IDs.
		specs, err := jobs.ReadChunkedFileToJobInfo(ctx, replicationPartitionInfoFilename, txn, ingestionJobID)
		if err != nil {
			return err
		}

		var partitionSpecs execinfrapb.StreamIngestionPartitionSpecs
		if err := protoutil.Unmarshal(specs, &partitionSpecs); err != nil {
			return err
		}

		// Now, read the latest snapshot of the frontier that tells us what
		// timestamp each span has been replicated up to.
		frontierEntries, err := jobs.ReadChunkedFileToJobInfo(ctx, frontierEntriesFilename, txn, ingestionJobID)
		if err != nil {
			return err
		}

		var frontierSpans execinfrapb.FrontierEntries
		if err := protoutil.Unmarshal(frontierEntries, &frontierSpans); err != nil {
			return err
		}
		executionDetails, err := constructSpanFrontierExecutionDetails(partitionSpecs, frontierSpans)
		if err != nil {
			return err
		}

		header := "Src Instance\tDest Instance\tSpan\tFrontier Timestamp\tBehind By"
		if skipBehindBy {
			header = "Src Instance\tDest Instance\tSpan\tFrontier Timestamp"
		}
		fmt.Fprintln(w, header)
		for _, ed := range executionDetails {
			if skipBehindBy {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
					ed.srcInstanceID, ed.destInstanceID, ed.span, ed.frontierTS.GoTime())
			} else {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
					ed.srcInstanceID, ed.destInstanceID, ed.span, ed.frontierTS.GoTime(), ed.behindBy)
			}
		}

		filename := fmt.Sprintf("replication-frontier.%s.txt", timeutil.Now().Format("20060102_150405.00"))
		if err := w.Flush(); err != nil {
			return err
		}
		return jobs.WriteExecutionDetailFile(ctx, filename, sb.Bytes(), txn, ingestionJobID)
	})
}

func repackagePartitionSpecs(
	streamIngestionSpecs map[base.SQLInstanceID][]execinfrapb.StreamIngestionDataSpec,
) execinfrapb.StreamIngestionPartitionSpecs {
	specs := make([]*execinfrapb.StreamIngestionPartitionSpec, 0)
	partitionSpecs := execinfrapb.StreamIngestionPartitionSpecs{Specs: specs}
	for _, nodeProcs := range streamIngestionSpecs {
		for _, proc := range nodeProcs {
			for _, partitionSpec := range proc.PartitionSpecs {
				partitionSpecs.Specs = append(partitionSpecs.Specs, &partitionSpec)
			}
		}
	}
	return partitionSpecs
}

// persistStreamIngestionPartitionSpecs persists all
// StreamIngestionPartitionSpecs in a serialized form to the job_info table.
// This information is used when the Resumer is requested to construct a
// replication-frontier.txt file.
func persistStreamIngestionPartitionSpecs(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	ingestionJobID jobspb.JobID,
	streamIngestionSpecs map[base.SQLInstanceID][]execinfrapb.StreamIngestionDataSpec,
) error {
	partitionSpecs := repackagePartitionSpecs(streamIngestionSpecs)
	specBytes, err := protoutil.Marshal(&partitionSpecs)
	if err != nil {
		return err
	}
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return jobs.WriteChunkedFileToJobInfo(ctx, replicationPartitionInfoFilename, specBytes, txn, ingestionJobID)
	}); err != nil {
		return err
	}
	if knobs := execCfg.StreamingTestingKnobs; knobs != nil && knobs.AfterPersistingPartitionSpecs != nil {
		knobs.AfterPersistingPartitionSpecs()
	}
	return nil
}
