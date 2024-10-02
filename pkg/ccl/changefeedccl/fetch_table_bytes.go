// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/errors"
)

// FetchChangefeedUsageBytes fetches the total number of bytes of data watched
// by the given changefeed.
func FetchChangefeedUsageBytes(
	ctx context.Context, execCfg *sql.ExecutorConfig, payload jobspb.Payload,
) (int64, error) {
	details, err := detailsFromPayload(payload)
	if err != nil {
		return 0, err
	}

	uniqueTableIDs := make(map[descpb.ID]struct{})
	// Check both possible locations for table data due to older proto version.
	// Inspired by AllTargets in changefeedccl/changefeed.go.
	if len(details.TargetSpecifications) > 0 {
		for _, ts := range details.TargetSpecifications {
			if ts.TableID > 0 {
				uniqueTableIDs[ts.TableID] = struct{}{}
			}
		}
	} else {
		for id := range details.Tables {
			uniqueTableIDs[id] = struct{}{}
		}
	}

	tableIDs := make([]descpb.ID, 0, len(uniqueTableIDs))
	for id := range uniqueTableIDs {
		tableIDs = append(tableIDs, id)
	}

	total, err := fetchSizes(ctx, execCfg, tableIDs)
	if err != nil {
		return 0, err
	}
	return total, nil
}

func fetchSizes(
	ctx context.Context, execCfg *sql.ExecutorConfig, tableIDs []descpb.ID,
) (total int64, err error) {
	// Build list of spans for all tables.
	allSpans := make([]roachpb.Span, 0, len(tableIDs))
	for _, id := range tableIDs {
		desc, err := getTableDesc(ctx, execCfg, id)
		if err != nil {
			return 0, err
		}

		// Include only the primary index.
		allSpans = append(allSpans, desc.PrimaryIndexSpan(execCfg.Codec))
	}

	// Fetch span stats and fill in table sizes.
	// NodeID=0 means "fan out to all nodes".
	req := &roachpb.SpanStatsRequest{NodeID: "0", Spans: allSpans}
	resp, err := execCfg.TenantStatusServer.SpanStats(ctx, req)
	if err != nil {
		return 0, err
	}
	if len(resp.Errors) > 0 {
		return 0, errors.Newf("errors fetching span stats: %v", resp.Errors)
	}

	for _, stats := range resp.SpanToStats {
		total += stats.TotalStats.LiveBytes
	}

	return total, nil
}

func getTableDesc(
	ctx context.Context, execCfg *sql.ExecutorConfig, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	var desc catalog.TableDescriptor
	f := func(ctx context.Context, txn descs.Txn) error {
		tableDesc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return err
		}
		desc = tableDesc
		return nil
	}
	if err := execCfg.InternalDB.DescsTxn(ctx, f, isql.WithPriority(admissionpb.LowPri)); err != nil {
		return nil, err
	}
	return desc, nil
}

func detailsFromPayload(payload jobspb.Payload) (*jobspb.ChangefeedDetails, error) {
	details := payload.GetDetails()
	if details == nil {
		return nil, errors.AssertionFailedf("no details")
	}
	cfDetails, ok := details.(*jobspb.Payload_Changefeed)
	if !ok {
		return nil, errors.AssertionFailedf("unexpected details type %T", details)
	}
	return cfDetails.Changefeed, nil
}
