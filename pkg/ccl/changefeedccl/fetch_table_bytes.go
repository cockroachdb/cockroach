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

// FetchChangefeedBillingBytes fetches the total number of bytes of data watched
// by the given running or paused changefeed.
func FetchChangefeedBillingBytes(
	ctx context.Context, execCfg *sql.ExecutorConfig, payload jobspb.Payload,
) (int64, error) {
	details, err := detailsFromPayload(payload)
	if err != nil {
		return 0, err
	}

	tableIDs := make([]descpb.ID, 0, 1)
	// Check both possible locations for table data due to older proto version. TODO: is this correct?
	// Inspired by AllTargets in changefeedccl/changefeed.go.
	if len(details.TargetSpecifications) > 0 {
		for _, ts := range details.TargetSpecifications {
			if ts.TableID > 0 { // i think this is just a sanity check, but TODO: do we need to do it here? what about below?
				tableIDs = append(tableIDs, ts.TableID)
			}
		}
	} else {
		for id := range details.Tables {
			tableIDs = append(tableIDs, id)
		}
	}

	tableSizes, err := fetchTableSizes(ctx, execCfg, tableIDs)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, id := range tableIDs {
		total += tableSizes[id]
	}
	return total, nil
}

func fetchTableSizes(
	ctx context.Context, execCfg *sql.ExecutorConfig, tableIDs []descpb.ID,
) (map[descpb.ID]int64, error) {
	tableSizes := make(map[descpb.ID]int64, len(tableIDs))

	type spanInfo struct {
		span  roachpb.Span
		table descpb.ID
	}

	// Build list of spans for all tables.
	spanSizes := make(map[string]spanInfo, len(tableIDs))
	spans := make([]roachpb.Span, 0, len(tableIDs))
	for _, id := range tableIDs {
		desc, err := getTableDesc(ctx, execCfg, id)
		if err != nil {
			// we don't need to handle the case where the table was dropped, because if that's the case this feed will be shutting down soon anyway
			return nil, err
		}

		// Include all indexes, not just the primary.
		tableSpans := desc.AllIndexSpans(execCfg.Codec)
		spans = append(spans, tableSpans...)
		for _, span := range spans {
			spanSizes[span.String()] = spanInfo{span: span, table: id}
		}
	}

	// Fetch span stats and fill in table sizes.
	// NodeID=0 means "fan out to all nodes".
	req := &roachpb.SpanStatsRequest{NodeID: "0", Spans: spans}
	resp, err := execCfg.TenantStatusServer.SpanStats(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(resp.Errors) > 0 {
		return nil, errors.Newf("errors fetching span stats: %v", resp.Errors)
	}
	for spanStr, stats := range resp.SpanToStats {
		si := spanSizes[spanStr]
		tableSizes[si.table] += stats.ApproximateTotalStats.LiveBytes // TODO: confirm with ppl that this is the right metric
	}

	return tableSizes, nil
}

func getTableDesc(
	ctx context.Context, execCfg *sql.ExecutorConfig, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	var desc catalog.TableDescriptor
	f := func(ctx context.Context, txn descs.Txn) error {
		tableDesc, err := txn.Descriptors().ByID(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
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
