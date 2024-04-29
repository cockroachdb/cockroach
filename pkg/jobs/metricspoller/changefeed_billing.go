package metricspoller

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var changefeedBillingBytes = metric.NewGauge(metric.Metadata{
	Name:        "cdc.changefeed_table_bytes",
	Help:        "Aggregated number of bytes of data per table per changefeed",
	Measurement: "Storage",
	Unit:        metric.Unit_BYTES,
})

// updateChangefeedBillingMetrics emits the changefeed billing metric -- a sum of watched bytes.
func updateChangefeedBillingMetrics(ctx context.Context, execCtx sql.JobExecContext) error {
	metricsRegistry := execCtx.ExecCfg().MetricsRecorder.AppRegistry()
	metricsRegistry.AddMetric(changefeedBillingBytes)

	bytes, err := fetchChangefeedBillingBytes(ctx, execCtx)
	if err != nil {
		return err
	}

	changefeedBillingBytes.Update(bytes)
	return nil
}

func fetchChangefeedBillingBytes(ctx context.Context, execCtx sql.JobExecContext) (int64, error) {
	var deets []jobspb.ChangefeedDetails // TODO: get this somehow

	feedsTableIds := make(map[int][]descpb.ID, len(deets))
	tableSizes := make(map[descpb.ID]int64, len(deets))
	for cdi, cd := range deets {
		if len(cd.TargetSpecifications) > 0 {
			for _, ts := range cd.TargetSpecifications {
				if ts.TableID > 0 {
					feedsTableIds[cdi] = append(feedsTableIds[cdi], ts.TableID)
					tableSizes[ts.TableID] = 0
				}
			}
		} else {
			for id := range cd.Tables {
				feedsTableIds[cdi] = append(feedsTableIds[cdi], id)
				tableSizes[id] = 0
			}
		}
	}

	type spanInfo struct {
		span  roachpb.Span
		table descpb.ID
	}

	spanSizes := make(map[string]spanInfo, len(deets))
	spans := make([]roachpb.Span, 0, len(deets))
	// fetch & fill in table descriptors
	for id := range tableSizes {
		// fetch table descriptor
		var desc catalog.TableDescriptor
		fetchTableDesc := func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			tableDesc, err := descriptors.ByID(txn.KV()).WithoutNonPublic().Get().Table(ctx, id)
			if err != nil {
				return err
			}
			desc = tableDesc
			return nil
		}
		if err := sql.DescsTxn(ctx, execCtx.ExecCfg(), fetchTableDesc); err != nil {
			if errors.Is(err, catalog.ErrDescriptorDropped) {
				// if the table was dropped, we can ignore it this cycle
				continue
			}
			return 0, err
		}

		// TODO: do we need to count the sizes of other indexes?
		span := desc.PrimaryIndexSpan(execCtx.ExecCfg().Codec)
		spans = append(spans, span)
		spanSizes[span.Key.String()] = spanInfo{span: span, table: id}
	}

	// fetch span stats and fill in table sizes
	resp, err := execCtx.ExecCfg().TenantStatusServer.SpanStats(
		ctx,
		&roachpb.SpanStatsRequest{
			NodeID: "0", // fan out
			Spans:  spans,
		},
	)
	if err != nil {
		return 0, err
	}
	for spanStr, stats := range resp.SpanToStats {
		si := spanSizes[spanStr]
		tableSizes[si.table] += stats.ApproximateTotalStats.LiveBytes
	}

	var total int64
	for _, tableIds := range feedsTableIds {
		for _, id := range tableIds {
			total += tableSizes[id]
		}
	}
	return total, nil
}
