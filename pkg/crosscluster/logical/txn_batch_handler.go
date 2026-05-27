// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnwriter"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
)

// txnBatchHandler adapts a txnwriter.TransactionWriter to the BatchHandler
// interface so the transactionWriter code path can be tested with
// testBatchHandlerExhaustive.
type txnBatchHandler struct {
	decoder  *ldrdecoder.CoalescingDecoder
	writer   txnwriter.TransactionWriter
	settings *cluster.Settings
	discard  jobspb.LogicalReplicationDetails_Discard
}

var _ BatchHandler = (*txnBatchHandler)(nil)

func (h *txnBatchHandler) HandleBatch(
	ctx context.Context, batch []streampb.StreamEvent_KV,
) (batchStats, error) {
	events, err := h.decoder.DecodeAndCoalesceEvents(ctx, batch, h.discard)
	if err != nil {
		return batchStats{}, err
	}

	// Wrap each decoded event as a single-row transaction.
	transactions := make([]ldrdecoder.Transaction, len(events))
	for i, event := range events {
		transactions[i] = ldrdecoder.Transaction{
			TxnID:    ldrdecoder.TxnID{Timestamp: event.RowTimestamp},
			WriteSet: []ldrdecoder.DecodedRow{event},
		}
	}

	results, err := h.writer.ApplyBatch(ctx, transactions)
	if err != nil {
		return batchStats{}, err
	}

	var s batchStats
	for _, r := range results {
		if r.DlqReason != nil {
			return batchStats{}, r.DlqReason
		}
		s.kvWriteTooOld += int64(r.LwwLoserRows)
	}
	return s, nil
}

func (h *txnBatchHandler) BatchSize() int {
	return int(flushBatchSize.Get(&h.settings.SV))
}

func (h *txnBatchHandler) GetLastRow() cdcevent.Row {
	return h.decoder.LastRow
}

func (h *txnBatchHandler) SetSyntheticFailurePercent(uint32) {}

func (h *txnBatchHandler) ReportMutations(context.Context, *stats.Refresher) {}

func (h *txnBatchHandler) ReleaseLeases(ctx context.Context) {
	h.writer.ReleaseLeases(ctx)
}

func (h *txnBatchHandler) Close(ctx context.Context) {
	h.writer.Close(ctx)
}

func newTxnBatchHandlerFromConfig(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	discard jobspb.LogicalReplicationDetails_Discard,
	configByTable map[descpb.ID]sqlProcessorTableConfig,
) (BatchHandler, error) {
	tableMappings := make([]ldrdecoder.TableMapping, 0, len(configByTable))
	for dstID, cfg := range configByTable {
		tableMappings = append(tableMappings, ldrdecoder.TableMapping{
			SourceDescriptor: cfg.srcDesc,
			DestID:           dstID,
		})
	}
	decoder, err := ldrdecoder.NewCoalescingDecoder(
		ctx, flowCtx.Cfg.DB, flowCtx.Cfg.Settings, tableMappings)
	if err != nil {
		return nil, err
	}

	writer, err := txnwriter.NewTransactionWriter(
		ctx,
		flowCtx.Cfg.DB.(isql.DB),
		flowCtx.Cfg.LeaseManager.(*lease.Manager),
		flowCtx.Codec(),
		flowCtx.Cfg.Settings,
	)
	if err != nil {
		return nil, err
	}

	return &txnBatchHandler{
		decoder:  decoder,
		writer:   writer,
		settings: flowCtx.Cfg.Settings,
		discard:  discard,
	}, nil
}
