// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// sqlCrudWriter is a batch writer that implements the BatchHandler interface
// using simple update, delete, and insert statements that assert the expected
// previous value of the row.
//
// The sqlCrudWriter decodes the events, but it relies on per-table handlers to
// process the batches.
type sqlCrudWriter struct {
	decoder  *eventDecoder
	handlers map[descpb.ID]*tableHandler
	settings *cluster.Settings
	discard  jobspb.LogicalReplicationDetails_Discard
}

var _ BatchHandler = &sqlCrudWriter{}

func newCrudSqlWriter(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	evalCtx *eval.Context,
	sd *sessiondata.SessionData,
	discard jobspb.LogicalReplicationDetails_Discard,
	procConfigByDestID map[descpb.ID]sqlProcessorTableConfig,
) (*sqlCrudWriter, error) {
	decoder, err := newEventDecoder(ctx, cfg.DB, evalCtx.Settings, procConfigByDestID)
	if err != nil {
		return nil, err
	}

	handlers := make(map[descpb.ID]*tableHandler)
	for dstDescID := range procConfigByDestID {
		handler, err := newTableHandler(
			dstDescID,
			cfg.DB,
			cfg.Codec,
			sd,
			cfg.LeaseManager.(*lease.Manager),
			evalCtx.Settings,
		)
		if err != nil {
			return nil, err
		}
		handlers[dstDescID] = handler
	}

	return &sqlCrudWriter{
		decoder:  decoder,
		handlers: handlers,
		settings: evalCtx.Settings,
		discard:  discard,
	}, nil
}

func (c *sqlCrudWriter) HandleBatch(
	ctx context.Context, batch []streampb.StreamEvent_KV,
) (b batchStats, err error) {
	ctx, sp := tracing.ChildSpan(ctx, "crudBatcher.HandleBatch")
	defer sp.Finish()
	defer func() {
		if r := recover(); r != nil {
			err = errors.AssertionFailedf("panic in crudBatcher.HandleBatch: %v", r)
		}
	}()

	sortedEvents, err := c.decodeEvents(ctx, batch)
	if err != nil {
		return batchStats{}, err
	}

	var combinedStats batchStats
	for _, events := range eventsByTable(sortedEvents) {
		handler := c.handlers[events[0].dstDescID]
		_, err := handler.handleDecodedBatch(ctx, events)
		if err != nil {
			return batchStats{}, err
		}
		// combinedStats.Add(stats)
	}

	return combinedStats, nil
}

func (c *sqlCrudWriter) decodeEvents(
	ctx context.Context, batch []streampb.StreamEvent_KV,
) ([]decodedEvent, error) {
	events := make([]decodedEvent, 0, len(batch))
	for _, event := range batch {
		if c.discard == jobspb.LogicalReplicationDetails_DiscardAllDeletes && len(event.KeyValue.Value.RawBytes) == 0 {
			continue
		}
		decoded, err := c.decoder.decodeEvent(ctx, event)
		if err != nil {
			return nil, err
		}
		events = append(events, decoded)
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].dstDescID < events[j].dstDescID
	})
	return events, nil
}

// eventsByTable is an iterator that groups events by their destination
// descriptor ID. For optimal batching  input events should be sorted by
// destination descriptor ID because the iterator groups runs of events with
// the same destination.
func eventsByTable(events []decodedEvent) func(yield func(descpb.ID, []decodedEvent) bool) {
	return func(yield func(descpb.ID, []decodedEvent) bool) {
		if len(events) == 0 {
			return
		}

		start := 0
		for i, event := range events {
			if 1 <= i && events[i-1].dstDescID != event.dstDescID {
				if !yield(events[start].dstDescID, events[start:i]) {
					return
				}
				start = i
			}
		}

		_ = yield(events[start].dstDescID, events[start:])
	}
}

// Close implements BatchHandler.
func (c *sqlCrudWriter) Close(ctx context.Context) {
}

// GetLastRow implements BatchHandler.
func (c *sqlCrudWriter) GetLastRow() cdcevent.Row {
	return c.decoder.lastRow
}

// ReleaseLeases implements BatchHandler.
func (c *sqlCrudWriter) ReleaseLeases(ctx context.Context) {
	for _, handler := range c.handlers {
		handler.ReleaseLeases(ctx)
	}
}

// ReportMutations implements BatchHandler.
func (c *sqlCrudWriter) ReportMutations(*stats.Refresher) {
}

// SetSyntheticFailurePercent implements BatchHandler.
func (c *sqlCrudWriter) SetSyntheticFailurePercent(uint32) {

}

// BatchSize implements BatchHandler.
func (c *sqlCrudWriter) BatchSize() int {
	return int(flushBatchSize.Get(&c.settings.SV))
}
