// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perftrace

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Flusher periodically flushes work spans from the collector to the system table.
type Flusher struct {
	collector *Collector
	db        isql.DB
	settings  *cluster.Settings
	stopper   *stop.Stopper
}

// NewFlusher creates a new Flusher.
func NewFlusher(
	collector *Collector, db isql.DB, settings *cluster.Settings, stopper *stop.Stopper,
) *Flusher {
	return &Flusher{
		collector: collector,
		db:        db,
		settings:  settings,
		stopper:   stopper,
	}
}

// Start begins the background flush loop.
func (f *Flusher) Start(ctx context.Context) {
	_ = f.stopper.RunAsyncTask(ctx, "work-span-flusher", func(ctx context.Context) {
		var timer timeutil.Timer
		defer timer.Stop()

		for {
			flushInterval := FlushInterval.Get(&f.settings.SV)
			timer.Reset(flushInterval)

			select {
			case <-timer.C:
				timer.Read = true
				if Enabled.Get(&f.settings.SV) {
					if err := f.flush(ctx); err != nil {
						log.Ops.Warningf(ctx, "work span flush failed: %v", err)
					}
				}
			case <-f.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// flush performs a single flush cycle:
// 1. Delete a fraction of existing spans for this node
// 2. Insert new spans from the reservoir
// 3. Clear the reservoir
func (f *Flusher) flush(ctx context.Context) error {
	deleteRatio := DeleteRatio.Get(&f.settings.SV)
	nodeID := f.collector.NodeID()

	// Step 1: Delete a fraction of existing spans for this node
	if deleteRatio > 0 {
		deleteQuery := `DELETE FROM system.work_span WHERE node_id = $1 AND random() < $2`
		if err := f.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := txn.ExecEx(
				ctx, "work-span-delete", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				deleteQuery, nodeID, deleteRatio,
			)
			return err
		}, isql.WithPriority(admissionpb.UserLowPri)); err != nil {
			log.Ops.Warningf(ctx, "work span delete failed: %v", err)
			// Continue with insert even if delete fails
		}
	}

	// Step 2: Drain the reservoir
	spans := f.collector.Reservoir().Drain()
	if len(spans) == 0 {
		return nil
	}

	// Step 3: Insert new spans
	return f.insertSpans(ctx, spans)
}

// insertSpans inserts a batch of spans into the system table.
func (f *Flusher) insertSpans(ctx context.Context, spans []WorkSpan) error {
	if len(spans) == 0 {
		return nil
	}

	// Build batch insert query
	// INSERT INTO system.work_span (id, parent_id, node_id, statement_fingerprint_id, ts, duration, cpu_time, lock_wait_time, span_type, span_name)
	// VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10), ...

	const batchSize = 50 // Insert in batches to avoid query size limits
	for i := 0; i < len(spans); i += batchSize {
		end := i + batchSize
		if end > len(spans) {
			end = len(spans)
		}
		batch := spans[i:end]

		if err := f.insertBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (f *Flusher) insertBatch(ctx context.Context, spans []WorkSpan) error {
	var valueClauses []string
	var args []interface{}
	argIdx := 1

	for _, span := range spans {
		// Build placeholder clause: ($1, $2, $3, ...)
		placeholders := make([]string, 10)
		for j := 0; j < 10; j++ {
			placeholders[j] = fmt.Sprintf("$%d", argIdx+j)
		}
		valueClauses = append(valueClauses, "("+strings.Join(placeholders, ", ")+")")

		// Add arguments
		var parentID interface{}
		if span.ParentID != 0 {
			parentID = span.ParentID
		}
		var stmtFingerprintID interface{}
		if span.StatementFingerprintID != 0 {
			stmtFingerprintID = int64(span.StatementFingerprintID)
		}

		args = append(args,
			span.ID,
			parentID,
			span.NodeID,
			stmtFingerprintID,
			span.Timestamp,
			span.Duration.Nanoseconds(),
			span.CPUTime.Nanoseconds(),
			span.ContentionTime.Nanoseconds(),
			string(span.SpanType),
			span.SpanName,
		)
		argIdx += 10
	}

	query := fmt.Sprintf(`
		INSERT INTO system.work_span
		(id, parent_id, node_id, statement_fingerprint_id, ts, duration, cpu_time, contention_time, span_type, span_name)
		VALUES %s
	`, strings.Join(valueClauses, ", "))

	return f.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.ExecEx(
			ctx, "work-span-insert", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			query, args...,
		)
		return err
	}, isql.WithPriority(admissionpb.UserLowPri))
}

// FlushNow performs an immediate flush (useful for testing).
func (f *Flusher) FlushNow(ctx context.Context) error {
	return f.flush(ctx)
}
