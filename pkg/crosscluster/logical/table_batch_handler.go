// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// tableHandler applies batches of replication events that are destined for a
// sinlgle table.
type tableHandler struct {
	sqlReader        *sqlRowReader
	sqlWriter        *sqlRowWriter
	db               descs.DB
	tombstoneUpdater *tombstoneUpdater
}

type tableBatchStats struct {
	// refreshedRows are the number of rows that were re-read from the local
	// database.
	refreshedRows int64
	// inserts is the number of rows that were inserted.
	inserts int64
	// updates is the number of rows that were updated.
	updates int64
	// deletes is the number of rows that were deleted.
	deletes int64
	// tombstoneUpdates is the number of tombstones that were updated. This case
	// only occurs if the event is a replicated delete and the row did not exist
	// locally.
	tombstoneUpdates int64
	// refreshLwwLosers is the number of rows that were dropped as lww losers
	// after reading them locally.
	refreshLwwLosers int64
	// kvLwwLosers is the number of rows that were dropped as lww losers after
	// attempting to write to the KV layer. This case only occurs if there is a
	// tombstone that is more recent than the replicated action.
	kvLwwLosers int64
}

func (t *tableBatchStats) Add(o tableBatchStats) {
	t.refreshedRows += o.refreshedRows
	t.inserts += o.inserts
	t.updates += o.updates
	t.deletes += o.deletes
	t.tombstoneUpdates += o.tombstoneUpdates
	t.refreshLwwLosers += o.refreshLwwLosers
	t.kvLwwLosers += o.kvLwwLosers
}

func (t *tableBatchStats) AddTo(bs *batchStats) {
	// TODO(jeffswenson): rework batch stats so they record interesting crud
	// writer behavior. The values in batch stats are currently designed mostly
	// for the legacy kv writer.
	bs.kvWriteTooOld += t.kvLwwLosers + t.refreshLwwLosers
	if t.refreshedRows != 0 {
		bs.kvWriteValueRefreshes += 1
	}
}

// newTableHandler creates a new tableHandler for the given table descriptor ID.
// It internally constructs the sqlReader and sqlWriter components.
func newTableHandler(
	ctx context.Context,
	tableID descpb.ID,
	db descs.DB,
	codec keys.SQLCodec,
	sd *sessiondata.SessionData,
	jobID jobspb.JobID,
	leaseMgr *lease.Manager,
	settings *cluster.Settings,
) (*tableHandler, error) {
	var table catalog.TableDescriptor

	// NOTE: we don't hold a lease on the table descriptor, but validation
	// prevents users from changing the set of columns or the primary key of an
	// LDR replicated table.
	err := db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var err error
		table, err = txn.Descriptors().GetLeasedImmutableTableByID(ctx, txn.KV(), tableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Set an applicaiton name that makes it clear which ldr job the queries
	// belong to.
	sessionOverride := ieOverrideBase
	sessionOverride.ApplicationName = fmt.Sprintf("%s-logical-replication-%d", sd.ApplicationName, jobID)

	reader, err := newSQLRowReader(table, sessionOverride)
	if err != nil {
		return nil, err
	}

	writer, err := newSQLRowWriter(table, sessionOverride)
	if err != nil {
		return nil, err
	}

	tombstoneUpdater := newTombstoneUpdater(codec, db.KV(), leaseMgr, tableID, sd, settings)

	return &tableHandler{
		sqlReader:        reader,
		sqlWriter:        writer,
		db:               db,
		tombstoneUpdater: tombstoneUpdater,
	}, nil
}

func (t *tableHandler) handleDecodedBatch(
	ctx context.Context, batch []decodedEvent,
) (tableBatchStats, error) {
	stats, err := t.attemptBatch(ctx, batch)
	if err == nil {
		return stats, nil
	}

	refreshedBatch, refreshStats, err := t.refreshPrevRows(ctx, batch)
	if err != nil {
		return stats, err
	}

	stats, err = t.attemptBatch(ctx, refreshedBatch)
	if err != nil {
		return tableBatchStats{}, err
	}

	stats.Add(refreshStats)

	return stats, nil
}

func (t *tableHandler) attemptBatch(
	ctx context.Context, batch []decodedEvent,
) (tableBatchStats, error) {
	var stats tableBatchStats
	err := t.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, event := range batch {
			switch {
			case event.isDelete && len(event.prevRow) != 0:
				stats.deletes++
				err := t.sqlWriter.DeleteRow(ctx, txn, event.originTimestamp, event.prevRow)
				if err != nil {
					return err
				}
			case event.isDelete && len(event.prevRow) == 0:
				stats.tombstoneUpdates++
				tombstoneUpdateStats, err := t.tombstoneUpdater.updateTombstone(ctx, txn, event.originTimestamp, event.row)
				if err != nil {
					return err
				}
				stats.kvLwwLosers += tombstoneUpdateStats.kvWriteTooOld
			case event.prevRow == nil:
				stats.inserts++
				err := t.sqlWriter.InsertRow(ctx, txn, event.originTimestamp, event.row)
				if isLwwLoser(err) {
					// Insert may observe a LWW failure if it attempts to write over a tombstone.
					stats.kvLwwLosers++
					continue
				}
				if err != nil {
					return err
				}
			case event.prevRow != nil:
				stats.updates++
				err := t.sqlWriter.UpdateRow(ctx, txn, event.originTimestamp, event.prevRow, event.row)
				if err != nil {
					return err
				}
			default:
				return errors.AssertionFailedf("unhandled event type: %v", event)
			}
		}
		return nil
	})
	if err != nil {
		return tableBatchStats{}, err
	}
	return stats, nil
}

// refreshPrevRows refreshes the prevRow field for each event in the batch. If
// any event is known to be a lww loser based on the read, its dropped from the
// batch.
func (t *tableHandler) refreshPrevRows(
	ctx context.Context, batch []decodedEvent,
) ([]decodedEvent, tableBatchStats, error) {
	var stats tableBatchStats
	stats.refreshedRows = int64(len(batch))

	rows := make([]tree.Datums, 0, len(batch))
	for _, event := range batch {
		rows = append(rows, event.row)
	}

	var refreshedRows map[int]priorRow
	err := t.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		// TODO(jeffswenson): should we apply the batch in the same transaction
		// that we perform the read refresh? We could maybe even use locking reads.
		refreshedRows, err = t.sqlReader.ReadRows(ctx, txn, rows)
		return err
	})
	if err != nil {
		return nil, tableBatchStats{}, err
	}

	refreshedBatch := make([]decodedEvent, 0, len(batch))
	for i, event := range batch {
		var prevRow tree.Datums
		if refreshed, found := refreshedRows[i]; found {
			if !refreshed.logicalTimestamp.Less(event.originTimestamp) {
				// TODO(jeffswenson): update this logic when its time to handle
				// ties.
				// Skip the row because it is a lww loser. Note: we can only identify LWW
				// losers during the read refresh if the row exists. If its a tombstone,
				// the local value may win LWW, but we have to attempt the
				// insert/tombstone update to find out. We even have to do this if the
				// replicated event is a delete because the local tombstone may have an
				// older logical timestamp.
				stats.refreshLwwLosers++
				continue
			}
			prevRow = refreshed.row
		}
		refreshedEvent := decodedEvent{
			dstDescID:       event.dstDescID,
			isDelete:        event.isDelete,
			row:             event.row,
			originTimestamp: event.originTimestamp,
			prevRow:         prevRow,
		}
		refreshedBatch = append(refreshedBatch, refreshedEvent)
	}

	return refreshedBatch, stats, nil
}

func (t *tableHandler) ReleaseLeases(ctx context.Context) {
	t.tombstoneUpdater.ReleaseLeases(ctx)
}
