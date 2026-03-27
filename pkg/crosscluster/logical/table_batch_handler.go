// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// tableHandler applies batches of replication events that are destined for a
// sinlgle table.
type tableHandler struct {
	sqlReader        sqlwriter.RowReader
	sqlWriter        *sqlwriter.RowWriter
	session          isql.Session
	db               descs.DB
	tombstoneUpdater *tombstoneUpdater
	columns          []string
	settings         *cluster.Settings
	tableID          descpb.ID
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
) (_ *tableHandler, err error) {
	var table catalog.TableDescriptor
	session, err := sqlwriter.NewInternalSession(ctx, db, sd, settings)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			session.Close(ctx)
		}
	}()

	// NOTE: we don't hold a lease on the table descriptor, but validation
	// prevents users from changing the set of columns or the primary key of an
	// LDR replicated table.
	err = db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
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

	reader, err := sqlwriter.NewRowReader(ctx, table, session)
	if err != nil {
		return nil, err
	}

	writer, err := sqlwriter.NewRowWriter(ctx, table, session)
	if err != nil {
		return nil, err
	}

	tombstoneUpdater := newTombstoneUpdater(codec, db.KV(), leaseMgr, tableID, sd, settings)

	return &tableHandler{
		sqlReader:        reader,
		sqlWriter:        writer,
		db:               db,
		tombstoneUpdater: tombstoneUpdater,
		session:          session,
		columns:          writer.Columns(),
		settings:         settings,
		tableID:          tableID,
	}, nil
}

func (t *tableHandler) traceEnabled() bool {
	return ldrTraceRowsEnabled.Get(&t.settings.SV)
}

func (t *tableHandler) Close(ctx context.Context) {
	t.session.Close(ctx)
}

func (t *tableHandler) handleDecodedBatch(
	ctx context.Context, batch []ldrdecoder.DecodedRow,
) (tableBatchStats, error) {
	stats, err := t.attemptBatch(ctx, batch, false /* winLwwTie */)
	if err == nil {
		return stats, nil
	}

	refreshedBatch, refreshStats, err := t.refreshPrevRows(ctx, batch)
	if err != nil {
		return stats, err
	}

	stats, err = t.attemptBatch(ctx, refreshedBatch, true /* winLwwTie */)
	if err != nil {
		return tableBatchStats{}, err
	}

	stats.Add(refreshStats)

	return stats, nil
}

func (t *tableHandler) attemptBatch(
	ctx context.Context, batch []ldrdecoder.DecodedRow, winLwwTie bool,
) (tableBatchStats, error) {
	var stats tableBatchStats

	var hasTombstoneUpdates bool
	err := t.session.Txn(ctx, func(ctx context.Context) error {
		for _, event := range batch {
			switch {
			case event.IsDeleteRow():
				stats.deletes++
				err := t.sqlWriter.DeleteRow(ctx, event.RowTimestamp, event.PrevRow)
				if err != nil {
					return err
				}
				if t.traceEnabled() {
					log.Replication.Infof(ctx, "LDR delete table=%d prevRow=%s",
						t.tableID, sqlwriter.FormatRow(t.columns, event.PrevRow))
				}
			case event.IsTombstoneUpdate():
				hasTombstoneUpdates = true
				// Skip: handled in its own transaction.
			case event.IsInsertRow():
				stats.inserts++
				// Inserts always win origin timestamp ties. If the
				// insert conflicts with a non-tombstone row it will
				// trigger a read refresh.
				err := t.sqlWriter.InsertRow(ctx, event.RowTimestamp, event.Row)
				if isLwwLoser(err) {
					// Insert may observe a LWW failure if it loses to an existing
					// row or to a tombstone with a higher timestamp.
					stats.kvLwwLosers++
					if t.traceEnabled() {
						log.Replication.Infof(ctx, "LDR lww-loser table=%d row=%s",
							t.tableID, sqlwriter.FormatRow(t.columns, event.Row))
					}
					continue
				}
				if err != nil {
					return err
				}
				if t.traceEnabled() {
					log.Replication.Infof(ctx, "LDR insert table=%d row=%s",
						t.tableID, sqlwriter.FormatRow(t.columns, event.Row))
				}
			case event.IsUpdateRow():
				stats.updates++
				err := t.sqlWriter.UpdateRow(ctx, event.RowTimestamp, event.PrevRow, event.Row, winLwwTie)
				if err != nil {
					return err
				}
				if t.traceEnabled() {
					log.Replication.Infof(ctx, "LDR update table=%d prevRow=%s newRow=%s",
						t.tableID, sqlwriter.FormatRow(t.columns, event.PrevRow),
						sqlwriter.FormatRow(t.columns, event.Row))
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

	if hasTombstoneUpdates {
		// TODO(jeffswenson): once we have a way to expose the transaction used by
		// the Session, we should bundle this with the other txn. The purpose of
		// these transactions is batching writes in a transaction increases
		// efficiency. The transactions are not needed for correctness.
		err = t.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			for _, event := range batch {
				if event.IsTombstoneUpdate() {
					stats.tombstoneUpdates++
					tombstoneUpdateStats, err := t.tombstoneUpdater.updateTombstone(ctx, txn, event.RowTimestamp, event.Row)
					if err != nil {
						return err
					}
					stats.kvLwwLosers += tombstoneUpdateStats.kvWriteTooOld
				}
			}
			return nil
		})
		if err != nil {
			return tableBatchStats{}, err
		}
	}

	return stats, nil
}

// refreshPrevRows refreshes the prevRow field for each event in the batch. If
// any event is known to be a lww loser based on the read, it is dropped from
// the batch.
//
// When the existing and incoming timestamps are equal, column values are
// compared element-wise to break the tie deterministically. Both clusters
// perform the same comparison and arrive at the same winner.
func (t *tableHandler) refreshPrevRows(
	ctx context.Context, batch []ldrdecoder.DecodedRow,
) ([]ldrdecoder.DecodedRow, tableBatchStats, error) {
	var stats tableBatchStats
	stats.refreshedRows = int64(len(batch))

	rows := make([]tree.Datums, 0, len(batch))
	for _, event := range batch {
		rows = append(rows, event.Row)
	}

	refreshedRows, err := t.sqlReader.ReadRows(ctx, rows)
	if err != nil {
		return nil, tableBatchStats{}, err
	}

	refreshedBatch := make([]ldrdecoder.DecodedRow, 0, len(batch))
	for i, event := range batch {
		var prevRow tree.Datums
		if refreshed, found := refreshedRows[i]; found {
			if t.traceEnabled() {
				log.Replication.Infof(ctx, "LDR read-refresh table=%d row=%s ts=%s isLocal=%t",
					t.tableID, sqlwriter.FormatRow(t.columns, refreshed.Row),
					refreshed.LogicalTimestamp, refreshed.IsLocal)
			}
			won, err := sqlwriter.IsLwwWinner(event.RowTimestamp, refreshed.LogicalTimestamp, event.Row, refreshed.Row)
			if err != nil {
				return nil, tableBatchStats{}, err
			}
			if !won {
				log.VEventf(ctx, 2, "LWW: incoming row lost or identical")
				stats.refreshLwwLosers++
				if t.traceEnabled() {
					log.Replication.Infof(ctx, "LDR refresh-lww-loser table=%d row=%s",
						t.tableID, sqlwriter.FormatRow(t.columns, event.Row))
				}
				continue
			}
			// Note: we can only identify LWW losers during the read refresh
			// if the row exists. If it is a tombstone, the local value may win
			// LWW, but we have to attempt the insert/tombstone update to find
			// out.
			prevRow = refreshed.Row
		}
		refreshedEvent := ldrdecoder.DecodedRow{
			TableID:      event.TableID,
			IsDelete:     event.IsDelete,
			Row:          event.Row,
			RowTimestamp: event.RowTimestamp,
			PrevRow:      prevRow,
		}
		refreshedBatch = append(refreshedBatch, refreshedEvent)
	}

	return refreshedBatch, stats, nil
}

func (t *tableHandler) ReleaseLeases(ctx context.Context) {
	t.tombstoneUpdater.ReleaseLeases(ctx)
}
