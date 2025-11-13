// queuefeed is a somthing
package queuefeed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/queuefeed/queuebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// watch queue partition table
// and create it too??
type Manager struct {
	executor          isql.DB
	rff               *rangefeed.Factory
	rdi               rangedesc.IteratorFactory
	rc                *rangecache.RangeCache
	codec             keys.SQLCodec
	leaseMgr          *lease.Manager
	sqlLivenessReader sqlliveness.Reader

	mu struct {
		syncutil.Mutex
		queueAssignment map[string]*PartitionAssignments
	}

	// watchCtx and watchCancel are used to control the watchForDeadSessions goroutine.
	watchCtx    context.Context
	watchCancel context.CancelFunc
	watchWg     sync.WaitGroup
}

func NewManager(
	ctx context.Context,
	executor isql.DB,
	rff *rangefeed.Factory,
	rdi rangedesc.IteratorFactory,
	codec keys.SQLCodec,
	leaseMgr *lease.Manager,
	sqlLivenessReader sqlliveness.Reader,
) *Manager {
	// setup rangefeed on partitions table (/poll)
	// handle handoff from one server to another
	watchCtx, watchCancel := context.WithCancel(ctx)
	m := &Manager{
		executor:          executor,
		rff:               rff,
		rdi:               rdi,
		codec:             codec,
		leaseMgr:          leaseMgr,
		sqlLivenessReader: sqlLivenessReader,
		watchCtx:          watchCtx,
		watchCancel:       watchCancel,
	}
	m.mu.queueAssignment = make(map[string]*PartitionAssignments)

	m.watchWg.Add(1)
	go func() {
		defer m.watchWg.Done()
		m.watchForDeadSessions(watchCtx)
	}()

	return m
}

const createQueueCursorTableSQL = `
CREATE TABLE IF NOT EXISTS defaultdb.queue_cursor_%s (
	partition_id INT8 PRIMARY KEY,
	updated_at   TIMESTAMPTZ,
	cursor       bytea
)`

const createQueueTableSQL = `
CREATE TABLE IF NOT EXISTS defaultdb.queue_feeds (
	queue_feed_name STRING PRIMARY KEY,
	table_desc_id INT8 NOT NULL
)`

const insertQueueFeedSQL = `
INSERT INTO defaultdb.queue_feeds (queue_feed_name, table_desc_id) VALUES ($1, $2)
`

const fetchQueueFeedSQL = `
SELECT table_desc_id FROM defaultdb.queue_feeds WHERE queue_feed_name = $1
`

const updateCheckpointSQL = `
UPSERT INTO defaultdb.queue_cursor_%s (partition_id, updated_at, cursor)
VALUES ($1, now(), $2)
`

const readCheckpointSQL = `
SELECT cursor FROM defaultdb.queue_cursor_%s WHERE partition_id = $1
`

// should take a txn
func (m *Manager) CreateQueue(ctx context.Context, queueName string, tableDescID int64) error {
	err := m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx, "create_q", txn.KV(), createQueueTableSQL)
		if err != nil {
			return err
		}

		pt := &partitionTable{queueName: queueName}
		err = pt.CreateSchema(ctx, txn)
		if err != nil {
			return err
		}

		_, err = txn.Exec(ctx, "create_qc", txn.KV(), fmt.Sprintf(createQueueCursorTableSQL, queueName))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "creating queue tables for %s", queueName)
	}

	return m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// TODO(queuefeed): figure out how we want to integrate with schema changes.
		descriptor, err := m.leaseMgr.Acquire(ctx, lease.TimestampToReadTimestamp(txn.KV().ReadTimestamp()), descpb.ID(tableDescID))
		if err != nil {
			return err
		}
		tableDesc := descriptor.Underlying().(catalog.TableDescriptor)
		defer descriptor.Release(ctx)

		_, err = txn.Exec(ctx, "insert_q", txn.KV(), insertQueueFeedSQL, queueName, tableDescID)
		if err != nil {
			return err
		}

		pt := &partitionTable{queueName: queueName}

		// Make a partition for each range of the table's primary span, covering the span of that range.
		primaryIndexPrefix := m.codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(tableDesc.GetPrimaryIndexID()))
		primaryKeySpan := roachpb.Span{
			Key:    primaryIndexPrefix,
			EndKey: primaryIndexPrefix.PrefixEnd(),
		}

		spans, err := m.splitOnRanges(ctx, primaryKeySpan)
		if err != nil {
			return err
		}

		partitionID := int64(1)
		for _, span := range spans {
			partition := Partition{
				ID:   partitionID,
				Span: span,
			}

			if err := pt.InsertPartition(ctx, txn, partition); err != nil {
				return errors.Wrapf(err, "inserting partition %d for range", partitionID)
			}

			partitionID++
		}

		return nil
	})
}

func (m *Manager) newReaderLocked(
	ctx context.Context, name string, session Session,
) (*Reader, error) {
	var tableDescID int64

	// TODO: this ctx on the other hand should be stmt scoped
	err := m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx, "create_q", txn.KV(), createQueueTableSQL)
		if err != nil {
			return err
		}

		vals, err := txn.QueryRowEx(ctx, "fetch_q", txn.KV(),
			sessiondata.NodeUserSessionDataOverride, fetchQueueFeedSQL, name)
		if err != nil {
			return err
		}
		if len(vals) == 0 {
			return errors.Errorf("queue feed not found")
		}
		tableDescID = int64(tree.MustBeDInt(vals[0]))
		return nil
	})
	if err != nil {
		return nil, err
	}

	assigner, ok := m.mu.queueAssignment[name]
	if !ok {
		var err error
		assigner, err = NewPartitionAssignments(m.executor, name)
		if err != nil {
			return nil, err
		}
		m.mu.queueAssignment[name] = assigner
	}

	fmt.Printf("get or init reader for queue %s with table desc id: %d\n", name, tableDescID)
	return NewReader(ctx, m.executor, m, m.rff, m.codec, m.leaseMgr, session, assigner, name)
}

func (m *Manager) reassessAssignments(ctx context.Context, name string) (bool, error) {
	return false, nil
}

// CreateReaderForSession creates a new reader for the given queue name and session.
func (m *Manager) CreateReaderForSession(
	ctx context.Context, name string, session Session,
) (*Reader, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.newReaderLocked(ctx, name, session)
}

func (m *Manager) WriteCheckpoint(
	ctx context.Context, queueName string, partitionID int64, ts hlc.Timestamp,
) error {
	// Serialize the timestamp as bytes
	cursorBytes, err := ts.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshaling checkpoint timestamp")
	}

	sql := fmt.Sprintf(updateCheckpointSQL, queueName)
	return m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx, "write_checkpoint", txn.KV(), sql, partitionID, cursorBytes)
		return err
	})
}

func (m *Manager) ReadCheckpoint(
	ctx context.Context, queueName string, partitionID int64,
) (hlc.Timestamp, error) {
	var ts hlc.Timestamp
	sql := fmt.Sprintf(readCheckpointSQL, queueName)

	err := m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		row, err := txn.QueryRowEx(ctx, "read_checkpoint", txn.KV(),
			sessiondata.NodeUserSessionDataOverride, sql, partitionID)
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}

		cursorBytes := []byte(*row[0].(*tree.DBytes))
		if err := ts.Unmarshal(cursorBytes); err != nil {
			return errors.Wrap(err, "unmarshaling checkpoint timestamp")
		}
		return nil
	})

	return ts, err
}

func (m *Manager) splitOnRanges(ctx context.Context, span roachpb.Span) ([]roachpb.Span, error) {
	const pageSize = 100
	rdi, err := m.rdi.NewLazyIterator(ctx, span, pageSize)
	if err != nil {
		return nil, err
	}

	var spans []roachpb.Span
	remainingSpan := span

	for ; rdi.Valid(); rdi.Next() {
		rangeDesc := rdi.CurRangeDescriptor()
		rangeSpan := roachpb.Span{Key: rangeDesc.StartKey.AsRawKey(), EndKey: rangeDesc.EndKey.AsRawKey()}
		subspan := remainingSpan.Intersect(rangeSpan)
		if !subspan.Valid() {
			return nil, errors.AssertionFailedf("%s not in %s of %s", rangeSpan, remainingSpan, span)
		}
		spans = append(spans, subspan)
		remainingSpan.Key = subspan.EndKey
	}

	if err := rdi.Error(); err != nil {
		return nil, err
	}

	if remainingSpan.Valid() {
		spans = append(spans, remainingSpan)
	}

	return spans, nil
}

// A loop that looks for partitions that are assigned to sql liveness sessions
// that are no longer alive and removes all of their partition claims. (see the
// IsAlive method in the sqlliveness packages)
func (m *Manager) watchForDeadSessions(ctx context.Context) {
	// Check for dead sessions every 10 seconds.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.checkAndClearDeadSessions(ctx); err != nil {
				log.Dev.Warningf(ctx, "error checking for dead sessions: %v", err)
			}
		}
	}
}

const listQueueFeedsSQL = `SELECT queue_feed_name FROM defaultdb.queue_feeds`

// checkAndClearDeadSessions checks all partitions across all queues for dead sessions
// and clears their claims.
func (m *Manager) checkAndClearDeadSessions(ctx context.Context) error {
	// Get all queue names.
	var queueNames []string
	err := m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rows, err := txn.QueryBuffered(ctx, "list-queue-feeds", txn.KV(), listQueueFeedsSQL)
		if err != nil {
			return err
		}
		queueNames = make([]string, 0, len(rows))
		for _, row := range rows {
			queueNames = append(queueNames, string(tree.MustBeDString(row[0])))
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "listing queue feeds")
	}

	// Check each queue for dead sessions.
	for _, queueName := range queueNames {
		if err := m.checkQueueForDeadSessions(ctx, queueName); err != nil {
			log.Dev.Warningf(ctx, "error checking queue %s for dead sessions: %v", queueName, err)
			// Continue checking other queues even if one fails.
		}
	}

	return nil
}

// checkQueueForDeadSessions checks all partitions in a queue for dead sessions
// and clears their claims.
func (m *Manager) checkQueueForDeadSessions(ctx context.Context, queueName string) error {
	pt := &partitionTable{queueName: queueName}
	var partitionsToUpdate []Partition

	err := m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		if err != nil {
			return err
		}

		for _, partition := range partitions {
			needsUpdate := false
			updatedPartition := partition

			// Check if the Session is assigned to a dead session.
			if partition.Session.LivenessID != "" {
				alive, err := m.sqlLivenessReader.IsAlive(ctx, partition.Session.LivenessID)
				if err != nil {
					// If we can't determine liveness, err on the side of caution and don't clear.
					log.Dev.Warningf(ctx, "error checking liveness for session %s: %v", partition.Session.LivenessID, err)
					continue
				}
				if !alive {
					// Session is dead. Clear the claim.
					// If there's a successor, promote it to Session.
					if !partition.Successor.Empty() {
						updatedPartition.Session = partition.Successor
						updatedPartition.Successor = Session{}
					} else {
						updatedPartition.Session = Session{}
					}
					needsUpdate = true
				}
			}

			// Check if the Successor is assigned to a dead session.
			if partition.Successor.LivenessID != "" {
				alive, err := m.sqlLivenessReader.IsAlive(ctx, partition.Successor.LivenessID)
				if err != nil {
					log.Dev.Warningf(ctx, "error checking liveness for successor session %s: %v", partition.Successor.LivenessID, err)
					continue
				}
				if !alive {
					// Successor session is dead. Clear it.
					updatedPartition.Successor = Session{}
					needsUpdate = true
				}
			}

			if needsUpdate {
				partitionsToUpdate = append(partitionsToUpdate, updatedPartition)
			}
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "listing partitions for queue %s", queueName)
	}

	// Update partitions that need to be cleared.
	if len(partitionsToUpdate) > 0 {
		return m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			for _, partition := range partitionsToUpdate {
				if err := pt.UpdatePartition(ctx, txn, partition); err != nil {
					return errors.Wrapf(err, "updating partition %d for queue %s", partition.ID, queueName)
				}
				fmt.Printf("pruning dead sessions: updated partition %d for queue %s\n", partition.ID, queueName)
			}
			return nil
		})
	}

	return nil
}

// Close stops the Manager and waits for all background goroutines to exit.
func (m *Manager) Close() {
	m.watchCancel()
	m.watchWg.Wait()
}

var _ queuebase.Manager = &Manager{}
