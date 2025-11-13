// queuefeed is a somthing
package queuefeed

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// watch queue partition table
// and create it too??
type Manager struct {
	executor isql.DB
	rff      *rangefeed.Factory
	codec    keys.SQLCodec
	leaseMgr *lease.Manager

	mu struct {
		syncutil.Mutex
		// name -> reader
		// TODO: this should actually be a map of (session id, name) -> reader, or smth
		readers map[string]*Reader

		queueAssignment map[string]*PartitionAssignments
	}
}

func NewManager(
	_ context.Context,
	executor isql.DB,
	rff *rangefeed.Factory,
	codec keys.SQLCodec,
	leaseMgr *lease.Manager,
) *Manager {
	// setup rangefeed on partitions table (/poll)
	// handle handoff from one server to another
	m := &Manager{executor: executor, rff: rff, codec: codec, leaseMgr: leaseMgr}
	m.mu.readers = make(map[string]*Reader)
	m.mu.queueAssignment = make(map[string]*PartitionAssignments)
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

		// Extract DistSender from the executor's KV DB to iterate over ranges.
		txnWrapperSender, ok := m.executor.KV().NonTransactionalSender().(*kv.CrossRangeTxnWrapperSender)
		if !ok {
			return errors.Errorf("failed to extract a %T from %T",
				(*kv.CrossRangeTxnWrapperSender)(nil), m.executor.KV().NonTransactionalSender())
		}
		distSender, ok := txnWrapperSender.Wrapped().(*kvcoord.DistSender)
		if !ok {
			return errors.Errorf("failed to extract a %T from %T",
				(*kvcoord.DistSender)(nil), txnWrapperSender.Wrapped())
		}

		// Convert the span to an RSpan for range iteration.
		rSpan, err := keys.SpanAddr(primaryKeySpan)
		if err != nil {
			return errors.Wrapf(err, "converting primary key span to address span")
		}

		// Iterate over all ranges covering the primary key span.
		it := kvcoord.MakeRangeIterator(distSender)
		partitionID := int64(1)
		for it.Seek(ctx, rSpan.Key, kvcoord.Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return errors.Wrapf(it.Error(), "iterating ranges for primary key span")
			}

			// Get the range descriptor and trim its span to the primary key span boundaries.
			desc := it.Desc()
			startKey := desc.StartKey
			if startKey.Compare(rSpan.Key) < 0 {
				startKey = rSpan.Key
			}
			endKey := desc.EndKey
			if endKey.Compare(rSpan.EndKey) > 0 {
				endKey = rSpan.EndKey
			}

			partition := Partition{
				ID:   partitionID,
				Span: roachpb.Span{Key: startKey.AsRawKey(), EndKey: endKey.AsRawKey()},
			}

			if err := pt.InsertPartition(ctx, txn, partition); err != nil {
				return errors.Wrapf(err, "inserting partition %d for range", partitionID)
			}

			partitionID++

			// Check if we need to continue to the next range.
			if !it.NeedAnother(rSpan) {
				break
			}
		}

		return nil
	})
}

func (m *Manager) GetOrInitReader(ctx context.Context, name string) (queuebase.Reader, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reader, ok := m.mu.readers[name]
	if ok && reader.IsAlive() {
		fmt.Printf("get or init reader for queue %s found in cache\n", name)
		return reader, nil
	}
	fmt.Printf("get or init reader for queue %s not found in cache\n", name)
	reader, err := m.newReaderLocked(ctx, name, Session{
		// TODO(queuefeed): get a real session here.
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID("1"),
	})
	if err != nil {
		return nil, err
	}
	m.mu.readers[name] = reader
	return reader, nil
}

func (m *Manager) newReaderLocked(
	ctx context.Context, name string, session Session,
) (*Reader, error) {
	var tableDescID int64

	assigner, ok := m.mu.queueAssignment[name]
	if !ok {
		assigner = NewPartitionAssignments(m.executor, name)
		m.mu.queueAssignment[name] = assigner
	}

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
	fmt.Printf("get or init reader for queue %s with table desc id: %d\n", name, tableDescID)
	return NewReader(ctx, m.executor, m, m.rff, m.codec, m.leaseMgr, session, assigner, name)
}

func (m *Manager) reassessAssignments(ctx context.Context, name string) (bool, error) {
	return false, nil
}

func (m *Manager) forgetReader(name string) {
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.mu.readers, name)
	}()
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

var _ queuebase.Manager = &Manager{}
