// queuefeed is a somthing
package queuefeed

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/queuefeed/queuebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// watch queue partition table
// and create it too??
type Manager struct {
	executor isql.DB
	rff      *rangefeed.Factory
	codec    keys.SQLCodec
	leaseMgr *lease.Manager
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
	return &Manager{executor: executor, rff: rff, codec: codec, leaseMgr: leaseMgr}
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

		// Create a single initial partition that covers the table's primary key.
		primaryIndexPrefix := m.codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(tableDesc.GetPrimaryIndexID()))
		primaryKeySpan := roachpb.Span{
			Key:    primaryIndexPrefix,
			EndKey: primaryIndexPrefix.PrefixEnd(),
		}
		partition := Partition{
			ID:   1,
			Span: &primaryKeySpan,
		}

		return pt.InsertPartition(ctx, txn, partition)
	})
}

func (m *Manager) GetOrInitReader(ctx context.Context, name string) (queuebase.Reader, error) {
	// TODO: get if exists already
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
	fmt.Printf("get or init reader for queue %s with table desc id: %d\n", name, tableDescID)
	reader := NewReader(ctx, m.executor, m, m.rff, m.codec, m.leaseMgr, name, tableDescID)
	return reader, nil
}

func (m *Manager) reassessAssignments(ctx context.Context, name string) (bool, error) {
	return false, nil
}

var _ queuebase.Manager = &Manager{}

type PartitionAssignment struct{}
