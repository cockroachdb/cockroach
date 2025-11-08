// queuefeed is a somthing
package queuefeed

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/isql"
)

// watch queue partition table
// and create it too??
type Manager struct {
	executor isql.DB
}

func NewManager(executor isql.DB) *Manager {
	// setup rangefeed on partitions table (/poll)
	// handle handoff from one server to another
	return &Manager{executor: executor}
}

const createQueuePartitionTableSQL = `
CREATE TABLE IF NOT EXISTS defaultdb.queue_partition_%s (
	partition_id INT8 PRIMARY KEY,
	-- is the sql server assigned dead
	sql_liveness_session UUID NOT NULL,
	-- pgwire session
	user_session UUID NOT NULL,
	sql_liveness_session_successor UUID,
	user_session_successor UUID,
	partition_spec bytea,
	updated_at TIMESTAMPTZ
)`

const createQueueCursorTableSQL = `
CREATE TABLE IF NOT EXISTS defaultdb.queue_cursor_%s (
	partition_id INT8 PRIMARY KEY,
	updated_at   TIMESTAMPTZ,
	cursor       bytea
)`

// should take a txn
func (m *Manager) CreateQueueTables(ctx context.Context, queueName string) error {
	return m.executor.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx, "create_qp", txn.KV(), fmt.Sprintf(createQueuePartitionTableSQL, queueName))
		if err != nil {
			return err
		}
		_, err = txn.Exec(ctx, "create_qc", txn.KV(), fmt.Sprintf(createQueueCursorTableSQL, queueName))
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *Manager) GetOrInitReader(ctx context.Context, name string) (*Reader, error) {
	err := m.CreateQueueTables(ctx, name)
	if err != nil {
		return nil, err
	}
	return NewReader(ctx, m.executor, m, name), nil
}

func (m *Manager) reassessAssignments(ctx context.Context, name string) {}

type PartitionAssignment struct{}
