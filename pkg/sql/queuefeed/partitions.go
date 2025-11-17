package queuefeed

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type Partition struct {
	// ID is the `partition_id` column in the queue partition table.
	ID int64
	// Session is the `user_session` and `sql_liveness_session` assigned to this
	// partition.
	Session Session
	// Successor is the `user_session_successor` and
	// `sql_liveness_session_successor` assigned to the partition.
	Successor Session
	// Span is decoded from the `partition_spec` column.
	Span roachpb.Span
}

func PartitionFromDatums(row tree.Datums) (Partition, error) {
	var session, successor Session
	if !(row[1] == tree.DNull || row[2] == tree.DNull) {
		session = Session{
			LivenessID:   sqlliveness.SessionID(tree.MustBeDBytes(row[1])),
			ConnectionID: tree.MustBeDUuid(row[2]).UUID,
		}
	}
	if !(row[3] == tree.DNull || row[4] == tree.DNull) {
		successor = Session{
			LivenessID:   sqlliveness.SessionID(tree.MustBeDBytes(row[3])),
			ConnectionID: tree.MustBeDUuid(row[4]).UUID,
		}
	}

	var span roachpb.Span
	if row[5] != tree.DNull {
		var err error
		span, err = decodeSpan([]byte(*row[5].(*tree.DBytes)))
		if err != nil {
			return Partition{}, err
		}
	}

	return Partition{
		ID:        int64(tree.MustBeDInt(row[0])),
		Session:   session,
		Successor: successor,
		Span:      span,
	}, nil
}

type partitionTable struct {
	queueName string
}

func (p *partitionTable) CreateSchema(ctx context.Context, txn isql.Txn) error {
	_, err := txn.Exec(ctx, "create-partition-table", txn.KV(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS defaultdb.queue_partition_%s (
			partition_id BIGSERIAL PRIMARY KEY,
			sql_liveness_session BYTES,
			user_session UUID,
			sql_liveness_session_successor BYTES,
			user_session_successor UUID,
			partition_spec BYTES
		)`, p.queueName))
	return err
}

func (p *partitionTable) ListPartitions(ctx context.Context, txn isql.Txn) ([]Partition, error) {
	rows, err := txn.QueryBuffered(ctx, "list-partitions", txn.KV(), fmt.Sprintf(`
		SELECT
			partition_id,
			sql_liveness_session,
			user_session,
			sql_liveness_session_successor,
			user_session_successor,
			partition_spec
		FROM defaultdb.queue_partition_%s`, p.queueName))
	if err != nil {
		return nil, err
	}
	partitions := make([]Partition, len(rows))
	for i, row := range rows {
		var err error
		partitions[i], err = PartitionFromDatums(row)
		if err != nil {
			return nil, err
		}
	}
	return partitions, nil
}

// FetchPartitions fetches all of the partitions with the given IDs. The len of
// the returned map is eqaul to the number of unique partitionIDs passed in. If
// a partition id is not found, it will be present in the map with a zero-value
// Partition.
func (p *partitionTable) FetchPartitions(
	ctx context.Context, txn isql.Txn, partitionIDs []int64,
) (map[int64]Partition, error) {
	if len(partitionIDs) == 0 {
		return make(map[int64]Partition), nil
	}

	// Initialize result map with zero-value partitions for all unique IDs
	result := make(map[int64]Partition)
	for _, id := range partitionIDs {
		result[id] = Partition{} // Zero-value partition as placeholder
	}

	datumArray := tree.NewDArray(types.Int)
	for _, id := range partitionIDs {
		if err := datumArray.Append(tree.NewDInt(tree.DInt(id))); err != nil {
			return nil, err
		}
	}

	rows, err := txn.QueryBuffered(ctx, "fetch-partitions", txn.KV(), fmt.Sprintf(`
		SELECT 
			partition_id,
			sql_liveness_session,
			user_session,
			sql_liveness_session_successor,
			user_session_successor,
			partition_spec
		FROM defaultdb.queue_partition_%s
		WHERE partition_id = ANY($1)`, p.queueName), datumArray)
	if err != nil {
		return nil, err
	}

	// Process found partitions
	for _, row := range rows {
		partition, err := PartitionFromDatums(row)
		if err != nil {
			return nil, err
		}
		result[partition.ID] = partition
	}

	return result, nil
}

// Get retrieves a single partition by ID. Returns an error if the partition
// is not found.
func (p *partitionTable) Get(
	ctx context.Context, txn isql.Txn, partitionID int64,
) (Partition, error) {
	row, err := txn.QueryRow(ctx, "get-partition", txn.KV(),
		fmt.Sprintf(`
		SELECT 
			partition_id,
			sql_liveness_session,
			user_session,
			sql_liveness_session_successor,
			user_session_successor,
			partition_spec
		FROM defaultdb.queue_partition_%s
		WHERE partition_id = $1`, p.queueName), partitionID)
	if err != nil {
		return Partition{}, err
	}

	if row == nil {
		return Partition{}, errors.Newf("no partition found with id %d", partitionID)
	}

	partition, err := PartitionFromDatums(row)
	if err != nil {
		return Partition{}, err
	}

	return partition, nil
}

func (p *partitionTable) InsertPartition(
	ctx context.Context, txn isql.Txn, partition Partition,
) error {
	var sessionLivenessID, sessionConnectionID interface{}
	var successorLivenessID, successorConnectionID interface{}

	if !partition.Session.Empty() {
		sessionLivenessID = []byte(partition.Session.LivenessID)
		sessionConnectionID = partition.Session.ConnectionID
	} else {
		sessionLivenessID = nil
		sessionConnectionID = nil
	}

	if !partition.Successor.Empty() {
		successorLivenessID = []byte(partition.Successor.LivenessID)
		successorConnectionID = partition.Successor.ConnectionID
	} else {
		successorLivenessID = nil
		successorConnectionID = nil
	}

	spanBytes := encodeSpan(partition.Span)

	_, err := txn.Exec(ctx, "insert-partition", txn.KV(),
		fmt.Sprintf(`INSERT INTO defaultdb.queue_partition_%s
			(partition_id, sql_liveness_session, user_session, sql_liveness_session_successor, user_session_successor, partition_spec)
			VALUES ($1, $2, $3, $4, $5, $6)`, p.queueName),
		partition.ID, sessionLivenessID, sessionConnectionID,
		successorLivenessID, successorConnectionID, spanBytes)

	return err
}

func (p *partitionTable) UpdatePartition(
	ctx context.Context, txn isql.Txn, partition Partition,
) error {
	var sessionLivenessID, sessionConnectionID interface{}
	var successorLivenessID, successorConnectionID interface{}

	if !partition.Session.Empty() {
		sessionLivenessID = []byte(partition.Session.LivenessID)
		sessionConnectionID = partition.Session.ConnectionID
	} else {
		sessionLivenessID = nil
		sessionConnectionID = nil
	}

	if !partition.Successor.Empty() {
		successorLivenessID = []byte(partition.Successor.LivenessID)
		successorConnectionID = partition.Successor.ConnectionID
	} else {
		successorLivenessID = nil
		successorConnectionID = nil
	}

	spanBytes := encodeSpan(partition.Span)

	_, err := txn.Exec(ctx, "update-partition", txn.KV(),
		fmt.Sprintf(`UPDATE defaultdb.queue_partition_%s
			SET sql_liveness_session = $2,
				user_session = $3,
				sql_liveness_session_successor = $4,
				user_session_successor = $5,
				partition_spec = $6
			WHERE partition_id = $1`, p.queueName),
		partition.ID, sessionLivenessID, sessionConnectionID,
		successorLivenessID, successorConnectionID, spanBytes)

	return err
}

// UnregisterSession removes the given session from all assignments and
// partition claims, it returns the updated partitions.
func (p *partitionTable) UnregisterSession(
	ctx context.Context, txn isql.Txn, session Session,
) (updates map[int64]Partition, err error) {
	sessionLivenessID := []byte(session.LivenessID)
	sessionConnectionID := session.ConnectionID

	rows, err := txn.QueryBuffered(ctx, "unregister-session", txn.KV(), fmt.Sprintf(`
		UPDATE defaultdb.queue_partition_%s 
		SET 
			sql_liveness_session = CASE 
				WHEN sql_liveness_session = $1 AND user_session = $2 THEN sql_liveness_session_successor
				ELSE sql_liveness_session
			END,
			user_session = CASE 
				WHEN sql_liveness_session = $1 AND user_session = $2 THEN user_session_successor
				ELSE user_session
			END,
			sql_liveness_session_successor = CASE 
				WHEN sql_liveness_session = $1 AND user_session = $2 THEN NULL
				WHEN sql_liveness_session_successor = $1 AND user_session_successor = $2 THEN NULL
				ELSE sql_liveness_session_successor
			END,
			user_session_successor = CASE 
				WHEN sql_liveness_session = $1 AND user_session = $2 THEN NULL
				WHEN sql_liveness_session_successor = $1 AND user_session_successor = $2 THEN NULL
				ELSE user_session_successor
			END
		WHERE (sql_liveness_session = $1 AND user_session = $2) 
		   OR (sql_liveness_session_successor = $1 AND user_session_successor = $2)
		RETURNING partition_id, sql_liveness_session, user_session, 
				  sql_liveness_session_successor, user_session_successor, partition_spec`, p.queueName), sessionLivenessID, sessionConnectionID)
	if err != nil {
		return nil, err
	}

	updates = make(map[int64]Partition)
	for _, row := range rows {
		partition, err := PartitionFromDatums(row)
		if err != nil {
			return nil, err
		}
		updates[partition.ID] = partition
	}

	return updates, nil
}

func (p Partition) Empty() bool {
	return p.ID == 0
}

// Equal returns true if two partitions are equal in all fields.
func (p Partition) Equal(other Partition) bool {
	return p.ID == other.ID &&
		p.Session == other.Session &&
		p.Successor == other.Successor &&
		p.Span.Equal(other.Span)
}

type Session struct {
	// ConnectionID is the ID of the underlying connection.
	ConnectionID uuid.UUID
	// LivenessID is the session ID for the server. Its used to identify sessions
	// that belong to dead sql servers.
	LivenessID sqlliveness.SessionID
}

func (s Session) Empty() bool {
	return s.ConnectionID == uuid.Nil && s.LivenessID == ""
}

func decodeSpan(data []byte) (roachpb.Span, error) {
	var span roachpb.Span
	if err := span.Unmarshal(data); err != nil {
		return roachpb.Span{}, err
	}
	return span, nil
}

func encodeSpan(span roachpb.Span) []byte {
	data, err := span.Marshal()
	if err != nil {
		return nil
	}
	return data
}

func TestNewPartitionsTable(queueName string) *partitionTable {
	return &partitionTable{queueName: queueName}
}
