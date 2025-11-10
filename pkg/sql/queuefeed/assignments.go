package queuefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

type Assignment struct {
	// Version is unique per process level and can be used to efficiently detect
	// assignment changes.
	Version int64
	Session Session
	// Partitions is the list of partitions assigned to the session. It is sorted
	// by ID.
	Partitions []Partition
}

type PartitionAssignments struct {
	db             isql.DB
	partitionTable *partitionTable
}

func NewPartitionAssignments(db isql.DB, queueName string) *PartitionAssignments {
	return &PartitionAssignments{
		db:             db,
		partitionTable: &partitionTable{queueName: queueName},
	}
}

// RefreshAssignment refreshes the assignment for the given session. It returnrns
// nil if the assignment has not changed.
//
// If the session is caught up (i.e. it has proceessed up to a recent timestamp
// for all assigned partitions), then it may be assigned new partitions.
//
// If a partition has a successor session, then calling RefreshAssignment will
// return an assignment that does not include that partition.
func (p *PartitionAssignments) RefreshAssignment(
	session Session, caughtUp bool,
) (updatedAssignment *Assignment, err error) {
	// This is a stub implementation that assumes there is a single partition.
	return nil, nil
}

// RegisterSession registers a new session. The session may be assigned zero
// partitions if there are no unassigned partitions. If it is assigned no
// partitions, the caller can periodically call RefreshAssignment claim
// partitions if they become available.
func (p *PartitionAssignments) RegisterSession(
	ctx context.Context, session Session,
) (*Assignment, error) {
	// TODO(jeffswenson): this is a stub implementation that simply assigns all
	// unclaimed partitions to the current session.

	var result *Assignment
	err := p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		result = &Assignment{Session: session}

		partitions, err := p.partitionTable.ListPartitions(ctx, txn)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			if !partition.Session.Empty() {
				continue
			}
			partition.Session = session
			if err := p.partitionTable.UpdatePartition(ctx, txn, partition); err != nil {
				return errors.Wrapf(err, "updating partition %d for session %s", partition.ID, session.ConnectionID)
			}
			result.Partitions = append(result.Partitions, partition)
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "registering session")
	}
	return result, nil
}

func (p *PartitionAssignments) UnregisterSession(ctx context.Context, session Session) error {
	return p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := p.partitionTable.ListPartitions(ctx, txn)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			if partition.Session == session {
				partition.Session = partition.Successor
				if err := p.partitionTable.UpdatePartition(ctx, txn, partition); err != nil {
					return errors.Wrapf(err, "updating partition %d for session %s", partition.ID, session.ConnectionID)
				}
			}
			if partition.Successor == session {
				partition.Successor = Session{}
				if err := p.partitionTable.UpdatePartition(ctx, txn, partition); err != nil {
					return errors.Wrapf(err, "updating partition %d for session %s", partition.ID, session.ConnectionID)
				}
			}
		}
		return nil
	})
}

func (p *PartitionAssignments) constructAssignment(session Session) (*Assignment, error) {
	// Build an assignment for the given session from the partition cache.
	return nil, errors.New("not implemented")
}

func (p *PartitionAssignments) tryClaim(session Session, partition *Partition) (Partition, error) {
	// Try to claim an unassigned partition for the given session.
	return Partition{}, nil
}

func (p *PartitionAssignments) tryRelease(session Session, toRelease []Partition) error {
	// Release the given partitions from the session.
	return nil
}
