package queuefeed

import (
	"cmp"
	"context"
	"slices"

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
	ctx context.Context, session Session, caughtUp bool,
) (updatedAssignment *Assignment, err error) {
	// find my assignments and see if any of them have a successor session. return the ones that don't.
	// TODO: this should be done in sql
	// TODO: this handles partition handoff, but not hand...on... (?)
	var myPartitions []Partition
	anyChanged := false
	err = p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := p.partitionTable.ListPartitions(ctx, txn)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			if partition.Session != session {
				continue
			}
			if !partition.Successor.Empty() {
				anyChanged = true
				continue
			}
			myPartitions = append(myPartitions, partition)
		}
		return nil
	})
	if !anyChanged {
		return nil, nil
	}

	slices.SortFunc(myPartitions, func(a, b Partition) int { return cmp.Compare(a.ID, b.ID) })
	return &Assignment{Session: session, Partitions: myPartitions}, nil
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
			// TODO we really shouldn't force assign partitions, but we are not watch
			// sql liveness so we can't detect dead sessions yet.
			//if !partition.Session.Empty() {
			//	continue
			//}
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
	// TODO: this should probably be pushed onto some task queue that is
	// independent of the pgwire session so we can retry without block connection
	// cleanup.
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

// Try to claim a partition for the given session.
func (p *PartitionAssignments) TryClaim(ctx context.Context, session Session, partition Partition) (Partition, error) {
	partition.Successor = session
	err := p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return p.partitionTable.UpdatePartition(ctx, txn, partition)
	})
	if err != nil {
		return Partition{}, errors.Wrapf(err, "updating partition %d for session %s", partition.ID, session.ConnectionID)
	}
	return partition, nil
}

func (p *PartitionAssignments) tryRelease(session Session, toRelease []Partition) error {
	// Release the given partitions from the session.
	return nil
}
