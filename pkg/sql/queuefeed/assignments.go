package queuefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

func (a *Assignment) Spans() []roachpb.Span {
	sg := roachpb.SpanGroup{}
	for _, partition := range a.Partitions {
		sg.Add(partition.Span)
	}
	return sg.Slice()
}

type PartitionAssignments struct {
	db             isql.DB
	partitionTable *partitionTable

	refresh struct {
		// Lock ordering: refresh may be locked before mu
		lastRefresh time.Time
		syncutil.Mutex
	}

	mu struct {
		syncutil.Mutex
		cache partitionCache
	}
}

func NewPartitionAssignments(db isql.DB, queueName string) (*PartitionAssignments, error) {
	pa := &PartitionAssignments{
		db:             db,
		partitionTable: &partitionTable{queueName: queueName},
	}

	var partitions []Partition
	err := db.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
		var err error
		partitions, err = pa.partitionTable.ListPartitions(ctx, txn)
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to load initial partitions")
	}

	pa.mu.cache.Init(partitions)
	pa.refresh.lastRefresh = time.Now()

	return pa, nil
}

func (p *PartitionAssignments) maybeRefreshCache() error {
	// TODO handle deletions
	// TODO add a version mechanism to avoid races between write through updates and refereshes
	// TODO use a rangefeed instead of polling

	p.refresh.Lock()
	defer p.refresh.Unlock()

	if time.Since(p.refresh.lastRefresh) < 5*time.Second {
		return nil
	}

	var partitions []Partition
	err := p.db.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
		var err error
		partitions, err = p.partitionTable.ListPartitions(ctx, txn)
		return err
	})
	if err != nil {
		return err
	}

	updates := make(map[int64]Partition)
	for _, partition := range partitions {
		updates[partition.ID] = partition
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.cache.Update(updates)
	p.refresh.lastRefresh = time.Now()
	return nil
}

// RegisterSession registers a new session. The session may be assigned zero
// partitions if there are no unassigned partitions. If it is assigned no
// partitions, the caller can periodically call RefreshAssignment claim
// partitions if they become available.
func (p *PartitionAssignments) RegisterSession(
	ctx context.Context, session Session,
) (*Assignment, error) {
	if err := p.maybeRefreshCache(); err != nil {
		return nil, errors.Wrap(err, "refreshing partition cache")
	}

	var err error
	var done bool
	for !done {
		tryClaim, trySteal := func() (Partition, Partition) {
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.mu.cache.planRegister(session, p.mu.cache)
		}()
		switch {
		case !tryClaim.Empty():
			err, done = p.tryClaim(session, tryClaim)
			if err != nil {
				return nil, errors.Wrap(err, "claiming partition")
			}
		case !trySteal.Empty():
			err, done = p.trySteal(session, trySteal)
			if err != nil {
				return nil, errors.Wrap(err, "stealing partition")
			}
		default:
			done = true
		}
	}
	return p.constructAssignment(session), nil
}

func (p *PartitionAssignments) tryClaim(session Session, toClaim Partition) (error, bool) {
	var updates map[int64]Partition
	var done bool
	err := p.db.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
		done, updates = false, nil

		var err error
		updates, err = p.anyStale(ctx, txn, []Partition{toClaim})
		if err != nil || len(updates) != 0 {
			return err
		}

		updates = make(map[int64]Partition)
		toClaim.Session = session
		updates[toClaim.ID] = toClaim
		if err := p.partitionTable.UpdatePartition(ctx, txn, toClaim); err != nil {
			return err
		}

		done = true
		return nil
	})
	if err != nil {
		return err, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.cache.Update(updates)
	return nil, done
}

func (p *PartitionAssignments) trySteal(session Session, toSteal Partition) (error, bool) {
	var updates map[int64]Partition
	var done bool
	err := p.db.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
		done, updates = false, nil

		var err error
		updates, err = p.anyStale(ctx, txn, []Partition{toSteal})
		if err != nil || len(updates) != 0 {
			return err
		}

		updates = make(map[int64]Partition)
		toSteal.Successor = session
		updates[toSteal.ID] = toSteal
		if err := p.partitionTable.UpdatePartition(ctx, txn, toSteal); err != nil {
			return err
		}

		done = true
		return nil
	})
	if err != nil {
		return err, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.cache.Update(updates)
	return nil, done
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
	ctx context.Context, assignment *Assignment, caughtUp bool,
) (*Assignment, error) {
	if err := p.maybeRefreshCache(); err != nil {
		return nil, errors.Wrap(err, "refreshing partition cache")
	}

	var done bool
	var err error
	for !done {
		tryRelease, tryClaim, trySteal := func() ([]Partition, Partition, Partition) {
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.mu.cache.planAssignment(assignment.Session, caughtUp, p.mu.cache)
		}()
		switch {
		case len(tryRelease) != 0:
			err, done = p.tryRelease(assignment.Session, tryRelease)
			if err != nil {
				return nil, errors.Wrap(err, "releasing partition")
			}
		case !tryClaim.Empty():
			err, done = p.tryClaim(assignment.Session, tryClaim)
			if err != nil {
				return nil, errors.Wrap(err, "claiming partition")
			}
		case !trySteal.Empty():
			err, done = p.trySteal(assignment.Session, trySteal)
			if err != nil {
				return nil, errors.Wrap(err, "stealing partition")
			}
		default:
			stale := func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.mu.cache.isStale(assignment)
			}()
			if !stale {
				return nil, nil
			}
			done = true
		}
	}
	if err != nil {
		return nil, err
	}
	return p.constructAssignment(assignment.Session), nil
}

// anyStale checks if any of the provided partitions have become stale by
// comparing them with the current state in the database. Returns a map of
// partition ID to the updated partition that can be applied to the cache.
func (p *PartitionAssignments) anyStale(
	ctx context.Context, txn isql.Txn, partitions []Partition,
) (map[int64]Partition, error) {
	if len(partitions) == 0 {
		return make(map[int64]Partition), nil
	}

	// Extract partition IDs
	partitionIDs := make([]int64, len(partitions))
	for i, partition := range partitions {
		partitionIDs[i] = partition.ID
	}

	// Fetch current state from database
	currentPartitions, err := p.partitionTable.FetchPartitions(ctx, txn, partitionIDs)
	if err != nil {
		return nil, err
	}

	// Compare cached vs current state and collect stale partitions
	stalePartitions := make(map[int64]Partition)
	for _, cachedPartition := range partitions {
		currentPartition := currentPartitions[cachedPartition.ID]

		// If partition was deleted from database, mark it as empty in updates
		if currentPartition.Empty() {
			stalePartitions[cachedPartition.ID] = Partition{}
		} else if !cachedPartition.Equal(currentPartition) {
			// If partition has changed, include the updated version
			stalePartitions[cachedPartition.ID] = currentPartition
		}
	}

	return stalePartitions, nil
}

func (p *PartitionAssignments) tryRelease(session Session, toRelease []Partition) (error, bool) {
	var updates map[int64]Partition
	var done bool
	err := p.db.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
		done, updates = false, nil

		var err error
		updates, err = p.anyStale(ctx, txn, toRelease)
		if err != nil || len(updates) != 0 {
			return err
		}

		updates = make(map[int64]Partition)
		for _, partition := range toRelease {
			partition.Session = partition.Successor
			updates[partition.ID] = partition
			if err := p.partitionTable.UpdatePartition(ctx, txn, partition); err != nil {
				return err
			}
		}

		done = true
		return nil
	})
	if err != nil {
		return err, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.cache.Update(updates)
	return nil, done
}

func (p *PartitionAssignments) constructAssignment(session Session) *Assignment {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.cache.constructAssignment(session)
}

func (p *PartitionAssignments) UnregisterSession(ctx context.Context, session Session) error {
	var updates map[int64]Partition
	err := p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		updates, err = p.partitionTable.UnregisterSession(ctx, txn, session)
		return err
	})
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.cache.Update(updates)

	return nil
}
