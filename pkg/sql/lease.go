// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var errRenewLease = errors.New("renew lease on id")
var errReadOlderTableVersion = errors.New("read older table version from store")

// A lease stored in system.lease.
type storedTableLease struct {
	id         sqlbase.ID
	version    int
	expiration tree.DTimestamp
}

// tableVersionState holds the state for a table version. This includes
// the lease information for a table version.
// TODO(vivek): A node only needs to manage lease information on what it
// thinks is the latest version for a table descriptor.
type tableVersionState struct {
	// This descriptor is immutable and can be shared by many goroutines.
	// Care must be taken to not modify it.
	sqlbase.ImmutableTableDescriptor

	// The expiration time for the table version. A transaction with
	// timestamp T can use this table descriptor version iff
	// TableDescriptor.ModificationTime <= T < expiration
	//
	// The expiration time is either the expiration time of the lease
	// when a lease is associated with the table version, or the
	// ModificationTime of the next version when the table version
	// isn't associated with a lease.
	expiration hlc.Timestamp

	mu struct {
		syncutil.Mutex

		refcount int
		// Set if the node has a lease on this descriptor version.
		// Leases can only be held for the two latest versions of
		// a table descriptor. The latest version known to a node
		// (can be different than the current latest version in the store)
		// is always associated with a lease. The previous version known to
		// a node might not necessarily be associated with a lease.
		lease *storedTableLease
	}
}

func (s *tableVersionState) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stringLocked()
}

// stringLocked reads mu.refcount and thus needs to have mu held.
func (s *tableVersionState) stringLocked() string {
	return fmt.Sprintf("%d(%q) ver=%d:%s, refcount=%d", s.ID, s.Name, s.Version, s.expiration, s.mu.refcount)
}

// hasExpired checks if the table is too old to be used (by a txn operating)
// at the given timestamp
func (s *tableVersionState) hasExpired(timestamp hlc.Timestamp) bool {
	return s.expiration.LessEq(timestamp)
}

// hasValidExpiration checks that this table have a larger expiration than
// the existing one it is replacing. This can be used to check the
// monotonicity of the expiration times on a table at a particular version.
// The version is not explicitly checked here.
func (s *tableVersionState) hasValidExpiration(existing *tableVersionState) bool {
	return existing.expiration.Less(s.expiration)
}

func (s *tableVersionState) incRefcount() {
	s.mu.Lock()
	s.incRefcountLocked()
	s.mu.Unlock()
}

func (s *tableVersionState) incRefcountLocked() {
	s.mu.refcount++
	if log.V(2) {
		log.VEventf(context.TODO(), 2, "tableVersionState.incRef: %s", s.stringLocked())
	}
}

// The lease expiration stored in the database is of a different type.
// We've decided that it's too much work to change the type to
// hlc.Timestamp, so we're using this method to give us the stored
// type: tree.DTimestamp.
func storedLeaseExpiration(expiration hlc.Timestamp) tree.DTimestamp {
	return tree.DTimestamp{Time: timeutil.Unix(0, expiration.WallTime).Round(time.Microsecond)}
}

// LeaseStore implements the operations for acquiring and releasing leases and
// publishing a new version of a descriptor. Exported only for testing.
type LeaseStore struct {
	nodeIDContainer  *base.NodeIDContainer
	db               *kv.DB
	clock            *hlc.Clock
	internalExecutor sqlutil.InternalExecutor
	settings         *cluster.Settings

	// group is used for all calls made to acquireNodeLease to prevent
	// concurrent lease acquisitions from the store.
	group *singleflight.Group

	// leaseDuration is the mean duration a lease will be acquired for. The
	// actual duration is jittered using leaseJitterFraction. Jittering is done to
	// prevent multiple leases from being renewed simultaneously if they were all
	// acquired simultaneously.
	leaseDuration time.Duration
	// leaseJitterFraction is the factor that we use to randomly jitter the lease
	// duration when acquiring a new lease and the lease renewal timeout. The
	// range of the actual lease duration will be
	// [(1-leaseJitterFraction) * leaseDuration, (1+leaseJitterFraction) * leaseDuration]
	leaseJitterFraction float64
	// leaseRenewalTimeout is the time before a lease expires when
	// acquisition to renew the lease begins.
	leaseRenewalTimeout time.Duration

	testingKnobs LeaseStoreTestingKnobs
}

// jitteredLeaseDuration returns a randomly jittered duration from the interval
// [(1-leaseJitterFraction) * leaseDuration, (1+leaseJitterFraction) * leaseDuration].
func (s LeaseStore) jitteredLeaseDuration() time.Duration {
	return time.Duration(float64(s.leaseDuration) * (1 - s.leaseJitterFraction +
		2*s.leaseJitterFraction*rand.Float64()))
}

// acquire a lease on the most recent version of a table descriptor.
// If the lease cannot be obtained because the descriptor is in the process of
// being dropped or offline, the error will be of type inactiveTableError.
// The expiration time set for the lease > minExpiration.
func (s LeaseStore) acquire(
	ctx context.Context, minExpiration hlc.Timestamp, tableID sqlbase.ID,
) (*tableVersionState, error) {
	var table *tableVersionState
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Run the descriptor read as high-priority, thereby pushing any intents out
		// of its way. We don't want schema changes to prevent lease acquisitions;
		// we'd rather force them to refresh. Also this prevents deadlocks in cases
		// where the name resolution is triggered by the transaction doing the
		// schema change itself.
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		expiration := txn.ReadTimestamp()
		expiration.WallTime += int64(s.jitteredLeaseDuration())
		if expiration.LessEq(minExpiration) {
			// In the rare circumstances where expiration <= minExpiration
			// use an expiration based on the minExpiration to guarantee
			// a monotonically increasing expiration.
			expiration = minExpiration.Add(int64(time.Millisecond), 0)
		}

		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return err
		}
		if err := FilterTableState(tableDesc); err != nil {
			return err
		}
		if err := tableDesc.MaybeFillInDescriptor(ctx, txn); err != nil {
			return err
		}
		// Once the descriptor is set it is immutable and care must be taken
		// to not modify it.
		storedLease := &storedTableLease{
			id:         tableDesc.ID,
			version:    int(tableDesc.Version),
			expiration: storedLeaseExpiration(expiration),
		}
		table = &tableVersionState{
			ImmutableTableDescriptor: *sqlbase.NewImmutableTableDescriptor(*tableDesc),
			expiration:               expiration,
		}
		log.VEventf(ctx, 2, "LeaseStore acquired lease %+v", storedLease)
		table.mu.lease = storedLease

		// ValidateTable instead of Validate, even though we have a txn available,
		// so we don't block reads waiting for this table version.
		if err := table.ValidateTable(); err != nil {
			return err
		}

		nodeID := s.nodeIDContainer.Get()
		if nodeID == 0 {
			panic("zero nodeID")
		}

		// We use string interpolation here, instead of passing the arguments to
		// InternalExecutor.Exec() because we don't want to pay for preparing the
		// statement (which would happen if we'd pass arguments). Besides the
		// general cost of preparing, preparing this statement always requires a
		// read from the database for the special descriptor of a system table
		// (#23937).
		insertLease := fmt.Sprintf(
			`INSERT INTO system.public.lease ("descID", version, "nodeID", expiration) VALUES (%d, %d, %d, %s)`,
			storedLease.id, storedLease.version, nodeID, &storedLease.expiration,
		)
		count, err := s.internalExecutor.Exec(ctx, "lease-insert", txn, insertLease)
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.Errorf("%s: expected 1 result, found %d", insertLease, count)
		}
		return nil
	})
	if err == nil && s.testingKnobs.LeaseAcquiredEvent != nil {
		s.testingKnobs.LeaseAcquiredEvent(table.TableDescriptor, nil)
	}
	return table, err
}

// Release a previously acquired table descriptor. Never let this method
// read a table descriptor because it can be called while modifying a
// descriptor through a schema change before the schema change has committed
// that can result in a deadlock.
func (s LeaseStore) release(ctx context.Context, stopper *stop.Stopper, lease *storedTableLease) {
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = stopper.ShouldQuiesce()
	firstAttempt := true
	// This transaction is idempotent; the retry was put in place because of
	// NodeUnavailableErrors.
	for r := retry.Start(retryOptions); r.Next(); {
		log.VEventf(ctx, 2, "LeaseStore releasing lease %+v", lease)
		nodeID := s.nodeIDContainer.Get()
		if nodeID == 0 {
			panic("zero nodeID")
		}
		const deleteLease = `DELETE FROM system.public.lease ` +
			`WHERE ("descID", version, "nodeID", expiration) = ($1, $2, $3, $4)`
		count, err := s.internalExecutor.Exec(
			ctx,
			"lease-release",
			nil, /* txn */
			deleteLease,
			lease.id, lease.version, nodeID, &lease.expiration,
		)
		if err != nil {
			log.Warningf(ctx, "error releasing lease %q: %s", lease, err)
			firstAttempt = false
			continue
		}
		// We allow count == 0 after the first attempt.
		if count > 1 || (count == 0 && firstAttempt) {
			log.Warningf(ctx, "unexpected results while deleting lease %+v: "+
				"expected 1 result, found %d", lease, count)
		}

		if s.testingKnobs.LeaseReleasedEvent != nil {
			s.testingKnobs.LeaseReleasedEvent(
				lease.id, sqlbase.DescriptorVersion(lease.version), err)
		}
		break
	}
}

// WaitForOneVersion returns once there are no unexpired leases on the
// previous version of the table descriptor. It returns the current version.
// After returning there can only be versions of the descriptor >= to the
// returned version. Lease acquisition (see acquire()) maintains the
// invariant that no new leases for desc.Version-1 will be granted once
// desc.Version exists.
func (s LeaseStore) WaitForOneVersion(
	ctx context.Context, tableID sqlbase.ID, retryOpts retry.Options,
) (sqlbase.DescriptorVersion, error) {
	var tableDesc *sqlbase.TableDescriptor
	var err error
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		// Get the current version of the table descriptor non-transactionally.
		//
		// TODO(pmattis): Do an inconsistent read here?
		tableDesc, err = sqlbase.GetTableDescFromID(ctx, s.db, tableID)
		if err != nil {
			return 0, err
		}
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := s.clock.Now()
		tables := []IDVersion{NewIDVersionPrev(tableDesc)}
		count, err := CountLeases(ctx, s.internalExecutor, tables, now)
		if err != nil {
			return 0, err
		}
		if count == 0 {
			break
		}
		if count != lastCount {
			lastCount = count
			log.Infof(ctx, "waiting for %d leases to expire: desc=%v", count, tables)
		}
	}
	return tableDesc.Version, nil
}

var errDidntUpdateDescriptor = errors.New("didn't update the table descriptor")

// PublishMultiple updates multiple table descriptors, maintaining the invariant
// that there are at most two versions of each descriptor out in the wild at any
// time by first waiting for all nodes to be on the current (pre-update) version
// of the table desc.
//
// The update closure for all tables is called after the wait. The map argument
// is a map of the table descriptors with the IDs given in tableIDs, and the
// closure mutates those descriptors. The txn argument closure is intended to be
// used for updating jobs. Note that it can't be used for anything except
// writing to system tables, since we set the system config trigger to write the
// schema changes.
//
// The closure may be called multiple times if retries occur; make sure it does
// not have side effects.
//
// Returns the updated versions of the descriptors.
//
// TODO (lucy): Providing the txn for the update closure just to update a job
// is not ideal. There must be a better API for this.
func (s LeaseStore) PublishMultiple(
	ctx context.Context,
	tableIDs []sqlbase.ID,
	update func(*kv.Txn, map[sqlbase.ID]*sqlbase.MutableTableDescriptor) error,
	logEvent func(*kv.Txn) error,
) (map[sqlbase.ID]*sqlbase.ImmutableTableDescriptor, error) {
	errLeaseVersionChanged := errors.New("lease version changed")
	// Retry while getting errLeaseVersionChanged.
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		// Wait until there are no unexpired leases on the previous versions
		// of the tables.
		expectedVersions := make(map[sqlbase.ID]sqlbase.DescriptorVersion)
		for _, id := range tableIDs {
			expected, err := s.WaitForOneVersion(ctx, id, base.DefaultRetryOptions())
			if err != nil {
				return nil, err
			}
			expectedVersions[id] = expected
		}

		tableDescs := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
		// There should be only one version of the descriptor, but it's
		// a race now to update to the next version.
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			versions := make(map[sqlbase.ID]sqlbase.DescriptorVersion)
			descsToUpdate := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
			for _, id := range tableIDs {
				// Re-read the current versions of the table descriptor, this time
				// transactionally.
				var err error
				descsToUpdate[id], err = sqlbase.GetMutableTableDescFromID(ctx, txn, id)
				if err != nil {
					return err
				}

				if expectedVersions[id] != descsToUpdate[id].Version {
					// The version changed out from under us. Someone else must be
					// performing a schema change operation.
					if log.V(3) {
						log.Infof(ctx, "publish (version changed): %d != %d", expectedVersions[id], descsToUpdate[id].Version)
					}
					return errLeaseVersionChanged
				}

				versions[id] = descsToUpdate[id].Version
			}

			// This is to write the updated descriptors.
			if err := txn.SetSystemConfigTrigger(); err != nil {
				return err
			}

			// Run the update closure.
			if err := update(txn, descsToUpdate); err != nil {
				return err
			}
			for _, id := range tableIDs {
				if versions[id] != descsToUpdate[id].Version {
					return errors.Errorf("updated version to: %d, expected: %d",
						descsToUpdate[id].Version, versions[id])
				}

				if err := descsToUpdate[id].MaybeIncrementVersion(ctx, txn, s.settings); err != nil {
					return err
				}
				if err := descsToUpdate[id].ValidateTable(); err != nil {
					return err
				}

				tableDescs[id] = descsToUpdate[id]
			}

			b := txn.NewBatch()
			for tableID, tableDesc := range tableDescs {
				if err := writeDescToBatch(ctx, false /* kvTrace */, s.settings, b, tableID, tableDesc.TableDesc()); err != nil {
					return err
				}
			}
			if logEvent != nil {
				// If an event log is required for this update, ensure that the
				// descriptor change occurs first in the transaction. This is
				// necessary to ensure that the System configuration change is
				// gossiped. See the documentation for
				// transaction.SetSystemConfigTrigger() for more information.
				if err := txn.Run(ctx, b); err != nil {
					return err
				}
				if err := logEvent(txn); err != nil {
					return err
				}
				return txn.Commit(ctx)
			}
			// More efficient batching can be used if no event log message
			// is required.
			return txn.CommitInBatch(ctx, b)
		})

		switch err {
		case nil, errDidntUpdateDescriptor:
			immutTableDescs := make(map[sqlbase.ID]*ImmutableTableDescriptor)
			for id, tableDesc := range tableDescs {
				immutTableDescs[id] = sqlbase.NewImmutableTableDescriptor(tableDesc.TableDescriptor)
			}
			return immutTableDescs, nil
		case errLeaseVersionChanged:
			// will loop around to retry
		default:
			return nil, err
		}
	}

	panic("not reached")
}

// Publish updates a table descriptor. It also maintains the invariant that
// there are at most two versions of the descriptor out in the wild at any time
// by first waiting for all nodes to be on the current (pre-update) version of
// the table desc.
//
// The update closure is called after the wait, and it provides the new version
// of the descriptor to be written. In a multi-step schema operation, this
// update should perform a single step.
//
// The closure may be called multiple times if retries occur; make sure it does
// not have side effects.
//
// Returns the updated version of the descriptor.
// TODO (lucy): Maybe have the closure take a *kv.Txn to match
// PublishMultiple.
func (s LeaseStore) Publish(
	ctx context.Context,
	tableID sqlbase.ID,
	update func(*sqlbase.MutableTableDescriptor) error,
	logEvent func(*kv.Txn) error,
) (*sqlbase.ImmutableTableDescriptor, error) {
	tableIDs := []sqlbase.ID{tableID}
	updates := func(_ *kv.Txn, descs map[sqlbase.ID]*sqlbase.MutableTableDescriptor) error {
		desc, ok := descs[tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", tableID)
		}
		return update(desc)
	}

	results, err := s.PublishMultiple(ctx, tableIDs, updates, logEvent)
	if err != nil {
		return nil, err
	}
	return results[tableID], nil
}

// IDVersion represents a descriptor ID, version pair that are
// meant to map to a single immutable descriptor.
type IDVersion struct {
	// name only provided for pretty printing.
	name    string
	id      sqlbase.ID
	version sqlbase.DescriptorVersion
}

// NewIDVersionPrev returns an initialized IDVersion with the
// previous version of the descriptor.
func NewIDVersionPrev(desc *sqlbase.TableDescriptor) IDVersion {
	return IDVersion{name: desc.Name, id: desc.ID, version: desc.Version - 1}
}

// CountLeases returns the number of unexpired leases for a number of tables
// each at a particular version at a particular time.
func CountLeases(
	ctx context.Context, executor sqlutil.InternalExecutor, tables []IDVersion, at hlc.Timestamp,
) (int, error) {
	var whereClauses []string
	for _, t := range tables {
		whereClauses = append(whereClauses,
			fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
				t.id, t.version),
		)
	}

	stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME %s WHERE `,
		at.AsOfSystemTime()) +
		strings.Join(whereClauses, " OR ")
	values, err := executor.QueryRowEx(
		ctx, "count-leases", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt, at.GoTime(),
	)
	if err != nil {
		return 0, err
	}
	count := int(tree.MustBeDInt(values[0]))
	return count, nil
}

// Get the table descriptor valid for the expiration time from the store.
// We use a timestamp that is just less than the expiration time to read
// a version of the table descriptor. A tableVersionState with the
// expiration time set to expiration is returned.
//
// This returns an error when Replica.checkTSAboveGCThresholdRLocked()
// returns an error when the expiration timestamp is less than the storage
// layer GC threshold.
func (s LeaseStore) getForExpiration(
	ctx context.Context, expiration hlc.Timestamp, id sqlbase.ID,
) (*tableVersionState, error) {
	var table *tableVersionState
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		descKey := sqlbase.MakeDescMetadataKey(id)
		prevTimestamp := expiration.Prev()
		txn.SetFixedTimestamp(ctx, prevTimestamp)
		var desc sqlbase.Descriptor
		ts, err := txn.GetProtoTs(ctx, descKey, &desc)
		if err != nil {
			return err
		}
		tableDesc := desc.Table(ts)
		if tableDesc == nil {
			return sqlbase.ErrDescriptorNotFound
		}
		if prevTimestamp.LessEq(tableDesc.ModificationTime) {
			return errors.AssertionFailedf("unable to read table= (%d, %s)", id, expiration)
		}
		if err := tableDesc.MaybeFillInDescriptor(ctx, txn); err != nil {
			return err
		}
		// Create a tableVersionState with the table and without a lease.
		table = &tableVersionState{
			ImmutableTableDescriptor: *sqlbase.NewImmutableTableDescriptor(*tableDesc),
			expiration:               expiration,
		}
		return nil
	})
	return table, err
}

// leaseToken is an opaque token representing a lease. It's distinct from a
// lease to define restricted capabilities and prevent improper use of a lease
// where we instead have leaseTokens.
type leaseToken *tableVersionState

// tableSet maintains an ordered set of tableVersionState objects sorted
// by version. It supports addition and removal of elements, finding the
// table for a particular version, or finding the most recent table version.
// The order is maintained by insert and remove and there can only be a
// unique entry for a version. Only the last two versions can be leased,
// with the last one being the latest one which is always leased.
//
// Each entry represents a time span [ModificationTime, expiration)
// and can be used by a transaction iif:
// ModificationTime <= transaction.Timestamp < expiration.
type tableSet struct {
	data []*tableVersionState
}

func (l *tableSet) String() string {
	var buf bytes.Buffer
	for i, s := range l.data {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(fmt.Sprintf("%d:%d", s.Version, s.expiration.WallTime))
	}
	return buf.String()
}

func (l *tableSet) insert(s *tableVersionState) {
	i, match := l.findIndex(s.Version)
	if match {
		panic("unable to insert duplicate lease")
	}
	if i == len(l.data) {
		l.data = append(l.data, s)
		return
	}
	l.data = append(l.data, nil)
	copy(l.data[i+1:], l.data[i:])
	l.data[i] = s
}

func (l *tableSet) remove(s *tableVersionState) {
	i, match := l.findIndex(s.Version)
	if !match {
		panic(fmt.Sprintf("can't find lease to remove: %s", s))
	}
	l.data = append(l.data[:i], l.data[i+1:]...)
}

func (l *tableSet) find(version sqlbase.DescriptorVersion) *tableVersionState {
	if i, match := l.findIndex(version); match {
		return l.data[i]
	}
	return nil
}

func (l *tableSet) findIndex(version sqlbase.DescriptorVersion) (int, bool) {
	i := sort.Search(len(l.data), func(i int) bool {
		s := l.data[i]
		return s.Version >= version
	})
	if i < len(l.data) {
		s := l.data[i]
		if s.Version == version {
			return i, true
		}
	}
	return i, false
}

func (l *tableSet) findNewest() *tableVersionState {
	if len(l.data) == 0 {
		return nil
	}
	return l.data[len(l.data)-1]
}

func (l *tableSet) findVersion(version sqlbase.DescriptorVersion) *tableVersionState {
	if len(l.data) == 0 {
		return nil
	}
	// Find the index of the first lease with version > targetVersion.
	i := sort.Search(len(l.data), func(i int) bool {
		return l.data[i].Version > version
	})
	if i == 0 {
		return nil
	}
	// i-1 is the index of the newest lease for the previous version (the version
	// we're looking for).
	s := l.data[i-1]
	if s.Version == version {
		return s
	}
	return nil
}

type tableState struct {
	id      sqlbase.ID
	stopper *stop.Stopper

	// renewalInProgress is an atomic indicator for when a renewal for a
	// lease has begun. This is atomic to prevent multiple routines from
	// entering renewal initialization.
	renewalInProgress int32

	mu struct {
		syncutil.Mutex

		// table descriptors sorted by increasing version. This set always
		// contains a table descriptor version with a lease as the latest
		// entry. There may be more than one active lease when the system is
		// transitioning from one version of the descriptor to another or
		// when the node preemptively acquires a new lease for a version
		// when the old lease has not yet expired. In the latter case, a new
		// entry is created with the expiration time of the new lease and
		// the older entry is removed.
		active tableSet
		// Indicates that the descriptor has been, or is being, dropped or taken
		// offline.
		// If set, leases are released from the store as soon as their
		// refcount drops to 0, as opposed to waiting until they expire.
		// This flag will be unset by any subsequent lease acquisition, which can
		// happen after the table came back online again after having been taken
		// offline temporarily (as opposed to dropped).
		takenOffline bool
	}
}

// ensureVersion ensures that the latest version >= minVersion. It will
// check if the latest known version meets the criterion, or attempt to
// acquire a lease at the latest version with the hope that it meets
// the criterion.
func ensureVersion(
	ctx context.Context, tableID sqlbase.ID, minVersion sqlbase.DescriptorVersion, m *LeaseManager,
) error {
	if s := m.findNewest(tableID); s != nil && minVersion <= s.Version {
		return nil
	}

	if err := m.AcquireFreshestFromStore(ctx, tableID); err != nil {
		return err
	}

	if s := m.findNewest(tableID); s != nil && s.Version < minVersion {
		return errors.Errorf("version %d for table %s does not exist yet", minVersion, s.Name)
	}
	return nil
}

// findForTimestamp finds a table descriptor valid for the timestamp.
// In the most common case the timestamp passed to this method is close
// to the current time and in all likelihood the latest version of a
// table descriptor if valid is returned.
//
// This returns errRenewLease when there is no table descriptor cached
// or the latest descriptor version's ModificationTime satisfies the
// timestamp while it's expiration time doesn't satisfy the timestamp.
// This is an optimistic strategy betting that in all likelihood a
// higher layer renewing the lease on the descriptor and populating
// tableState will satisfy the timestamp on a subsequent call.
//
// In all other circumstances where a descriptor cannot be found for the
// timestamp errOlderReadTableVersion is returned requesting a higher layer
// to populate the tableState with a valid older version of the descriptor
// before calling.
//
// The refcount for the returned tableVersionState is incremented.
// It returns true if the descriptor returned is the known latest version
// of the descriptor.
func (t *tableState) findForTimestamp(
	ctx context.Context, timestamp hlc.Timestamp,
) (*tableVersionState, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Acquire a lease if no table descriptor exists in the cache.
	if len(t.mu.active.data) == 0 {
		return nil, false, errRenewLease
	}

	// Walk back the versions to find one that is valid for the timestamp.
	for i := len(t.mu.active.data) - 1; i >= 0; i-- {
		// Check to see if the ModificationTime is valid.
		if table := t.mu.active.data[i]; table.ModificationTime.LessEq(timestamp) {
			latest := i+1 == len(t.mu.active.data)
			if !table.hasExpired(timestamp) {
				// Existing valid table version.
				table.incRefcount()
				return table, latest, nil
			}

			if latest {
				// Renew the lease if the lease has expired
				// The latest descriptor always has a lease.
				return nil, false, errRenewLease
			}
			break
		}
	}

	return nil, false, errReadOlderTableVersion
}

// Read an older table descriptor version for the particular timestamp
// from the store. We unfortunately need to read more than one table
// version just so that we can set the expiration time on the descriptor
// properly.
//
// TODO(vivek): Future work:
// 1. Read multiple versions of a descriptor through one kv call.
// 2. Translate multiple simultaneous calls to this method into a single call
//    as is done for acquireNodeLease().
// 3. Figure out a sane policy on when these descriptors should be purged.
//    They are currently purged in PurgeOldVersions.
func (m *LeaseManager) readOlderVersionForTimestamp(
	ctx context.Context, tableID sqlbase.ID, timestamp hlc.Timestamp,
) ([]*tableVersionState, error) {
	expiration, done := func() (hlc.Timestamp, bool) {
		t := m.findTableState(tableID, false /* create */)
		t.mu.Lock()
		defer t.mu.Unlock()
		afterIdx := 0
		// Walk back the versions to find one that is valid for the timestamp.
		for i := len(t.mu.active.data) - 1; i >= 0; i-- {
			// Check to see if the ModificationTime is valid.
			if table := t.mu.active.data[i]; table.ModificationTime.LessEq(timestamp) {
				if timestamp.Less(table.expiration) {
					// Existing valid table version.
					return table.expiration, true
				}
				// We need a version after data[i], but before data[i+1].
				// We could very well use the timestamp to read the table
				// descriptor, but unfortunately we will not be able to assign
				// it a proper expiration time. Therefore, we read table
				// descriptors versions one by one from afterIdx back into the
				// past until we find a valid one.
				afterIdx = i + 1
				break
			}
		}

		if afterIdx == len(t.mu.active.data) {
			return hlc.Timestamp{}, true
		}

		// Read table descriptor versions one by one into the past until we
		// find a valid one. Every version is assigned an expiration time that
		// is the ModificationTime of the previous one read.
		return t.mu.active.data[afterIdx].ModificationTime, false
	}()
	if done {
		return nil, nil
	}

	// Read descriptors from the store.
	var versions []*tableVersionState
	for {
		table, err := m.LeaseStore.getForExpiration(ctx, expiration, tableID)
		if err != nil {
			return nil, err
		}
		versions = append(versions, table)
		if table.ModificationTime.LessEq(timestamp) {
			break
		}
		// Set the expiration time for the next table.
		expiration = table.ModificationTime
	}

	return versions, nil
}

// Insert table versions. The versions provided are not in
// any particular order.
func (m *LeaseManager) insertTableVersions(tableID sqlbase.ID, versions []*tableVersionState) {
	t := m.findTableState(tableID, false /* create */)
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, tableVersion := range versions {
		// Since we gave up the lock while reading the versions from
		// the store we have to ensure that no one else inserted the
		// same table version.
		table := t.mu.active.findVersion(tableVersion.Version)
		if table == nil {
			t.mu.active.insert(tableVersion)
		}
	}
}

// AcquireFreshestFromStore acquires a new lease from the store and
// inserts it into the active set. It guarantees that the lease returned is
// the one acquired after the call is made. Use this if the lease we want to
// get needs to see some descriptor updates that we know happened recently.
func (m *LeaseManager) AcquireFreshestFromStore(ctx context.Context, tableID sqlbase.ID) error {
	// Create tableState if needed.
	_ = m.findTableState(tableID, true /* create */)
	// We need to acquire a lease on a "fresh" descriptor, meaning that joining
	// a potential in-progress lease acquisition is generally not good enough.
	// If we are to join an in-progress acquisition, it needs to be an acquisition
	// initiated after this point.
	// So, we handle two cases:
	// 1. The first DoChan() call tells us that we didn't join an in-progress
	//     acquisition. Great, the lease that's being acquired is good.
	// 2. The first DoChan() call tells us that we did join an in-progress acq.
	//     We have to wait this acquisition out; it's not good for us. But any
	//     future acquisition is good, so the next time around the loop it doesn't
	//     matter if we initiate a request or join an in-progress one.
	// In both cases, we need to check if the lease we want is still valid because
	// lease acquisition is done without holding the tableState lock, so anything
	// can happen in between lease acquisition and us getting control again.
	attemptsMade := 0
	for {
		// Acquire a fresh table lease.
		didAcquire, err := acquireNodeLease(ctx, m, tableID)
		if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
			m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(LeaseAcquireFreshestBlock)
		}
		if err != nil {
			return err
		}

		if didAcquire {
			// Case 1: we didn't join an in-progress call and the lease is still
			// valid.
			break
		} else if attemptsMade > 1 {
			// Case 2: more than one acquisition has happened and the lease is still
			// valid.
			break
		}
		attemptsMade++
	}
	return nil
}

// upsertLocked inserts a lease for a particular table version.
// If an existing lease exists for the table version it replaces
// it and returns it.
func (t *tableState) upsertLocked(
	ctx context.Context, table *tableVersionState,
) (*storedTableLease, error) {
	s := t.mu.active.find(table.Version)
	if s == nil {
		if t.mu.active.findNewest() != nil {
			log.Infof(ctx, "new lease: %s", table)
		}
		t.mu.active.insert(table)
		return nil, nil
	}

	// The table is replacing an existing one at the same version.
	if !table.hasValidExpiration(s) {
		// This is a violation of an invariant and can actually not
		// happen. We return an error here to aid in further investigations.
		return nil, errors.Errorf("lease expiration monotonicity violation, (%s) vs (%s)", s, table)
	}

	s.mu.Lock()
	table.mu.Lock()
	// subsume the refcount of the older lease. This is permitted because
	// the new lease has a greater expiration than the older lease and
	// any transaction using the older lease can safely use a deadline set
	// to the older lease's expiration even though the older lease is
	// released! This is because the new lease is valid at the same table
	// version at a greater expiration.
	table.mu.refcount += s.mu.refcount
	s.mu.refcount = 0
	l := s.mu.lease
	s.mu.lease = nil
	if log.V(2) {
		log.VEventf(ctx, 2, "replaced lease: %s with %s", s.stringLocked(), table.stringLocked())
	}
	table.mu.Unlock()
	s.mu.Unlock()
	t.mu.active.remove(s)
	t.mu.active.insert(table)
	return l, nil
}

// removeInactiveVersions removes inactive versions in t.mu.active.data with refcount 0.
// t.mu must be locked. It returns table version state that need to be released.
func (t *tableState) removeInactiveVersions() []*storedTableLease {
	var leases []*storedTableLease
	// A copy of t.mu.active.data must be made since t.mu.active.data will be changed
	// within the loop.
	for _, table := range append([]*tableVersionState(nil), t.mu.active.data...) {
		func() {
			table.mu.Lock()
			defer table.mu.Unlock()
			if table.mu.refcount == 0 {
				t.mu.active.remove(table)
				if l := table.mu.lease; l != nil {
					table.mu.lease = nil
					leases = append(leases, l)
				}
			}
		}()
	}
	return leases
}

// If the lease cannot be obtained because the descriptor is in the process of
// being dropped or offline, the error will be of type inactiveTableError.
// The boolean returned is true if this call was actually responsible for the
// lease acquisition.
func acquireNodeLease(ctx context.Context, m *LeaseManager, id sqlbase.ID) (bool, error) {
	var toRelease *storedTableLease
	resultChan, didAcquire := m.group.DoChan(fmt.Sprintf("acquire%d", id), func() (interface{}, error) {
		// Note that we use a new `context` here to avoid a situation where a cancellation
		// of the first context cancels other callers to the `acquireNodeLease()` method,
		// because of its use of `singleflight.Group`. See issue #41780 for how this has
		// happened.
		newCtx, cancel := m.stopper.WithCancelOnQuiesce(logtags.WithTags(context.Background(), logtags.FromContext(ctx)))
		defer cancel()
		if m.isDraining() {
			return nil, errors.New("cannot acquire lease when draining")
		}
		newest := m.findNewest(id)
		var minExpiration hlc.Timestamp
		if newest != nil {
			minExpiration = newest.expiration
		}
		table, err := m.LeaseStore.acquire(newCtx, minExpiration, id)
		if err != nil {
			return nil, err
		}
		t := m.findTableState(id, false /* create */)
		t.mu.Lock()
		t.mu.takenOffline = false
		defer t.mu.Unlock()
		toRelease, err = t.upsertLocked(newCtx, table)
		if err != nil {
			return nil, err
		}
		m.tableNames.insert(table)
		if toRelease != nil {
			releaseLease(toRelease, m)
		}
		return leaseToken(table), nil
	})
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case result := <-resultChan:
		if result.Err != nil {
			return false, result.Err
		}
	}
	return didAcquire, nil
}

// release returns a tableVersionState that needs to be released from
// the store.
func (t *tableState) release(
	table *sqlbase.ImmutableTableDescriptor, removeOnceDereferenced bool,
) (*storedTableLease, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.mu.active.find(table.Version)
	if s == nil {
		return nil, errors.Errorf("table %d version %d not found", table.ID, table.Version)
	}
	// Decrements the refcount and returns true if the lease has to be removed
	// from the store.
	decRefcount := func(s *tableVersionState) *storedTableLease {
		// Figure out if we'd like to remove the lease from the store asap (i.e.
		// when the refcount drops to 0). If so, we'll need to mark the lease as
		// invalid.
		removeOnceDereferenced = removeOnceDereferenced ||
			// Release from the store if the descriptor has been dropped or taken
			// offline.
			t.mu.takenOffline ||
			// Release from the store if the lease is not for the latest
			// version; only leases for the latest version can be acquired.
			s != t.mu.active.findNewest()

		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.refcount--
		if log.V(2) {
			log.VEventf(context.TODO(), 2, "release: %s", s.stringLocked())
		}
		if s.mu.refcount < 0 {
			panic(fmt.Sprintf("negative ref count: %s", s))
		}

		if s.mu.refcount == 0 && s.mu.lease != nil && removeOnceDereferenced {
			l := s.mu.lease
			s.mu.lease = nil
			return l
		}
		return nil
	}
	if l := decRefcount(s); l != nil {
		t.mu.active.remove(s)
		return l, nil
	}
	return nil, nil
}

// releaseLease from store.
func releaseLease(lease *storedTableLease, m *LeaseManager) {
	ctx := context.TODO()
	if m.isDraining() {
		// Release synchronously to guarantee release before exiting.
		m.LeaseStore.release(ctx, m.stopper, lease)
		return
	}

	// Release to the store asynchronously, without the tableState lock.
	if err := m.stopper.RunAsyncTask(
		ctx, "sql.tableState: releasing descriptor lease",
		func(ctx context.Context) {
			m.LeaseStore.release(ctx, m.stopper, lease)
		}); err != nil {
		log.Warningf(ctx, "error: %s, not releasing lease: %q", err, lease)
	}
}

// purgeOldVersions removes old unused table descriptor versions older than
// minVersion and releases any associated leases.
// If takenOffline is set, minVersion is ignored; no lease is acquired and all
// existing unused versions are removed. The table is further marked dropped,
// which will cause existing in-use leases to be eagerly released once
// they're not in use any more.
// If t has no active leases, nothing is done.
func purgeOldVersions(
	ctx context.Context,
	db *kv.DB,
	id sqlbase.ID,
	takenOffline bool,
	minVersion sqlbase.DescriptorVersion,
	m *LeaseManager,
) error {
	t := m.findTableState(id, false /*create*/)
	if t == nil {
		return nil
	}
	t.mu.Lock()
	empty := len(t.mu.active.data) == 0
	t.mu.Unlock()
	if empty {
		// We don't currently have a version on this table, so no need to refresh
		// anything.
		return nil
	}

	removeInactives := func(takenOffline bool) {
		t.mu.Lock()
		t.mu.takenOffline = takenOffline
		leases := t.removeInactiveVersions()
		t.mu.Unlock()
		for _, l := range leases {
			releaseLease(l, m)
		}
	}

	if takenOffline {
		removeInactives(takenOffline)
		return nil
	}

	if err := ensureVersion(ctx, id, minVersion, m); err != nil {
		return err
	}

	// Acquire a refcount on the table on the latest version to maintain an
	// active lease, so that it doesn't get released when removeInactives()
	// is called below. Release this lease after calling removeInactives().
	table, _, err := t.findForTimestamp(ctx, m.clock.Now())
	if _, ok := err.(*inactiveTableError); ok || err == nil {
		isInactive := ok
		removeInactives(isInactive)
		if table != nil {
			s, err := t.release(&table.ImmutableTableDescriptor, m.removeOnceDereferenced())
			if err != nil {
				return err
			}
			if s != nil {
				releaseLease(s, m)
			}
			return nil
		}
		return nil
	}
	return err
}

// maybeQueueLeaseRenewal queues a lease renewal if there is not already a lease
// renewal in progress.
func (t *tableState) maybeQueueLeaseRenewal(
	ctx context.Context, m *LeaseManager, tableID sqlbase.ID, tableName string,
) error {
	if !atomic.CompareAndSwapInt32(&t.renewalInProgress, 0, 1) {
		return nil
	}

	// Start the renewal. When it finishes, it will reset t.renewalInProgress.
	return t.stopper.RunAsyncTask(context.Background(),
		"lease renewal", func(ctx context.Context) {
			var cleanup func()
			ctx, cleanup = tracing.EnsureContext(ctx, m.ambientCtx.Tracer, "lease renewal")
			defer cleanup()
			t.startLeaseRenewal(ctx, m, tableID, tableName)
		})
}

// startLeaseRenewal starts a singleflight.Group to acquire a lease.
// This function blocks until lease acquisition completes.
// t.renewalInProgress must be set to 1 before calling.
func (t *tableState) startLeaseRenewal(
	ctx context.Context, m *LeaseManager, tableID sqlbase.ID, tableName string,
) {
	log.VEventf(ctx, 1,
		"background lease renewal beginning for tableID=%d tableName=%q",
		tableID, tableName)
	if _, err := acquireNodeLease(ctx, m, tableID); err != nil {
		log.Errorf(ctx,
			"background lease renewal for tableID=%d tableName=%q failed: %s",
			tableID, tableName, err)
	} else {
		log.VEventf(ctx, 1,
			"background lease renewal finished for tableID=%d tableName=%q",
			tableID, tableName)
	}
	atomic.StoreInt32(&t.renewalInProgress, 0)
}

// LeaseAcquireBlockType is the type of blocking result event when
// calling LeaseAcquireResultBlockEvent.
type LeaseAcquireBlockType int

const (
	// LeaseAcquireBlock denotes the LeaseAcquireResultBlockEvent is
	// coming from tableState.acquire().
	LeaseAcquireBlock LeaseAcquireBlockType = iota
	// LeaseAcquireFreshestBlock denotes the LeaseAcquireResultBlockEvent is
	// from tableState.acquireFreshestFromStore().
	LeaseAcquireFreshestBlock
)

// LeaseStoreTestingKnobs contains testing knobs.
type LeaseStoreTestingKnobs struct {
	// Called after a lease is removed from the store, with any operation error.
	// See LeaseRemovalTracker.
	LeaseReleasedEvent func(id sqlbase.ID, version sqlbase.DescriptorVersion, err error)
	// Called after a lease is acquired, with any operation error.
	LeaseAcquiredEvent func(table sqlbase.TableDescriptor, err error)
	// Called before waiting on a results from a DoChan call of acquireNodeLease
	// in tableState.acquire() and tableState.acquireFreshestFromStore().
	LeaseAcquireResultBlockEvent func(leaseBlockType LeaseAcquireBlockType)
	// RemoveOnceDereferenced forces leases to be removed
	// as soon as they are dereferenced.
	RemoveOnceDereferenced bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*LeaseStoreTestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = &LeaseStoreTestingKnobs{}

// LeaseManagerTestingKnobs contains test knobs.
type LeaseManagerTestingKnobs struct {
	// A callback called when a gossip update is received, before the leases are
	// refreshed. Careful when using this to block for too long - you can block
	// all the gossip users in the system. If it returns an error the gossip
	// update is ignored.
	GossipUpdateEvent func(*config.SystemConfig) error
	// A callback called after the leases are refreshed as a result of a gossip update.
	TestingLeasesRefreshedEvent func(*config.SystemConfig)
	// To disable the deletion of orphaned leases at server startup.
	DisableDeleteOrphanedLeases bool
	LeaseStoreTestingKnobs      LeaseStoreTestingKnobs
}

var _ base.ModuleTestingKnobs = &LeaseManagerTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*LeaseManagerTestingKnobs) ModuleTestingKnobs() {}

type tableNameCacheKey struct {
	dbID                sqlbase.ID
	schemaID            sqlbase.ID
	normalizeTabledName string
}

// tableNameCache is a cache of table name -> latest table version mappings.
// The LeaseManager updates the cache every time a lease is acquired or released
// from the store. The cache maintains the latest version for each table name.
// All methods are thread-safe.
type tableNameCache struct {
	mu     syncutil.Mutex
	tables map[tableNameCacheKey]*tableVersionState
}

// Resolves a (database ID, table name) to the table descriptor's ID.
// Returns a valid tableVersionState for the table with that name,
// if the name had been previously cached and the cache has a table
// version that has not expired. Returns nil otherwise.
// This method handles normalizing the table name.
// The table's refcount is incremented before returning, so the caller
// is responsible for releasing it to the leaseManager.
func (c *tableNameCache) get(
	dbID sqlbase.ID, schemaID sqlbase.ID, tableName string, timestamp hlc.Timestamp,
) *tableVersionState {
	c.mu.Lock()
	table, ok := c.tables[makeTableNameCacheKey(dbID, schemaID, tableName)]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	table.mu.Lock()
	if table.mu.lease == nil {
		table.mu.Unlock()
		// This get() raced with a release operation. Remove this cache
		// entry if needed.
		c.remove(table)
		return nil
	}

	defer table.mu.Unlock()

	if !nameMatchesTable(
		&table.ImmutableTableDescriptor.TableDescriptor,
		dbID,
		schemaID,
		tableName,
	) {
		panic(fmt.Sprintf("Out of sync entry in the name cache. "+
			"Cache entry: %d.%q -> %d. Lease: %d.%q.",
			dbID, tableName, table.ID, table.ParentID, table.Name))
	}

	// Expired table. Don't hand it out.
	if table.hasExpired(timestamp) {
		return nil
	}

	table.incRefcountLocked()
	return table
}

func (c *tableNameCache) insert(table *tableVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeTableNameCacheKey(table.ParentID, table.GetParentSchemaID(), table.Name)
	existing, ok := c.tables[key]
	if !ok {
		c.tables[key] = table
		return
	}
	// If we already have a lease in the cache for this name, see if this one is
	// better (higher version or later expiration).
	if table.Version > existing.Version ||
		(table.Version == existing.Version && table.hasValidExpiration(existing)) {
		// Overwrite the old table. The new one is better. From now on, we want
		// clients to use the new one.
		c.tables[key] = table
	}
}

func (c *tableNameCache) remove(table *tableVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeTableNameCacheKey(table.ParentID, table.GetParentSchemaID(), table.Name)
	existing, ok := c.tables[key]
	if !ok {
		// Table for lease not found in table name cache. This can happen if we had
		// a more recent lease on the table in the tableNameCache, then the table
		// gets dropped, then the more recent lease is remove()d - which clears the
		// cache.
		return
	}
	// If this was the lease that the cache had for the table name, remove it.
	// If the cache had some other table, this remove is a no-op.
	if existing == table {
		delete(c.tables, key)
	}
}

func makeTableNameCacheKey(
	dbID sqlbase.ID, schemaID sqlbase.ID, tableName string,
) tableNameCacheKey {
	return tableNameCacheKey{dbID, schemaID, tableName}
}

// LeaseManager manages acquiring and releasing per-table leases. It also
// handles resolving table names to descriptor IDs. The leases are managed
// internally with a table descriptor and expiration time exported by the
// API. The table descriptor acquired needs to be released. A transaction
// can use a table descriptor as long as its timestamp is within the
// validity window for the descriptor:
// descriptor.ModificationTime <= txn.Timestamp < expirationTime
//
// Exported only for testing.
//
// The locking order is:
// LeaseManager.mu > tableState.mu > tableNameCache.mu > tableVersionState.mu
type LeaseManager struct {
	LeaseStore
	mu struct {
		syncutil.Mutex
		tables map[sqlbase.ID]*tableState
	}

	draining atomic.Value

	// tableNames is a cache for name -> id mappings. A mapping for the cache
	// should only be used if we currently have an active lease on the respective
	// id; otherwise, the mapping may well be stale.
	// Not protected by mu.
	tableNames   tableNameCache
	testingKnobs LeaseManagerTestingKnobs
	ambientCtx   log.AmbientContext
	stopper      *stop.Stopper
	sem          *quotapool.IntPool
}

const leaseConcurrencyLimit = 5

// NewLeaseManager creates a new LeaseManager.
//
// internalExecutor can be nil to help bootstrapping, but then it needs to be set via
// SetInternalExecutor before the LeaseManager is used.
//
// stopper is used to run async tasks. Can be nil in tests.
func NewLeaseManager(
	ambientCtx log.AmbientContext,
	nodeIDContainer *base.NodeIDContainer,
	db *kv.DB,
	clock *hlc.Clock,
	internalExecutor sqlutil.InternalExecutor,
	settings *cluster.Settings,
	testingKnobs LeaseManagerTestingKnobs,
	stopper *stop.Stopper,
	cfg *base.LeaseManagerConfig,
) *LeaseManager {
	lm := &LeaseManager{
		LeaseStore: LeaseStore{
			nodeIDContainer:     nodeIDContainer,
			db:                  db,
			clock:               clock,
			internalExecutor:    internalExecutor,
			settings:            settings,
			group:               &singleflight.Group{},
			leaseDuration:       cfg.TableDescriptorLeaseDuration,
			leaseJitterFraction: cfg.TableDescriptorLeaseJitterFraction,
			leaseRenewalTimeout: cfg.TableDescriptorLeaseRenewalTimeout,
			testingKnobs:        testingKnobs.LeaseStoreTestingKnobs,
		},
		testingKnobs: testingKnobs,
		tableNames: tableNameCache{
			tables: make(map[tableNameCacheKey]*tableVersionState),
		},
		ambientCtx: ambientCtx,
		stopper:    stopper,
		sem:        quotapool.NewIntPool("lease manager", leaseConcurrencyLimit),
	}
	lm.stopper.AddCloser(lm.sem.Closer("stopper"))
	lm.mu.tables = make(map[sqlbase.ID]*tableState)

	lm.draining.Store(false)
	return lm
}

// SetInternalExecutor has to be called if a nil execCfg was passed to NewLeaseManager.
func (m *LeaseManager) SetInternalExecutor(executor sqlutil.InternalExecutor) {
	m.internalExecutor = executor
}

func nameMatchesTable(
	table *sqlbase.TableDescriptor, dbID sqlbase.ID, schemaID sqlbase.ID, tableName string,
) bool {
	return table.ParentID == dbID && table.Name == tableName &&
		table.GetParentSchemaID() == schemaID
}

// findNewest returns the newest table version state for the tableID.
func (m *LeaseManager) findNewest(tableID sqlbase.ID) *tableVersionState {
	t := m.findTableState(tableID, false /* create */)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.active.findNewest()
}

// AcquireByName returns a table version for the specified table valid for
// the timestamp. It returns the table descriptor and a expiration time.
// A transaction using this descriptor must ensure that its
// commit-timestamp < expiration-time. Care must be taken to not modify
// the returned descriptor. Renewal of a lease may begin in the
// background. Renewal is done in order to prevent blocking on future
// acquisitions.
//
// Known limitation: AcquireByName() calls Acquire() and therefore suffers
// from the same limitation as Acquire (See Acquire). AcquireByName() is
// unable to function correctly on a timestamp less than the timestamp
// of a transaction with a DROP/TRUNCATE on a table. The limitation in
// the face of a DROP follows directly from the limitation on Acquire().
// A TRUNCATE is implemented by changing the name -> id mapping for a table
// and by dropping the descriptor with the old id. While AcquireByName
// can use the timestamp and get the correct name->id  mapping at a
// timestamp, it uses Acquire() to get a descriptor with the corresponding
// id and fails because the id has been dropped by the TRUNCATE.
func (m *LeaseManager) AcquireByName(
	ctx context.Context,
	timestamp hlc.Timestamp,
	dbID sqlbase.ID,
	schemaID sqlbase.ID,
	tableName string,
) (*sqlbase.ImmutableTableDescriptor, hlc.Timestamp, error) {
	// Check if we have cached an ID for this name.
	tableVersion := m.tableNames.get(dbID, schemaID, tableName, timestamp)
	if tableVersion != nil {
		if tableVersion.ModificationTime.LessEq(timestamp) {
			// If this lease is nearly expired, ensure a renewal is queued.
			durationUntilExpiry := time.Duration(tableVersion.expiration.WallTime - timestamp.WallTime)
			if durationUntilExpiry < m.LeaseStore.leaseRenewalTimeout {
				if t := m.findTableState(tableVersion.ID, false /* create */); t != nil {
					if err := t.maybeQueueLeaseRenewal(
						ctx, m, tableVersion.ID, tableName); err != nil {
						return nil, hlc.Timestamp{}, err
					}
				}
			}
			return &tableVersion.ImmutableTableDescriptor, tableVersion.expiration, nil
		}
		if err := m.Release(&tableVersion.ImmutableTableDescriptor); err != nil {
			return nil, hlc.Timestamp{}, err
		}
		// Return a valid table descriptor for the timestamp.
		table, expiration, err := m.Acquire(ctx, timestamp, tableVersion.ID)
		if err != nil {
			return nil, hlc.Timestamp{}, err
		}
		return table, expiration, nil
	}

	// We failed to find something in the cache, or what we found is not
	// guaranteed to be valid by the time we use it because we don't have a
	// lease with at least a bit of lifetime left in it. So, we do it the hard
	// way: look in the database to resolve the name, then acquire a new table.
	var err error
	tableID, err := m.resolveName(ctx, timestamp, dbID, schemaID, tableName)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	table, expiration, err := m.Acquire(ctx, timestamp, tableID)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	if !nameMatchesTable(&table.TableDescriptor, dbID, schemaID, tableName) {
		// We resolved name `tableName`, but the lease has a different name in it.
		// That can mean two things. Assume the table is being renamed from A to B.
		// a) `tableName` is A. The transaction doing the RENAME committed (so the
		// descriptor has been updated to B), but its schema changer has not
		// finished yet. B is the new name of the table, queries should use that. If
		// we already had a lease with name A, we would've allowed to use it (but we
		// don't, otherwise the cache lookup above would've given it to us).  Since
		// we don't, let's not allow A to be used, given that the lease now has name
		// B in it. It'd be sketchy to allow A to be used with an inconsistent name
		// in the table.
		//
		// b) `tableName` is B. Like in a), the transaction doing the RENAME
		// committed (so the descriptor has been updated to B), but its schema
		// change has not finished yet. We still had a valid lease with name A in
		// it. What to do, what to do? We could allow name B to be used, but who
		// knows what consequences that would have, since its not consistent with
		// the table. We could say "table B not found", but that means that, until
		// the next gossip update, this node would not service queries for this
		// table under the name B. That's no bueno, as B should be available to be
		// used immediately after the RENAME transaction has committed.
		// The problem is that we have a lease that we know is stale (the descriptor
		// in the DB doesn't necessarily have a new version yet, but it definitely
		// has a new name). So, lets force getting a fresh table.
		// This case (modulo the "committed" part) also applies when the txn doing a
		// RENAME had a lease on the old name, and then tries to use the new name
		// after the RENAME statement.
		//
		// How do we disambiguate between the a) and b)? We get a fresh lease on
		// the descriptor, as required by b), and then we'll know if we're trying to
		// resolve the current or the old name.
		//
		// TODO(vivek): check if the entire above comment is indeed true. Review the
		// use of nameMatchesTable() throughout this function.
		if err := m.Release(table); err != nil {
			log.Warningf(ctx, "error releasing lease: %s", err)
		}
		if err := m.AcquireFreshestFromStore(ctx, tableID); err != nil {
			return nil, hlc.Timestamp{}, err
		}
		table, expiration, err = m.Acquire(ctx, timestamp, tableID)
		if err != nil {
			return nil, hlc.Timestamp{}, err
		}
		if !nameMatchesTable(&table.TableDescriptor, dbID, schemaID, tableName) {
			// If the name we had doesn't match the newest descriptor in the DB, then
			// we're trying to use an old name.
			if err := m.Release(table); err != nil {
				log.Warningf(ctx, "error releasing lease: %s", err)
			}
			return nil, hlc.Timestamp{}, sqlbase.ErrDescriptorNotFound
		}
	}
	return table, expiration, nil
}

// resolveName resolves a table name to a descriptor ID at a particular
// timestamp by looking in the database. If the mapping is not found,
// sqlbase.ErrDescriptorNotFound is returned.
func (m *LeaseManager) resolveName(
	ctx context.Context,
	timestamp hlc.Timestamp,
	dbID sqlbase.ID,
	schemaID sqlbase.ID,
	tableName string,
) (sqlbase.ID, error) {
	id := sqlbase.InvalidID
	if err := m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Run the name lookup as high-priority, thereby pushing any intents out of
		// its way. We don't want schema changes to prevent name resolution/lease
		// acquisitions; we'd rather force them to refresh. Also this prevents
		// deadlocks in cases where the name resolution is triggered by the
		// transaction doing the schema change itself.
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		txn.SetFixedTimestamp(ctx, timestamp)
		var found bool
		var err error
		found, id, err = sqlbase.LookupObjectID(ctx, txn, dbID, schemaID, tableName)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		return nil
	}); err != nil {
		return id, err
	}
	if id == sqlbase.InvalidID {
		return id, sqlbase.ErrDescriptorNotFound
	}
	return id, nil
}

// Acquire acquires a read lease for the specified table ID valid for
// the timestamp. It returns the table descriptor and a expiration time.
// A transaction using this descriptor must ensure that its
// commit-timestamp < expiration-time. Care must be taken to not modify
// the returned descriptor.
//
// Known limitation: Acquire() can return an error after the table with
// the tableID has been dropped. This is true even when using a timestamp
// less than the timestamp of the DROP command. This is because Acquire
// can only return an older version of a descriptor if the latest version
// can be leased; as it stands a dropped table cannot be leased.
func (m *LeaseManager) Acquire(
	ctx context.Context, timestamp hlc.Timestamp, tableID sqlbase.ID,
) (*sqlbase.ImmutableTableDescriptor, hlc.Timestamp, error) {
	for {
		t := m.findTableState(tableID, true /*create*/)
		table, latest, err := t.findForTimestamp(ctx, timestamp)
		if err == nil {
			// If the latest lease is nearly expired, ensure a renewal is queued.
			if latest {
				durationUntilExpiry := time.Duration(table.expiration.WallTime - timestamp.WallTime)
				if durationUntilExpiry < m.LeaseStore.leaseRenewalTimeout {
					if err := t.maybeQueueLeaseRenewal(ctx, m, tableID, table.Name); err != nil {
						return nil, hlc.Timestamp{}, err
					}
				}
			}
			return &table.ImmutableTableDescriptor, table.expiration, nil
		}
		switch err {
		case errRenewLease:
			// Renew lease and retry. This will block until the lease is acquired.
			if _, errLease := acquireNodeLease(ctx, m, tableID); errLease != nil {
				return nil, hlc.Timestamp{}, errLease
			}
			if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
				m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(LeaseAcquireBlock)
			}

		case errReadOlderTableVersion:
			// Read old table versions from the store. This can block while reading
			// old table versions from the store.
			versions, errRead := m.readOlderVersionForTimestamp(ctx, tableID, timestamp)
			if errRead != nil {
				return nil, hlc.Timestamp{}, errRead
			}
			m.insertTableVersions(tableID, versions)

		default:
			return nil, hlc.Timestamp{}, err
		}
	}
}

// Release releases a previously acquired table.
func (m *LeaseManager) Release(desc *sqlbase.ImmutableTableDescriptor) error {
	t := m.findTableState(desc.ID, false /* create */)
	if t == nil {
		return errors.Errorf("table %d not found", desc.ID)
	}
	// TODO(pmattis): Can/should we delete from LeaseManager.tables if the
	// tableState becomes empty?
	// TODO(andrei): I think we never delete from LeaseManager.tables... which
	// could be bad if a lot of tables keep being created. I looked into cleaning
	// up a bit, but it seems tricky to do with the current locking which is split
	// between LeaseManager and tableState.
	l, err := t.release(desc, m.removeOnceDereferenced())
	if err != nil {
		return err
	}
	if l != nil {
		releaseLease(l, m)
	}
	return nil
}

// removeOnceDereferenced returns true if the LeaseManager thinks
// a tableVersionState can be removed after its refcount goes to 0.
func (m *LeaseManager) removeOnceDereferenced() bool {
	return m.LeaseStore.testingKnobs.RemoveOnceDereferenced ||
		// Release from the store if the LeaseManager is draining.
		m.isDraining()
}

func (m *LeaseManager) isDraining() bool {
	return m.draining.Load().(bool)
}

// SetDraining (when called with 'true') removes all inactive leases. Any leases
// that are active will be removed once the lease's reference count drops to 0.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (m *LeaseManager) SetDraining(drain bool, reporter func(int, string)) {
	m.draining.Store(drain)
	if !drain {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.mu.tables {
		t.mu.Lock()
		leases := t.removeInactiveVersions()
		t.mu.Unlock()
		for _, l := range leases {
			releaseLease(l, m)
		}
		if reporter != nil {
			// Report progress through the Drain RPC.
			reporter(len(leases), "table leases")
		}
	}
}

// If create is set, cache and stopper need to be set as well.
func (m *LeaseManager) findTableState(tableID sqlbase.ID, create bool) *tableState {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.mu.tables[tableID]
	if t == nil && create {
		t = &tableState{id: tableID, stopper: m.stopper}
		m.mu.tables[tableID] = t
	}
	return t
}

// RefreshLeases starts a goroutine that refreshes the lease manager
// leases for tables received in the latest system configuration via gossip.
func (m *LeaseManager) RefreshLeases(s *stop.Stopper, db *kv.DB, g *gossip.Gossip) {
	ctx := context.TODO()
	s.RunWorker(ctx, func(ctx context.Context) {
		descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
		cfgFilter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)
		gossipUpdateC := g.RegisterSystemConfigChannel()
		for {
			select {
			case <-gossipUpdateC:
				cfg := g.GetSystemConfig()
				if m.testingKnobs.GossipUpdateEvent != nil {
					if err := m.testingKnobs.GossipUpdateEvent(cfg); err != nil {
						break
					}
				}
				// Read all tables and their versions
				if log.V(2) {
					log.Info(ctx, "received a new config; will refresh leases")
				}

				cfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor sqlbase.Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf(ctx, "%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						return
					}
					switch union := descriptor.Union.(type) {
					case *sqlbase.Descriptor_Table:
						table := union.Table
						// Note that we don't need to "fill in" the descriptor here. Nobody
						// actually reads the table, but it's necessary for the call to
						// ValidateTable().
						if err := table.MaybeFillInDescriptor(ctx, nil); err != nil {
							log.Warningf(ctx, "%s: unable to fill in table descriptor %v", kv.Key, table)
							return
						}
						if err := table.ValidateTable(); err != nil {
							log.Errorf(ctx, "%s: received invalid table descriptor: %s. Desc: %v",
								kv.Key, err, table,
							)
							return
						}
						if log.V(2) {
							log.Infof(ctx, "%s: refreshing lease table: %d (%s), version: %d, dropped: %t",
								kv.Key, table.ID, table.Name, table.Version, table.Dropped())
						}
						// Try to refresh the table lease to one >= this version.
						if err := purgeOldVersions(
							ctx, db, table.ID, table.GoingOffline(), table.Version, m); err != nil {
							log.Warningf(ctx, "error purging leases for table %d(%s): %s",
								table.ID, table.Name, err)
						}
					case *sqlbase.Descriptor_Database:
						// Ignore.
					}
				})
				if m.testingKnobs.TestingLeasesRefreshedEvent != nil {
					m.testingKnobs.TestingLeasesRefreshedEvent(cfg)
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}

// tableLeaseRefreshLimit is the upper-limit on the number of table leases
// that will continuously have their lease refreshed.
var tableLeaseRefreshLimit = settings.RegisterIntSetting(
	"sql.tablecache.lease.refresh_limit",
	"maximum number of tables to periodically refresh leases for",
	50,
)

// PeriodicallyRefreshSomeLeases so that leases are fresh and can serve
// traffic immediately.
// TODO(vivek): Remove once epoch based table leases are implemented.
func (m *LeaseManager) PeriodicallyRefreshSomeLeases() {
	m.stopper.RunWorker(context.Background(), func(ctx context.Context) {
		if m.leaseDuration <= 0 {
			return
		}
		refreshTimer := timeutil.NewTimer()
		defer refreshTimer.Stop()
		refreshTimer.Reset(m.LeaseStore.jitteredLeaseDuration() / 2)
		for {
			select {
			case <-m.stopper.ShouldQuiesce():
				return

			case <-refreshTimer.C:
				refreshTimer.Read = true
				refreshTimer.Reset(m.LeaseStore.jitteredLeaseDuration() / 2)

				m.refreshSomeLeases(ctx)
			}
		}
	})
}

// Refresh some of the current leases.
func (m *LeaseManager) refreshSomeLeases(ctx context.Context) {
	limit := tableLeaseRefreshLimit.Get(&m.settings.SV)
	if limit <= 0 {
		return
	}
	// Construct a list of tables needing their leases to be reacquired.
	m.mu.Lock()
	ids := make([]sqlbase.ID, 0, len(m.mu.tables))
	var i int64
	for k, table := range m.mu.tables {
		if i++; i > limit {
			break
		}
		table.mu.Lock()
		takenOffline := table.mu.takenOffline
		table.mu.Unlock()
		if !takenOffline {
			ids = append(ids, k)
		}
	}
	m.mu.Unlock()
	// Limit the number of concurrent lease refreshes.
	var wg sync.WaitGroup
	for i := range ids {
		id := ids[i]
		wg.Add(1)
		if err := m.stopper.RunLimitedAsyncTask(
			ctx, fmt.Sprintf("refresh table:%d lease", id), m.sem, true /*wait*/, func(ctx context.Context) {
				defer wg.Done()
				if _, err := acquireNodeLease(ctx, m, id); err != nil {
					log.Infof(ctx, "refreshing table: %d lease failed: %s", id, err)
				}
			}); err != nil {
			log.Infof(ctx, "didnt refresh table: %d lease: %s", id, err)
			wg.Done()
		}
	}
	wg.Wait()
}

// DeleteOrphanedLeases releases all orphaned leases created by a prior
// instance of this node. timeThreshold is a walltime lower than the
// lowest hlc timestamp that the current instance of the node can use.
func (m *LeaseManager) DeleteOrphanedLeases(timeThreshold int64) {
	if m.testingKnobs.DisableDeleteOrphanedLeases {
		return
	}
	nodeID := m.LeaseStore.nodeIDContainer.Get()
	if nodeID == 0 {
		panic("zero nodeID")
	}

	// Run as async worker to prevent blocking the main server Start method.
	// Exit after releasing all the orphaned leases.
	m.stopper.RunWorker(context.Background(), func(ctx context.Context) {
		// This could have been implemented using DELETE WHERE, but DELETE WHERE
		// doesn't implement AS OF SYSTEM TIME.

		// Read orphaned leases.
		sqlQuery := fmt.Sprintf(`
SELECT "descID", version, expiration FROM system.public.lease AS OF SYSTEM TIME %d WHERE "nodeID" = %d
`, timeThreshold, nodeID)
		var rows []tree.Datums
		retryOptions := base.DefaultRetryOptions()
		retryOptions.Closer = m.stopper.ShouldQuiesce()
		// The retry is required because of errors caused by node restarts. Retry 30 times.
		if err := retry.WithMaxAttempts(ctx, retryOptions, 30, func() error {
			var err error
			rows, err = m.LeaseStore.internalExecutor.Query(
				ctx, "read orphaned table leases", nil /*txn*/, sqlQuery)
			return err
		}); err != nil {
			log.Warningf(ctx, "unable to read orphaned leases: %+v", err)
			return
		}

		var wg sync.WaitGroup
		defer wg.Wait()
		for i := range rows {
			// Early exit?
			row := rows[i]
			wg.Add(1)
			lease := storedTableLease{
				id:         sqlbase.ID(tree.MustBeDInt(row[0])),
				version:    int(tree.MustBeDInt(row[1])),
				expiration: tree.MustBeDTimestamp(row[2]),
			}
			if err := m.stopper.RunLimitedAsyncTask(
				ctx, fmt.Sprintf("release table lease %+v", lease), m.sem, true /*wait*/, func(ctx context.Context) {
					m.LeaseStore.release(ctx, m.stopper, &lease)
					log.Infof(ctx, "released orphaned table lease: %+v", lease)
					wg.Done()
				}); err != nil {
				log.Warningf(ctx, "did not release orphaned table lease: %+v, err = %s", lease, err)
				wg.Done()
			}
		}
	})
}
