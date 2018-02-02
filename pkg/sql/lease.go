// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// tableVersionState holds the state for a table version. This includes
// the lease information for a table version.
// TODO(vivek): A node only needs to manage lease information on what it
// thinks is the latest version for a table descriptor.
type tableVersionState struct {
	// This descriptor is immutable and can be shared by many goroutines.
	// Care must be taken to not modify it.
	sqlbase.TableDescriptor

	// The expiration time for the table version. A transaction with
	// timestamp T can use this table descriptor version iff
	// TableDescriptor.ModificationTime <= T < expiration
	//
	// The expiration time is either the expiration time of the lease
	// when a lease is associated with the table version, or the
	// ModificationTime of the next version when the table version
	// isn't associated with a lease.
	expiration hlc.Timestamp

	// mu protects refcount and leased.
	mu       syncutil.Mutex
	refcount int
	// Set if the node has a lease on this descriptor version.
	// Leases can only be held for the two latest versions of
	// a table descriptor. The latest version known to a node
	// (can be different than the current latest version in the store)
	// is always associated with a lease. The previous version known to
	// a node might not necessarily be associated with a lease.
	leased bool
}

func (s *tableVersionState) String() string {
	return fmt.Sprintf("%d(%q) ver=%d:%s, refcount=%d", s.ID, s.Name, s.Version, s.expiration, s.refcount)
}

// hasExpired checks if the table is too old to be used (by a txn operating)
// at the given timestamp
func (s *tableVersionState) hasExpired(timestamp hlc.Timestamp) bool {
	return !timestamp.Less(s.expiration)
}

func (s *tableVersionState) incRefcount() {
	s.mu.Lock()
	s.incRefcountLocked()
	s.mu.Unlock()
}

func (s *tableVersionState) incRefcountLocked() {
	s.refcount++
	log.VEventf(context.TODO(), 2, "tableVersionState.incRef: %s", s)
}

// The lease expiration stored in the database is of a different type.
// We've decided that it's too much work to change the type to
// hlc.Timestamp, so we're using this method to give us the stored
// type: tree.DTimestamp.
func (s *tableVersionState) leaseExpiration() tree.DTimestamp {
	return tree.DTimestamp{Time: timeutil.Unix(0, s.expiration.WallTime).Round(time.Microsecond)}
}

// LeaseStore implements the operations for acquiring and releasing leases and
// publishing a new version of a descriptor. Exported only for testing.
type LeaseStore struct {
	execCfg *ExecutorConfig

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
	memMetrics   *MemoryMetrics
}

// jitteredLeaseDuration returns a randomly jittered duration from the interval
// [(1-leaseJitterFraction) * leaseDuration, (1+leaseJitterFraction) * leaseDuration].
func (s LeaseStore) jitteredLeaseDuration() time.Duration {
	return time.Duration(float64(s.leaseDuration) * (1 - s.leaseJitterFraction +
		2*s.leaseJitterFraction*rand.Float64()))
}

// acquire a lease on the most recent version of a table descriptor.
// If the lease cannot be obtained because the descriptor is in the process of
// being dropped, the error will be errTableDropped.
func (s LeaseStore) acquire(
	ctx context.Context, tableID sqlbase.ID, minExpirationTime hlc.Timestamp,
) (*tableVersionState, error) {
	var table *tableVersionState
	err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		expiration := txn.OrigTimestamp()
		expiration.WallTime += int64(s.jitteredLeaseDuration())
		if expiration.Less(minExpirationTime) {
			expiration = minExpirationTime
		}

		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return err
		}
		if err := filterTableState(tableDesc); err != nil {
			return err
		}
		tableDesc.MaybeUpgradeFormatVersion()
		// Once the descriptor is set it is immutable and care must be taken
		// to not modify it.
		table = &tableVersionState{
			TableDescriptor: *tableDesc,
			expiration:      expiration,
			leased:          true,
		}

		// ValidateTable instead of Validate, even though we have a txn available,
		// so we don't block reads waiting for this table version.
		if err := table.ValidateTable(); err != nil {
			return err
		}

		nodeID := s.execCfg.NodeID.Get()
		if nodeID == 0 {
			panic("zero nodeID")
		}
		p, cleanup := newInternalPlanner(
			"lease-insert", txn, security.RootUser, s.memMetrics, s.execCfg)
		defer cleanup()
		const insertLease = `INSERT INTO system.public.lease ("descID", version, "nodeID", expiration) ` +
			`VALUES ($1, $2, $3, $4)`
		leaseExpiration := table.leaseExpiration()
		count, err := p.exec(
			ctx, insertLease, table.ID, int(table.Version), nodeID, &leaseExpiration,
		)
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

// Release a previously acquired table descriptor.
func (s LeaseStore) release(ctx context.Context, stopper *stop.Stopper, table *tableVersionState) {
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = stopper.ShouldQuiesce()
	firstAttempt := true
	for r := retry.Start(retryOptions); r.Next(); {
		// This transaction is idempotent.
		err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			log.VEventf(ctx, 2, "LeaseStore releasing lease %s", table)
			nodeID := s.execCfg.NodeID.Get()
			if nodeID == 0 {
				panic("zero nodeID")
			}
			p, cleanup := newInternalPlanner(
				"lease-release", txn, security.RootUser, s.memMetrics, s.execCfg)
			defer cleanup()
			const deleteLease = `DELETE FROM system.public.lease ` +
				`WHERE ("descID", version, "nodeID", expiration) = ($1, $2, $3, $4)`
			leaseExpiration := table.leaseExpiration()
			count, err := p.exec(
				ctx, deleteLease, table.ID, int(table.Version), nodeID, &leaseExpiration)
			if err != nil {
				return err
			}
			// We allow count == 0 after the first attempt.
			if count > 1 || (count == 0 && firstAttempt) {
				log.Warningf(ctx, "unexpected results while deleting lease %s: "+
					"expected 1 result, found %d", table, count)
			}
			return nil
		})
		if s.testingKnobs.LeaseReleasedEvent != nil {
			s.testingKnobs.LeaseReleasedEvent(table.TableDescriptor, err)
		}
		if err == nil {
			break
		}
		log.Warningf(ctx, "error releasing lease %q: %s", table, err)
		firstAttempt = false
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
	desc := &sqlbase.Descriptor{}
	descKey := sqlbase.MakeDescMetadataKey(tableID)
	var tableDesc *sqlbase.TableDescriptor
	for r := retry.Start(retryOpts); r.Next(); {
		// Get the current version of the table descriptor non-transactionally.
		//
		// TODO(pmattis): Do an inconsistent read here?
		if err := s.execCfg.DB.GetProto(ctx, descKey, desc); err != nil {
			return 0, err
		}
		tableDesc = desc.GetTable()
		if tableDesc == nil {
			return 0, errors.Errorf("ID %d is not a table", tableID)
		}
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := s.execCfg.Clock.Now()
		count, err := s.countLeases(ctx, tableDesc.ID, tableDesc.Version-1, now.GoTime())
		if err != nil {
			return 0, err
		}
		if count == 0 {
			break
		}
		log.Infof(context.TODO(), "publish (count leases): descID=%d name=%s version=%d count=%d",
			tableDesc.ID, tableDesc.Name, tableDesc.Version-1, count)
	}
	return tableDesc.Version, nil
}

var errDidntUpdateDescriptor = errors.New("didn't update the table descriptor")

// Publish updates a table descriptor. It also maintains the invariant that
// there are at most two versions of the descriptor out in the wild at any time
// by first waiting for all nodes to be on the current (pre-update) version of
// the table desc.
// The update closure is called after the wait, and it provides the new version
// of the descriptor to be written. In a multi-step schema operation, this
// update should perform a single step.
// The closure may be called multiple times if retries occur; make sure it does
// not have side effects.
// Returns the updated version of the descriptor.
func (s LeaseStore) Publish(
	ctx context.Context,
	tableID sqlbase.ID,
	update func(*sqlbase.TableDescriptor) error,
	logEvent func(*client.Txn) error,
) (*sqlbase.Descriptor, error) {
	errLeaseVersionChanged := errors.New("lease version changed")
	// Retry while getting errLeaseVersionChanged.
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		// Wait until there are no unexpired leases on the previous version
		// of the table.
		expectedVersion, err := s.WaitForOneVersion(ctx, tableID, base.DefaultRetryOptions())
		if err != nil {
			return nil, err
		}

		desc := &sqlbase.Descriptor{}
		// There should be only one version of the descriptor, but it's
		// a race now to update to the next version.
		err = s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			descKey := sqlbase.MakeDescMetadataKey(tableID)

			// Re-read the current version of the table descriptor, this time
			// transactionally.
			if err := txn.GetProto(ctx, descKey, desc); err != nil {
				return err
			}
			tableDesc := desc.GetTable()
			if tableDesc == nil {
				return errors.Errorf("ID %d is not a table", tableID)
			}
			if expectedVersion != tableDesc.Version {
				// The version changed out from under us. Someone else must be
				// performing a schema change operation.
				if log.V(3) {
					log.Infof(ctx, "publish (version changed): %d != %d", expectedVersion, tableDesc.Version)
				}
				return errLeaseVersionChanged
			}

			// Run the update closure.
			version := tableDesc.Version
			if err := update(tableDesc); err != nil {
				return err
			}
			if version != tableDesc.Version {
				return errors.Errorf("updated version to: %d, expected: %d",
					tableDesc.Version, version)
			}

			tableDesc.Version++
			// We need to set ModificationTime to the transaction's commit
			// timestamp. Since this is a SERIALZIABLE transaction, that will
			// be OrigTimestamp.
			modTime := txn.OrigTimestamp()
			tableDesc.ModificationTime = modTime
			log.Infof(ctx, "publish: descID=%d (%s) version=%d mtime=%s",
				tableDesc.ID, tableDesc.Name, tableDesc.Version, modTime.GoTime())
			if err := tableDesc.ValidateTable(); err != nil {
				return err
			}

			// Write the updated descriptor.
			if err := txn.SetSystemConfigTrigger(); err != nil {
				return err
			}
			b := txn.NewBatch()
			b.Put(descKey, desc)
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
			return desc, nil
		case errLeaseVersionChanged:
			// will loop around to retry
		default:
			return nil, err
		}
	}

	panic("not reached")
}

// countLeases returns the number of unexpired leases for a particular version
// of a descriptor.
func (s LeaseStore) countLeases(
	ctx context.Context, descID sqlbase.ID, version sqlbase.DescriptorVersion, expiration time.Time,
) (int, error) {
	var count int
	err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		p, cleanup := newInternalPlanner(
			"leases-count", txn, security.RootUser, s.memMetrics, s.execCfg)
		defer cleanup()
		const countLeases = `SELECT COUNT(version) FROM system.public.lease ` +
			`WHERE "descID" = $1 AND version = $2 AND expiration > $3`
		values, err := p.QueryRow(ctx, countLeases, descID, int(version), expiration)
		if err != nil {
			return err
		}
		count = int(tree.MustBeDInt(values[0]))
		return nil
	})
	return count, err
}

// Get the table descriptor valid for the expiration time from the store.
// We use a timestamp that is just less than the expiration time to read
// a version of the table descriptor. A tableVersionState with the
// expiration time set to expiration is returned.
//
// This returns an error when Replica.requestCanProceed() returns an
// error when the expiration timestamp is less than the storage layer
// GC threshold.
func (s LeaseStore) getForExpiration(
	ctx context.Context, expiration hlc.Timestamp, id sqlbase.ID,
) (*tableVersionState, error) {
	var table *tableVersionState
	err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		descKey := sqlbase.MakeDescMetadataKey(id)
		prevTimestamp := expiration
		prevTimestamp.WallTime--
		txn.SetFixedTimestamp(ctx, prevTimestamp)
		var desc sqlbase.Descriptor
		if err := txn.GetProto(ctx, descKey, &desc); err != nil {
			return err
		}
		tableDesc := desc.GetTable()
		if tableDesc == nil {
			return errors.Errorf("id %d is not a table", id)
		}
		if !tableDesc.ModificationTime.Less(prevTimestamp) {
			return errors.Errorf("internal error: unable to read table= (%d, %s)", id, expiration)
		}
		// Create a tableVersionState with the table and without a lease.
		table = &tableVersionState{
			TableDescriptor: *tableDesc,
			expiration:      expiration,
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

// isNewest checks if the leaseToken represents the newest lease in the
// tableSet.
func (l *tableSet) isNewest(t leaseToken) bool {
	if len(l.data) == 0 {
		return false
	}
	return leaseToken(l.data[len(l.data)-1]) == t
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

const acquireGroupKey = "acquire"

type tableState struct {
	id sqlbase.ID
	// The cache is updated every time we acquire or release a table.
	tableNameCache *tableNameCache
	stopper        *stop.Stopper

	// renewalInProgress is an atomic indicator for when a renewal for a
	// lease has begun. This is atomic to prevent multiple routines from
	// entering renewal initialization.
	renewalInProgress int32

	mu struct {
		syncutil.Mutex

		// group is used for all calls made to acquireNodeLease to prevent
		// concurrent lease acquisitions from the store.
		group singleflight.Group
		// table descriptors sorted by increasing version. This set always
		// contains a table descriptor version with a lease as the latest
		// entry. There may be more than one active lease when the system is
		// transitioning from one version of the descriptor to another or
		// when the node preemptively acquires a new lease for a version
		// when the old lease has not yet expired. In the latter case, a new
		// entry is created with the expiration time of the new lease and
		// the older entry is removed.
		active tableSet
		// Indicates that the table has been dropped, or is being dropped.
		// If set, leases are released from the store as soon as their
		// refcount drops to 0, as opposed to waiting until they expire.
		dropped bool
	}
}

// acquire returns a version of the table appropriate for the timestamp
// The table will have its refcount incremented, so the caller is
// responsible for calling release() on it.
func (t *tableState) acquire(
	ctx context.Context, timestamp hlc.Timestamp, m *LeaseManager,
) (*tableVersionState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Acquire a lease if no lease exists or if the latest lease is about to
	// expire. Looping is necessary because lease acquisition is done without
	// holding the tableState lock, so anything can happen in between lease
	// acquisition and us getting control again.
	for s := t.mu.active.findNewest(); s == nil || s.hasExpired(timestamp); s = t.mu.active.findNewest() {
		var resultChan <-chan singleflight.Result
		resultChan, _ = t.mu.group.DoChan(acquireGroupKey, func() (interface{}, error) {
			return t.acquireNodeLease(ctx, m, hlc.Timestamp{})
		})
		t.mu.Unlock()
		if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
			m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(LeaseAcquireBlock)
		}
		result := <-resultChan
		t.mu.Lock()
		if result.Err != nil {
			return nil, result.Err
		}
	}

	return t.findForTimestamp(ctx, timestamp, m)
}

// ensureVersion ensures that the latest version >= minVersion. It will
// check if the latest known version meets the criterion, or attempt to
// acquire a lease at the latest version with the hope that it meets
// the criterion.
func (t *tableState) ensureVersion(
	ctx context.Context, minVersion sqlbase.DescriptorVersion, m *LeaseManager,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if s := t.mu.active.findNewest(); s != nil && minVersion <= s.Version {
		return nil
	}

	if err := t.acquireFreshestFromStoreLocked(ctx, m); err != nil {
		return err
	}

	if s := t.mu.active.findNewest(); s != nil && s.Version < minVersion {
		return errors.Errorf("version %d for table %s does not exist yet", minVersion, s.Name)
	}
	return nil
}

// Find the table descriptor valid for the particular timestamp. This
// function is called after ensuring that there is a lease for the latest
// version of the table descriptor and the lease is far from expiring.
// Normally the latest version of a table descriptor if valid is returned.
// If the valid version doesn't exist it is read from the store. The refcount
// for the returned tableVersionState is incremented.
func (t *tableState) findForTimestamp(
	ctx context.Context, timestamp hlc.Timestamp, m *LeaseManager,
) (*tableVersionState, error) {
	afterIdx := 0
	// Walk back the versions to find one that is valid for the timestamp.
	for i := len(t.mu.active.data) - 1; i >= 0; i-- {
		// Check to see if the ModififcationTime is valid.
		if table := t.mu.active.data[i]; !timestamp.Less(table.ModificationTime) {
			if timestamp.Less(table.expiration) {
				// Existing valid table version.
				table.incRefcount()
				return table, nil
			}
			// We need a version after data[i], but before data[i+1].
			// We could very well use the timestamp to read the table
			// descriptor, but unfortunately we will not be able to assign
			// it a proper expiration time. Therefore, we read table
			// descriptors versions one by one from afterIdx back into the
			// past until we find a valid one.
			afterIdx = i + 1
			if afterIdx == len(t.mu.active.data) {
				return nil, fmt.Errorf("requesting a table version ahead of latest version")
			}
			break
		}
	}

	// Read table descriptor versions one by one into the past until we
	// find a valid one. Every version is assigned an expiration time that
	// is the ModificationTime of the previous one read.
	expiration := t.mu.active.data[afterIdx].ModificationTime

	var versions []*tableVersionState
	// We're called with mu locked, but need to unlock it while reading
	// the descriptors from the store.
	t.mu.Unlock()
	for {
		table, err := m.LeaseStore.getForExpiration(ctx, expiration, t.id)
		if err != nil {
			t.mu.Lock()
			return nil, err
		}
		versions = append(versions, table)
		if !timestamp.Less(table.ModificationTime) {
			break
		}
		// Set the expiration time for the next table.
		expiration = table.ModificationTime
	}
	t.mu.Lock()

	// Insert all the table versions and return the last one.
	var table *tableVersionState
	for _, tableVersion := range versions {
		// Since we gave up the lock while reading the versions from
		// the store we have to ensure that no one else inserted the
		// same table version.
		table = t.mu.active.findVersion(tableVersion.Version)
		if table == nil {
			table = tableVersion
			t.mu.active.insert(tableVersion)
		}
	}
	table.incRefcount()
	return table, nil
}

// acquireFreshestFromStoreLocked acquires a new lease from the store and
// inserts it into the active set. It guarantees that the lease returned is
// the one acquired after the call is made. Use this if the lease we want to
// get needs to see some descriptor updates that we know happened recently
// (but that didn't cause the version to be incremented). E.g. if we suspect
// there's a new name for a table, the caller can insist on getting a lease
// reflecting this new name. Moreover, upon returning, the new lease is
// guaranteed to be the last lease in t.mu.active (note that this is not
// generally guaranteed, as leases are assigned random expiration times).
//
// t.mu must be locked.
func (t *tableState) acquireFreshestFromStoreLocked(ctx context.Context, m *LeaseManager) error {
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
		// Move forward the expiry to acquire a fresh table lease.

		// Set the min expiration time to guarantee that the lease acquired is the
		// last lease in t.mu.active.
		// TODO(vivek): the expiration time is no longer needed to sort the
		// tableVersionState. Get rid of this.
		minExpirationTime := hlc.Timestamp{}
		s := t.mu.active.findNewest()
		if s != nil {
			minExpirationTime = s.expiration.Add(int64(time.Millisecond), 0)
		}

		resultChan, wasCalled := t.mu.group.DoChan(acquireGroupKey, func() (interface{}, error) {
			return t.acquireNodeLease(ctx, m, minExpirationTime)
		})
		t.mu.Unlock()
		if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
			m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(LeaseAcquireFreshestBlock)
		}
		result := <-resultChan
		t.mu.Lock()
		if result.Err != nil {
			return result.Err
		}
		l := result.Val.(leaseToken)
		if wasCalled && t.mu.active.isNewest(l) {
			// Case 1: we didn't join an in-progress call and the lease is still
			// valid.
			break
		} else if attemptsMade > 1 && t.mu.active.isNewest(l) {
			// Case 2: more than one acquisition has happened and the lease is still
			// valid.
			break
		}
		attemptsMade++
	}
	return nil
}

// upsertLocked inserts a lease for a particular table version.
// If an existing lease exists for the table version, it releases
// the older lease and replaces it.
func (t *tableState) upsertLocked(ctx context.Context, table *tableVersionState, m *LeaseManager) {
	s := t.mu.active.find(table.Version)
	if s == nil {
		if t.mu.active.findNewest() != nil {
			log.Infof(ctx, "new lease: %s", table)
		}
		t.mu.active.insert(table)
		return
	}

	s.mu.Lock()
	table.mu.Lock()
	// subsume the refcount of the older lease.
	table.refcount += s.refcount
	s.refcount = 0
	s.leased = false
	table.mu.Unlock()
	s.mu.Unlock()
	log.VEventf(ctx, 2, "replaced lease: %s with %s", s, table)
	t.mu.active.remove(s)
	t.mu.active.insert(table)
	t.releaseLease(s, m)
}

// removeInactiveVersions removes inactive versions in t.mu.active.data with refcount 0.
// t.mu must be locked.
func (t *tableState) removeInactiveVersions(m *LeaseManager) {
	// A copy of t.mu.active.data must be made since t.mu.active.data will be changed
	// within the loop.
	for _, table := range append([]*tableVersionState(nil), t.mu.active.data...) {
		func() {
			table.mu.Lock()
			defer table.mu.Unlock()
			if table.refcount == 0 {
				t.mu.active.remove(table)
				if table.leased {
					table.leased = false
					t.releaseLease(table, m)
				}
			}
		}()
	}
}

// If the lease cannot be obtained because the descriptor is in the process of
// being dropped, the error will be errTableDropped.
// minExpirationTime, if not set to the zero value, will be used as a lower
// bound on the expiration of the new table. This can be used to eliminate the
// jitter in the expiration time, and guarantee that we get a lease that will be
// inserted at the end of the lease set (i.e. it will be returned by
// findNewest() from now on).
func (t *tableState) acquireNodeLease(
	ctx context.Context, m *LeaseManager, minExpirationTime hlc.Timestamp,
) (leaseToken, error) {
	if m.isDraining() {
		return nil, errors.New("cannot acquire lease when draining")
	}
	table, err := m.LeaseStore.acquire(ctx, t.id, minExpirationTime)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.upsertLocked(ctx, table, m)
	t.tableNameCache.insert(table)
	return leaseToken(table), nil
}

func (t *tableState) release(table *sqlbase.TableDescriptor, m *LeaseManager) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.mu.active.find(table.Version)
	if s == nil {
		return errors.Errorf("table %d version %d not found", table.ID, table.Version)
	}
	// Decrements the refcount and returns true if the lease has to be removed
	// from the store.
	decRefcount := func(s *tableVersionState) bool {
		// Figure out if we'd like to remove the lease from the store asap (i.e.
		// when the refcount drops to 0). If so, we'll need to mark the lease as
		// invalid.
		removeOnceDereferenced := m.LeaseStore.testingKnobs.RemoveOnceDereferenced ||
			// Release from the store if the table has been dropped; no leases
			// can be acquired any more.
			t.mu.dropped ||
			// Release from the store if the LeaseManager is draining.
			m.isDraining() ||
			// Release from the store if the lease is not for the latest
			// version; only leases for the latest version can be acquired.
			s != t.mu.active.findNewest()

		s.mu.Lock()
		defer s.mu.Unlock()
		s.refcount--
		log.VEventf(context.TODO(), 2, "release: %s", s)
		if s.refcount < 0 {
			panic(fmt.Sprintf("negative ref count: %s", s))
		}

		if s.refcount == 0 && s.leased && removeOnceDereferenced {
			s.leased = false
			return true
		}
		return false
	}
	if decRefcount(s) {
		t.mu.active.remove(s)
		t.releaseLease(s, m)
	}
	return nil
}

// release the lease associated with the table version.
// t.mu needs to be locked.
func (t *tableState) releaseLease(table *tableVersionState, m *LeaseManager) {
	t.tableNameCache.remove(table)

	ctx := context.TODO()
	if m.isDraining() {
		// Release synchronously to guarantee release before exiting.
		m.LeaseStore.release(ctx, t.stopper, table)
		return
	}

	// Release to the store asynchronously, without the tableState lock.
	if err := t.stopper.RunAsyncTask(
		ctx, "sql.tableState: releasing descriptor lease",
		func(ctx context.Context) {
			m.LeaseStore.release(ctx, t.stopper, table)
		}); err != nil {
		log.Warningf(ctx, "error: %s, not releasing lease: %q", err, table)
	}
}

// purgeOldVersions removes old unused table descriptor versions older than
// minVersion and releases any associated leases.
// If dropped is set, minVersion is ignored; no lease is acquired and all
// existing unused versions are removed. The table is further marked dropped,
// which will cause existing in-use leases to be eagerly released once
// they're not in use any more.
// If t has no active leases, nothing is done.
func (t *tableState) purgeOldVersions(
	ctx context.Context,
	db *client.DB,
	dropped bool,
	minVersion sqlbase.DescriptorVersion,
	m *LeaseManager,
) error {
	t.mu.Lock()
	empty := len(t.mu.active.data) == 0
	t.mu.Unlock()
	if empty {
		// We don't currently have a version on this table, so no need to refresh
		// anything.
		return nil
	}

	removeInactives := func(drop bool) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.dropped = drop
		t.removeInactiveVersions(m)
	}

	if dropped {
		removeInactives(dropped)
		return nil
	}

	if err := t.ensureVersion(ctx, minVersion, m); err != nil {
		return err
	}

	// Acquire a lease on the table on the latest version to maintain an
	// active lease, so that it doesn't get released when removeInactives()
	// is called below. Release this lease after calling removeInactives().
	table, err := t.acquire(ctx, m.execCfg.Clock.Now(), m)

	if dropped := err == errTableDropped; dropped || err == nil {
		removeInactives(dropped)
		if table != nil {
			return t.release(&table.TableDescriptor, m)
		}
		return nil
	}
	return err
}

// startLeaseRenewal starts a singleflight.Group to acquire a lease.
// This function blocks until lease acquisition completes.
// t.renewalInProgress must be set to 1 before calling.
func (t *tableState) startLeaseRenewal(
	ctx context.Context, m *LeaseManager, tableVersion *tableVersionState,
) {
	resultChan, _ := t.mu.group.DoChan(acquireGroupKey, func() (interface{}, error) {
		log.VEventf(ctx, 1,
			"background lease renewal beginning for tableID=%d tableName=%q",
			t.id, tableVersion.TableDescriptor.Name)
		token, err := t.acquireNodeLease(ctx, m, hlc.Timestamp{})
		if err != nil {
			log.Errorf(ctx,
				"background lease renewal for tableID=%d tableName=%q failed: %s",
				t.id, tableVersion.TableDescriptor.Name, err)
		} else {
			log.VEventf(ctx, 1,
				"background lease renewal finished for tableID=%d tableName=%q",
				t.id, tableVersion.TableDescriptor.Name)
		}
		return token, err

	})
	<-resultChan
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
	// from tableState.acquireFreshestFromStoreLocked().
	LeaseAcquireFreshestBlock
)

// LeaseStoreTestingKnobs contains testing knobs.
type LeaseStoreTestingKnobs struct {
	// Called after a lease is removed from the store, with any operation error.
	// See LeaseRemovalTracker.
	LeaseReleasedEvent func(table sqlbase.TableDescriptor, err error)
	// Called after a lease is acquired, with any operation error.
	LeaseAcquiredEvent func(table sqlbase.TableDescriptor, err error)
	// Called before waiting on a results from a DoChan call of acquireNodeLease
	// in tableState.acquire() and tableState.acquireFreshestFromStoreLocked().
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
	// all the gossip users in the system.
	GossipUpdateEvent func(config.SystemConfig)
	// A callback called after the leases are refreshed as a result of a gossip update.
	TestingLeasesRefreshedEvent func(config.SystemConfig)

	LeaseStoreTestingKnobs LeaseStoreTestingKnobs
}

var _ base.ModuleTestingKnobs = &LeaseManagerTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*LeaseManagerTestingKnobs) ModuleTestingKnobs() {}

type tableNameCacheKey struct {
	dbID                sqlbase.ID
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
	dbID sqlbase.ID, tableName string, timestamp hlc.Timestamp,
) *tableVersionState {
	c.mu.Lock()
	table, ok := c.tables[makeTableNameCacheKey(dbID, tableName)]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if !nameMatchesTable(&table.TableDescriptor, dbID, tableName) {
		panic(fmt.Sprintf("Out of sync entry in the name cache. "+
			"Cache entry: %d.%q -> %d. Lease: %d.%q.",
			dbID, tableName, table.ID, table.ParentID, table.Name))
	}

	if !table.leased {
		// This get() raced with a release operation. The leaseManager should remove
		// this cache entry soon.
		return nil
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

	key := makeTableNameCacheKey(table.ParentID, table.Name)
	existing, ok := c.tables[key]
	if !ok {
		c.tables[key] = table
		return
	}
	// If we already have a lease in the cache for this name, see if this one is
	// better (higher version or later expiration).
	if table.Version > existing.Version ||
		(table.Version == existing.Version && (existing.expiration.Less(table.expiration))) {
		// Overwrite the old table. The new one is better. From now on, we want
		// clients to use the new one.
		c.tables[key] = table
	}
}

func (c *tableNameCache) remove(table *tableVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeTableNameCacheKey(table.ParentID, table.Name)
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

func makeTableNameCacheKey(dbID sqlbase.ID, tableName string) tableNameCacheKey {
	return tableNameCacheKey{dbID, tableName}
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
}

// NewLeaseManager creates a new LeaseManager.
//
// execCfg can be nil to help bootstrapping, but then it needs to be set via
// SetExecCfg before the LeaseManager is used.
//
// stopper is used to run async tasks. Can be nil in tests.
func NewLeaseManager(
	ambientCtx log.AmbientContext,
	execCfg *ExecutorConfig,
	testingKnobs LeaseManagerTestingKnobs,
	stopper *stop.Stopper,
	memMetrics *MemoryMetrics,
	cfg *base.LeaseManagerConfig,
) *LeaseManager {
	lm := &LeaseManager{
		LeaseStore: LeaseStore{
			execCfg:             execCfg,
			leaseDuration:       cfg.TableDescriptorLeaseDuration,
			leaseJitterFraction: cfg.TableDescriptorLeaseJitterFraction,
			leaseRenewalTimeout: cfg.TableDescriptorLeaseRenewalTimeout,
			testingKnobs:        testingKnobs.LeaseStoreTestingKnobs,
			memMetrics:          memMetrics,
		},
		testingKnobs: testingKnobs,
		tableNames: tableNameCache{
			tables: make(map[tableNameCacheKey]*tableVersionState),
		},
		ambientCtx: ambientCtx,
		stopper:    stopper,
	}

	lm.mu.tables = make(map[sqlbase.ID]*tableState)

	lm.draining.Store(false)
	return lm
}

// SetExecCfg has to be called if a nil execCfg was passed to NewLeaseManager.
func (m *LeaseManager) SetExecCfg(execCfg *ExecutorConfig) {
	m.execCfg = execCfg
}

func nameMatchesTable(table *sqlbase.TableDescriptor, dbID sqlbase.ID, tableName string) bool {
	return table.ParentID == dbID && table.Name == tableName
}

// AcquireByName returns a table version for the specified table valid for
// the timestamp. It returns the table descriptor and a expiration time.
// A transaction using this descriptor must ensure that its
// commit-timestamp < expiration-time. Care must be taken to not modify
// the returned descriptor. Renewal of a lease may begin in the
// background. Renewal is done in order to prevent blocking on future
// acquisitions.
func (m *LeaseManager) AcquireByName(
	ctx context.Context, timestamp hlc.Timestamp, dbID sqlbase.ID, tableName string,
) (*sqlbase.TableDescriptor, hlc.Timestamp, error) {
	// Check if we have cached an ID for this name.
	tableVersion := m.tableNames.get(dbID, tableName, timestamp)
	if tableVersion != nil {
		if !timestamp.Less(tableVersion.ModificationTime) {

			// Atomically check and begin a renewal if one has not already
			// been set.

			durationUntilExpiry := time.Duration(tableVersion.expiration.WallTime - timestamp.WallTime)
			if durationUntilExpiry < m.LeaseStore.leaseRenewalTimeout {
				if t := m.findTableState(tableVersion.ID, false /* create */); t != nil &&
					atomic.CompareAndSwapInt32(&t.renewalInProgress, 0, 1) {
					// Start the renewal. When it finishes, it will reset t.renewalInProgress.
					if err := t.stopper.RunAsyncTask(context.Background(),
						"lease renewal", func(ctx context.Context) {
							var cleanup func()
							ctx, cleanup = tracing.EnsureContext(ctx, m.ambientCtx.Tracer, "lease renewal")
							defer cleanup()
							t.startLeaseRenewal(ctx, m, tableVersion)
						}); err != nil {
						return &tableVersion.TableDescriptor, tableVersion.expiration, err
					}
				}
			}

			return &tableVersion.TableDescriptor, tableVersion.expiration, nil
		}
		if err := m.Release(&tableVersion.TableDescriptor); err != nil {
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
	tableID, err := m.resolveName(ctx, timestamp, dbID, tableName)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	table, expiration, err := m.Acquire(ctx, timestamp, tableID)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	if !nameMatchesTable(table, dbID, tableName) {
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
		if err := m.acquireFreshestFromStore(ctx, tableID); err != nil {
			return nil, hlc.Timestamp{}, err
		}
		table, expiration, err = m.Acquire(ctx, timestamp, tableID)
		if err != nil {
			return nil, hlc.Timestamp{}, err
		}
		if !nameMatchesTable(table, dbID, tableName) {
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
	ctx context.Context, timestamp hlc.Timestamp, dbID sqlbase.ID, tableName string,
) (sqlbase.ID, error) {
	nameKey := tableKey{dbID, tableName}
	key := nameKey.Key()
	id := sqlbase.InvalidID
	if err := m.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, timestamp)
		gr, err := txn.Get(ctx, key)
		if err != nil {
			return err
		}
		if !gr.Exists() {
			return nil
		}
		id = sqlbase.ID(gr.ValueInt())
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
func (m *LeaseManager) Acquire(
	ctx context.Context, timestamp hlc.Timestamp, tableID sqlbase.ID,
) (*sqlbase.TableDescriptor, hlc.Timestamp, error) {
	t := m.findTableState(tableID, true)
	table, err := t.acquire(ctx, timestamp, m)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	return &table.TableDescriptor, table.expiration, nil
}

// acquireFreshestFromStore acquires a new lease from the store. The lease
// is guaranteed to have a version of the descriptor at least as recent as
// the time of the call (i.e. if we were in the process of acquiring a lease
// already, that lease is not good enough).
func (m *LeaseManager) acquireFreshestFromStore(ctx context.Context, tableID sqlbase.ID) error {
	t := m.findTableState(tableID, true)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.acquireFreshestFromStoreLocked(ctx, m)
}

// Release releases a previously acquired table.
func (m *LeaseManager) Release(desc *sqlbase.TableDescriptor) error {
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
	return t.release(desc, m)
}

func (m *LeaseManager) isDraining() bool {
	return m.draining.Load().(bool)
}

// SetDraining (when called with 'true') removes all inactive leases. Any leases
// that are active will be removed once the lease's reference count drops to 0.
func (m *LeaseManager) SetDraining(drain bool) {
	m.draining.Store(drain)
	if !drain {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.mu.tables {
		t.mu.Lock()
		t.removeInactiveVersions(m)
		t.mu.Unlock()
	}
}

// If create is set, cache and stopper need to be set as well.
func (m *LeaseManager) findTableState(tableID sqlbase.ID, create bool) *tableState {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.mu.tables[tableID]
	if t == nil && create {
		t = &tableState{id: tableID, tableNameCache: &m.tableNames, stopper: m.stopper}
		m.mu.tables[tableID] = t
	}
	return t
}

// RefreshLeases starts a goroutine that refreshes the lease manager
// leases for tables received in the latest system configuration via gossip.
func (m *LeaseManager) RefreshLeases(s *stop.Stopper, db *client.DB, g *gossip.Gossip) {
	ctx := context.TODO()
	s.RunWorker(ctx, func(ctx context.Context) {
		descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
		cfgFilter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)
		gossipUpdateC := g.RegisterSystemConfigChannel()
		for {
			select {
			case <-gossipUpdateC:
				cfg, _ := g.GetSystemConfig()
				if m.testingKnobs.GossipUpdateEvent != nil {
					m.testingKnobs.GossipUpdateEvent(cfg)
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
						table.MaybeUpgradeFormatVersion()
						if err := table.ValidateTable(); err != nil {
							log.Errorf(ctx, "%s: received invalid table descriptor: %v", kv.Key, table)
							return
						}
						if log.V(2) {
							log.Infof(ctx, "%s: refreshing lease table: %d (%s), version: %d, dropped: %t",
								kv.Key, table.ID, table.Name, table.Version, table.Dropped())
						}
						// Try to refresh the table lease to one >= this version.
						if t := m.findTableState(table.ID, false /* create */); t != nil {
							if err := t.purgeOldVersions(
								ctx, db, table.Dropped(), table.Version, m); err != nil {
								log.Warningf(ctx, "error purging leases for table %d(%s): %s",
									table.ID, table.Name, err)
							}
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
