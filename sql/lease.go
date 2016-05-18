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
//
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// TODO(pmattis): Periodically renew leases for tables that were used recently and
// for which the lease will expire soon.

var (
	// LeaseDuration is the mean duration a lease will be acquired for. The
	// actual duration is jittered in the range
	// [0.75,1.25]*LeaseDuration. Exported for testing purposes only.
	LeaseDuration = 5 * time.Minute
	// MinLeaseDuration is the minimum duration a lease will have remaining upon
	// acquisition. Exported for testing purposes only.
	MinLeaseDuration = time.Minute
)

// LeaseState holds the state for a lease. Exported only for testing.
type LeaseState struct {
	sqlbase.TableDescriptor
	expiration parser.DTimestamp
	// mu protects refcount and released
	mu       sync.Mutex
	refcount int
	// Set if the lease has been released and cannot be handed out any more. The
	// table name cache can have references to such leases since releasing a lease
	// and updating the cache is not atomic.
	released bool
}

func (s *LeaseState) String() string {
	return fmt.Sprintf("%d(%q) ver=%d:%d", s.ID, s.Name, s.Version, s.expiration.UnixNano())
}

// Expiration returns the expiration time of the lease.
func (s *LeaseState) Expiration() time.Time {
	return s.expiration.Time
}

// hasSomeLifeLeft returns true if the lease has at least a minimum of lifetime
// left until expiration, and thus can be used.
func (s *LeaseState) hasSomeLifeLeft(clock *hlc.Clock) bool {
	minDesiredExpiration := clock.Now().GoTime().Add(MinLeaseDuration)
	return s.expiration.After(minDesiredExpiration)
}

// Refcount returns the reference count of the lease.
func (s *LeaseState) Refcount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refcount
}

// LeaseStore implements the operations for acquiring and releasing leases and
// publishing a new version of a descriptor. Exported only for testing.
type LeaseStore struct {
	db     client.DB
	clock  *hlc.Clock
	nodeID uint32
}

// jitteredLeaseDuration returns a randomly jittered duration from the interval
// [0.75 * leaseDuration, 1.25 * leaseDuration].
func jitteredLeaseDuration() time.Duration {
	return time.Duration(float64(LeaseDuration) * (0.75 + 0.5*rand.Float64()))
}

var errTableDeleted = errors.New("table is being deleted")

// Acquire a lease on the most recent version of a table descriptor.
// If the lease cannot be obtained because the descriptor is in the process of
// being deleted, the error will be errTableDeleted.
func (s LeaseStore) Acquire(
	txn *client.Txn,
	tableID sqlbase.ID,
	minVersion sqlbase.DescriptorVersion,
	minExpirationTime parser.DTimestamp,
) (*LeaseState, error) {
	lease := &LeaseState{}
	expiration := time.Unix(0, s.clock.Now().WallTime).Add(jitteredLeaseDuration())
	if !minExpirationTime.IsZero() && expiration.Before(minExpirationTime.Time) {
		expiration = minExpirationTime.Time
	}
	lease.expiration = parser.DTimestamp{Time: expiration}

	// Use the supplied (user) transaction to look up the descriptor because the
	// descriptor might have been created within the transaction.
	p := makePlanner()
	p.txn = txn
	p.session.User = security.RootUser

	const getDescriptor = `SELECT descriptor FROM system.descriptor WHERE id = $1`
	values, err := p.queryRow(getDescriptor, int(tableID))
	if err != nil {
		return nil, err
	}
	if values == nil {
		return nil, errDescriptorNotFound
	}
	desc := &sqlbase.Descriptor{}
	if err := proto.Unmarshal([]byte(*values[0].(*parser.DBytes)), desc); err != nil {
		return nil, err
	}

	tableDesc := desc.GetTable()
	if tableDesc != nil && tableDesc.Deleted() {
		return nil, errTableDeleted
	}
	if tableDesc == nil || tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
		return nil, util.Errorf("ID %d is not a table", tableID)
	}

	lease.TableDescriptor = *tableDesc

	if err := lease.Validate(); err != nil {
		return nil, err
	}
	if lease.Version < minVersion {
		return nil, util.Errorf("version %d of table %d does not exist yet", minVersion, tableID)
	}

	// Insert the entry in the lease table in a separate transaction. This is
	// necessary because we want to ensure that the lease entry is added and the
	// transaction passed to Acquire() might be aborted. The lease entry needs to
	// be added because we store the returned LeaseState in local in-memory maps
	// and cannot handle the entry being reverted. This is safe because either
	// the descriptor we're acquiring the lease on existed prior to the acquire
	// transaction in which case acquiring the lease is kosher, or the descriptor
	// was created within the acquire transaction. The second case is more
	// subtle. We might create a lease entry for a table that doesn't exist, but
	// there is no harm in that as no other transaction will be attempting to
	// modify the descriptor and even if the descriptor is never created we'll
	// just have a dangling lease entry which will eventually get GC'd.
	err = s.db.Txn(func(txn *client.Txn) error {
		p := makePlanner()
		p.txn = txn
		p.session.User = security.RootUser
		const insertLease = `INSERT INTO system.lease (descID, version, nodeID, expiration) ` +
			`VALUES ($1, $2, $3, $4)`
		count, err := p.exec(insertLease, lease.ID, int(lease.Version), s.nodeID, &lease.expiration)
		if err != nil {
			return err
		}
		if count != 1 {
			return util.Errorf("%s: expected 1 result, found %d", insertLease, count)
		}
		return nil
	})
	return lease, err
}

// Release a previously acquired table descriptor lease.
func (s LeaseStore) Release(lease *LeaseState) error {
	err := s.db.Txn(func(txn *client.Txn) error {
		p := makePlanner()
		p.txn = txn
		p.session.User = security.RootUser

		const deleteLease = `DELETE FROM system.lease ` +
			`WHERE (descID, version, nodeID, expiration) = ($1, $2, $3, $4)`
		count, err := p.exec(deleteLease, lease.ID, int(lease.Version), s.nodeID, &lease.expiration)
		if err != nil {
			return err
		}
		if count != 1 {
			return util.Errorf("%s: expected 1 result, found %d", deleteLease, count)
		}
		return nil
	})
	return err
}

// waitForOneVersion returns once there are no unexpired leases on the
// previous version of the table descriptor. It returns the current version.
// After returning there can only be versions of the descriptor >= to the
// returned verson. Lease acquisition (see acquire()) maintains the
// invariant that no new leases for desc.Version-1 will be granted once
// desc.Version exists.
func (s LeaseStore) waitForOneVersion(tableID sqlbase.ID, retryOpts retry.Options) (
	sqlbase.DescriptorVersion, error,
) {
	desc := &sqlbase.Descriptor{}
	descKey := sqlbase.MakeDescMetadataKey(tableID)
	var tableDesc *sqlbase.TableDescriptor
	for r := retry.Start(retryOpts); r.Next(); {
		// Get the current version of the table descriptor non-transactionally.
		//
		// TODO(pmattis): Do an inconsistent read here?
		if err := s.db.GetProto(descKey, desc); err != nil {
			return 0, err
		}
		tableDesc = desc.GetTable()
		if tableDesc == nil {
			return 0, util.Errorf("ID %d is not a table", tableID)
		}
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := s.clock.Now()
		count, err := s.countLeases(tableDesc.ID, tableDesc.Version-1, now.GoTime())
		if err != nil {
			return 0, err
		}
		if count == 0 {
			break
		}
		log.Infof("publish (count leases): descID=%d name=%s version=%d count=%d",
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
	tableID sqlbase.ID, update func(*sqlbase.TableDescriptor) error,
) (*sqlbase.Descriptor, error) {
	errLeaseVersionChanged := errors.New("lease version changed")
	// Retry while getting errLeaseVersionChanged.
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		// Wait until there are no unexpired leases on the previous version
		// of the table.
		expectedVersion, err := s.waitForOneVersion(tableID, base.DefaultRetryOptions())
		if err != nil {
			return nil, err
		}

		desc := &sqlbase.Descriptor{}
		// There should be only one version of the descriptor, but it's
		// a race now to update to the next version.
		err = s.db.Txn(func(txn *client.Txn) error {
			descKey := sqlbase.MakeDescMetadataKey(tableID)

			// Re-read the current version of the table descriptor, this time
			// transactionally.
			if err := txn.GetProto(descKey, desc); err != nil {
				return err
			}
			tableDesc := desc.GetTable()
			if tableDesc == nil {
				return util.Errorf("ID %d is not a table", tableID)
			}
			if expectedVersion != tableDesc.Version {
				// The version changed out from under us. Someone else must be
				// performing a schema change operation.
				if log.V(3) {
					log.Infof("publish (version changed): %d != %d", expectedVersion, tableDesc.Version)
				}
				return errLeaseVersionChanged
			}

			// Run the update closure.
			if err := update(tableDesc); err != nil {
				return err
			}

			// Bump the version and modification time.
			tableDesc.Version++
			now := s.clock.Now()
			tableDesc.ModificationTime = now
			if log.V(3) {
				log.Infof("publish: descID=%d (%s) version=%d mtime=%s",
					tableDesc.ID, tableDesc.Name, tableDesc.Version, now.GoTime())
			}
			if err := tableDesc.Validate(); err != nil {
				return err
			}

			// Write the updated descriptor.
			b := txn.NewBatch()
			b.Put(descKey, desc)
			txn.SetSystemConfigTrigger()
			return txn.CommitInBatch(b)
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
	descID sqlbase.ID, version sqlbase.DescriptorVersion, expiration time.Time,
) (int, error) {
	var count int
	err := s.db.Txn(func(txn *client.Txn) error {
		p := makePlanner()
		p.txn = txn
		p.session.User = security.RootUser

		const countLeases = `SELECT COUNT(version) FROM system.lease ` +
			`WHERE descID = $1 AND version = $2 AND expiration > $3`
		values, err := p.queryRow(countLeases, descID, int(version), expiration)
		if err != nil {
			return err
		}
		count = int(*(values[0].(*parser.DInt)))
		return nil
	})
	return count, err
}

// leaseSet maintains an ordered set of LeaseState objects. It supports
// addition and removal of elements, finding a specific lease, finding the
// newest lease for a particular version and finding the newest lease for the
// most recent version.
type leaseSet struct {
	// The lease state data is stored in a sorted slice ordered by <version,
	// expiration>. Ordering is maintained by insert and remove.
	data []*LeaseState
}

func (l *leaseSet) String() string {
	var buf bytes.Buffer
	for i, s := range l.data {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(fmt.Sprintf("%d:%d", s.Version, s.Expiration().UnixNano()))
	}
	return buf.String()
}

func (l *leaseSet) insert(s *LeaseState) {
	i, match := l.findIndex(s.Version, s.expiration)
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

func (l *leaseSet) remove(s *LeaseState) {
	i, match := l.findIndex(s.Version, s.expiration)
	if !match {
		return
	}
	l.data = append(l.data[:i], l.data[i+1:]...)
}

func (l *leaseSet) find(version sqlbase.DescriptorVersion, expiration parser.DTimestamp) *LeaseState {
	if i, match := l.findIndex(version, expiration); match {
		return l.data[i]
	}
	return nil
}

func (l *leaseSet) findIndex(version sqlbase.DescriptorVersion, expiration parser.DTimestamp) (int, bool) {
	i := sort.Search(len(l.data), func(i int) bool {
		s := l.data[i]
		if s.Version == version {
			// a >= b -> !a.Before(b)
			return !s.expiration.Before(expiration.Time)
		}
		return s.Version > version
	})
	if i < len(l.data) {
		s := l.data[i]
		if s.Version == version && s.expiration.Equal(expiration.Time) {
			return i, true
		}
	}
	return i, false
}

func (l *leaseSet) findNewest(version sqlbase.DescriptorVersion) *LeaseState {
	if len(l.data) == 0 {
		return nil
	}
	if version == 0 {
		// No explicitly version, return the newest lease of the latest version.
		return l.data[len(l.data)-1]
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
	id sqlbase.ID
	// The cache is updated every time we acquire or release a lease.
	tableNameCache *tableNameCache
	// Protects both active and acquiring.
	mu sync.Mutex
	// The active leases for the table: sorted by their version and expiration
	// time. There may be more than one active lease when the system is
	// transitioning from one version of the descriptor to another or when the
	// node preemptively acquires a new lease for a version when the old lease
	// has not yet expired.
	active leaseSet
	// A channel used to indicate whether a lease is actively being acquired.
	// nil if there is no lease acquisition in progress for the table. If
	// non-nil, the channel will be closed when lease acquisition completes.
	acquiring chan struct{}
	// Indicates that the table has been deleted, or has an outstanding deletion.
	// If set, leases are released from the store as soon as their refcount drops
	// to 0, as opposed to waiting until they expire.
	deleted bool
}

// acquire returns a lease at the specifies version. The lease will have its
// refcount incremented, so the caller is responsible to release() it.
func (t *tableState) acquire(
	txn *client.Txn, version sqlbase.DescriptorVersion, store LeaseStore,
) (*LeaseState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for {
		s := t.active.findNewest(version)
		if s != nil {
			if checkedLease := t.checkLease(s, version, store.clock); checkedLease != nil {
				return checkedLease, nil
			}
		} else if version != 0 {
			n := t.active.findNewest(0)
			if n != nil && version < n.Version {
				return nil, util.Errorf("table %d unable to acquire lease on old version: %d < %d",
					t.id, version, n.Version)
			}
		}

		if err := t.acquireFromStoreLocked(
			txn, version, store, false /* need freshest */); err != nil {
			return nil, err
		}
		// A new lease was added, so loop and perform the lookup again.
	}
}

// checkLease checks whether lease is eligible to be returned to a client which
// requested a lease at a specified version (version can also be 0).
// Returns the lease after having incremented its refcount if it's OK to give it
// to the client. Returns nil otherwise.
//
// t.mu needs to be locked
func (t *tableState) checkLease(
	lease *LeaseState, version sqlbase.DescriptorVersion, clock *hlc.Clock,
) *LeaseState {
	// If a lease was requested for an old version of the descriptor,
	// return it even if there is only a short time left before it
	// expires, or even if it's expired. We can't renew this lease as doing so
	// would violate the invariant that we only get leases on the newest
	// version. The transaction will either finish before the lease expires or
	// it will abort, which is what will happen if we returned an error here.
	skipLifeCheck := version != 0 && lease != t.active.findNewest(0)
	if !skipLifeCheck && !lease.hasSomeLifeLeft(clock) {
		return nil
	}
	lease.mu.Lock()
	lease.refcount++
	lease.mu.Unlock()
	if log.V(3) {
		log.Infof("acquire: descID=%d name=%q version=%d refcount=%d",
			lease.ID, lease.Name, lease.Version, lease.refcount)
	}
	return lease
}

// acquireFromStoreLocked acquires a new lease from the store and inserts it
// into the active set.
// If needFreshest is set, we'll guarantee that the lease returned was acquired
// after the call is made. If not set, we might return a lease that we were
// already in the process of acquiring. Set it if the lease we want to get needs
// to see some descriptor updates that we know happened recently (but that
// didn't cause the version to be incremented). E.g. if we suspect there's a new
// name for a table, the caller can insist on getting a lease reflecting this
// new name. Moreover, upon returning, the new lease is guaranteed to be the
// last lease in t.active (note that this is not generally guaranteed, as leases
// are assigned random expiration times).
//
// t.mu must be locked.
func (t *tableState) acquireFromStoreLocked(
	txn *client.Txn,
	version sqlbase.DescriptorVersion,
	store LeaseStore,
	needFreshest bool,
) error {
	if t.acquiring != nil {
		// There is already a lease acquisition in progress. Wait for it to complete.
		t.acquireWait()
		// If needFreshest is set, then the lease we were in the process of
		// acquiring is not good enough. We need to acquire anew.
		if !needFreshest {
			return nil
		}
	}
	t.acquiring = make(chan struct{})
	minExpirationTime := parser.DTimestamp{}
	if needFreshest {
		newestLease := t.active.findNewest(0)
		if newestLease != nil {
			minExpirationTime = parser.DTimestamp{
				Time: newestLease.expiration.Add(time.Millisecond)}
		}
	}
	s, err := t.acquireNodeLease(txn, version, store, minExpirationTime)
	close(t.acquiring)
	t.acquiring = nil
	if err != nil {
		return err
	}
	t.active.insert(s)
	return nil
}

// releaseLeasesIfNotActive releases the leases in `leases` with refcount 0.
// t.mu must be locked.
// leases must be a not overlap t.active.data, since t.active.data will
// be changed by this function.
func (t *tableState) releaseLeasesIfNotActive(
	leases []*LeaseState, store LeaseStore,
) error {
	for _, s := range leases {
		err := func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.refcount != 0 {
				return nil
			}
			t.active.remove(s)
			return t.releaseNodeLease(s, store)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *tableState) acquireWait() {
	// We're called with mu locked, but need to unlock it while we wait for the
	// in-progress lease acquisition to finish.
	acquiring := t.acquiring
	t.mu.Unlock()
	defer t.mu.Lock()
	<-acquiring
}

// If the lease cannot be obtained because the descriptor is in the process of
// being deleted, the error will be errDescriptorDeleted.
// minExpirationTime, if not set to the zero value, will be used as a lower
// bound on the expiration of the new lease. This can be used to eliminate the
// jitter in the expiration time, and guarantee that we get a lease that will be
// inserted at the end of the lease set (i.e. it will be returned by
// findNewest() from now on).
//
// t.mu needs to be locked.
func (t *tableState) acquireNodeLease(
	txn *client.Txn,
	minVersion sqlbase.DescriptorVersion,
	store LeaseStore,
	minExpirationTime parser.DTimestamp,
) (*LeaseState, error) {
	// We're called with mu locked, but need to unlock it during lease
	// acquisition.
	t.mu.Unlock()
	defer t.mu.Lock()
	lease, err := store.Acquire(txn, t.id, minVersion, minExpirationTime)
	if err != nil {
		return nil, err
	}
	t.tableNameCache.insert(lease)
	return lease, nil
}

func (t *tableState) release(lease *LeaseState, store LeaseStore) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.active.find(lease.Version, lease.expiration)
	if s == nil {
		return util.Errorf("table %d version %d not found", lease.ID, lease.Version)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refcount--
	if log.V(3) {
		log.Infof("release: descID=%d name:%q version=%d refcount=%d", s.ID, s.Name, s.Version, s.refcount)
	}
	if s.refcount < 0 {
		panic(fmt.Sprintf("negative ref count: descID=%d(%q) version=%d refcount=%d", s.ID, s.Name, s.Version, s.refcount))
	}
	if s.refcount == 0 {
		if t.deleted {
			t.active.remove(s)
			return t.releaseNodeLease(s, store)
		}
		n := t.active.findNewest(0)
		if s != n {
			if s.Version < n.Version {
				// TODO(pmattis): If an active transaction is releasing the lease for
				// an older version, hold on to it for a few seconds in anticipation of
				// another operation being performed within the transaction. If we
				// release the lease immediately the transaction will necessarily abort
				// on the next operation due to not being able to get the lease.
			}
			t.active.remove(s)
			return t.releaseNodeLease(s, store)
		}
	}
	return nil
}

// t.mu needs to be locked.
// lease.mu needs to be locked.
func (t *tableState) releaseNodeLease(lease *LeaseState, store LeaseStore) error {
	// We're called with mu locked, but need to unlock it while releasing the
	// lease.
	t.mu.Unlock()
	defer t.mu.Lock()
	lease.released = true
	t.tableNameCache.remove(lease)
	return store.Release(lease)
}

// purgeOldLeases refreshes the leases on a table. Unused leases older than
// minVersion will be released.
// If deleted is set, minVersion is ignored; no lease is acquired and all
// existing unused leases are released. The table is further marked for
// deletion, which will cause existing in-use leases to be eagerly released once
// they're not in use any more.
// If t has no active leases, nothing is done.
func (t *tableState) purgeOldLeases(
	db *client.DB, deleted bool, minVersion sqlbase.DescriptorVersion, store LeaseStore,
) error {
	t.mu.Lock()
	empty := len(t.active.data) == 0
	t.mu.Unlock()
	if empty {
		// We don't currently have a lease on this table, so no need to refresh
		// anything.
		return nil
	}

	// Acquire and release a lease on the table at a version >= minVersion.
	var lease *LeaseState
	err := db.Txn(func(txn *client.Txn) error {
		var err error
		if !deleted {
			lease, err = t.acquire(txn, minVersion, store)
			if err == errTableDeleted {
				deleted = true
			}
		}
		if err == nil || deleted {
			t.mu.Lock()
			defer t.mu.Unlock()
			var toRelease []*LeaseState
			if deleted {
				t.deleted = true
				// If the table has been deleted, all leases are stale.
				toRelease = append([]*LeaseState(nil), t.active.data...)
			} else {
				// Otherwise, all but the lease we just took are stale.
				toRelease = append([]*LeaseState(nil), t.active.data[:len(t.active.data)-1]...)
			}
			if err := t.releaseLeasesIfNotActive(toRelease, store); err != nil {
				return err
			}
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	if lease == nil {
		return nil
	}
	return t.release(lease, store)
}

// LeaseManagerTestingKnobs contains test affordances.
type LeaseManagerTestingKnobs struct {
	// A callback called after the leases are refreshed as a result of a gossip update.
	TestingLeasesRefreshedEvent func(config.SystemConfig)
}

var _ base.ModuleTestingKnobs = &LeaseManagerTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*LeaseManagerTestingKnobs) ModuleTestingKnobs() {}

type tableNameCacheKey struct {
	dbID                sqlbase.ID
	normalizeTabledName string
}

// tableNameCache represents a cache of table name -> lease mappings.
// The LeaseManager updates the cache every time a lease is acquired or released
// from the store. The cache maintains the newest lease for each table name.
// All methods are thread-safe.
type tableNameCache struct {
	mu     sync.Mutex
	tables map[tableNameCacheKey]*LeaseState
}

// Resolves a (database ID, table name) to the table descriptor's ID. Returns
// a valid lease for the table with that name, if the name had been previously
// cached and the cache has a lease with at least some amount of life
// left in it. Returns nil otherwise.
// This method handles normalizing the table name.
// The lease's refcount is incremented before returning, so the caller is
// responsible for releasing it to the leaseManager.
func (c *tableNameCache) get(dbID sqlbase.ID, tableName string, clock *hlc.Clock,
) *LeaseState {
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.tables[c.makeCacheKey(dbID, tableName)]
	if !ok {
		return nil
	}
	if lease == nil {
		panic("nil lease in name cache")
	}
	if !nameMatchesLease(lease, dbID, tableName) {
		panic(fmt.Sprintf("Out of sync entry in the name cache. "+
			"Cache entry: %d.%q -> %d. Lease: %d.%q.",
			dbID, tableName, lease.ID, lease.ParentID, lease.Name))
	}

	if !lease.hasSomeLifeLeft(clock) {
		// Expired, or almost expired, lease. Don't hand it out.
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		// This get() raced with a release operation. The leaseManager should remove
		// this cache entry soon.
		return nil
	}
	lease.refcount++

	return lease
}

func (c *tableNameCache) insert(lease *LeaseState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.makeCacheKey(lease.ParentID, lease.Name)
	existing, ok := c.tables[key]
	if !ok {
		c.tables[key] = lease
		return
	}
	// If we already have a lease in the cache for this name, see if this one is
	// better (higher version or later expiration).
	if lease.Version > existing.Version ||
		(lease.Version == existing.Version && lease.Expiration().After(existing.Expiration())) {
		// Overwrite the old lease. The new one is better. From now on, we want
		// clients to use the new one.
		c.tables[key] = lease
	}
}

func (c *tableNameCache) remove(lease *LeaseState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.makeCacheKey(lease.ParentID, lease.Name)
	existing, ok := c.tables[key]
	if !ok {
		panic(fmt.Sprintf("table for lease not found in table name cache: %s", lease))
	}
	// If this was the lease that the cache had for the table name, remove it.
	// If the cache had some other lease, this remove is a no-op.
	if existing == lease {
		delete(c.tables, key)
	}
}

func (c *tableNameCache) makeCacheKey(dbID sqlbase.ID, tableName string) tableNameCacheKey {
	return tableNameCacheKey{dbID, sqlbase.NormalizeName(tableName)}
}

// LeaseManager manages acquiring and releasing per-table leases. It also
// handles resolving table names to descriptor IDs.
// Exported only for testing.
type LeaseManager struct {
	LeaseStore
	mu     sync.Mutex
	tables map[sqlbase.ID]*tableState

	// tableNames is a cache for name -> id mappings. A mapping for the cache
	// should only be used if we currently have an active lease on the respective
	// id; otherwise, the mapping may well be stale.
	// Not protected by mu.
	tableNames   tableNameCache
	testingKnobs LeaseManagerTestingKnobs
}

// NewLeaseManager creates a new LeaseManager.
// Args:
//  testingLeasesRefreshedEvent: if not nil, a callback called after the leases
//  are refreshed as a result of a gossip update.
func NewLeaseManager(
	nodeID uint32,
	db client.DB,
	clock *hlc.Clock,
	testingKnobs LeaseManagerTestingKnobs,
) *LeaseManager {
	lm := &LeaseManager{
		LeaseStore: LeaseStore{
			db:     db,
			clock:  clock,
			nodeID: nodeID,
		},
		tables:       make(map[sqlbase.ID]*tableState),
		testingKnobs: testingKnobs,
		tableNames: tableNameCache{
			tables: make(map[tableNameCacheKey]*LeaseState),
		},
	}
	return lm
}

func nameMatchesLease(lease *LeaseState, dbID sqlbase.ID, tableName string) bool {
	return lease.ParentID == dbID &&
		sqlbase.NormalizeName(lease.Name) == sqlbase.NormalizeName(tableName)
}

// AcquireByName acquires a read lease for the specified table.
// The lease is grabbed for the most recent version of the descriptor that the
// lease manager knows about.
func (m *LeaseManager) AcquireByName(
	txn *client.Txn, dbID sqlbase.ID, tableName string,
) (*LeaseState, error) {
	// Check if we have cached an ID for this name.
	lease := m.tableNames.get(dbID, tableName, m.clock)
	if lease != nil {
		return lease, nil
	}

	// We failed to find something in the cache, or what we found is not
	// guaranteed to be valid by the time we use it because we don't have a
	// lease with at least a bit of lifetime left in it. So, we do it the hard
	// way: look in the database to resolve the name, then acquire a new lease.
	var err error
	tableID, err := m.resolveName(txn, dbID, tableName)
	if err != nil {
		return nil, err
	}
	lease, err = m.Acquire(txn, tableID, 0)
	if err != nil {
		return nil, err
	}
	if !nameMatchesLease(lease, dbID, tableName) {
		// We resolved name `tableName`, but the lease has a different name in it.
		// That can mean two things. Assume the table is being renamed from A to B.
		// a) `tableName` is A. The transaction doing the RENAME committed (so the
		// descriptor has been updated to B), but its schema changer has not
		// finished yet. B is the new name of the table, queries should use that. If
		// we already had a lease with name A, we would've allowed to use it (but we
		// don't, otherwise the cache lookup above would've given it to us).  Since
		// we don't, let's not allow A to be used, given that the lease now has name
		// B in it. It'd be sketchy to allow A to be used with an inconsistent name
		// in the lease.
		//
		// b) `tableName` is B. Like in a), the transaction doing the RENAME
		// committed (so the descriptor has been updated to B), but its schema
		// change has not finished yet. We still had a valid lease with name A in
		// it. What to do, what to do? We could allow name B to be used, but who
		// knows what consequences that would have, since its not consistent with
		// the lease. We could say "table B not found", but that means that, until
		// the next gossip update, this node would not service queries for this
		// table under the name B. That's no bueno, as B should be available to be
		// used immediately after the RENAME transaction has committed.
		// The problem is that we have a lease that we know is stale (the descriptor
		// in the DB doesn't necessarily have a new version yet, but it definitely
		// has a new name). So, lets force getting a fresh lease.
		// This case (modulo the "committed" part) also applies when the txn doing a
		// RENAME had a lease on the old name, and then tries to use the new name
		// after the RENAME statement.
		//
		// How do we dissambiguate between the a) and b)? We get a fresh lease on
		// the descriptor, as required by b), and then we'll know if we're trying to
		// resolve the current or the old name.

		if err := m.Release(lease); err != nil {
			log.Warningf("error releasing lease: %s", err)
		}
		lease, err = m.acquireFreshestFromStore(txn, tableID)
		if err != nil {
			return nil, err
		}
		if lease == nil || !nameMatchesLease(lease, dbID, tableName) {
			// If the name we had doesn't match the newest descriptor in the DB, then
			// we're trying to use an old name.
			if err := m.Release(lease); err != nil {
				log.Warningf("error releasing lease: %s", err)
			}
			return nil, errDescriptorNotFound
		}
	}
	return lease, nil
}

// resolveName resolves a table name to a descriptor ID by looking in the
// database. If the mapping is not found, errDescriptorNotFound is returned.
func (m *LeaseManager) resolveName(
	txn *client.Txn, dbID sqlbase.ID, tableName string,
) (sqlbase.ID, error) {
	nameKey := tableKey{dbID, tableName}
	key := nameKey.Key()
	gr, err := txn.Get(key)
	if err != nil {
		return 0, err
	}
	if !gr.Exists() {
		return 0, errDescriptorNotFound
	}
	return sqlbase.ID(gr.ValueInt()), nil
}

// Acquire acquires a read lease for the specified table ID. If version is
// non-zero the lease is grabbed for the specified version. Otherwise it is
// grabbed for the most recent version of the descriptor that the lease manager
// knows about.
// TODO(andrei): move the tests that use this to the sql package and un-export
// it.
func (m *LeaseManager) Acquire(
	txn *client.Txn, tableID sqlbase.ID, version sqlbase.DescriptorVersion,
) (*LeaseState, error) {
	t := m.findTableState(tableID, true, &m.tableNames)
	return t.acquire(txn, version, m.LeaseStore)
}

// acquireFreshestFromStore acquires a new lease from the store. The returned
// lease is guaranteed to have a version of the descriptor at least as recent as
// the time of the call (i.e. if we were in the process of acquiring a lease
// already, that lease is not good enough).
// The returned lease had its refcount incremented, so the caller is responsible
// for release()ing it.
func (m *LeaseManager) acquireFreshestFromStore(
	txn *client.Txn, tableID sqlbase.ID,
) (*LeaseState, error) {
	t := m.findTableState(tableID, true, &m.tableNames)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.acquireFromStoreLocked(
		txn, 0 /* version */, m.LeaseStore, true /* needFreshest */); err != nil {
		return nil, err
	}
	lease := t.active.findNewest(0)
	if lease == nil {
		panic("no lease in active set after having just acquired one")
	}
	lease.mu.Lock()
	lease.refcount++
	lease.mu.Unlock()
	return lease, nil
}

// Release releases a previously acquired read lease.
func (m *LeaseManager) Release(lease *LeaseState) error {
	t := m.findTableState(lease.ID, false, nil)
	if t == nil {
		return util.Errorf("table %d not found", lease.ID)
	}
	// TODO(pmattis): Can/should we delete from LeaseManager.tables if the
	// tableState becomes empty?
	// TODO(andrei): I think we never delete from LeaseManager.tables... which
	// could be bad if a lot of tables keep being created. I looked into cleaning
	// up a bit, but it seems tricky to do with the current locking which is split
	// between LeaseManager and tableState.
	return t.release(lease, m.LeaseStore)
}

// If create is set, cache needs to be set as well.
func (m *LeaseManager) findTableState(tableID sqlbase.ID, create bool, cache *tableNameCache) *tableState {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tables[tableID]
	if t == nil && create {
		t = &tableState{id: tableID, tableNameCache: cache}
		m.tables[tableID] = t
	}
	return t
}

// RefreshLeases starts a goroutine that refreshes the lease manager
// leases for tables received in the latest system configuration via gossip.
func (m *LeaseManager) RefreshLeases(s *stop.Stopper, db *client.DB, gossip *gossip.Gossip) {
	s.RunWorker(func() {
		descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
		gossipUpdateC := gossip.RegisterSystemConfigChannel()
		for {
			select {
			case <-gossipUpdateC:
				cfg, _ := gossip.GetSystemConfig()
				// Read all tables and their versions
				if log.V(2) {
					log.Info("received a new config; will refresh leases")
				}

				// Loop through the configuration to find all the tables.
				for _, kv := range cfg.Values {
					if !bytes.HasPrefix(kv.Key, descKeyPrefix) {
						continue
					}
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor sqlbase.Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf("%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						continue
					}
					switch union := descriptor.Union.(type) {
					case *sqlbase.Descriptor_Table:
						table := union.Table
						if err := table.Validate(); err != nil {
							log.Errorf("%s: received invalid table descriptor: %v", kv.Key, table)
							continue
						}
						if log.V(2) {
							log.Infof("%s: refreshing lease table: %d (%s), version: %d",
								kv.Key, table.ID, table.Name, table.Version)
						}
						// Try to refresh the table lease to one >= this version.
						if t := m.findTableState(table.ID, false /* create */, nil); t != nil {
							if err := t.purgeOldLeases(
								db, table.Deleted(), table.Version, m.LeaseStore); err != nil {
								log.Warningf("error purging leases for table %d(%s): %s",
									table.ID, table.Name, err)
							}
						}
					case *sqlbase.Descriptor_Database:
						// Ignore.
					}
				}
				if m.testingKnobs.TestingLeasesRefreshedEvent != nil {
					m.testingKnobs.TestingLeasesRefreshedEvent(cfg)
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}
