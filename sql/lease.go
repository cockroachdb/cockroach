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
	refcount   int
}

func (s *LeaseState) String() string {
	return fmt.Sprintf("%d:%d", s.Version, s.expiration.UnixNano())
}

// Expiration returns the expiration time of the lease.
func (s *LeaseState) Expiration() time.Time {
	return s.expiration.Time
}

// Refcount returns the reference count of the lease.
func (s *LeaseState) Refcount() int {
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
func (s LeaseStore) Acquire(txn *client.Txn, tableID sqlbase.ID, minVersion sqlbase.DescriptorVersion) (*LeaseState, error) {
	lease := &LeaseState{}
	lease.expiration = parser.DTimestamp{
		Time: time.Unix(0, s.clock.Now().WallTime).Add(jitteredLeaseDuration()),
	}

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
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
	}

	errLeaseVersionChanged := errors.New("lease version changed")
	// Retry while getting errLeaseVersionChanged.
	for r := retry.Start(retryOpts); r.Next(); {
		// Wait until there are no unexpired leases on the previous version
		// of the table.
		expectedVersion, err := s.waitForOneVersion(tableID, retryOpts)
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
		buf.WriteString(s.String())
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
	// The cache is updated every time we get a lease with a changed name.
	tableNameCache *tableNameCache
	// Protects both active and acquiring.
	mu sync.Mutex
	// The name of the table, as of the most recent lease.
	// This name should be normalized using sqlbase.NormalizeName() before
	// comparing it.
	tableName string
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

func (t *tableState) acquire(
	txn *client.Txn, version sqlbase.DescriptorVersion, store LeaseStore,
) (*LeaseState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for {
		s := t.active.findNewest(version)
		if s != nil {
			if version != 0 && s != t.active.findNewest(0) {
				// If a lease was requested for an old version of the descriptor,
				// return it even if there is only a short time left before it
				// expires. We can't renew this lease as doing so would violate the
				// invariant that we only get leases on the newest version. The
				// transaction will either finish before the lease expires or it will
				// abort, which is what will happen if we returned an error here.
				s.refcount++
				if log.V(3) {
					log.Infof("acquire: descID=%d version=%d refcount=%d", s.ID, s.Version, s.refcount)
				}
				return s, nil
			}
			minDesiredExpiration := store.clock.Now().GoTime().Add(MinLeaseDuration)
			if s.expiration.After(minDesiredExpiration) {
				s.refcount++
				if log.V(3) {
					log.Infof("acquire: descID=%d version=%d refcount=%d", s.ID, s.Version, s.refcount)
				}
				return s, nil
			}
		} else if version != 0 {
			n := t.active.findNewest(0)
			if n != nil && version < n.Version {
				return nil, util.Errorf("table %d unable to acquire lease on old version: %d < %d",
					t.id, version, n.Version)
			}
		}

		if t.acquiring != nil {
			// There is already a lease acquisition in progress. Wait for it to complete.
			t.acquireWait()
		} else {
			// There is no active lease acquisition so we'll go ahead and perform
			// one.
			t.acquiring = make(chan struct{})
			s, err := t.acquireNodeLease(txn, version, store)
			close(t.acquiring)
			t.acquiring = nil
			if err != nil {
				return nil, err
			}
			// If we'll be returning a lease with a new name for the table (or if this is
			// the first time we get a lease on a table with this name), update the name
			// cache.
			if sqlbase.NormalizeName(t.tableName) != sqlbase.NormalizeName(s.Name) {
				t.tableNameCache.upsert(s.ParentID, t.tableName, s.Name, t.id)
				t.tableName = s.Name
			}
			t.active.insert(s)
		}

		// A new lease was added, so loop and perform the lookup again.
	}
}

// releaseLeasesIfNotActive releases the leases in `leases` with refcount 0.
// t.mu must be locked.
func (t *tableState) releaseLeasesIfNotActive(
	leases []*LeaseState, store LeaseStore) error {

	for _, s := range leases {
		if s.Refcount() != 0 {
			continue
		}
		t.active.remove(s)
		if err := t.releaseNodeLease(s, store); err != nil {
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
func (t *tableState) acquireNodeLease(
	txn *client.Txn, minVersion sqlbase.DescriptorVersion, store LeaseStore,
) (*LeaseState, error) {
	// We're called with mu locked, but need to unlock it during lease
	// acquisition.
	t.mu.Unlock()
	defer t.mu.Lock()
	return store.Acquire(txn, t.id, minVersion)
}

func (t *tableState) release(lease *LeaseState, store LeaseStore) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.active.find(lease.Version, lease.expiration)
	if s == nil {
		return util.Errorf("table %d version %d not found", lease.ID, lease.Version)
	}
	s.refcount--
	if log.V(3) {
		log.Infof("release: descID=%d(%q) version=%d refcount=%d", s.ID, s.Name, s.Version, s.refcount)
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

func (t *tableState) releaseNodeLease(lease *LeaseState, store LeaseStore) error {
	// We're called with mu locked, but need to unlock it while releasing the
	// lease.
	t.mu.Unlock()
	defer t.mu.Lock()
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
				toRelease = t.active.data
			} else {
				// Otherwise, all but the lease we just took are stale.
				toRelease = t.active.data[:len(t.active.data)-1]
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

type tableNameCache struct {
	mu     sync.Mutex
	tables map[tableNameCacheKey]sqlbase.ID
}

// Resolves a (database ID, table name) to the table descriptor's ID. Returns
// false as the first component if the name is not part of the cache.
// This method handles normalizing the table name.
func (t *tableNameCache) get(dbID sqlbase.ID, tableName string) (sqlbase.ID, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id, ok := t.tables[t.makeCacheKey(dbID, tableName)]
	return id, ok
}

// upsert inserts or updates a name -> id mapping in the cache. If oldName is
// "", then the entry is inserted. Otherwise, it's replaced.
// The replacing is best-effort (tables being renamed from one database to
// another are not handled), so stale entries have to be detected outside of the
// cache.
func (t *tableNameCache) upsert(
	dbID sqlbase.ID, oldName string, newName string, tableID sqlbase.ID,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if oldName != "" {
		// We remove the old name from the cache unless its been claimed by some
		// other id since the table that's now being updated owned the name.
		key := t.makeCacheKey(dbID, oldName)
		if t.tables[key] == tableID {
			delete(t.tables, key)
		}
	}
	t.tables[t.makeCacheKey(dbID, newName)] = tableID
}

func (t *tableNameCache) makeCacheKey(dbID sqlbase.ID, tableName string) tableNameCacheKey {
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
	return &LeaseManager{
		LeaseStore: LeaseStore{
			db:     db,
			clock:  clock,
			nodeID: nodeID,
		},
		tables: make(map[sqlbase.ID]*tableState),
		tableNames: tableNameCache{
			tables: make(map[tableNameCacheKey]sqlbase.ID),
		},
		testingKnobs: testingKnobs,
	}
}

// AcquireByName acquires a read lease for the specified table.
// The lease is grabbed for the most recent version of the descriptor that the
// lease manager knows about.
func (m *LeaseManager) AcquireByName(
	txn *client.Txn, dbID sqlbase.ID, tableName string,
) (*LeaseState, error) {
	tableID, ok := m.tableNames.get(dbID, tableName)
	hadLease := ok && m.haveUsableLease(m.LeaseStore, tableID)
	if !hadLease {
		// We failed to find something in the cache, or what we found is not
		// guaranteed to be valid by the time we use it because we don't have a
		// lease with at least a bit of lifetime left in it.
		var err error
		tableID, err = m.resolveName(txn, dbID, tableName)
		if err != nil {
			return nil, err
		}
	}
	lease, err := m.Acquire(txn, tableID, 0)
	if err != nil {
		return nil, err
	}
	if sqlbase.NormalizeName(lease.Name) != sqlbase.NormalizeName(tableName) {
		// The table name has changed from under us. This means that we raced with a
		// rename, and we lost. So, declare that the table doesn't exist any more.
		if hadLease {
			return nil, util.Errorf("inconsistent name found in cache for table for "+
				"which we had a lease, but that can't be... Cached name: %q, "+
				"descriptor name: %q, lease: %+v", tableName, lease.Name, lease)
		}
		return nil, errDescriptorNotFound
	}
	return lease, nil
}

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

// haveUsableLease returns true if we have a lease on the specified descriptor
// and the lease has at least some amount on life left in it.
func (m *LeaseManager) haveUsableLease(store LeaseStore, tableID sqlbase.ID) bool {
	ts := m.findTableState(tableID, false, nil)
	lease := ts.active.findNewest(0)
	if lease == nil {
		return false
	}
	minDesiredExpiration := store.clock.Now().GoTime().Add(MinLeaseDuration)
	return lease.expiration.After(minDesiredExpiration)
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
