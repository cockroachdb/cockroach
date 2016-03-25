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

package sql

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
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
	TableDescriptor
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

// Acquire a lease on the most recent version of a table descriptor.
func (s LeaseStore) Acquire(txn *client.Txn, tableID ID, minVersion DescriptorVersion) (*LeaseState, *roachpb.Error) {
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
	values, pErr := p.queryRow(getDescriptor, int(tableID))
	if pErr != nil {
		return nil, pErr
	}
	if values == nil {
		return nil, roachpb.NewErrorf("table ID %d not found", tableID)
	}
	desc := &Descriptor{}
	if err := proto.Unmarshal([]byte(values[0].(parser.DBytes)), desc); err != nil {
		return nil, roachpb.NewError(err)
	}

	tableDesc := desc.GetTable()
	if tableDesc == nil {
		return nil, roachpb.NewErrorf("ID %d is not a table", tableID)
	}
	lease.TableDescriptor = *tableDesc

	if err := lease.Validate(); err != nil {
		return nil, roachpb.NewError(err)
	}
	if lease.Version < minVersion {
		return nil, roachpb.NewErrorf("version %d of table %d does not exist yet", minVersion, tableID)
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
	pErr = s.db.Txn(func(txn *client.Txn) *roachpb.Error {
		p := makePlanner()
		p.txn = txn
		p.session.User = security.RootUser
		const insertLease = `INSERT INTO system.lease (descID, version, nodeID, expiration) ` +
			`VALUES ($1, $2, $3, $4)`
		count, epErr := p.exec(insertLease, lease.ID, int(lease.Version), s.nodeID, lease.expiration)
		if epErr != nil {
			return epErr
		}
		if count != 1 {
			return roachpb.NewErrorf("%s: expected 1 result, found %d", insertLease, count)
		}
		return nil
	})
	return lease, pErr
}

// Release a previously acquired table descriptor lease.
func (s LeaseStore) Release(lease *LeaseState) error {
	pErr := s.db.Txn(func(txn *client.Txn) *roachpb.Error {
		p := makePlanner()
		p.txn = txn
		p.session.User = security.RootUser

		const deleteLease = `DELETE FROM system.lease ` +
			`WHERE (descID, version, nodeID, expiration) = ($1, $2, $3, $4)`
		count, pErr := p.exec(deleteLease, lease.ID, int(lease.Version), s.nodeID, lease.expiration)
		if pErr != nil {
			return pErr
		}
		if count != 1 {
			return roachpb.NewErrorf("%s: expected 1 result, found %d", deleteLease, count)
		}
		return nil
	})
	return pErr.GoError()
}

// waitForOneVersion returns once there are no unexpired leases on the
// previous version of the table descriptor. It returns the current version.
// After returning there can only be versions of the descriptor >= to the
// returned verson. Lease acquisition (see acquire()) maintains the
// invariant that no new leases for desc.Version-1 will be granted once
// desc.Version exists.
func (s LeaseStore) waitForOneVersion(tableID ID, retryOpts retry.Options) (DescriptorVersion, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(tableID)
	var tableDesc *TableDescriptor
	for r := retry.Start(retryOpts); r.Next(); {
		// Get the current version of the table descriptor non-transactionally.
		//
		// TODO(pmattis): Do an inconsistent read here?
		if pErr := s.db.GetProto(descKey, desc); pErr != nil {
			return 0, pErr.GoError()
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

// Publish a new version of a table descriptor. The update closure may be
// called multiple times if retries occur: make sure it does not have side
// effects.
func (s LeaseStore) Publish(tableID ID, update func(*TableDescriptor) error) *roachpb.Error {
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
	}

	for r := retry.Start(retryOpts); r.Next(); {
		// Wait until there are no unexpired leases on the previous version
		// of the table.
		expectedVersion, err := s.waitForOneVersion(tableID, retryOpts)
		if err != nil {
			return roachpb.NewError(err)
		}

		// There should be only one version of the descriptor, but it's
		// a race now to update to the next version.
		pErr := s.db.Txn(func(txn *client.Txn) *roachpb.Error {
			desc := &Descriptor{}
			descKey := MakeDescMetadataKey(tableID)

			// Re-read the current version of the table descriptor, this time
			// transactionally.
			if err := txn.GetProto(descKey, desc); err != nil {
				return err
			}
			tableDesc := desc.GetTable()
			if tableDesc == nil {
				return roachpb.NewErrorf("ID %d is not a table", tableID)
			}
			if expectedVersion != tableDesc.Version {
				// The version changed out from under us. Someone else must be
				// performing a schema change operation.
				if log.V(3) {
					log.Infof("publish (version changed): %d != %d", expectedVersion, tableDesc.Version)
				}
				return roachpb.NewError(&roachpb.LeaseVersionChangedError{})
			}

			// Run the update closure which is intended to perform a single step in a
			// multi-step schema change operation.
			if err := update(tableDesc); err != nil {
				return roachpb.NewError(err)
			}

			// Bump the version and modification time.
			tableDesc.Version++
			now := s.clock.Now()
			tableDesc.ModificationTime = now
			if log.V(3) {
				log.Infof("publish: descID=%d version=%d mtime=%s",
					tableDesc.ID, tableDesc.Version, now.GoTime())
			}
			if err := tableDesc.Validate(); err != nil {
				return roachpb.NewError(err)
			}

			// Write the updated descriptor.
			b := txn.NewBatch()
			b.Put(descKey, desc)
			txn.SetSystemConfigTrigger()
			return txn.CommitInBatch(b)
		})

		if _, ok := pErr.GetDetail().(*roachpb.LeaseVersionChangedError); !ok {
			if _, ok := pErr.GetDetail().(*roachpb.DidntUpdateDescriptorError); ok {
				return nil
			}
			return pErr
		}
	}

	panic("not reached")
}

// countLeases returns the number of unexpired leases for a particular version
// of a descriptor.
func (s LeaseStore) countLeases(descID ID, version DescriptorVersion, expiration time.Time) (int, error) {
	var count int
	pErr := s.db.Txn(func(txn *client.Txn) *roachpb.Error {
		p := makePlanner()
		p.txn = txn
		p.session.User = security.RootUser

		const countLeases = `SELECT COUNT(version) FROM system.lease ` +
			`WHERE descID = $1 AND version = $2 AND expiration > $3`
		values, pErr := p.queryRow(countLeases, descID, int(version), expiration)
		if pErr != nil {
			return pErr
		}
		count = int(values[0].(parser.DInt))
		return nil
	})
	return count, pErr.GoError()
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

func (l *leaseSet) find(version DescriptorVersion, expiration parser.DTimestamp) *LeaseState {
	if i, match := l.findIndex(version, expiration); match {
		return l.data[i]
	}
	return nil
}

func (l *leaseSet) findIndex(version DescriptorVersion, expiration parser.DTimestamp) (int, bool) {
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

func (l *leaseSet) findNewest(version DescriptorVersion) *LeaseState {
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
	id ID
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
}

func (t *tableState) acquire(txn *client.Txn, version DescriptorVersion, store LeaseStore) (*LeaseState, *roachpb.Error) {
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
				return nil, roachpb.NewErrorf("table %d unable to acquire lease on old version: %d < %d",
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
			s, pErr := t.acquireNodeLease(txn, version, store)
			close(t.acquiring)
			t.acquiring = nil
			if pErr != nil {
				return nil, pErr
			}
			t.active.insert(s)
			if err := t.releaseNonLatest(store); err != nil {
				log.Warning(err)
			}
		}

		// A new lease was added, so loop and perform the lookup again.
	}
}

// releaseNonLatest releases all unused non-latest leases.
func (t *tableState) releaseNonLatest(store LeaseStore) error {
	// Skip the last lease.
	for i := 0; i < len(t.active.data)-1; {
		s := t.active.data[i]
		if s.Refcount() == 0 {
			t.active.remove(s)
			if err := t.releaseNodeLease(s, store); err != nil {
				return err
			}
		} else {
			i++
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

func (t *tableState) acquireNodeLease(
	txn *client.Txn, minVersion DescriptorVersion, store LeaseStore,
) (*LeaseState, *roachpb.Error) {
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
		log.Infof("release: descID=%d version=%d refcount=%d", s.ID, s.Version, s.refcount)
	}
	if s.refcount == 0 {
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

// LeaseManager manages acquiring and releasing per-table leases. Exported only
// for testing.
type LeaseManager struct {
	LeaseStore
	mu     sync.Mutex
	tables map[ID]*tableState
}

// NewLeaseManager creates a new LeaseManager.
func NewLeaseManager(nodeID uint32, db client.DB, clock *hlc.Clock) *LeaseManager {
	return &LeaseManager{
		LeaseStore: LeaseStore{
			db:     db,
			clock:  clock,
			nodeID: nodeID,
		},
		tables: make(map[ID]*tableState),
	}
}

// Acquire acquires a read lease for the specified table ID. If version is
// non-zero the lease is grabbed for the specified version. Otherwise it is
// grabbed for the most recent version of the descriptor that the lease manager
// knows about.
func (m *LeaseManager) Acquire(txn *client.Txn, tableID ID, version DescriptorVersion) (*LeaseState, *roachpb.Error) {
	t := m.findTableState(tableID, true)
	return t.acquire(txn, version, m.LeaseStore)
}

// Release releases a previously acquired read lease.
func (m *LeaseManager) Release(lease *LeaseState) error {
	t := m.findTableState(lease.ID, false)
	if t == nil {
		return util.Errorf("table %d not found", lease.ID)
	}
	// TODO(pmattis): Can/should we delete from LeaseManager.tables if the
	// tableState becomes empty?
	return t.release(lease, m.LeaseStore)
}

func (m *LeaseManager) findTableState(tableID ID, create bool) *tableState {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tables[tableID]
	if t == nil && create {
		t = &tableState{id: tableID}
		m.tables[tableID] = t
	}
	return t
}

// RefreshLeases starts a goroutine that refreshes the lease manager
// leases for tables received in the latest system configuration via gossip.
func (m *LeaseManager) RefreshLeases(s *stop.Stopper, db *client.DB, gossip *gossip.Gossip) {
	s.RunWorker(func() {
		descKeyPrefix := keys.MakeTablePrefix(uint32(descriptorTable.ID))
		gossipUpdateC := gossip.RegisterSystemConfigChannel()
		for {
			select {
			case <-gossipUpdateC:
				cfg, _ := gossip.GetSystemConfig()
				// Read all tables and their versions
				if log.V(2) {
					log.Info("received a new config %v", cfg)
				}

				// Loop through the configuration to find all the tables.
				for _, kv := range cfg.Values {
					if !bytes.HasPrefix(kv.Key, descKeyPrefix) {
						continue
					}
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf("%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						continue
					}
					switch union := descriptor.Union.(type) {
					case *Descriptor_Table:
						table := union.Table
						if err := table.Validate(); err != nil {
							log.Errorf("%s: received invalid table descriptor: %v", kv.Key, table)
							continue
						}
						if log.V(2) {
							log.Infof("%s: refreshing lease table: %d, version: %d",
								kv.Key, table.ID, table.Version)
						}
						// Try to refresh the table lease to one >= this version.
						if err := m.refreshLease(db, table.ID, table.Version); err != nil {
							log.Warningf("%s: %v", kv.Key, err)
						}

					case *Descriptor_Database:
						// Ignore.
					}
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}

// refreshLease tries to refresh the node's table lease.
func (m *LeaseManager) refreshLease(db *client.DB, id ID, minVersion DescriptorVersion) error {
	// Only attempt to update a lease for a table that is already leased.
	if t := m.findTableState(id, false); t == nil {
		return nil
	}
	// Acquire and release a lease on the table at a version >= minVersion.
	var lease *LeaseState
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		var pErr *roachpb.Error
		// Acquire() can only acquire a lease at a version if it has
		// already been acquired at that version, or that version
		// is the latest version. If the latest version is > minVersion
		// then the node acquires a lease at the latest version but
		// Acquire() itself returns an error. This is okay, because
		// we want to update the node lease.
		lease, pErr = m.Acquire(txn, id, minVersion)
		return pErr
	}); pErr != nil {
		return pErr.GoError()
	}
	return m.Release(lease)
}
