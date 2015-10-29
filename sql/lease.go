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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/gogo/protobuf/proto"
)

// TODO(pmattis): Periodically renew leases for tables that were used recently and
// for which the lease will expire soon.

const (
	leaseDuration    = 5 * time.Minute
	minLeaseDuration = time.Minute
)

var (
	errLeaseVersionChanged = errors.New("lease version changed")
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
func (s LeaseStore) jitteredLeaseDuration() time.Duration {
	return time.Duration(float64(leaseDuration) * (0.75 + 0.5*rand.Float64()))
}

// Acquire a lease on the most recent version of a table descriptor.
func (s LeaseStore) Acquire(txn *client.Txn, tableID ID, minVersion uint32) (*LeaseState, error) {
	lease := &LeaseState{}
	lease.expiration = parser.DTimestamp{
		Time: time.Unix(0, s.clock.Now().WallTime).Add(s.jitteredLeaseDuration()),
	}

	p := planner{txn: txn, user: security.RootUser}

	const getDescriptor = `SELECT descriptor FROM system.descriptor WHERE id = %d`
	sql := fmt.Sprintf(getDescriptor, tableID)
	values, err := p.queryRow(sql)
	if err != nil {
		return nil, err
	}
	if values == nil {
		return nil, util.Errorf("table ID %d not found", tableID)
	}
	desc := &Descriptor{}
	if err := proto.Unmarshal([]byte(values[0].(parser.DBytes)), desc); err != nil {
		return nil, err
	}

	tableDesc := desc.GetTable()
	if tableDesc == nil {
		return nil, util.Errorf("ID %d is not a table", tableID)
	}
	lease.TableDescriptor = *tableDesc

	if err := lease.Validate(); err != nil {
		return nil, err
	}
	if lease.Version < minVersion {
		return nil, util.Errorf("version %d of table %d does not exist yet", minVersion, tableID)
	}

	const insertLease = `INSERT INTO system.lease (descID, version, nodeID, expiration) ` +
		`VALUES (%d, %d, %d, '%s'::timestamp)`
	sql = fmt.Sprintf(insertLease, lease.ID, lease.Version, s.nodeID, lease.expiration)
	count, err := p.exec(sql)
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, util.Errorf("%s: expected 1 result, found %d", sql, count)
	}
	return lease, nil
}

// Release a previously acquired table descriptor lease.
func (s LeaseStore) Release(lease *LeaseState) error {
	return s.db.Txn(func(txn *client.Txn) error {
		p := planner{txn: txn, user: security.RootUser}

		const deleteLease = `DELETE FROM system.lease ` +
			`WHERE (descID, version, nodeID, expiration) = (%d, %d, %d, '%s'::timestamp)`
		sql := fmt.Sprintf(deleteLease, lease.ID, lease.Version, s.nodeID, lease.expiration)
		count, err := p.exec(sql)
		if err != nil {
			return err
		}
		if count != 1 {
			return util.Errorf("%s: expected 1 result, found %d", sql, count)
		}
		return nil
	})
}

// Publish a new version of a table descriptor. The update closure may be
// called multiple times if retries occur: make sure it does not have side
// effects.
func (s LeaseStore) Publish(tableID ID, update func(*TableDescriptor) error) error {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(tableID)

	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
	}

	for r := retry.Start(retryOpts); r.Next(); {
		// Get the current version of the table descriptor non-transactionally.
		//
		// TODO(pmattis): Do an inconsistent read here?
		if err := s.db.GetProto(descKey, desc); err != nil {
			return err
		}
		tableDesc := desc.GetTable()
		if tableDesc == nil {
			return util.Errorf("ID %d is not a table", tableID)
		}
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := s.clock.Now()
		count, err := s.countLeases(tableDesc.ID, tableDesc.Version-1, now.GoTime())
		if err != nil {
			return err
		}
		if count != 0 {
			log.Infof("publish (count leases): descID=%d version=%d count=%d",
				tableDesc.ID, tableDesc.Version-1, count)
			continue
		}

		// At this point, desc.Version is the only version of the descriptor that
		// has leases outstanding. Lease acquisition (see acquire()) maintains the
		// invariant that no new leases for desc.Version-1 will be granted once
		// desc.Version exists.
		expectedVersion := tableDesc.Version
		err = s.db.Txn(func(txn *client.Txn) error {
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

			// Run the update closure which is intended to perform a single step in a
			// multi-step schema change operation.
			if err := update(tableDesc); err != nil {
				return err
			}

			// Bump the version and modification time.
			tableDesc.Version = tableDesc.Version + 1
			tableDesc.ModificationTime = now
			if log.V(3) {
				log.Infof("publish: descID=%d version=%d mtime=%s",
					tableDesc.ID, tableDesc.Version, now.GoTime())
			}

			// Write the updated descriptor.
			b := &client.Batch{}
			b.Put(descKey, desc)
			txn.SetSystemDBTrigger()
			return txn.CommitInBatch(b)
		})

		if err != errLeaseVersionChanged {
			return err
		}
	}

	panic("not reached")
}

// countLeases returns the number of unexpired leases for a particular version
// of a descriptor.
func (s LeaseStore) countLeases(descID ID, version uint32, expiration time.Time) (int, error) {
	var count int
	err := s.db.Txn(func(txn *client.Txn) error {
		p := planner{txn: txn, user: security.RootUser}

		const countLeases = `SELECT COUNT(version) FROM system.lease ` +
			`WHERE descID = %d AND version = %d AND expiration > '%s'::timestamp`
		sql := fmt.Sprintf(countLeases, descID, version, parser.DTimestamp{Time: expiration})
		values, err := p.queryRow(sql)
		if err != nil {
			return err
		}
		count = int(values[0].(parser.DInt))
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

func (l *leaseSet) find(version uint32, expiration parser.DTimestamp) *LeaseState {
	if i, match := l.findIndex(version, expiration); match {
		return l.data[i]
	}
	return nil
}

func (l *leaseSet) findIndex(version uint32, expiration parser.DTimestamp) (int, bool) {
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

func (l *leaseSet) findNewest(version uint32) *LeaseState {
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

func (t *tableState) acquire(txn *client.Txn, version uint32, store LeaseStore) (*LeaseState, error) {
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
			minDesiredExpiration := store.clock.Now().GoTime().Add(minLeaseDuration)
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
			t.active.insert(s)
		}

		// A new lease was added, so loop and perform the lookup again.
	}
}

func (t *tableState) acquireWait() {
	// We're called with mu locked, but need to unlock it while we wait for the
	// in-progress lease acquisition to finish.
	acquiring := t.acquiring
	t.mu.Unlock()
	defer t.mu.Lock()
	<-acquiring
}

func (t *tableState) acquireNodeLease(txn *client.Txn, version uint32, store LeaseStore) (*LeaseState, error) {
	// We're called with mu locked, but need to unlock it during lease
	// acquisition.
	t.mu.Unlock()
	defer t.mu.Lock()
	return store.Acquire(txn, t.id, version)
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
func (m *LeaseManager) Acquire(txn *client.Txn, tableID ID, version uint32) (*LeaseState, error) {
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
