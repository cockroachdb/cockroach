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
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var errRenewLease = errors.New("renew lease on id")
var errReadOlderTableVersion = errors.New("read older table version from store")

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

	mu struct {
		syncutil.Mutex

		refcount int
		// Set if the node has a lease on this descriptor version.
		// Leases can only be held for the two latest versions of
		// a table descriptor. The latest version known to a node
		// (can be different than the current latest version in the store)
		// is always associated with a lease. The previous version known to
		// a node might not necessarily be associated with a lease.
		leased bool
	}
}

func (s *tableVersionState) String() string {
	return fmt.Sprintf("%d(%q) ver=%d:%s, refcount=%d", s.ID, s.Name, s.Version, s.expiration, s.mu.refcount)
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
	s.mu.refcount++
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
// being dropped, the error will be errTableDropped.
func (s LeaseStore) acquire(ctx context.Context, tableID sqlbase.ID) (*tableVersionState, error) {
	var table *tableVersionState
	err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		expiration := txn.OrigTimestamp()
		expiration.WallTime += int64(s.jitteredLeaseDuration())

		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return err
		}
		if err := filterTableState(tableDesc); err != nil {
			return err
		}
		tableDesc.MaybeFillInDescriptor()
		// Once the descriptor is set it is immutable and care must be taken
		// to not modify it.
		table = &tableVersionState{
			TableDescriptor: *tableDesc,
			expiration:      expiration,
		}
		table.mu.leased = true

		// ValidateTable instead of Validate, even though we have a txn available,
		// so we don't block reads waiting for this table version.
		if err := table.ValidateTable(s.execCfg.Settings); err != nil {
			return err
		}

		nodeID := s.execCfg.NodeID.Get()
		if nodeID == 0 {
			panic("zero nodeID")
		}
		leaseExpiration := table.leaseExpiration()

		// We use string interpolation here, instead of passing the arguments to
		// InternalExecutor.Exec() because we don't want to pay for preparing the
		// statement (which would happen if we'd pass arguments). Besides the
		// general cost of preparing, preparing this statement always requires a
		// read from the database for the special descriptor of a system table
		// (#23937).
		insertLease := fmt.Sprintf(
			`INSERT INTO system.lease ("descID", version, "nodeID", expiration) VALUES (%d, %d, %d, %s)`,
			table.ID, int(table.Version), nodeID, &leaseExpiration,
		)
		count, err := s.execCfg.InternalExecutor.Exec(ctx, "lease-insert", txn, insertLease)
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
	// This transaction is idempotent; the retry was put in place because of
	// NodeUnavailableErrors.
	for r := retry.Start(retryOptions); r.Next(); {
		log.VEventf(ctx, 2, "LeaseStore releasing lease %s", table)
		nodeID := s.execCfg.NodeID.Get()
		if nodeID == 0 {
			panic("zero nodeID")
		}
		const deleteLease = `DELETE FROM system.lease ` +
			`WHERE ("descID", version, "nodeID", expiration) = ($1, $2, $3, $4)`
		leaseExpiration := table.leaseExpiration()
		count, err := s.execCfg.InternalExecutor.Exec(
			ctx,
			"lease-release",
			nil, /* txn */
			deleteLease,
			table.ID, int(table.Version), nodeID, &leaseExpiration,
		)
		if err != nil {
			log.Warningf(ctx, "error releasing lease %q: %s", table, err)
			firstAttempt = false
			continue
		}
		// We allow count == 0 after the first attempt.
		if count > 1 || (count == 0 && firstAttempt) {
			log.Warningf(ctx, "unexpected results while deleting lease %s: "+
				"expected 1 result, found %d", table, count)
		}

		if s.testingKnobs.LeaseReleasedEvent != nil {
			s.testingKnobs.LeaseReleasedEvent(table.TableDescriptor, err)
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
		tables := []IDVersion{
			{
				name:    tableDesc.Name,
				id:      tableDesc.ID,
				version: tableDesc.Version - 1,
			},
		}
		count, err := CountLeases(ctx, s.execCfg.InternalExecutor, tables, now)
		if err != nil {
			return 0, err
		}
		if count == 0 {
			break
		}
		log.Infof(context.TODO(), "publish (%d leases): desc=%v", count, tables)
	}
	return tableDesc.Version, nil
}

func incrementVersion(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, txn *client.Txn,
) error {
	// Use SERIALIZABLE transactions so that the ModificationTime on the
	// descriptor is the commit timestamp.
	if txn.Isolation() != enginepb.SERIALIZABLE {
		return pgerror.NewErrorf(pgerror.CodeInvalidTransactionStateError,
			"transaction involving a schemas change needs to be SERIALIZABLE")
	}
	tableDesc.Version++
	// We need to set ModificationTime to the transaction's commit
	// timestamp. Using CommitTimestamp() guarantees that the
	// transaction will commit at the CommitTimestamp().
	//
	// TODO(vivek): Stop needing to do this by deprecating the
	// ModificationTime. A Descriptor modification time can be
	// the mvcc timestamp of the descriptor. This requires moving the
	// schema change lease out of the descriptor making the
	// descriptor truly immutable at a version.
	modTime := txn.CommitTimestamp()
	tableDesc.ModificationTime = modTime
	log.Infof(ctx, "publish: descID=%d (%s) version=%d mtime=%s",
		tableDesc.ID, tableDesc.Name, tableDesc.Version, modTime.GoTime())
	return nil
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

			if err := incrementVersion(ctx, tableDesc, txn); err != nil {
				return err
			}
			if err := tableDesc.ValidateTable(s.execCfg.Settings); err != nil {
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

// IDVersion represents a descriptor ID, version pair that are
// meant to map to a single immutable descriptor.
type IDVersion struct {
	// name only provided for pretty printing.
	name    string
	id      sqlbase.ID
	version sqlbase.DescriptorVersion
}

// NewIDVersion returns an initialized IDVersion.
func NewIDVersion(name string, id sqlbase.ID, version sqlbase.DescriptorVersion) IDVersion {
	return IDVersion{name: name, id: id, version: version}
}

// CountLeases returns the number of unexpired leases for a number of tables
// each at a particular version at a particular time.
func CountLeases(
	ctx context.Context, executor *InternalExecutor, tables []IDVersion, at hlc.Timestamp,
) (int, error) {
	var whereClauses []string
	for _, t := range tables {
		whereClauses = append(whereClauses,
			fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
				t.id, t.version),
		)
	}

	stmt := fmt.Sprintf(`SELECT count(1) FROM system.lease AS OF SYSTEM TIME %s WHERE `,
		at.AsOfSystemTime()) +
		strings.Join(whereClauses, " OR ")
	values, err := executor.QueryRow(
		ctx, "count-leases", nil /* txn */, stmt, at.GoTime(),
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
			return sqlbase.ErrDescriptorNotFound
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
		// Indicates that the table has been dropped, or is being dropped.
		// If set, leases are released from the store as soon as their
		// refcount drops to 0, as opposed to waiting until they expire.
		dropped bool
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

	if err := m.acquireFreshestFromStore(ctx, tableID); err != nil {
		return err
	}

	if s := m.findNewest(tableID); s != nil && s.Version < minVersion {
		return errors.Errorf("version %d for table %s does not exist yet", minVersion, s.Name)
	}
	return nil
}

// Find the table descriptor valid for the particular timestamp.
// Normally the latest version of a table descriptor if valid is returned.
// This returns errRenewLease when a lease doesn't exist of needs renewal.
// If the valid version doesn't exist errOlderReadTableVersion is returned
// requesting a higher layer to read the required table version before calling
// this. The refcount for the returned tableVersionState is incremented.
// It returns true if the descriptor returned is the known latest version
// of the descriptor.
func (t *tableState) findForTimestamp(
	ctx context.Context, timestamp hlc.Timestamp,
) (*tableVersionState, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Acquire a lease if no lease exists or if the latest lease is about to
	// expire. Looping is necessary because lease acquisition is done without
	// holding the tableState lock, so anything can happen in between lease
	// acquisition and us getting control again.
	s := t.mu.active.findNewest()
	if s == nil || s.hasExpired(timestamp) {
		return nil, false, errRenewLease
	}

	// Walk back the versions to find one that is valid for the timestamp.
	for i := len(t.mu.active.data) - 1; i >= 0; i-- {
		// Check to see if the ModififcationTime is valid.
		if table := t.mu.active.data[i]; !timestamp.Less(table.ModificationTime) {
			latest := i+1 == len(t.mu.active.data)
			if timestamp.Less(table.expiration) {
				// Existing valid table version.
				table.incRefcount()
				return table, latest, nil
			}

			if latest {
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
// TODO(vivek): We still do not use this cache for AS OF SYSTEM TIME
// queries so this implementation is rather complex. Consider simplifying
// it.
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
			// Check to see if the ModififcationTime is valid.
			if table := t.mu.active.data[i]; !timestamp.Less(table.ModificationTime) {
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
		if !timestamp.Less(table.ModificationTime) {
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

// acquireFreshestFromStoreLocked acquires a new lease from the store and
// inserts it into the active set. It guarantees that the lease returned is
// the one acquired after the call is made. Use this if the lease we want to
// get needs to see some descriptor updates that we know happened recently.
func (m *LeaseManager) acquireFreshestFromStore(ctx context.Context, tableID sqlbase.ID) error {
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
) *tableVersionState {
	s := t.mu.active.find(table.Version)
	if s == nil {
		if t.mu.active.findNewest() != nil {
			log.Infof(ctx, "new lease: %s", table)
		}
		t.mu.active.insert(table)
		return nil
	}

	s.mu.Lock()
	table.mu.Lock()
	// subsume the refcount of the older lease.
	table.mu.refcount += s.mu.refcount
	s.mu.refcount = 0
	s.mu.leased = false
	table.mu.Unlock()
	s.mu.Unlock()
	log.VEventf(ctx, 2, "replaced lease: %s with %s", s, table)
	t.mu.active.remove(s)
	t.mu.active.insert(table)
	return s
}

// removeInactiveVersions removes inactive versions in t.mu.active.data with refcount 0.
// t.mu must be locked. It returns table version state that need to be released.
func (t *tableState) removeInactiveVersions() []*tableVersionState {
	var leases []*tableVersionState
	// A copy of t.mu.active.data must be made since t.mu.active.data will be changed
	// within the loop.
	for _, table := range append([]*tableVersionState(nil), t.mu.active.data...) {
		func() {
			table.mu.Lock()
			defer table.mu.Unlock()
			if table.mu.refcount == 0 {
				t.mu.active.remove(table)
				if table.mu.leased {
					table.mu.leased = false
					leases = append(leases, table)
				}
			}
		}()
	}
	return leases
}

// If the lease cannot be obtained because the descriptor is in the process of
// being dropped, the error will be errTableDropped.
// minExpirationTime, if not set to the zero value, will be used as a lower
// bound on the expiration of the new table. This can be used to eliminate the
// jitter in the expiration time, and guarantee that we get a lease that will be
// inserted at the end of the lease set (i.e. it will be returned by
// findNewest() from now on). The boolean returned is true if this call was actually
// responsible for the lease acquisition.
func acquireNodeLease(ctx context.Context, m *LeaseManager, id sqlbase.ID) (bool, error) {
	var toRelease *tableVersionState
	resultChan, didAcquire := m.group.DoChan(fmt.Sprintf("acquire%d", id), func() (interface{}, error) {
		if m.isDraining() {
			return nil, errors.New("cannot acquire lease when draining")
		}
		table, err := m.LeaseStore.acquire(ctx, id)
		if err != nil {
			return nil, err
		}
		t := m.findTableState(id, false /* create */)
		t.mu.Lock()
		defer t.mu.Unlock()
		toRelease = t.upsertLocked(ctx, table)
		m.tableNames.insert(table)
		return leaseToken(table), nil
	})
	result := <-resultChan
	if result.Err != nil {
		return false, result.Err
	}
	if toRelease != nil {
		releaseLease(toRelease, m)
	}
	return didAcquire, nil
}

// release returns a tableVersionState that needs to be released from
// the store.
func (t *tableState) release(
	table *sqlbase.TableDescriptor, removeOnceDereferenced bool,
) (*tableVersionState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.mu.active.find(table.Version)
	if s == nil {
		return nil, errors.Errorf("table %d version %d not found", table.ID, table.Version)
	}
	// Decrements the refcount and returns true if the lease has to be removed
	// from the store.
	decRefcount := func(s *tableVersionState) bool {
		// Figure out if we'd like to remove the lease from the store asap (i.e.
		// when the refcount drops to 0). If so, we'll need to mark the lease as
		// invalid.
		removeOnceDereferenced = removeOnceDereferenced ||
			// Release from the store if the table has been dropped; no leases
			// can be acquired any more.
			t.mu.dropped ||
			// Release from the store if the lease is not for the latest
			// version; only leases for the latest version can be acquired.
			s != t.mu.active.findNewest()

		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.refcount--
		log.VEventf(context.TODO(), 2, "release: %s", s)
		if s.mu.refcount < 0 {
			panic(fmt.Sprintf("negative ref count: %s", s))
		}

		if s.mu.refcount == 0 && s.mu.leased && removeOnceDereferenced {
			s.mu.leased = false
			return true
		}
		return false
	}
	if decRefcount(s) {
		t.mu.active.remove(s)
		return s, nil
	}
	return nil, nil
}

// release the lease associated with the table version.
func releaseLease(table *tableVersionState, m *LeaseManager) {
	m.tableNames.remove(table)

	ctx := context.TODO()
	if m.isDraining() {
		// Release synchronously to guarantee release before exiting.
		m.LeaseStore.release(ctx, m.stopper, table)
		return
	}

	// Release to the store asynchronously, without the tableState lock.
	if err := m.stopper.RunAsyncTask(
		ctx, "sql.tableState: releasing descriptor lease",
		func(ctx context.Context) {
			m.LeaseStore.release(ctx, m.stopper, table)
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
func purgeOldVersions(
	ctx context.Context,
	db *client.DB,
	id sqlbase.ID,
	dropped bool,
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

	removeInactives := func(drop bool) {
		t.mu.Lock()
		t.mu.dropped = drop
		leases := t.removeInactiveVersions()
		t.mu.Unlock()
		for _, l := range leases {
			releaseLease(l, m)
		}
	}

	if dropped {
		removeInactives(dropped)
		return nil
	}

	if err := ensureVersion(ctx, id, minVersion, m); err != nil {
		return err
	}

	// Acquire a refcount on the table on the latest version to maintain an
	// active lease, so that it doesn't get released when removeInactives()
	// is called below. Release this lease after calling removeInactives().
	table, _, err := t.findForTimestamp(ctx, m.execCfg.Clock.Now())
	if dropped := err == errTableDropped; dropped || err == nil {
		removeInactives(dropped)
		if table != nil {
			s, err := t.release(&table.TableDescriptor, m.removeOnceDereferenced())
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
	LeaseReleasedEvent func(table sqlbase.TableDescriptor, err error)
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

	if !table.mu.leased {
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
	cfg *base.LeaseManagerConfig,
) *LeaseManager {
	lm := &LeaseManager{
		LeaseStore: LeaseStore{
			execCfg:             execCfg,
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
func (m *LeaseManager) AcquireByName(
	ctx context.Context, timestamp hlc.Timestamp, dbID sqlbase.ID, tableName string,
) (*sqlbase.TableDescriptor, hlc.Timestamp, error) {
	// Check if we have cached an ID for this name.
	tableVersion := m.tableNames.get(dbID, tableName, timestamp)
	if tableVersion != nil {
		if !timestamp.Less(tableVersion.ModificationTime) {
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
	for {
		t := m.findTableState(tableID, true)
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
			return &table.TableDescriptor, table.expiration, nil
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
	l, err := t.release(desc, m.removeOnceDereferenced())
	if err != nil {
		return err
	}
	if l != nil {
		releaseLease(l, m)
	}
	return nil
}

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
func (m *LeaseManager) SetDraining(drain bool) {
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
						table.MaybeFillInDescriptor()
						if err := table.ValidateTable(m.execCfg.Settings); err != nil {
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
							ctx, db, table.ID, table.Dropped(), table.Version, m); err != nil {
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
