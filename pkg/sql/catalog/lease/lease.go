// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package lease provides functionality to create and manage sql schema leases.
package lease

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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
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
var errReadOlderVersion = errors.New("read older descriptor version from store")

// A lease stored in system.lease.
type storedLease struct {
	id         sqlbase.ID
	version    int
	expiration tree.DTimestamp
}

// descriptorVersionState holds the state for a descriptor version. This
// includes the lease information for a descriptor version.
// TODO(vivek): A node only needs to manage lease information on what it
// thinks is the latest version for a descriptor.
type descriptorVersionState struct {
	// This descriptor is immutable and can be shared by many goroutines.
	// Care must be taken to not modify it.
	catalog.Descriptor

	// The expiration time for the descriptor version. A transaction with
	// timestamp T can use this descriptor version iff
	// Descriptor.GetModificationTime() <= T < expiration
	//
	// The expiration time is either the expiration time of the lease when a lease
	// is associated with the version, or the ModificationTime of the next version
	// when the version isn't associated with a lease.
	expiration hlc.Timestamp

	mu struct {
		syncutil.Mutex

		refcount int
		// Set if the node has a lease on this descriptor version.
		// Leases can only be held for the two latest versions of
		// a descriptor. The latest version known to a node
		// (can be different than the current latest version in the store)
		// is always associated with a lease. The previous version known to
		// a node might not necessarily be associated with a lease.
		lease *storedLease
	}
}

func (s *descriptorVersionState) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stringLocked()
}

// stringLocked reads mu.refcount and thus needs to have mu held.
func (s *descriptorVersionState) stringLocked() string {
	return fmt.Sprintf("%d(%q) ver=%d:%s, refcount=%d", s.GetID(), s.GetName(), s.GetVersion(), s.expiration, s.mu.refcount)
}

// hasExpired checks if the descriptor is too old to be used (by a txn
// operating) at the given timestamp.
func (s *descriptorVersionState) hasExpired(timestamp hlc.Timestamp) bool {
	return s.expiration.LessEq(timestamp)
}

// hasValidExpiration checks that this descriptor has a later expiration than
// the existing one it is replacing. This can be used to check the
// monotonicity of the expiration times on a descriptor at a particular version.
// The version is not explicitly checked here.
func (s *descriptorVersionState) hasValidExpiration(existing *descriptorVersionState) bool {
	return existing.expiration.Less(s.expiration)
}

func (s *descriptorVersionState) incRefcount() {
	s.mu.Lock()
	s.incRefcountLocked()
	s.mu.Unlock()
}

func (s *descriptorVersionState) incRefcountLocked() {
	s.mu.refcount++
	if log.V(2) {
		log.VEventf(context.TODO(), 2, "descriptorVersionState.incRef: %s", s.stringLocked())
	}
}

// The lease expiration stored in the database is of a different type.
// We've decided that it's too much work to change the type to
// hlc.Timestamp, so we're using this method to give us the stored
// type: tree.DTimestamp.
func storedLeaseExpiration(expiration hlc.Timestamp) tree.DTimestamp {
	return tree.DTimestamp{Time: timeutil.Unix(0, expiration.WallTime).Round(time.Microsecond)}
}

// storage implements the operations for acquiring and releasing leases and
// publishing a new version of a descriptor.
// TODO (lucy,ajwerner): This could go in its own package and expose an API for
// the manager. Some of these fields belong on the manager, in any case, since
// they're only used by the manager and not by the store itself.
type storage struct {
	nodeIDContainer  *base.SQLIDContainer
	db               *kv.DB
	clock            *hlc.Clock
	internalExecutor sqlutil.InternalExecutor
	settings         *cluster.Settings
	codec            keys.SQLCodec

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

	testingKnobs StorageTestingKnobs
}

// jitteredLeaseDuration returns a randomly jittered duration from the interval
// [(1-leaseJitterFraction) * leaseDuration, (1+leaseJitterFraction) * leaseDuration].
func (s storage) jitteredLeaseDuration() time.Duration {
	return time.Duration(float64(s.leaseDuration) * (1 - s.leaseJitterFraction +
		2*s.leaseJitterFraction*rand.Float64()))
}

// acquire a lease on the most recent version of a descriptor. If the lease
// cannot be obtained because the descriptor is in the process of being dropped
// or offline (currently only applicable to tables), the error will be of type
// inactiveTableError. The expiration time set for the lease > minExpiration.
func (s storage) acquire(
	ctx context.Context, minExpiration hlc.Timestamp, id sqlbase.ID,
) (*descriptorVersionState, error) {
	var descVersionState *descriptorVersionState
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

		// TODO (lucy): Previously this called GetTableDescFromID followed by a call
		// to ValidateTable() instead of Validate(), to avoid the cross-table
		// checks. Does this actually matter? We already potentially do cross-table
		// checks when populating pre-19.2 foreign keys.
		desc, err := catalogkv.GetDescriptorByID(ctx, txn, s.codec, id)
		if err != nil {
			return err
		}
		if desc == nil {
			return sqlbase.ErrDescriptorNotFound
		}
		// TODO (lucy): We need a more general concept of an offline descriptor that
		// can't be leased. For now we just have a special case for tables.
		if tableDesc := desc.TableDesc(); tableDesc != nil {
			if err := sqlbase.FilterTableState(tableDesc); err != nil {
				return err
			}
		}
		// Once the descriptor is set it is immutable and care must be taken
		// to not modify it.
		storedLease := &storedLease{
			id:         desc.GetID(),
			version:    int(desc.GetVersion()),
			expiration: storedLeaseExpiration(expiration),
		}
		descVersionState = &descriptorVersionState{
			Descriptor: desc,
			expiration: expiration,
		}
		log.VEventf(ctx, 2, "storage acquired lease %+v", storedLease)
		descVersionState.mu.lease = storedLease

		nodeID := s.nodeIDContainer.SQLInstanceID()
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
		s.testingKnobs.LeaseAcquiredEvent(descVersionState.Descriptor, nil)
	}
	return descVersionState, err
}

// Release a previously acquired descriptor. Never let this method
// read a descriptor because it can be called while modifying a
// descriptor through a schema change before the schema change has committed
// that can result in a deadlock.
func (s storage) release(ctx context.Context, stopper *stop.Stopper, lease *storedLease) {
	retryOptions := base.DefaultRetryOptions()
	retryOptions.Closer = stopper.ShouldQuiesce()
	firstAttempt := true
	// This transaction is idempotent; the retry was put in place because of
	// NodeUnavailableErrors.
	for r := retry.Start(retryOptions); r.Next(); {
		log.VEventf(ctx, 2, "storage releasing lease %+v", lease)
		nodeID := s.nodeIDContainer.SQLInstanceID()
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
// previous version of the descriptor. It returns the current version.
// After returning there can only be versions of the descriptor >= to the
// returned version. Lease acquisition (see acquire()) maintains the
// invariant that no new leases for desc.Version-1 will be granted once
// desc.Version exists.
func (m *Manager) WaitForOneVersion(
	ctx context.Context, id sqlbase.ID, retryOpts retry.Options,
) (sqlbase.DescriptorVersion, error) {
	var version sqlbase.DescriptorVersion
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		// Get the current version of the descriptor non-transactionally.
		//
		// TODO(pmattis): Do an inconsistent read here?
		descKey := sqlbase.MakeDescMetadataKey(m.Codec(), id)
		desc := &sqlbase.Descriptor{}
		ts, err := m.DB().GetProtoTs(ctx, descKey, desc)
		if err != nil {
			return 0, err
		}
		if desc.Union == nil {
			return 0, sqlbase.ErrDescriptorNotFound
		}
		desc.MaybeSetModificationTimeFromMVCCTimestamp(ctx, ts)
		version = desc.GetVersion()

		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := m.storage.clock.Now()
		descs := []IDVersion{NewIDVersionPrev(desc.GetName(), desc.GetID(), desc.GetVersion())}
		count, err := CountLeases(ctx, m.storage.internalExecutor, descs, now)
		if err != nil {
			return 0, err
		}
		if count == 0 {
			break
		}
		if count != lastCount {
			lastCount = count
			log.Infof(ctx, "waiting for %d leases to expire: desc=%v", count, descs)
		}
	}
	return version, nil
}

// ErrDidntUpdateDescriptor can be returned from the update function passed to
// PublishMultiple to suppress an error being returned and return the original
// values.
var ErrDidntUpdateDescriptor = errors.New("didn't update the descriptor")

// PublishMultiple updates multiple descriptors, maintaining the invariant
// that there are at most two versions of each descriptor out in the wild at any
// time by first waiting for all nodes to be on the current (pre-update) version
// of the descriptor.
//
// The update closure for all descriptors is called after the wait. The map argument
// is a map of the descriptors with the IDs given in the ids slice, and the
// closure mutates those descriptors. The txn argument closure is intended to be
// used for updating jobs. Note that it can't be used for anything except
// writing to system descriptors, since we set the system config trigger to write the
// schema changes.
//
// The closure may be called multiple times if retries occur; make sure it does
// not have side effects.
//
// Returns the updated versions of the descriptors.
//
// TODO (lucy): Providing the txn for the update closure just to update a job
// is not ideal. There must be a better API for this.
func (m *Manager) PublishMultiple(
	ctx context.Context,
	ids []sqlbase.ID,
	update func(*kv.Txn, map[sqlbase.ID]catalog.MutableDescriptor) error,
	logEvent func(*kv.Txn) error,
) (map[sqlbase.ID]catalog.Descriptor, error) {
	errLeaseVersionChanged := errors.New("lease version changed")
	// Retry while getting errLeaseVersionChanged.
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		// Wait until there are no unexpired leases on the previous versions
		// of the descriptors.
		expectedVersions := make(map[sqlbase.ID]sqlbase.DescriptorVersion)
		for _, id := range ids {
			expected, err := m.WaitForOneVersion(ctx, id, base.DefaultRetryOptions())
			if err != nil {
				return nil, err
			}
			expectedVersions[id] = expected
		}

		descs := make(map[sqlbase.ID]catalog.MutableDescriptor)
		// There should be only one version of the descriptor, but it's
		// a race now to update to the next version.
		err := m.storage.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			versions := make(map[sqlbase.ID]sqlbase.DescriptorVersion)
			descsToUpdate := make(map[sqlbase.ID]catalog.MutableDescriptor)
			for _, id := range ids {
				// Re-read the current versions of the descriptor, this time
				// transactionally.
				var err error
				descsToUpdate[id], err = catalogkv.GetMutableDescriptorByID(ctx, txn, m.storage.codec, id)
				if err != nil {
					return err
				}

				if expectedVersions[id] != descsToUpdate[id].GetVersion() {
					// The version changed out from under us. Someone else must be
					// performing a schema change operation.
					if log.V(3) {
						log.Infof(ctx, "publish (version changed): %d != %d", expectedVersions[id], descsToUpdate[id].GetVersion())
					}
					return errLeaseVersionChanged
				}

				versions[id] = descsToUpdate[id].GetVersion()
			}

			// This is to write the updated descriptors.
			if err := txn.SetSystemConfigTrigger(); err != nil {
				return err
			}

			// Run the update closure.
			if err := update(txn, descsToUpdate); err != nil {
				return err
			}
			for _, id := range ids {
				if versions[id] != descsToUpdate[id].GetVersion() {
					return errors.Errorf("updated version to: %d, expected: %d",
						descsToUpdate[id].GetVersion(), versions[id])
				}
				descsToUpdate[id].MaybeIncrementVersion()
				descs[id] = descsToUpdate[id]
			}

			b := txn.NewBatch()
			for id, desc := range descs {
				if err := catalogkv.WriteDescToBatch(ctx, false /* kvTrace */, m.storage.settings, b, m.storage.codec, id, desc); err != nil {
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

		switch {
		case err == nil || errors.Is(err, ErrDidntUpdateDescriptor):
			immutDescs := make(map[sqlbase.ID]catalog.Descriptor)
			for id, desc := range descs {
				immutDescs[id] = desc.Immutable()
			}
			return immutDescs, nil
		case errors.Is(err, errLeaseVersionChanged):
			// will loop around to retry
		default:
			return nil, err
		}
	}

	panic("not reached")
}

// Publish updates a descriptor. It also maintains the invariant that
// there are at most two versions of the descriptor out in the wild at any time
// by first waiting for all nodes to be on the current (pre-update) version of
// the descriptor.
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
func (m *Manager) Publish(
	ctx context.Context,
	id sqlbase.ID,
	update func(catalog.MutableDescriptor) error,
	logEvent func(*kv.Txn) error,
) (catalog.Descriptor, error) {
	ids := []sqlbase.ID{id}
	updates := func(_ *kv.Txn, descs map[sqlbase.ID]catalog.MutableDescriptor) error {
		desc, ok := descs[id]
		if !ok {
			return errors.AssertionFailedf("required descriptor with ID %d not provided to update closure", id)
		}
		return update(desc)
	}

	results, err := m.PublishMultiple(ctx, ids, updates, logEvent)
	if err != nil {
		return nil, err
	}
	return results[id], nil
}

// IDVersion represents a descriptor ID, version pair that are
// meant to map to a single immutable descriptor.
type IDVersion struct {
	// Name is only provided for pretty printing.
	Name    string
	ID      sqlbase.ID
	Version sqlbase.DescriptorVersion
}

// NewIDVersionPrev returns an initialized IDVersion with the
// previous version of the descriptor.
func NewIDVersionPrev(name string, id sqlbase.ID, currVersion sqlbase.DescriptorVersion) IDVersion {
	return IDVersion{Name: name, ID: id, Version: currVersion - 1}
}

// CountLeases returns the number of unexpired leases for a number of descriptors
// each at a particular version at a particular time.
func CountLeases(
	ctx context.Context, executor sqlutil.InternalExecutor, versions []IDVersion, at hlc.Timestamp,
) (int, error) {
	var whereClauses []string
	for _, t := range versions {
		whereClauses = append(whereClauses,
			fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
				t.ID, t.Version),
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

// Get the descriptor valid for the expiration time from the store.
// We use a timestamp that is just less than the expiration time to read
// a version of the descriptor. A descriptorVersionState with the
// expiration time set to expiration is returned.
//
// This returns an error when Replica.checkTSAboveGCThresholdRLocked()
// returns an error when the expiration timestamp is less than the storage
// layer GC threshold.
func (s storage) getForExpiration(
	ctx context.Context, expiration hlc.Timestamp, id sqlbase.ID,
) (*descriptorVersionState, error) {
	var descVersionState *descriptorVersionState
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		prevTimestamp := expiration.Prev()
		txn.SetFixedTimestamp(ctx, prevTimestamp)
		desc, err := catalogkv.GetDescriptorByID(ctx, txn, s.codec, id)
		if err != nil {
			return err
		}
		if desc == nil {
			return sqlbase.ErrDescriptorNotFound
		}
		if prevTimestamp.LessEq(desc.GetModificationTime()) {
			return errors.AssertionFailedf("unable to read descriptor (%d, %s)", id, expiration)
		}
		// Create a descriptorVersionState with the descriptor and without a lease.
		descVersionState = &descriptorVersionState{
			Descriptor: desc,
			expiration: expiration,
		}
		return nil
	})
	return descVersionState, err
}

// leaseToken is an opaque token representing a lease. It's distinct from a
// lease to define restricted capabilities and prevent improper use of a lease
// where we instead have leaseTokens.
type leaseToken *descriptorVersionState

// descriptorSet maintains an ordered set of descriptorVersionState objects
// sorted by version. It supports addition and removal of elements, finding the
// descriptor for a particular version, or finding the most recent version.
// The order is maintained by insert and remove and there can only be a
// unique entry for a version. Only the last two versions can be leased,
// with the last one being the latest one which is always leased.
//
// Each entry represents a time span [ModificationTime, expiration)
// and can be used by a transaction iif:
// ModificationTime <= transaction.Timestamp < expiration.
type descriptorSet struct {
	data []*descriptorVersionState
}

func (l *descriptorSet) String() string {
	var buf bytes.Buffer
	for i, s := range l.data {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(fmt.Sprintf("%d:%d", s.GetVersion(), s.expiration.WallTime))
	}
	return buf.String()
}

func (l *descriptorSet) insert(s *descriptorVersionState) {
	i, match := l.findIndex(s.GetVersion())
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

func (l *descriptorSet) remove(s *descriptorVersionState) {
	i, match := l.findIndex(s.GetVersion())
	if !match {
		panic(fmt.Sprintf("can't find lease to remove: %s", s))
	}
	l.data = append(l.data[:i], l.data[i+1:]...)
}

func (l *descriptorSet) find(version sqlbase.DescriptorVersion) *descriptorVersionState {
	if i, match := l.findIndex(version); match {
		return l.data[i]
	}
	return nil
}

func (l *descriptorSet) findIndex(version sqlbase.DescriptorVersion) (int, bool) {
	i := sort.Search(len(l.data), func(i int) bool {
		s := l.data[i]
		return s.GetVersion() >= version
	})
	if i < len(l.data) {
		s := l.data[i]
		if s.GetVersion() == version {
			return i, true
		}
	}
	return i, false
}

func (l *descriptorSet) findNewest() *descriptorVersionState {
	if len(l.data) == 0 {
		return nil
	}
	return l.data[len(l.data)-1]
}

func (l *descriptorSet) findVersion(version sqlbase.DescriptorVersion) *descriptorVersionState {
	if len(l.data) == 0 {
		return nil
	}
	// Find the index of the first lease with version > targetVersion.
	i := sort.Search(len(l.data), func(i int) bool {
		return l.data[i].GetVersion() > version
	})
	if i == 0 {
		return nil
	}
	// i-1 is the index of the newest lease for the previous version (the version
	// we're looking for).
	s := l.data[i-1]
	if s.GetVersion() == version {
		return s
	}
	return nil
}

type descriptorState struct {
	id      sqlbase.ID
	stopper *stop.Stopper

	// renewalInProgress is an atomic indicator for when a renewal for a
	// lease has begun. This is atomic to prevent multiple routines from
	// entering renewal initialization.
	renewalInProgress int32

	mu struct {
		syncutil.Mutex

		// descriptors sorted by increasing version. This set always
		// contains a descriptor version with a lease as the latest
		// entry. There may be more than one active lease when the system is
		// transitioning from one version of the descriptor to another or
		// when the node preemptively acquires a new lease for a version
		// when the old lease has not yet expired. In the latter case, a new
		// entry is created with the expiration time of the new lease and
		// the older entry is removed.
		active descriptorSet
		// Indicates that the has been dropped, or is being dropped.
		// If set, leases are released from the store as soon as their
		// refcount drops to 0, as opposed to waiting until they expire.
		dropped bool

		// acquisitionsInProgress indicates that at least one caller is currently
		// in the process of performing an acquisition. This tracking is critical
		// to ensure that notifications of new versions which arrive before a lease
		// acquisition finishes but indicate that that new lease is expired are not
		// ignored.
		acquisitionsInProgress int
	}
}

// ensureVersion ensures that the latest version >= minVersion. It will
// check if the latest known version meets the criterion, or attempt to
// acquire a lease at the latest version with the hope that it meets
// the criterion.
func ensureVersion(
	ctx context.Context, id sqlbase.ID, minVersion sqlbase.DescriptorVersion, m *Manager,
) error {
	if s := m.findNewest(id); s != nil && minVersion <= s.GetVersion() {
		return nil
	}

	if err := m.AcquireFreshestFromStore(ctx, id); err != nil {
		return err
	}

	if s := m.findNewest(id); s != nil && s.GetVersion() < minVersion {
		return errors.Errorf("version %d for descriptor %s does not exist yet", minVersion, s.GetName())
	}
	return nil
}

// findForTimestamp finds a descriptor valid for the timestamp.
// In the most common case the timestamp passed to this method is close
// to the current time and in all likelihood the latest version of a
// descriptor if valid is returned.
//
// This returns errRenewLease when there is no descriptor cached
// or the latest descriptor version's ModificationTime satisfies the
// timestamp while it's expiration time doesn't satisfy the timestamp.
// This is an optimistic strategy betting that in all likelihood a
// higher layer renewing the lease on the descriptor and populating
// descriptorState will satisfy the timestamp on a subsequent call.
//
// In all other circumstances where a descriptor cannot be found for the
// timestamp errOlderReadTableVersion is returned requesting a higher layer
// to populate the descriptorState with a valid older version of the descriptor
// before calling.
//
// The refcount for the returned descriptorVersionState is incremented.
// It returns true if the descriptor returned is the known latest version
// of the descriptor.
func (t *descriptorState) findForTimestamp(
	ctx context.Context, timestamp hlc.Timestamp,
) (*descriptorVersionState, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Acquire a lease if no descriptor exists in the cache.
	if len(t.mu.active.data) == 0 {
		return nil, false, errRenewLease
	}

	// Walk back the versions to find one that is valid for the timestamp.
	for i := len(t.mu.active.data) - 1; i >= 0; i-- {
		// Check to see if the ModificationTime is valid.
		if desc := t.mu.active.data[i]; desc.GetModificationTime().LessEq(timestamp) {
			latest := i+1 == len(t.mu.active.data)
			if !desc.hasExpired(timestamp) {
				// Existing valid descriptor version.
				desc.incRefcount()
				return desc, latest, nil
			}

			if latest {
				// Renew the lease if the lease has expired
				// The latest descriptor always has a lease.
				return nil, false, errRenewLease
			}
			break
		}
	}

	return nil, false, errReadOlderVersion
}

// Read an older descriptor version for the particular timestamp
// from the store. We unfortunately need to read more than one descriptor
// version just so that we can set the expiration time on the descriptor
// properly.
//
// TODO(vivek): Future work:
// 1. Read multiple versions of a descriptor through one kv call.
// 2. Translate multiple simultaneous calls to this method into a single call
//    as is done for acquireNodeLease().
// 3. Figure out a sane policy on when these descriptors should be purged.
//    They are currently purged in PurgeOldVersions.
func (m *Manager) readOlderVersionForTimestamp(
	ctx context.Context, id sqlbase.ID, timestamp hlc.Timestamp,
) ([]*descriptorVersionState, error) {
	expiration, done := func() (hlc.Timestamp, bool) {
		t := m.findDescriptorState(id, false /* create */)
		t.mu.Lock()
		defer t.mu.Unlock()
		afterIdx := 0
		// Walk back the versions to find one that is valid for the timestamp.
		for i := len(t.mu.active.data) - 1; i >= 0; i-- {
			// Check to see if the ModificationTime is valid.
			if desc := t.mu.active.data[i]; desc.GetModificationTime().LessEq(timestamp) {
				if timestamp.Less(desc.expiration) {
					// Existing valid descriptor version.
					return desc.expiration, true
				}
				// We need a version after data[i], but before data[i+1].
				// We could very well use the timestamp to read the
				// descriptor, but unfortunately we will not be able to assign
				// it a proper expiration time. Therefore, we read
				// descriptor versions one by one from afterIdx back into the
				// past until we find a valid one.
				afterIdx = i + 1
				break
			}
		}

		if afterIdx == len(t.mu.active.data) {
			return hlc.Timestamp{}, true
		}

		// Read descriptor versions one by one into the past until we
		// find a valid one. Every version is assigned an expiration time that
		// is the ModificationTime of the previous one read.
		return t.mu.active.data[afterIdx].GetModificationTime(), false
	}()
	if done {
		return nil, nil
	}

	// Read descriptors from the store.
	var versions []*descriptorVersionState
	for {
		desc, err := m.storage.getForExpiration(ctx, expiration, id)
		if err != nil {
			return nil, err
		}
		versions = append(versions, desc)
		if desc.GetModificationTime().LessEq(timestamp) {
			break
		}
		// Set the expiration time for the next descriptor.
		expiration = desc.GetModificationTime()
	}

	return versions, nil
}

// Insert descriptor versions. The versions provided are not in
// any particular order.
func (m *Manager) insertDescriptorVersions(id sqlbase.ID, versions []*descriptorVersionState) {
	t := m.findDescriptorState(id, false /* create */)
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, version := range versions {
		// Since we gave up the lock while reading the versions from
		// the store we have to ensure that no one else inserted the
		// same version.
		existingVersion := t.mu.active.findVersion(version.GetVersion())
		if existingVersion == nil {
			t.mu.active.insert(version)
		}
	}
}

// AcquireFreshestFromStore acquires a new lease from the store and
// inserts it into the active set. It guarantees that the lease returned is
// the one acquired after the call is made. Use this if the lease we want to
// get needs to see some descriptor updates that we know happened recently.
func (m *Manager) AcquireFreshestFromStore(ctx context.Context, id sqlbase.ID) error {
	// Create descriptorState if needed.
	_ = m.findDescriptorState(id, true /* create */)
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
	// lease acquisition is done without holding the descriptorState lock, so anything
	// can happen in between lease acquisition and us getting control again.
	attemptsMade := 0
	for {
		// Acquire a fresh lease.
		didAcquire, err := acquireNodeLease(ctx, m, id)
		if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
			m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(AcquireFreshestBlock)
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

// upsertLocked inserts a lease for a particular descriptor version.
// If an existing lease exists for the descriptor version it replaces
// it and returns it.
func (t *descriptorState) upsertLocked(
	ctx context.Context, desc *descriptorVersionState,
) (_ *storedLease, _ error) {
	s := t.mu.active.find(desc.GetVersion())
	if s == nil {
		if t.mu.active.findNewest() != nil {
			log.Infof(ctx, "new lease: %s", desc)
		}
		t.mu.active.insert(desc)
		return nil, nil
	}

	// The desc is replacing an existing one at the same version.
	if !desc.hasValidExpiration(s) {
		// This is a violation of an invariant and can actually not
		// happen. We return an error here to aid in further investigations.
		return nil, errors.Errorf("lease expiration monotonicity violation, (%s) vs (%s)", s, desc)
	}

	s.mu.Lock()
	desc.mu.Lock()
	// subsume the refcount of the older lease. This is permitted because
	// the new lease has a greater expiration than the older lease and
	// any transaction using the older lease can safely use a deadline set
	// to the older lease's expiration even though the older lease is
	// released! This is because the new lease is valid at the same desc
	// version at a greater expiration.
	desc.mu.refcount += s.mu.refcount
	s.mu.refcount = 0
	l := s.mu.lease
	s.mu.lease = nil
	if log.V(2) {
		log.VEventf(ctx, 2, "replaced lease: %s with %s", s.stringLocked(), desc.stringLocked())
	}
	desc.mu.Unlock()
	s.mu.Unlock()
	t.mu.active.remove(s)
	t.mu.active.insert(desc)
	return l, nil
}

// removeInactiveVersions removes inactive versions in t.mu.active.data with
// refcount 0. t.mu must be locked. It returns leases that need to be released.
func (t *descriptorState) removeInactiveVersions() []*storedLease {
	var leases []*storedLease
	// A copy of t.mu.active.data must be made since t.mu.active.data will be changed
	// within the loop.
	for _, desc := range append([]*descriptorVersionState(nil), t.mu.active.data...) {
		func() {
			desc.mu.Lock()
			defer desc.mu.Unlock()
			if desc.mu.refcount == 0 {
				t.mu.active.remove(desc)
				if l := desc.mu.lease; l != nil {
					desc.mu.lease = nil
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
func acquireNodeLease(ctx context.Context, m *Manager, id sqlbase.ID) (bool, error) {
	var toRelease *storedLease
	resultChan, didAcquire := m.storage.group.DoChan(fmt.Sprintf("acquire%d", id), func() (interface{}, error) {
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
		desc, err := m.storage.acquire(newCtx, minExpiration, id)
		if err != nil {
			return nil, err
		}
		t := m.findDescriptorState(id, false /* create */)
		t.mu.Lock()
		defer t.mu.Unlock()
		toRelease, err = t.upsertLocked(newCtx, desc)
		if err != nil {
			return nil, err
		}
		m.names.insert(desc)
		if toRelease != nil {
			releaseLease(toRelease, m)
		}
		return leaseToken(desc), nil
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

// release returns a descriptorVersionState that needs to be released from
// the store.
func (t *descriptorState) release(
	desc catalog.Descriptor, removeOnceDereferenced bool,
) (*storedLease, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.mu.active.find(desc.GetVersion())
	if s == nil {
		return nil, errors.Errorf("descriptor %d version %d not found", desc.GetID(), desc.GetVersion())
	}
	// Decrements the refcount and returns true if the lease has to be removed
	// from the store.
	decRefcount := func(s *descriptorVersionState) *storedLease {
		// Figure out if we'd like to remove the lease from the store asap (i.e.
		// when the refcount drops to 0). If so, we'll need to mark the lease as
		// invalid.
		removeOnceDereferenced = removeOnceDereferenced ||
			// Release from the store if the descriptor has been dropped; no leases
			// can be acquired any more.
			t.mu.dropped ||
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
func releaseLease(lease *storedLease, m *Manager) {
	ctx := context.TODO()
	if m.isDraining() {
		// Release synchronously to guarantee release before exiting.
		m.storage.release(ctx, m.stopper, lease)
		return
	}

	// Release to the store asynchronously, without the descriptorState lock.
	if err := m.stopper.RunAsyncTask(
		ctx, "sql.descriptorState: releasing descriptor lease",
		func(ctx context.Context) {
			m.storage.release(ctx, m.stopper, lease)
		}); err != nil {
		log.Warningf(ctx, "error: %s, not releasing lease: %q", err, lease)
	}
}

// purgeOldVersions removes old unused descriptor versions older than
// minVersion and releases any associated leases.
// If takenOffline is set, minVersion is ignored; no lease is acquired and all
// existing unused versions are removed. The descriptor is further marked dropped,
// which will cause existing in-use leases to be eagerly released once
// they're not in use any more.
// If t has no active leases, nothing is done.
func purgeOldVersions(
	ctx context.Context,
	db *kv.DB,
	id sqlbase.ID,
	takenOffline bool,
	minVersion sqlbase.DescriptorVersion,
	m *Manager,
) error {
	t := m.findDescriptorState(id, false /*create*/)
	if t == nil {
		return nil
	}
	t.mu.Lock()
	empty := len(t.mu.active.data) == 0 && t.mu.acquisitionsInProgress == 0
	t.mu.Unlock()
	if empty {
		// We don't currently have a version on this descriptor, so no need to refresh
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

	if takenOffline {
		removeInactives(takenOffline)
		return nil
	}

	if err := ensureVersion(ctx, id, minVersion, m); err != nil {
		return err
	}

	// Acquire a refcount on the descriptor on the latest version to maintain an
	// active lease, so that it doesn't get released when removeInactives()
	// is called below. Release this lease after calling removeInactives().
	desc, _, err := t.findForTimestamp(ctx, m.storage.clock.Now())
	// TODO (lucy): see above comments about offline state
	if isInactive := sqlbase.HasInactiveTableError(err); err == nil || isInactive {
		removeInactives(isInactive)
		if desc != nil {
			s, err := t.release(desc.Descriptor, m.removeOnceDereferenced())
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
func (t *descriptorState) maybeQueueLeaseRenewal(
	ctx context.Context, m *Manager, id sqlbase.ID, name string,
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
			t.startLeaseRenewal(ctx, m, id, name)
		})
}

// startLeaseRenewal starts a singleflight.Group to acquire a lease.
// This function blocks until lease acquisition completes.
// t.renewalInProgress must be set to 1 before calling.
func (t *descriptorState) startLeaseRenewal(
	ctx context.Context, m *Manager, id sqlbase.ID, name string,
) {
	log.VEventf(ctx, 1,
		"background lease renewal beginning for id=%d name=%q",
		id, name)
	if _, err := acquireNodeLease(ctx, m, id); err != nil {
		log.Errorf(ctx,
			"background lease renewal for id=%d name=%q failed: %s",
			id, name, err)
	} else {
		log.VEventf(ctx, 1,
			"background lease renewal finished for id=%d name=%q",
			id, name)
	}
	atomic.StoreInt32(&t.renewalInProgress, 0)
}

// markAcquisitionStart increments the acquisitionsInProgress counter.
func (t *descriptorState) markAcquisitionStart(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.acquisitionsInProgress++
}

// markAcquisitionDone decrements the acquisitionsInProgress counter.
func (t *descriptorState) markAcquisitionDone(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.acquisitionsInProgress--
}

// AcquireBlockType is the type of blocking result event when
// calling LeaseAcquireResultBlockEvent.
type AcquireBlockType int

const (
	// AcquireBlock denotes the LeaseAcquireResultBlockEvent is
	// coming from descriptorState.acquire().
	AcquireBlock AcquireBlockType = iota
	// AcquireFreshestBlock denotes the LeaseAcquireResultBlockEvent is
	// from descriptorState.acquireFreshestFromStore().
	AcquireFreshestBlock
)

// StorageTestingKnobs contains testing knobs.
type StorageTestingKnobs struct {
	// Called after a lease is removed from the store, with any operation error.
	// See LeaseRemovalTracker.
	LeaseReleasedEvent func(id sqlbase.ID, version sqlbase.DescriptorVersion, err error)
	// Called after a lease is acquired, with any operation error.
	LeaseAcquiredEvent func(desc catalog.Descriptor, err error)
	// Called before waiting on a results from a DoChan call of acquireNodeLease
	// in descriptorState.acquire() and descriptorState.acquireFreshestFromStore().
	LeaseAcquireResultBlockEvent func(leaseBlockType AcquireBlockType)
	// RemoveOnceDereferenced forces leases to be removed
	// as soon as they are dereferenced.
	RemoveOnceDereferenced bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*StorageTestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = &StorageTestingKnobs{}

// ManagerTestingKnobs contains test knobs.
type ManagerTestingKnobs struct {

	// A callback called after the leases are refreshed as a result of a gossip update.
	TestingDescriptorRefreshedEvent func(descriptor *sqlbase.Descriptor)

	// TestingDescriptorUpdateEvent is a callback when an update is received, before
	// the leases are refreshed. If a non-nil error is returned, the update is
	// ignored.
	TestingDescriptorUpdateEvent func(descriptor *sqlbase.Descriptor) error

	// To disable the deletion of orphaned leases at server startup.
	DisableDeleteOrphanedLeases bool

	// AlwaysUseRangefeeds ensures that rangefeeds and not gossip are used to
	// detect changes to descriptors.
	AlwaysUseRangefeeds bool

	// VersionPollIntervalForRangefeeds controls the polling interval for the
	// check whether the requisite version for rangefeed-based notifications has
	// been finalized.
	//
	// TODO(ajwerner): Remove this and replace it with a callback.
	VersionPollIntervalForRangefeeds time.Duration

	LeaseStoreTestingKnobs StorageTestingKnobs
}

var _ base.ModuleTestingKnobs = &ManagerTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ManagerTestingKnobs) ModuleTestingKnobs() {}

// nameCacheKey is a key for the descriptor cache, with the same fields
// as the system.namespace key: name is the descriptor name; parentID is
// populated for schemas, descriptors, and types; and parentSchemaID is
// populated for descriptors and types.
type nameCacheKey struct {
	parentID       sqlbase.ID
	parentSchemaID sqlbase.ID
	name           string
}

// nameCache is a cache of descriptor name -> latest version mappings.
// The Manager updates the cache every time a lease is acquired or released
// from the store. The cache maintains the latest version for each name.
// All methods are thread-safe.
type nameCache struct {
	mu          syncutil.Mutex
	descriptors map[nameCacheKey]*descriptorVersionState
}

// Resolves a (qualified) name to the descriptor's ID.
// Returns a valid descriptorVersionState for descriptor with that name,
// if the name had been previously cached and the cache has a descriptor
// version that has not expired. Returns nil otherwise.
// This method handles normalizing the descriptor name.
// The descriptor's refcount is incremented before returning, so the caller
// is responsible for releasing it to the leaseManager.
func (c *nameCache) get(
	parentID sqlbase.ID, parentSchemaID sqlbase.ID, name string, timestamp hlc.Timestamp,
) *descriptorVersionState {
	c.mu.Lock()
	desc, ok := c.descriptors[makeNameCacheKey(parentID, parentSchemaID, name)]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	desc.mu.Lock()
	if desc.mu.lease == nil {
		desc.mu.Unlock()
		// This get() raced with a release operation. Remove this cache
		// entry if needed.
		c.remove(desc)
		return nil
	}

	defer desc.mu.Unlock()

	if !NameMatchesDescriptor(desc, parentID, parentSchemaID, name) {
		panic(fmt.Sprintf("Out of sync entry in the name cache. "+
			"Cache entry: (%d, %d, %q) -> %d. Lease: (%d, %d, %q).",
			parentID, parentSchemaID, name,
			desc.GetID(),
			desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName()),
		)
	}

	// Expired descriptor. Don't hand it out.
	if desc.hasExpired(timestamp) {
		return nil
	}

	desc.incRefcountLocked()
	return desc
}

func (c *nameCache) insert(desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeNameCacheKey(desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName())
	existing, ok := c.descriptors[key]
	if !ok {
		c.descriptors[key] = desc
		return
	}
	// If we already have a lease in the cache for this name, see if this one is
	// better (higher version or later expiration).
	if desc.GetVersion() > existing.GetVersion() ||
		(desc.GetVersion() == existing.GetVersion() && desc.hasValidExpiration(existing)) {
		// Overwrite the old lease. The new one is better. From now on, we want
		// clients to use the new one.
		c.descriptors[key] = desc
	}
}

func (c *nameCache) remove(desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeNameCacheKey(desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName())
	existing, ok := c.descriptors[key]
	if !ok {
		// Descriptor for lease not found in name cache. This can happen if we had
		// a more recent lease on the descriptor in the nameCache, then the
		// descriptor gets dropped, then the more recent lease is remove()d - which
		// clears the cache.
		return
	}
	// If this was the lease that the cache had for the descriptor name, remove
	// it. If the cache had some other descriptor, this remove is a no-op.
	if existing == desc {
		delete(c.descriptors, key)
	}
}

func makeNameCacheKey(parentID sqlbase.ID, parentSchemaID sqlbase.ID, name string) nameCacheKey {
	return nameCacheKey{parentID, parentSchemaID, name}
}

// Manager manages acquiring and releasing per-descriptor leases. It also
// handles resolving descriptor names to descriptor IDs. The leases are managed
// internally with a descriptor and expiration time exported by the
// API. The descriptor acquired needs to be released. A transaction
// can use a descriptor as long as its timestamp is within the
// validity window for the descriptor:
// descriptor.ModificationTime <= txn.Timestamp < expirationTime
//
// Exported only for testing.
//
// The locking order is:
// Manager.mu > descriptorState.mu > nameCache.mu > descriptorVersionState.mu
type Manager struct {
	storage storage
	mu      struct {
		syncutil.Mutex
		descriptors map[sqlbase.ID]*descriptorState

		// updatesResolvedTimestamp keeps track of a timestamp before which all
		// descriptor updates have already been seen.
		updatesResolvedTimestamp hlc.Timestamp
	}

	draining atomic.Value

	// names is a cache for name -> id mappings. A mapping for the cache
	// should only be used if we currently have an active lease on the respective
	// id; otherwise, the mapping may well be stale.
	// Not protected by mu.
	names        nameCache
	testingKnobs ManagerTestingKnobs
	ambientCtx   log.AmbientContext
	stopper      *stop.Stopper
	sem          *quotapool.IntPool
}

const leaseConcurrencyLimit = 5

// NewLeaseManager creates a new Manager.
//
// internalExecutor can be nil to help bootstrapping, but then it needs to be set via
// SetInternalExecutor before the Manager is used.
//
// stopper is used to run async tasks. Can be nil in tests.
func NewLeaseManager(
	ambientCtx log.AmbientContext,
	nodeIDContainer *base.SQLIDContainer,
	db *kv.DB,
	clock *hlc.Clock,
	internalExecutor sqlutil.InternalExecutor,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	testingKnobs ManagerTestingKnobs,
	stopper *stop.Stopper,
	cfg *base.LeaseManagerConfig,
) *Manager {
	lm := &Manager{
		storage: storage{
			nodeIDContainer:     nodeIDContainer,
			db:                  db,
			clock:               clock,
			internalExecutor:    internalExecutor,
			settings:            settings,
			codec:               codec,
			group:               &singleflight.Group{},
			leaseDuration:       cfg.DescriptorLeaseDuration,
			leaseJitterFraction: cfg.DescriptorLeaseJitterFraction,
			leaseRenewalTimeout: cfg.DescriptorLeaseRenewalTimeout,
			testingKnobs:        testingKnobs.LeaseStoreTestingKnobs,
		},
		testingKnobs: testingKnobs,
		names: nameCache{
			descriptors: make(map[nameCacheKey]*descriptorVersionState),
		},
		ambientCtx: ambientCtx,
		stopper:    stopper,
		sem:        quotapool.NewIntPool("lease manager", leaseConcurrencyLimit),
	}
	lm.stopper.AddCloser(lm.sem.Closer("stopper"))
	lm.mu.descriptors = make(map[sqlbase.ID]*descriptorState)
	lm.mu.updatesResolvedTimestamp = db.Clock().Now()

	lm.draining.Store(false)
	return lm
}

// NameMatchesDescriptor returns true if the provided name and IDs match this
// descriptor.
func NameMatchesDescriptor(
	desc catalog.Descriptor, parentID sqlbase.ID, parentSchemaID sqlbase.ID, name string,
) bool {
	return desc.GetParentID() == parentID &&
		desc.GetParentSchemaID() == parentSchemaID &&
		desc.GetName() == name
}

// findNewest returns the newest descriptor version state for the ID.
func (m *Manager) findNewest(id sqlbase.ID) *descriptorVersionState {
	t := m.findDescriptorState(id, false /* create */)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.active.findNewest()
}

// AcquireByName returns a version for the specified descriptor valid for
// the timestamp. It returns the descriptor and a expiration time.
// A transaction using this descriptor must ensure that its
// commit-timestamp < expiration-time. Care must be taken to not modify
// the returned descriptor. Renewal of a lease may begin in the
// background. Renewal is done in order to prevent blocking on future
// acquisitions.
//
// Known limitation: AcquireByName() calls Acquire() and therefore suffers
// from the same limitation as Acquire (See Acquire). AcquireByName() is
// unable to function correctly on a timestamp less than the timestamp
// of a transaction with a DROP/TRUNCATE on the descriptor. The limitation in
// the face of a DROP follows directly from the limitation on Acquire().
// A TRUNCATE is implemented by changing the name -> id mapping
// and by dropping the descriptor with the old id. While AcquireByName
// can use the timestamp and get the correct name->id  mapping at a
// timestamp, it uses Acquire() to get a descriptor with the corresponding
// id and fails because the id has been dropped by the TRUNCATE.
func (m *Manager) AcquireByName(
	ctx context.Context,
	timestamp hlc.Timestamp,
	parentID sqlbase.ID,
	parentSchemaID sqlbase.ID,
	name string,
) (catalog.Descriptor, hlc.Timestamp, error) {
	// Check if we have cached an ID for this name.
	descVersion := m.names.get(parentID, parentSchemaID, name, timestamp)
	if descVersion != nil {
		if descVersion.GetModificationTime().LessEq(timestamp) {
			// If this lease is nearly expired, ensure a renewal is queued.
			durationUntilExpiry := time.Duration(descVersion.expiration.WallTime - timestamp.WallTime)
			if durationUntilExpiry < m.storage.leaseRenewalTimeout {
				if t := m.findDescriptorState(descVersion.GetID(), false /* create */); t != nil {
					if err := t.maybeQueueLeaseRenewal(
						ctx, m, descVersion.GetID(), name); err != nil {
						return nil, hlc.Timestamp{}, err
					}
				}
			}
			return descVersion.Descriptor, descVersion.expiration, nil
		}
		if err := m.Release(descVersion); err != nil {
			return nil, hlc.Timestamp{}, err
		}
		// Return a valid descriptor for the timestamp.
		desc, expiration, err := m.Acquire(ctx, timestamp, descVersion.GetID())
		if err != nil {
			return nil, hlc.Timestamp{}, err
		}
		return desc, expiration, nil
	}

	// We failed to find something in the cache, or what we found is not
	// guaranteed to be valid by the time we use it because we don't have a
	// lease with at least a bit of lifetime left in it. So, we do it the hard
	// way: look in the database to resolve the name, then acquire a new lease.
	var err error
	id, err := m.resolveName(ctx, timestamp, parentID, parentSchemaID, name)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	desc, expiration, err := m.Acquire(ctx, timestamp, id)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	if !NameMatchesDescriptor(desc, parentID, parentSchemaID, name) {
		// We resolved name `name`, but the lease has a different name in it.
		// That can mean two things. Assume the descriptor is being renamed from A to B.
		// a) `name` is A. The transaction doing the RENAME committed (so the
		// descriptor has been updated to B), but its schema changer has not
		// finished yet. B is the new name of the descriptor, queries should use that. If
		// we already had a lease with name A, we would've allowed to use it (but we
		// don't, otherwise the cache lookup above would've given it to us).  Since
		// we don't, let's not allow A to be used, given that the lease now has name
		// B in it. It'd be sketchy to allow A to be used with an inconsistent name
		// in the descriptor.
		//
		// b) `name` is B. Like in a), the transaction doing the RENAME
		// committed (so the descriptor has been updated to B), but its schema
		// change has not finished yet. We still had a valid lease with name A in
		// it. What to do, what to do? We could allow name B to be used, but who
		// knows what consequences that would have, since its not consistent with
		// the descriptor. We could say "descriptor B not found", but that means that, until
		// the next gossip update, this node would not service queries for this
		// descriptor under the name B. That's no bueno, as B should be available to be
		// used immediately after the RENAME transaction has committed.
		// The problem is that we have a lease that we know is stale (the descriptor
		// in the DB doesn't necessarily have a new version yet, but it definitely
		// has a new name). So, lets force getting a fresh descriptor.
		// This case (modulo the "committed" part) also applies when the txn doing a
		// RENAME had a lease on the old name, and then tries to use the new name
		// after the RENAME statement.
		//
		// How do we disambiguate between the a) and b)? We get a fresh lease on
		// the descriptor, as required by b), and then we'll know if we're trying to
		// resolve the current or the old name.
		//
		// TODO(vivek): check if the entire above comment is indeed true. Review the
		// use of NameMatchesDescriptor() throughout this function.
		if err := m.Release(desc); err != nil {
			log.Warningf(ctx, "error releasing lease: %s", err)
		}
		if err := m.AcquireFreshestFromStore(ctx, id); err != nil {
			return nil, hlc.Timestamp{}, err
		}
		desc, expiration, err = m.Acquire(ctx, timestamp, id)
		if err != nil {
			return nil, hlc.Timestamp{}, err
		}
		if !NameMatchesDescriptor(desc, parentID, parentSchemaID, name) {
			// If the name we had doesn't match the newest descriptor in the DB, then
			// we're trying to use an old name.
			if err := m.Release(desc); err != nil {
				log.Warningf(ctx, "error releasing lease: %s", err)
			}
			return nil, hlc.Timestamp{}, sqlbase.ErrDescriptorNotFound
		}
	}
	return desc, expiration, nil
}

// resolveName resolves a descriptor name to a descriptor ID at a particular
// timestamp by looking in the database. If the mapping is not found,
// sqlbase.ErrDescriptorNotFound is returned.
func (m *Manager) resolveName(
	ctx context.Context,
	timestamp hlc.Timestamp,
	parentID sqlbase.ID,
	parentSchemaID sqlbase.ID,
	name string,
) (sqlbase.ID, error) {
	id := sqlbase.InvalidID
	if err := m.storage.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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
		found, id, err = sqlbase.LookupObjectID(ctx, txn, m.storage.codec, parentID, parentSchemaID, name)
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

// Acquire acquires a read lease for the specified descriptor ID valid for
// the timestamp. It returns the descriptor and a expiration time.
// A transaction using this descriptor must ensure that its
// commit-timestamp < expiration-time. Care must be taken to not modify
// the returned descriptor.
//
// Known limitation: Acquire() can return an error after the descriptor with
// the ID has been dropped. This is true even when using a timestamp
// less than the timestamp of the DROP command. This is because Acquire
// can only return an older version of a descriptor if the latest version
// can be leased; as it stands a dropped descriptor cannot be leased.
func (m *Manager) Acquire(
	ctx context.Context, timestamp hlc.Timestamp, id sqlbase.ID,
) (catalog.Descriptor, hlc.Timestamp, error) {
	for {
		t := m.findDescriptorState(id, true /*create*/)
		desc, latest, err := t.findForTimestamp(ctx, timestamp)
		if err == nil {
			// If the latest lease is nearly expired, ensure a renewal is queued.
			if latest {
				durationUntilExpiry := time.Duration(desc.expiration.WallTime - timestamp.WallTime)
				if durationUntilExpiry < m.storage.leaseRenewalTimeout {
					if err := t.maybeQueueLeaseRenewal(ctx, m, id, desc.GetName()); err != nil {
						return nil, hlc.Timestamp{}, err
					}
				}
			}
			return desc.Descriptor, desc.expiration, nil
		}
		switch {
		case errors.Is(err, errRenewLease):
			if err := func() error {
				t.markAcquisitionStart(ctx)
				defer t.markAcquisitionDone(ctx)
				// Renew lease and retry. This will block until the lease is acquired.
				_, errLease := acquireNodeLease(ctx, m, id)
				return errLease
			}(); err != nil {
				return nil, hlc.Timestamp{}, err
			}

			if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
				m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(AcquireBlock)
			}

		case errors.Is(err, errReadOlderVersion):
			// Read old versions from the store. This can block while reading.
			versions, errRead := m.readOlderVersionForTimestamp(ctx, id, timestamp)
			if errRead != nil {
				return nil, hlc.Timestamp{}, errRead
			}
			m.insertDescriptorVersions(id, versions)

		default:
			return nil, hlc.Timestamp{}, err
		}
	}
}

// Release releases a previously acquired lease.
func (m *Manager) Release(desc catalog.Descriptor) error {
	t := m.findDescriptorState(desc.GetID(), false /* create */)
	if t == nil {
		return errors.Errorf("descriptor %d not found", desc.GetID())
	}
	// TODO(pmattis): Can/should we delete from Manager.descriptors if the
	// descriptorState becomes empty?
	// TODO(andrei): I think we never delete from Manager.descriptors... which
	// could be bad if a lot of descriptors keep being created. I looked into cleaning
	// up a bit, but it seems tricky to do with the current locking which is split
	// between Manager and descriptorState.
	l, err := t.release(desc, m.removeOnceDereferenced())
	if err != nil {
		return err
	}
	if l != nil {
		releaseLease(l, m)
	}
	return nil
}

// removeOnceDereferenced returns true if the Manager thinks
// a descriptorVersionState can be removed after its refcount goes to 0.
func (m *Manager) removeOnceDereferenced() bool {
	return m.storage.testingKnobs.RemoveOnceDereferenced ||
		// Release from the store if the Manager is draining.
		m.isDraining()
}

func (m *Manager) isDraining() bool {
	return m.draining.Load().(bool)
}

// SetDraining (when called with 'true') removes all inactive leases. Any leases
// that are active will be removed once the lease's reference count drops to 0.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (m *Manager) SetDraining(drain bool, reporter func(int, string)) {
	m.draining.Store(drain)
	if !drain {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.mu.descriptors {
		t.mu.Lock()
		leases := t.removeInactiveVersions()
		t.mu.Unlock()
		for _, l := range leases {
			releaseLease(l, m)
		}
		if reporter != nil {
			// Report progress through the Drain RPC.
			reporter(len(leases), "descriptor leases")
		}
	}
}

// If create is set, cache and stopper need to be set as well.
func (m *Manager) findDescriptorState(id sqlbase.ID, create bool) *descriptorState {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.mu.descriptors[id]
	if t == nil && create {
		t = &descriptorState{id: id, stopper: m.stopper}
		m.mu.descriptors[id] = t
	}
	return t
}

// RefreshLeases starts a goroutine that refreshes the lease manager
// leases for descriptors received in the latest system configuration via gossip or
// rangefeeds. This function must be passed a non-nil gossip if
// VersionRangefeedLeases is not active.
func (m *Manager) RefreshLeases(
	ctx context.Context, s *stop.Stopper, db *kv.DB, g gossip.DeprecatedGossip,
) {
	s.RunWorker(ctx, func(ctx context.Context) {
		m.refreshLeases(ctx, g, db, s)
	})
}

func (m *Manager) refreshLeases(
	ctx context.Context, g gossip.DeprecatedGossip, db *kv.DB, s *stop.Stopper,
) {
	descUpdateCh := make(chan *sqlbase.Descriptor)
	m.watchForUpdates(ctx, s, db, g, descUpdateCh)
	s.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case desc := <-descUpdateCh:
				// NB: We allow nil descriptors to be sent to synchronize the updating of
				// descriptors.
				if desc == nil {
					continue
				}

				if evFunc := m.testingKnobs.TestingDescriptorUpdateEvent; evFunc != nil {
					if err := evFunc(desc); err != nil {
						log.Infof(ctx, "skipping update of %v due to knob: %v",
							desc, err)
					}
				}

				// Handle dropping/offline tables as a special case.
				// TODO (lucy): It's possible that with a more general API for offline
				// descriptors we'll need to rethink using sqlbase.Descriptor here.
				goingOffline := false
				if table := desc.Table(hlc.Timestamp{}); table != nil {
					goingOffline = table.GoingOffline()
				}
				// Try to refresh the lease to one >= this version.
				log.VEventf(ctx, 2, "purging old version of descriptor %d@%d (offline %v)",
					desc.GetID(), desc.GetVersion(), goingOffline)
				if err := purgeOldVersions(
					ctx, db, desc.GetID(), goingOffline, desc.GetVersion(), m); err != nil {
					log.Warningf(ctx, "error purging leases for descriptor %d(%s): %s",
						desc.GetID(), desc.GetName(), err)
				}

				if evFunc := m.testingKnobs.TestingDescriptorRefreshedEvent; evFunc != nil {
					evFunc(desc)
				}

			case <-s.ShouldQuiesce():
				return
			}
		}
	})
}

// watchForUpdates will watch either gossip or rangefeeds for updates. If the
// version does not currently support rangefeeds, gossip will be used until
// rangefeeds are supported, at which time, the system will shut down the
// gossip listener and start using rangefeeds.
func (m *Manager) watchForUpdates(
	ctx context.Context,
	s *stop.Stopper,
	db *kv.DB,
	g gossip.DeprecatedGossip,
	descUpdateCh chan *sqlbase.Descriptor,
) {
	useRangefeeds := m.testingKnobs.AlwaysUseRangefeeds ||
		m.storage.settings.Version.IsActive(ctx, clusterversion.VersionRangefeedLeases)
	if useRangefeeds {
		m.watchForRangefeedUpdates(ctx, s, db, descUpdateCh)
		return
	}
	gossipCtx, cancelWatchingGossip := context.WithCancel(ctx)
	m.watchForGossipUpdates(gossipCtx, s, g, descUpdateCh)
	canUseRangefeedsCh := m.waitForRangefeedsToBeUsable(ctx, s)
	if err := s.RunAsyncTask(ctx, "wait for upgrade", func(ctx context.Context) {
		select {
		case <-s.ShouldQuiesce():
			return
		case <-canUseRangefeedsCh:
			// Note: It's okay that the cancelation of gossip watching is
			// asynchronous. At worst we'd get duplicate updates or stale updates.
			// Both of those are handled.
			cancelWatchingGossip()
			// Note: It's safe to start watching for rangefeeds now. We know that all
			// nodes support rangefeeds in the system config span. Even though there
			// may not have been logical ops for all operations in the log, the
			// catch-up scan should take us up to the present.
			//
			// When the rangefeed starts up we'll pass it an initial timestamp which
			// is no newer than all updates to the system config span we've already
			// seen (see setResolvedTimestamp and its callers). The rangefeed API
			// ensures that we will see all updates from on or before that timestamp
			// at least once.
			m.watchForRangefeedUpdates(ctx, s, db, descUpdateCh)
		}
	}); err != nil {
		// Note: this can only happen if the stopper has been stopped.
		return
	}
}

func (m *Manager) watchForGossipUpdates(
	ctx context.Context,
	s *stop.Stopper,
	g gossip.DeprecatedGossip,
	descUpdateCh chan<- *sqlbase.Descriptor,
) {
	rawG, err := g.OptionalErr(47150)
	if err != nil {
		if v := clusterversion.VersionRangefeedLeases; !m.storage.settings.Version.IsActive(ctx, v) {
			log.Fatalf(ctx, "required gossip until %v is active: %v", clusterversion.VersionRangefeedLeases, err)
		}
		return
	}

	s.RunWorker(ctx, func(ctx context.Context) {
		descKeyPrefix := m.storage.codec.TablePrefix(uint32(sqlbase.DescriptorTable.ID))
		// TODO(ajwerner): Add a mechanism to unregister this channel upon return.
		// NB: this call is allowed to bypass DeprecatedGossip because we'll never
		// get here after VersionRangefeedLeases.
		gossipUpdateC := rawG.RegisterSystemConfigChannel()
		filter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)

		ctx, cancel := s.WithCancelOnQuiesce(ctx)
		defer cancel()
		for {
			select {
			case <-gossipUpdateC:
				m.handleUpdatedSystemCfg(ctx, rawG, &filter, descUpdateCh)
			case <-s.ShouldQuiesce():
				return
			}
		}
	})
}

func (m *Manager) watchForRangefeedUpdates(
	ctx context.Context, s *stop.Stopper, db *kv.DB, descUpdateCh chan<- *sqlbase.Descriptor,
) {
	if log.V(1) {
		log.Infof(ctx, "using rangefeeds for lease manager updates")
	}
	distSender := db.NonTransactionalSender().(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
	eventCh := make(chan *roachpb.RangeFeedEvent)
	ctx, _ = s.WithCancelOnQuiesce(ctx)
	if err := s.RunAsyncTask(ctx, "lease rangefeed", func(ctx context.Context) {
		for {
			ts := m.getResolvedTimestamp()
			descKeyPrefix := m.storage.codec.TablePrefix(uint32(sqlbase.DescriptorTable.ID))
			span := roachpb.Span{
				Key:    descKeyPrefix,
				EndKey: descKeyPrefix.PrefixEnd(),
			}
			// Note: We don't need to use withDiff to detect version changes because
			// the Manager already stores the relevant version information.
			const withDiff = false
			log.VEventf(ctx, 1, "starting rangefeed from %v on %v", ts, span)
			err := distSender.RangeFeed(ctx, span, ts, withDiff, eventCh)
			if err != nil && ctx.Err() == nil {
				log.Warningf(ctx, "lease rangefeed failed, restarting: %v", err)
			}
			if ctx.Err() != nil {
				log.VEventf(ctx, 1, "exiting rangefeed")
				return
			}
		}
	}); err != nil {
		// This will only fail if the stopper has been stopped.
		return
	}
	handleEvent := func(ev *roachpb.RangeFeedValue) {
		if len(ev.Value.RawBytes) == 0 {
			return
		}
		var descriptor sqlbase.Descriptor
		if err := ev.Value.GetProto(&descriptor); err != nil {
			log.ReportOrPanic(ctx, &m.storage.settings.SV,
				"%s: unable to unmarshal descriptor %v", ev.Key, ev.Value)
			return
		}
		if descriptor.Union == nil {
			return
		}
		descriptor.MaybeSetModificationTimeFromMVCCTimestamp(ctx, ev.Value.Timestamp)
		if log.V(2) {
			log.Infof(ctx, "%s: refreshing lease on descriptor: %d (%s), version: %d",
				ev.Key, descriptor.GetID(), descriptor.GetName(), descriptor.GetVersion())
		}
		select {
		case <-ctx.Done():
		case descUpdateCh <- &descriptor:
		}
	}
	s.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-eventCh:
				if e.Checkpoint != nil {
					log.VEventf(ctx, 2, "got rangefeed checkpoint %v", e.Checkpoint)
					m.setResolvedTimestamp(e.Checkpoint.ResolvedTS)
					continue
				}
				if e.Error != nil {
					log.Warningf(ctx, "got an error from a rangefeed: %v", e.Error.Error)
					continue
				}
				if e.Val != nil {
					handleEvent(e.Val)
				}
			}
		}
	})
}

func (m *Manager) handleUpdatedSystemCfg(
	ctx context.Context,
	rawG *gossip.Gossip,
	cfgFilter *gossip.SystemConfigDeltaFilter,
	descUpdateCh chan<- *sqlbase.Descriptor,
) {
	cfg := rawG.GetSystemConfig()
	// Read all descriptors and their versions
	if log.V(2) {
		log.Info(ctx, "received a new config; will refresh leases")
	}
	var latestTimestamp hlc.Timestamp
	cfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
		// Attempt to unmarshal config into a descriptor.
		var descriptor sqlbase.Descriptor
		if latestTimestamp.Less(kv.Value.Timestamp) {
			latestTimestamp = kv.Value.Timestamp
		}
		if err := kv.Value.GetProto(&descriptor); err != nil {
			log.Warningf(ctx, "%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
			return
		}
		if descriptor.Union == nil {
			return
		}
		descriptor.MaybeSetModificationTimeFromMVCCTimestamp(ctx, kv.Value.Timestamp)
		if log.V(2) {
			log.Infof(ctx, "%s: refreshing lease table: %d (%s), version: %d",
				kv.Key, descriptor.GetID(), descriptor.GetName(), descriptor.GetVersion())
		}
		select {
		case <-ctx.Done():
		case descUpdateCh <- &descriptor:
		}
	})
	if !latestTimestamp.IsEmpty() {
		m.setResolvedTimestamp(latestTimestamp)
	}
	// Attempt to shove a nil descriptor into the channel to ensure that
	// we've processed all of the events previously sent.
	select {
	case <-ctx.Done():
		// If we've been canceled, the other size of the channel will also have
		// been canceled.
	case descUpdateCh <- nil:
	}
}

// waitForRangefeedsToBeUsable returns a channel which is closed when rangefeeds
// are usable according to the cluster version.
func (m *Manager) waitForRangefeedsToBeUsable(ctx context.Context, s *stop.Stopper) chan struct{} {
	// TODO(ajwerner): Add a callback to notify about version changes.
	// Checking is pretty cheap but really this should be a callback.
	const defaultCheckInterval = 10 * time.Second
	checkInterval := defaultCheckInterval
	if m.testingKnobs.VersionPollIntervalForRangefeeds != 0 {
		checkInterval = m.testingKnobs.VersionPollIntervalForRangefeeds
	}
	upgradeChan := make(chan struct{})
	timer := timeutil.NewTimer()
	timer.Reset(0)
	s.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-timer.C:
				timer.Read = true
				if m.storage.settings.Version.IsActive(ctx, clusterversion.VersionRangefeedLeases) {
					close(upgradeChan)
					return
				}
				timer.Reset(checkInterval)
			case <-ctx.Done():
				return
			case <-s.ShouldQuiesce():
				return
			}
		}
	})
	return upgradeChan
}

// setResolvedTimestamp marks the Manager as having processed all updates
// up to this timestamp. It is set under the gossip path based on the highest
// timestamp seen in a system config and under the rangefeed path when a
// resolved timestamp is received.
func (m *Manager) setResolvedTimestamp(ts hlc.Timestamp) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.updatesResolvedTimestamp.Less(ts) {
		m.mu.updatesResolvedTimestamp = ts
	}
}

func (m *Manager) getResolvedTimestamp() hlc.Timestamp {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.updatesResolvedTimestamp
}

// leaseRefreshLimit is the upper-limit on the number of descriptor leases
// that will continuously have their lease refreshed.
var leaseRefreshLimit = settings.RegisterIntSetting(
	"sql.tablecache.lease.refresh_limit",
	"maximum number of descriptors to periodically refresh leases for",
	50,
)

// PeriodicallyRefreshSomeLeases so that leases are fresh and can serve
// traffic immediately.
// TODO(vivek): Remove once epoch based table leases are implemented.
func (m *Manager) PeriodicallyRefreshSomeLeases(ctx context.Context) {
	m.stopper.RunWorker(ctx, func(ctx context.Context) {
		if m.storage.leaseDuration <= 0 {
			return
		}
		refreshTimer := timeutil.NewTimer()
		defer refreshTimer.Stop()
		refreshTimer.Reset(m.storage.jitteredLeaseDuration() / 2)
		for {
			select {
			case <-m.stopper.ShouldQuiesce():
				return

			case <-refreshTimer.C:
				refreshTimer.Read = true
				refreshTimer.Reset(m.storage.jitteredLeaseDuration() / 2)

				m.refreshSomeLeases(ctx)
			}
		}
	})
}

// Refresh some of the current leases.
func (m *Manager) refreshSomeLeases(ctx context.Context) {
	limit := leaseRefreshLimit.Get(&m.storage.settings.SV)
	if limit <= 0 {
		return
	}
	// Construct a list of descriptors needing their leases to be reacquired.
	m.mu.Lock()
	ids := make([]sqlbase.ID, 0, len(m.mu.descriptors))
	var i int64
	for k, desc := range m.mu.descriptors {
		if i++; i > limit {
			break
		}
		desc.mu.Lock()
		dropped := desc.mu.dropped
		desc.mu.Unlock()
		if !dropped {
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
			ctx, fmt.Sprintf("refresh descriptor: %d lease", id), m.sem, true /*wait*/, func(ctx context.Context) {
				defer wg.Done()
				if _, err := acquireNodeLease(ctx, m, id); err != nil {
					log.Infof(ctx, "refreshing descriptor: %d lease failed: %s", id, err)
				}
			}); err != nil {
			log.Infof(ctx, "didnt refresh descriptor: %d lease: %s", id, err)
			wg.Done()
		}
	}
	wg.Wait()
}

// DeleteOrphanedLeases releases all orphaned leases created by a prior
// instance of this node. timeThreshold is a walltime lower than the
// lowest hlc timestamp that the current instance of the node can use.
func (m *Manager) DeleteOrphanedLeases(timeThreshold int64) {
	if m.testingKnobs.DisableDeleteOrphanedLeases {
		return
	}
	// TODO(asubiotto): clear up the nodeID naming here and in the table below,
	// tracked as https://github.com/cockroachdb/cockroach/issues/48271.
	nodeID := m.storage.nodeIDContainer.SQLInstanceID()
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
			rows, err = m.storage.internalExecutor.Query(
				ctx, "read orphaned leases", nil /*txn*/, sqlQuery)
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
			lease := storedLease{
				id:         sqlbase.ID(tree.MustBeDInt(row[0])),
				version:    int(tree.MustBeDInt(row[1])),
				expiration: tree.MustBeDTimestamp(row[2]),
			}
			if err := m.stopper.RunLimitedAsyncTask(
				ctx, fmt.Sprintf("release lease %+v", lease), m.sem, true /*wait*/, func(ctx context.Context) {
					m.storage.release(ctx, m.stopper, &lease)
					log.Infof(ctx, "released orphaned lease: %+v", lease)
					wg.Done()
				}); err != nil {
				log.Warningf(ctx, "did not release orphaned lease: %+v, err = %s", lease, err)
				wg.Done()
			}
		}
	})
}

// DB returns the Manager's handle to a kv.DB.
func (m *Manager) DB() *kv.DB {
	return m.storage.db
}

// Codec return the Manager's SQLCodec.
func (m *Manager) Codec() keys.SQLCodec {
	return m.storage.codec
}

// VisitLeases introspects the state of leases managed by the Manager.
//
// TODO(ajwerner): consider refactoring the function to take a struct, maybe
// called LeaseInfo.
func (m *Manager) VisitLeases(
	f func(desc catalog.Descriptor, dropped bool, refCount int, expiration tree.DTimestamp) (wantMore bool),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ts := range m.mu.descriptors {
		visitor := func() (wantMore bool) {
			ts.mu.Lock()
			defer ts.mu.Unlock()

			dropped := ts.mu.dropped

			for _, state := range ts.mu.active.data {
				state.mu.Lock()
				lease := state.mu.lease
				refCount := state.mu.refcount
				state.mu.Unlock()

				if lease == nil {
					continue
				}

				if !f(state.Descriptor, dropped, refCount, lease.expiration) {
					return false
				}
			}
			return true
		}
		if !visitor() {
			return
		}
	}
}

// TestingAcquireAndAssertMinVersion acquires a read lease for the specified
// ID. The lease is grabbed on the latest version if >= specified version.
// It returns a descriptor and an expiration time valid for the timestamp.
// This method is useful for testing and is only intended to be used in that
// context.
func (m *Manager) TestingAcquireAndAssertMinVersion(
	ctx context.Context, timestamp hlc.Timestamp, id sqlbase.ID, minVersion sqlbase.DescriptorVersion,
) (catalog.Descriptor, hlc.Timestamp, error) {
	t := m.findDescriptorState(id, true)
	if err := ensureVersion(ctx, id, minVersion, m); err != nil {
		return nil, hlc.Timestamp{}, err
	}
	desc, _, err := t.findForTimestamp(ctx, timestamp)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	return desc.Descriptor, desc.expiration, nil
}
