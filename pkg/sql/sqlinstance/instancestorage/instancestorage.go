// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package instancestorage package provides API to read from and write to the
// sql_instances system table.
package instancestorage

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ReclaimLoopInterval is the interval at which expired instance IDs are
// reclaimed and new ones will be preallocated.
var ReclaimLoopInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.instance_id.reclaim_interval",
	"interval at which instance IDs are reclaimed and preallocated",
	10*time.Minute,
	settings.PositiveDuration,
)

// PreallocatedCount refers to the number of preallocated instance IDs within
// the system.sql_instances table.
var PreallocatedCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.instance_id.preallocated_count",
	"number of preallocated instance IDs within the system.sql_instances table",
	10,
	// If this is 0, the assignment op will block forever if there are no
	// preallocated instance IDs.
	settings.IntInRange(1, math.MaxInt32),
)

var errNoPreallocatedRows = errors.New("no preallocated rows")

// Storage implements the storage layer for the sqlinstance subsystem.
//
// SQL Instance IDs must be globally unique. The SQL Instance table may be
// partitioned by region. In order to allow for fast cold starts, SQL Instances
// are pre-allocated into each region. If a sql_instance row does not have a
// session id, it is available for immediate use. It is also legal to reclaim
// instances ids if the owning session has expired.
type Storage struct {
	db            *kv.DB
	codec         keys.SQLCodec
	slReader      sqlliveness.Reader
	rowCodec      rowCodec
	settings      *cluster.Settings
	settingsWatch *settingswatcher.SettingsWatcher
	clock         *hlc.Clock
	f             *rangefeed.Factory
	cf            *descs.CollectionFactory
	// TestingKnobs refers to knobs used for testing.
	TestingKnobs struct {
		// JitteredIntervalFn corresponds to the function used to jitter the
		// reclaim loop timer.
		JitteredIntervalFn func(time.Duration) time.Duration
	}
}

// instancerow encapsulates data for a single row within the sql_instances table.
type instancerow struct {
	region        []byte
	instanceID    base.SQLInstanceID
	sqlAddr       string
	rpcAddr       string
	sessionID     sqlliveness.SessionID
	locality      roachpb.Locality
	binaryVersion roachpb.Version
	isDraining    bool
	timestamp     hlc.Timestamp
}

// isAvailable returns true if the instance row hasn't been claimed by a SQL pod
// (i.e. available for claiming), or false otherwise.
func (r *instancerow) isAvailable() bool {
	return r.sessionID == ""
}

// NewTestingStorage constructs a new storage with control for the database
// in which the sql_instances table should exist.
func NewTestingStorage(
	db *kv.DB,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	slReader sqlliveness.Reader,
	settings *cluster.Settings,
	clock *hlc.Clock,
	f *rangefeed.Factory,
	settingsWatch *settingswatcher.SettingsWatcher,
) *Storage {
	s := &Storage{
		db:            db,
		codec:         codec,
		rowCodec:      makeRowCodec(codec, table, true),
		slReader:      slReader,
		clock:         clock,
		f:             f,
		settings:      settings,
		settingsWatch: settingsWatch,
		cf:            descs.NewBareBonesCollectionFactory(settings, codec),
	}
	return s
}

// NewStorage creates a new storage struct.
func NewStorage(
	db *kv.DB,
	codec keys.SQLCodec,
	slReader sqlliveness.Reader,
	settings *cluster.Settings,
	clock *hlc.Clock,
	f *rangefeed.Factory,
	settingsWatcher *settingswatcher.SettingsWatcher,
) *Storage {
	return NewTestingStorage(db, codec, systemschema.SQLInstancesTable(), slReader, settings, clock, f, settingsWatcher)
}

// CreateNodeInstance claims a unique instance identifier for the SQL pod, and
// associates it with its SQL address and session information.
func (s *Storage) CreateNodeInstance(
	ctx context.Context,
	session sqlliveness.Session,
	rpcAddr string,
	sqlAddr string,
	locality roachpb.Locality,
	binaryVersion roachpb.Version,
	nodeID roachpb.NodeID,
) (instance sqlinstance.InstanceInfo, _ error) {
	return s.createInstanceRow(ctx, session, rpcAddr, sqlAddr, locality, binaryVersion, nodeID)
}

const noNodeID = 0

// CreateInstance claims a unique instance identifier for the SQL pod, and
// associates it with its SQL address and session information.
func (s *Storage) CreateInstance(
	ctx context.Context,
	session sqlliveness.Session,
	rpcAddr string,
	sqlAddr string,
	locality roachpb.Locality,
	binaryVersion roachpb.Version,
) (instance sqlinstance.InstanceInfo, _ error) {
	return s.createInstanceRow(ctx, session, rpcAddr, sqlAddr, locality, binaryVersion, noNodeID)
}

// getKeyAndInstance is a helper method to form key from session id and instance
// id and get the value with that key.
func (s *Storage) getKeyAndInstance(
	ctx context.Context, sessionID sqlliveness.SessionID, instanceID base.SQLInstanceID, txn *kv.Txn,
) (roachpb.Key, instancerow, error) {
	instance := instancerow{}
	region, _, err := slstorage.UnsafeDecodeSessionID(sessionID)
	if err != nil {
		return nil, instance, errors.Wrap(err, "unable to determine region for sql_instance")
	}

	key := s.rowCodec.encodeKey(region, instanceID)
	kv, err := txn.Get(ctx, key)
	if err != nil {
		return nil, instance, err
	}

	instance, err = s.rowCodec.decodeRow(kv.Key, kv.Value)
	if err != nil {
		return nil, instance, err
	}

	return key, instance, nil
}

// SetInstanceDraining sets the is_draining column of sql_instances system table
// to true.
func (s *Storage) SetInstanceDraining(
	ctx context.Context, sessionID sqlliveness.SessionID, instanceID base.SQLInstanceID,
) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		key, n, err := s.getKeyAndInstance(ctx, sessionID, instanceID, txn)
		if err != nil {
			return err
		}
		// TODO: When can be instance.sessionID unequal sessionID?

		batch := txn.NewBatch()
		value, err := s.rowCodec.encodeValue(
			n.rpcAddr, n.sqlAddr, n.sessionID, n.locality, n.binaryVersion,
			true /* encodeIsDraining */, true /* isDraining */)
		if err != nil {
			return err
		}
		batch.Put(key, value)
		return txn.CommitInBatch(ctx, batch)
	})
}

// ReleaseInstance deallocates the instance id iff it is currently owned by the
// provided sessionID.
func (s *Storage) ReleaseInstance(
	ctx context.Context, sessionID sqlliveness.SessionID, instanceID base.SQLInstanceID,
) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		key, instance, err := s.getKeyAndInstance(ctx, sessionID, instanceID, txn)
		if err != nil {
			return err
		}
		if instance.sessionID != sessionID {
			// Great! The session was already released or released and
			// claimed by another server.
			return nil
		}

		batch := txn.NewBatch()
		value, err := s.rowCodec.encodeAvailableValue(true /* encodeIsDraining */)
		if err != nil {
			return err
		}
		batch.Put(key, value)
		return txn.CommitInBatch(ctx, batch)
	})
}

func (s *Storage) createInstanceRow(
	ctx context.Context,
	session sqlliveness.Session,
	rpcAddr string,
	sqlAddr string,
	locality roachpb.Locality,
	binaryVersion roachpb.Version,
	nodeID roachpb.NodeID,
) (instance sqlinstance.InstanceInfo, _ error) {
	if len(sqlAddr) == 0 || len(rpcAddr) == 0 {
		return sqlinstance.InstanceInfo{}, errors.AssertionFailedf("missing sql or rpc address information for instance")
	}
	if len(session.ID()) == 0 {
		return sqlinstance.InstanceInfo{}, errors.AssertionFailedf("no session information for instance")
	}

	region, _, err := slstorage.UnsafeDecodeSessionID(session.ID())
	if err != nil {
		return sqlinstance.InstanceInfo{}, errors.Wrap(err, "unable to determine region for sql_instance")
	}

	// TODO(jeffswenson): advance session expiration. This can get stuck in a
	// loop if the session already expired.
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	assignInstance := func() (base.SQLInstanceID, error) {
		var availableID base.SQLInstanceID
		if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Run the claim transaction as high priority to ensure that it does not
			// contend with other transactions.
			err := txn.SetUserPriority(roachpb.MaxUserPriority)
			if err != nil {
				return err
			}

			// Set the transaction deadline to the session expiration to ensure
			// transaction commits before the session expires.
			err = txn.UpdateDeadline(ctx, session.Expiration())
			if err != nil {
				return err
			}

			// TODO(dt): do we need this at all? this keeps nodeID == instanceID when
			// running mixed KV and SQL nodes, but bakes in the assumption that any
			// clusters where this happens will contain _only_ mixed KV and SQL nodes
			// and thus do not need to worry about finding an _actually_ available ID
			// and avoiding conflicts. This is true today but may not be in more
			// complex deployments.
			if nodeID != noNodeID {
				availableID = base.SQLInstanceID(nodeID)
			} else {
				// Try to retrieve an available instance ID. This blocks until one
				// is available.
				availableID, err = s.getAvailableInstanceIDForRegion(ctx, region, txn)
				if err != nil {
					return err
				}
			}

			b := txn.NewBatch()

			value, err := s.rowCodec.encodeValue(rpcAddr, sqlAddr,
				session.ID(), locality, binaryVersion,
				true /* encodeIsDraining*/, false /* isDraining */)
			if err != nil {
				return err
			}
			b.Put(s.rowCodec.encodeKey(region, availableID), value)

			return txn.CommitInBatch(ctx, b)
		}); err != nil {
			return base.SQLInstanceID(0), err
		}
		return availableID, nil
	}
	// It's possible that all allocated IDs are claimed, so retry with a back
	// off.
	opts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		log.Infof(ctx, "assigning instance id to rpc addr %s and sql addr %s", rpcAddr, sqlAddr)
		instanceID, err := assignInstance()
		// Instance was successfully assigned an ID.
		if err == nil {
			return sqlinstance.InstanceInfo{
				Region:          region,
				InstanceID:      instanceID,
				InstanceRPCAddr: rpcAddr,
				InstanceSQLAddr: sqlAddr,
				SessionID:       session.ID(),
				Locality:        locality,
				BinaryVersion:   binaryVersion,
			}, err
		}
		if !errors.Is(err, errNoPreallocatedRows) {
			return sqlinstance.InstanceInfo{}, err
		}
		// If assignInstance failed because there are no available rows,
		// allocate new instance IDs for the local region.
		//
		// There is a choice during start up:
		//   1. Allocate for every region.
		//   2. Allocate only for the local region.
		//
		// Allocating only for the local region removes one global round trip.
		// In the uncontended case, allocating locally requires reading from
		// every region, then writing to the local region. Allocating globally
		// would require one round trip for reading and one round trip for
		// writes.
		if err := s.generateAvailableInstanceRows(ctx, [][]byte{region}, session.Expiration()); err != nil {
			log.Warningf(ctx, "failed to generate available instance rows: %v", err)
		}
	}

	// If we exit here, it has to be the case where the context has expired.
	return sqlinstance.InstanceInfo{}, ctx.Err()
}

// newInstanceCache constructs an instanceCache backed by a range feed over the
// sql_instances table. newInstanceCache blocks until the initial scan is
// complete.
func (s *Storage) newInstanceCache(ctx context.Context) (instanceCache, error) {
	return newRangeFeedCache(ctx, s.rowCodec, s.clock, s.f, s)
}

// getAvailableInstanceIDForRegion retrieves an available instance ID for the
// current region associated with Storage s, and returns errNoPreallocatedRows
// if there are no available rows.
func (s *Storage) getAvailableInstanceIDForRegion(
	ctx context.Context, region []byte, txn *kv.Txn,
) (base.SQLInstanceID, error) {
	rows, err := s.getInstanceRows(ctx, region, txn, lock.WaitPolicy_SkipLocked)
	if err != nil {
		return base.SQLInstanceID(0), err
	}

	for _, row := range rows {
		if row.isAvailable() {
			return row.instanceID, nil
		}
	}

	for _, row := range rows {
		// If the row has already been used, check if the session is alive.
		// This is beneficial since the rows already belong to the same region.
		// We will only do this after checking all **available** instance IDs.
		// If there are no locally available regions, the caller needs to
		// consult all regions to determine which IDs are safe to allocate.
		sessionAlive, _ := s.slReader.IsAlive(ctx, row.sessionID)
		if !sessionAlive {
			return row.instanceID, nil
		}
	}

	return base.SQLInstanceID(0), errNoPreallocatedRows
}

// reclaimRegion will reclaim instances belonging to expired sessions and
// delete surplus sessions. reclaimRegion should only be called by the
// background clean up and allocation job.
func (s *Storage) reclaimRegion(ctx context.Context, region []byte) error {
	// In a separate transaction, read all rows that exist in the region. This
	// allows us to check for expired sessions outside of a transaction. The
	// expired sessions are stored in a map that is consulted in the clean up
	// transaction. This is safe because once a session is in-active, it will
	// never become active again.
	var instances []instancerow
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		instances, err = s.getInstanceRows(ctx, region, txn, lock.WaitPolicy_Block)
		return err
	}); err != nil {
		return err
	}

	// Build a map of expired regions
	isExpired := map[sqlliveness.SessionID]bool{}
	for i := range instances {
		alive, err := s.slReader.IsAlive(ctx, instances[i].sessionID)
		if err != nil {
			return err
		}
		isExpired[instances[i].sessionID] = !alive
	}

	// Reclaim and delete rows
	target := int(PreallocatedCount.Get(&s.settings.SV))
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		instances, err := s.getInstanceRows(ctx, region, txn, lock.WaitPolicy_Block)
		if err != nil {
			return err
		}

		toReclaim, toDelete := idsToReclaim(target, instances, isExpired)

		writeBatch := txn.NewBatch()
		for _, instance := range toReclaim {
			availableValue, err := s.rowCodec.encodeAvailableValue(true /* encodeIsDraining */)
			if err != nil {
				return err
			}
			writeBatch.Put(s.rowCodec.encodeKey(region, instance), availableValue)
		}
		for _, instance := range toDelete {
			writeBatch.Del(s.rowCodec.encodeKey(region, instance))
		}

		return txn.CommitInBatch(ctx, writeBatch)
	})
}

// getAllInstanceRows returns all instance rows, including instance rows that
// are pre-allocated.
func (s *Storage) getAllInstanceRows(ctx context.Context, txn *kv.Txn) ([]instancerow, error) {
	return s.getInstanceRows(ctx, nil, txn, lock.WaitPolicy_Block)
}

// getInstanceRows decodes and returns all instance rows associated
// with a given region from the sql_instances table. This returns both used and
// available instance rows.
//
// TODO(rima): Add locking mechanism to prevent thrashing at startup in the
// case where multiple instances attempt to initialize their instance IDs
// simultaneously.
func (s *Storage) getInstanceRows(
	ctx context.Context, region []byte, txn *kv.Txn, waitPolicy lock.WaitPolicy,
) ([]instancerow, error) {
	var start roachpb.Key
	if region == nil {
		start = s.rowCodec.makeIndexPrefix()
	} else {
		start = s.rowCodec.makeRegionPrefix(region)
	}

	// Scan the entire range
	batch := txn.NewBatch()
	batch.Header.WaitPolicy = waitPolicy
	batch.Scan(start, start.PrefixEnd())
	if err := txn.Run(ctx, batch); err != nil {
		return nil, err
	}
	if len(batch.Results) != 1 {
		return nil, errors.AssertionFailedf("expected exactly on batch result found %d: %+v", len(batch.Results), batch.Results[1])
	}
	if err := batch.Results[0].Err; err != nil {
		return nil, err
	}
	rows := batch.Results[0].Rows

	// Convert the result to instancerows
	instances := make([]instancerow, len(rows))
	for i := range rows {
		var err error
		instances[i], err = s.rowCodec.decodeRow(rows[i].Key, rows[i].Value)
		if err != nil {
			return nil, err
		}
	}
	return instances, nil
}

// RunInstanceIDReclaimLoop runs a background task that allocates available
// instance IDs and reclaim expired ones within the sql_instances table.
func (s *Storage) RunInstanceIDReclaimLoop(
	ctx context.Context,
	stopper *stop.Stopper,
	ts timeutil.TimeSource,
	db descs.DB,
	sessionExpirationFn func() hlc.Timestamp,
) error {
	loadRegions := func(ctx context.Context) ([][]byte, error) {
		// Load regions from the system DB.
		var regionsBytes [][]byte
		if err := db.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			enumReps, _, err := sql.GetRegionEnumRepresentations(
				ctx, txn.KV(), keys.SystemDatabaseID, txn.Descriptors(),
			)
			if err != nil {
				if errors.Is(err, regions.ErrNotMultiRegionDatabase) {
					return nil
				}
				return err
			}
			for _, r := range enumReps {
				regionsBytes = append(regionsBytes, r)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		// The system database isn't multi-region.
		if len(regionsBytes) == 0 {
			regionsBytes = [][]byte{enum.One}
		}
		return regionsBytes, nil
	}

	return stopper.RunAsyncTask(ctx, "instance-id-reclaim-loop", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		jitter := jitteredInterval
		if s.TestingKnobs.JitteredIntervalFn != nil {
			jitter = s.TestingKnobs.JitteredIntervalFn
		}

		timer := ts.NewTimer()
		defer timer.Stop()
		for {
			timer.Reset(jitter(ReclaimLoopInterval.Get(&s.settings.SV)))
			select {
			case <-ctx.Done():
				return
			case <-timer.Ch():
				timer.MarkRead()

				// Load the regions each time we attempt to generate rows since
				// regions can be added/removed to/from the system DB.
				regions, err := loadRegions(ctx)
				if err != nil {
					log.Warningf(ctx, "failed to load regions from the system DB: %v", err)
					continue
				}

				// Mark instances that belong to expired sessions as available
				// and delete surplus IDs. Cleaning up surplus IDs is necessary
				// to avoid ID exhaustion.
				for _, region := range regions {

					if err := s.reclaimRegion(ctx, region); err != nil {
						log.Warningf(ctx, "failed to reclaim instances in region '%v': %v", region, err)
					}
				}

				// Allocate new ids regions that do not have enough pre-allocated sql instances.
				if err := s.generateAvailableInstanceRows(ctx, regions, sessionExpirationFn()); err != nil {
					log.Warningf(ctx, "failed to generate available instance rows: %v", err)
				}
			}
		}
	})
}

// readRegionsFromSystemDatabase reads physical representation for regions from the system database.
func (s *Storage) readRegionsFromSystemDatabase(
	ctx context.Context, txn *kv.Txn,
) ([][]byte, error) {
	descs := s.cf.NewCollection(ctx)
	descs.SetDescriptorSessionDataProvider(catsessiondata.DefaultDescriptorSessionDataProvider)
	defer descs.ReleaseAll(ctx)
	systemDB, err := descs.ByIDWithoutLeased(txn).Get().Database(ctx, keys.SystemDatabaseID)
	if err != nil {
		return nil, err
	}
	if !systemDB.IsMultiRegion() {
		return [][]byte{enum.One}, nil
	}
	regionEnumID, err := systemDB.MultiRegionEnumID()
	if err != nil {
		return nil, err
	}
	typeEnum, err := descs.ByIDWithoutLeased(txn).Get().Type(ctx, regionEnumID)
	if err != nil {
		return nil, err
	}
	regionEnum := typeEnum.AsRegionEnumTypeDescriptor()
	result := make([][]byte, 0, regionEnum.NumEnumMembers())
	for i := 0; i < regionEnum.NumEnumMembers(); i++ {
		result = append(result, regionEnum.GetMemberPhysicalRepresentation(i))
	}
	return result, nil
}

// generateAvailableInstanceRows allocates available instance IDs, and store
// them in the sql_instances table. When instance IDs are pre-allocated, all
// other fields in that row will be NULL.
func (s *Storage) generateAvailableInstanceRowsWithTxn(
	ctx context.Context, regions [][]byte, txn *kv.Txn, commit bool,
) error {
	target := int(PreallocatedCount.Get(&s.settings.SV))
	// Figure out which regions are down, so that we can skip them in the allocation
	// loop below.
	regionLiveness := regionliveness.NewLivenessProber(s.db, s.codec, nil, s.settings)
	downRegions, err := regionLiveness.QueryUnavailablePhysicalRegions(ctx, txn, true)
	if err != nil {
		return err
	}
	var onlineInstances []instancerow
	// Read all available regions from the system database.
	allRegions, err := s.readRegionsFromSystemDatabase(ctx, txn)
	if err != nil {
		return err
	}
	// Allocate instance rows by region, skipping over any regions
	// that were detected as down.
	for _, region := range allRegions {
		if downRegions.ContainsPhysicalRepresentation(string(region)) {
			continue
		}

		var instances []instancerow
		getInstanceRows := func(ctx context.Context) error {
			var err error
			instances, err = s.getInstanceRows(ctx, region /*global*/, txn, lock.WaitPolicy_Block)
			if err != nil {
				return err
			}
			return nil
		}
		// Attempt to fetch instance rows with a timeout when region liveness
		// is used. If the region times out, we are going to probe in later
		// on.
		if hasTimeout, timeout := regionLiveness.GetProbeTimeout(); hasTimeout {
			err = timeutil.RunWithTimeout(ctx, "get-instance-rows", timeout, getInstanceRows)
		} else {
			err = getInstanceRows(ctx)
		}
		if err != nil {
			if regionliveness.IsQueryTimeoutErr(err) {
				// Probe and mark the region potentially.
				probeErr := regionLiveness.ProbeLivenessWithPhysicalRegion(ctx, region)
				if probeErr != nil {
					err = errors.WithSecondaryError(err, probeErr)
					return err
				}
				return errors.Wrapf(err, "get-instance-rows timed out reading from a region")
			}
			return err
		}
		onlineInstances = append(onlineInstances, instances...)
	}

	b := txn.NewBatch()

	for _, row := range idsToAllocate(target, regions, onlineInstances) {
		value, err := s.rowCodec.encodeAvailableValue(true /* encodeIsDraining */)
		if err != nil {
			return errors.Wrapf(err, "failed to encode row for instance id %d", row.instanceID)
		}
		b.Put(s.rowCodec.encodeKey(row.region, row.instanceID), value)
	}
	if commit {
		return txn.CommitInBatch(ctx, b)
	}
	return txn.Run(ctx, b)
}

// generateAvailableInstanceRows allocates available instance IDs, and store
// them in the sql_instances table. When instance IDs are pre-allocated, all
// other fields in that row will be NULL.
func (s *Storage) generateAvailableInstanceRows(
	ctx context.Context, regions [][]byte, sessionExpiration hlc.Timestamp,
) error {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return s.generateAvailableInstanceRowsWithTxn(ctx, regions, txn, true)
	})
}

// idsToReclaim determines which instance rows with sessions should be
// reclaimed and which surplus instances should be deleted.
func idsToReclaim(
	target int, instances []instancerow, isExpired map[sqlliveness.SessionID]bool,
) (toReclaim []base.SQLInstanceID, toDelete []base.SQLInstanceID) {
	available := 0
	for _, instance := range instances {
		free := instance.isAvailable() || isExpired[instance.sessionID]
		switch {
		case !free:
			/* skip since it is in use */
		case target <= available:
			toDelete = append(toDelete, instance.instanceID)
		case instance.isAvailable():
			available += 1
		case isExpired[instance.sessionID]:
			available += 1
			toReclaim = append(toReclaim, instance.instanceID)
		}
	}
	return toReclaim, toDelete
}

// idsToAllocate inspects the allocated instances and determines which IDs
// should be allocated. It avoids any ID that is present in the existing
// instances. It only allocates for the passed in regions.
func idsToAllocate(
	target int, regions [][]byte, instances []instancerow,
) (toAllocate []instancerow) {
	availablePerRegion := map[string]int{}
	existingIDs := map[base.SQLInstanceID]struct{}{}
	for _, row := range instances {
		existingIDs[row.instanceID] = struct{}{}
		if row.isAvailable() {
			availablePerRegion[string(row.region)] += 1
		}
	}

	lastID := base.SQLInstanceID(0)
	nextID := func() base.SQLInstanceID {
		for {
			lastID += 1
			_, exists := existingIDs[lastID]
			if !exists {
				return lastID
			}
		}
	}

	for _, region := range regions {
		available := availablePerRegion[string(region)]
		for i := 0; i+available < target; i++ {
			toAllocate = append(toAllocate, instancerow{region: region, instanceID: nextID()})
		}
	}

	return toAllocate
}

// jitteredInterval returns a randomly jittered (+/-15%) duration.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.85 + 0.3*rand.Float64()))
}
