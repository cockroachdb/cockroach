// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instancestorage package provides API to read from and write to the
// sql_instances system table.
package instancestorage

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ReclaimLoopInterval is the interval at which expired instance IDs are
// reclaimed and new ones will be preallocated.
var ReclaimLoopInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.instance_id.reclaim_interval",
	"interval at which instance IDs are reclaimed and preallocated",
	10*time.Minute,
	settings.PositiveDuration,
)

// PreallocatedCount refers to the number of preallocated instance IDs within
// the system.sql_instances table.
var PreallocatedCount = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.instance_id.preallocated_count",
	"number of preallocated instance IDs within the system.sql_instances table",
	10,
	func(v int64) error {
		// If this is 0, the assignment op will block forever if there are no
		// preallocated instance IDs.
		if v < 1 {
			return errors.Errorf("cannot be less than 1: %d", v)
		}
		if v > math.MaxInt32 {
			return errors.Errorf("cannot be more than %d: %d", math.MaxInt32, v)
		}
		return nil
	},
)

var errNoPreallocatedRows = errors.New("no preallocated rows")

// Storage implements the storage layer for the sqlinstance subsystem.
type Storage struct {
	codec        keys.SQLCodec
	db           *kv.DB
	tableID      descpb.ID
	slReader     sqlliveness.Reader
	rowcodec     rowCodec
	settings     *cluster.Settings
	reclaimGroup singleflight.Group
	// TestingKnobs refers to knobs used for testing.
	TestingKnobs struct {
		// JitteredIntervalFn corresponds to the function used to jitter the
		// reclaim loop timer.
		JitteredIntervalFn func(time.Duration) time.Duration
	}
}

// instancerow encapsulates data for a single row within the sql_instances table.
type instancerow struct {
	instanceID base.SQLInstanceID
	addr       string
	sessionID  sqlliveness.SessionID
	locality   roachpb.Locality
	timestamp  hlc.Timestamp
}

// isAvailable returns true if the instance row hasn't been claimed by a SQL pod
// (i.e. available for claiming), or false otherwise.
func (r *instancerow) isAvailable() bool {
	return r.addr == ""
}

type dbScan interface {
	Scan(ctx context.Context, begin, end interface{}, maxRows int64) ([]kv.KeyValue, error)
}

// NewTestingStorage constructs a new storage with control for the database
// in which the sql_instances table should exist.
func NewTestingStorage(
	db *kv.DB,
	codec keys.SQLCodec,
	sqlInstancesTableID descpb.ID,
	slReader sqlliveness.Reader,
	settings *cluster.Settings,
) *Storage {
	s := &Storage{
		db:       db,
		codec:    codec,
		tableID:  sqlInstancesTableID,
		rowcodec: makeRowCodec(codec),
		slReader: slReader,
		settings: settings,
	}
	return s
}

// NewStorage creates a new storage struct.
func NewStorage(
	db *kv.DB, codec keys.SQLCodec, slReader sqlliveness.Reader, settings *cluster.Settings,
) *Storage {
	return NewTestingStorage(db, codec, keys.SQLInstancesTableID, slReader, settings)
}

// CreateInstance claims a unique instance identifier for the SQL pod, and
// associates it with its SQL address and session information.
func (s *Storage) CreateInstance(
	ctx context.Context,
	sessionID sqlliveness.SessionID,
	sessionExpiration hlc.Timestamp,
	addr string,
	locality roachpb.Locality,
) (instanceID base.SQLInstanceID, _ error) {
	if len(addr) == 0 {
		return base.SQLInstanceID(0), errors.New("no address information for instance")
	}
	if len(sessionID) == 0 {
		return base.SQLInstanceID(0), errors.New("no session information for instance")
	}
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
			err = txn.UpdateDeadline(ctx, sessionExpiration)
			if err != nil {
				return err
			}

			availableID, err = s.getAvailableInstanceIDForRegion(ctx, txn)
			if err != nil {
				return err
			}

			row, err := s.rowcodec.encodeRow(availableID, addr, sessionID, locality, s.codec, s.tableID)
			if err != nil {
				log.Warningf(ctx, "failed to encode row for instance id %d: %v", availableID, err)
				return err
			}

			b := txn.NewBatch()
			b.Put(row.Key, row.Value)
			return txn.CommitInBatch(ctx, b)
		}); err != nil {
			return base.SQLInstanceID(0), err
		}
		return availableID, nil
	}
	var err error
	// There's a possibility where there are no available rows, and the cached
	// reader takes time to fully find out that the sessions are dead, so retry
	// with a backoff.
	opts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		log.Infof(ctx, "assigning instance id to addr %s", addr)
		instanceID, err = assignInstance()
		// Instance was successfully assigned an ID.
		if err == nil {
			return instanceID, err
		}
		if !errors.Is(err, errNoPreallocatedRows) {
			return base.SQLInstanceID(0), err
		}
		// If the transaction failed because there were no pre-allocated rows,
		// trigger reclaiming, and retry. This blocks until the reclaim process
		// completes.
		if err := s.generateAvailableInstanceRows(ctx, sessionExpiration); err != nil {
			log.Warningf(ctx, "failed to generate available instance rows: %v", err)
		}
	}

	// If we exit here, it has to be the case where the context has expired.
	return base.SQLInstanceID(0), ctx.Err()
}

// getAvailableInstanceIDForRegion retrieves an available instance ID for the
// current region associated with Storage s, and returns errNoPreallocatedRows
// if there are no available rows.
//
// TODO(jaylim-crl): Store current region enum in s once we implement regional
// by row for the sql_instances table.
func (s *Storage) getAvailableInstanceIDForRegion(
	ctx context.Context, db dbScan,
) (base.SQLInstanceID, error) {
	rows, err := s.getRegionalInstanceRows(ctx, db)
	if err != nil {
		return base.SQLInstanceID(0), err
	}
	sortAvailableRowsFirst(rows)

	for i := 0; i < len(rows); i++ {
		if rows[i].isAvailable() {
			return rows[i].instanceID, nil
		}

		// If the row has already been used, check if the session is alive.
		// This is beneficial since the rows already belong to the same region.
		// We will only do this after checking all **available** instance IDs.
		sessionAlive, _ := s.slReader.IsAlive(ctx, rows[i].sessionID)
		if !sessionAlive {
			return rows[i].instanceID, nil
		}
	}
	return base.SQLInstanceID(0), errNoPreallocatedRows
}

// idsToReclaim retrieves two lists of instance IDs, one to claim, and another
// to delete.
func (s *Storage) idsToReclaim(
	ctx context.Context, db dbScan,
) (toClaim []base.SQLInstanceID, toDelete []base.SQLInstanceID, _ error) {
	rows, err := s.getGlobalInstanceRows(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	sortAvailableRowsFirst(rows)

	availableCount := 0
	for _, row := range rows {
		if row.isAvailable() {
			availableCount++
		} else {
			// Instance row has already been claimed.
			sessionAlive, _ := s.slReader.IsAlive(ctx, row.sessionID)
			if !sessionAlive {
				toClaim = append(toClaim, row.instanceID)
			}
		}
	}

	// Since PreallocatedCount is a cluster setting, there could be a scenario
	// where we have more pre-allocated rows than requested. In that case, we
	// will just ignore anyway. Eventually, it will converge to the requested
	// count.
	claimCount := int(math.Max(float64(int(PreallocatedCount.Get(&s.settings.SV))-availableCount), 0))

	// Truncate toClaim, delete the rest, and we are done here.
	if len(toClaim) > claimCount {
		return toClaim[:claimCount], toClaim[claimCount:], nil
	}

	// Sort in ascending order of instance IDs for the loop below.
	sort.SliceStable(rows, func(idx1, idx2 int) bool {
		return rows[idx1].instanceID < rows[idx2].instanceID
	})

	// Insufficient toClaim instances. Initialize prevInstanceID with starter
	// value of 0 as instanceIDs begin from 1.
	prevInstanceID := base.SQLInstanceID(0)
	for i := 0; i < len(rows) && len(toClaim) < claimCount; {
		// Check for a gap between adjacent instance IDs indicating the
		// availability of an unused instance ID.
		if rows[i].instanceID-prevInstanceID > 1 {
			toClaim = append(toClaim, prevInstanceID+1)
			prevInstanceID = prevInstanceID + 1
		} else {
			prevInstanceID = rows[i].instanceID
			i++
		}
	}

	remainingCount := claimCount - len(toClaim)
	for i := 1; i <= remainingCount; i++ {
		toClaim = append(toClaim, prevInstanceID+base.SQLInstanceID(i))
	}
	return toClaim, toDelete, nil
}

// sortAvailableRowsFirst sorts rows such that all available rows are placed
// in front.
func sortAvailableRowsFirst(rows []instancerow) {
	sort.SliceStable(rows, func(idx1, idx2 int) bool {
		addr1, addr2 := rows[idx1].addr, rows[idx2].addr
		switch {
		case addr1 == "" && addr2 == "":
			// Both are available.
			return rows[idx1].instanceID < rows[idx2].instanceID
		case addr1 == "":
			// addr1 should go before addr2.
			return true
		case addr2 == "":
			// addr2 should go before addr1.
			return false
		default:
			// Both are used.
			return rows[idx1].instanceID < rows[idx2].instanceID
		}
	})
}

// getGlobalInstanceRows decodes and returns all instance rows across all
// regions from the sql_instances table.
//
// TODO(jaylim-crl): For now, global and regional are the same.
func (s *Storage) getGlobalInstanceRows(
	ctx context.Context, db dbScan,
) (instances []instancerow, _ error) {
	return s.getRegionalInstanceRows(ctx, db)
}

// getRegionalInstanceRows decodes and returns all instance rows associated
// with a given region from the sql_instances table. This returns both used and
// available instance rows.
//
// TODO(rima): Add locking mechanism to prevent thrashing at startup in the
// case where multiple instances attempt to initialize their instance IDs
// simultaneously.
//
// TODO(jaylim-crl): This currently fetches all rows. We want to only fetch rows
// associated with the region of the SQL pod. This method will likely need to
// take in a region (or if we stored the region in s).
func (s *Storage) getRegionalInstanceRows(
	ctx context.Context, db dbScan,
) (instances []instancerow, _ error) {
	start := makeTablePrefix(s.codec, s.tableID)
	end := start.PrefixEnd()
	// Fetch all rows. The expected data size is small, so it should
	// be okay to fetch all rows together.
	const maxRows = 0
	rows, err := db.Scan(ctx, start, end, maxRows)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		instanceID, addr, sessionID, locality, timestamp, _, err := s.rowcodec.decodeRow(rows[i])
		if err != nil {
			log.Warningf(ctx, "failed to decode row %v: %v", rows[i].Key, err)
			return nil, err
		}
		curInstance := instancerow{
			instanceID: instanceID,
			addr:       addr,
			sessionID:  sessionID,
			timestamp:  timestamp,
			locality:   locality,
		}
		instances = append(instances, curInstance)
	}
	return instances, nil
}

// ReleaseInstanceID deletes an instance ID record. The instance ID can then be
// reused by another SQL pod of the same tenant.
func (s *Storage) ReleaseInstanceID(ctx context.Context, id base.SQLInstanceID) error {
	// TODO(andrei): Ensure that we do not delete an instance ID that we no longer
	// own, instead of deleting blindly.
	key := makeInstanceKey(s.codec, s.tableID, id)
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	if _, err := s.db.Del(ctx, key); err != nil {
		return errors.Wrapf(err, "could not delete instance %d", id)
	}
	return nil
}

// RunInstanceIDReclaimLoop runs a background task that allocates available
// instance IDs and reclaim expired ones within the sql_instances table.
func (s *Storage) RunInstanceIDReclaimLoop(
	ctx context.Context,
	stopper *stop.Stopper,
	ts timeutil.TimeSource,
	sessionExpirationFn func() hlc.Timestamp,
) error {
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
				if err := s.generateAvailableInstanceRows(ctx, sessionExpirationFn()); err != nil {
					log.Warningf(ctx, "failed to generate available instance rows: %v", err)
				}
			}
		}
	})
}

// generateAvailableInstanceRows allocates available instance IDs, and store
// them in the sql_instances table. When instance IDs are pre-allocated, all
// other fields in that row will be NULL.
//
// TODO(jaylim-crl): Handle multiple regions in this logic. encodeRow has to be
// updated with crdb_region. When we handle multiple regions, we have to figure
// out where to get the list of regions, and ensure that we don't do a global
// read for each region assuming that the number of pre-allocated entries is
// insufficient. One global KV read and write would be sufficient for **all**
// regions.
func (s *Storage) generateAvailableInstanceRows(
	ctx context.Context, sessionExpiration hlc.Timestamp,
) error {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	// We don't care about the results here.
	_, _, err := s.reclaimGroup.Do("reclaim-instance-ids", func() (interface{}, error) {
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Set the transaction deadline to the session expiration to ensure
			// transaction commits before the session expires.
			err := txn.UpdateDeadline(ctx, sessionExpiration)
			if err != nil {
				return err
			}

			// Fetch IDs to claim and delete.
			toClaim, toDelete, err := s.idsToReclaim(ctx, txn)
			if err != nil {
				return err
			}

			b := txn.NewBatch()
			for _, instanceID := range toClaim {
				row, err := s.rowcodec.encodeRow(
					instanceID, "", sqlliveness.SessionID([]byte{}), roachpb.Locality{}, s.codec, s.tableID,
				)
				if err != nil {
					log.Warningf(ctx, "failed to encode row for instance id %d: %v", instanceID, err)
					return err
				}
				b.Put(row.Key, row.Value)
			}
			for _, instanceID := range toDelete {
				key := makeInstanceKey(s.codec, s.tableID, instanceID)
				b.Del(key)
			}
			return txn.CommitInBatch(ctx, b)
		})
		return nil, err
	})
	return err
}

// jitteredInterval returns a randomly jittered (+/-15%) duration.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.85 + 0.3*rand.Float64()))
}
