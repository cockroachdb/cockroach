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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TODO(jaylim-crl): Consider making these values cluster settings, if necessary.
const (
	// AllocateLoopFrequency refers to the frequency in which the allocate
	// background task for instance rows will run.
	AllocateLoopFrequency = 30 * time.Second

	// preallocatedCount refers to the number of available instance rows that
	// we want to preallocate.
	preallocatedCount = 10
)

// Storage implements the storage layer for the sqlinstance subsystem.
type Storage struct {
	codec    keys.SQLCodec
	db       *kv.DB
	tableID  descpb.ID
	slReader sqlliveness.Reader
	rowcodec rowCodec
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
	db *kv.DB, codec keys.SQLCodec, sqlInstancesTableID descpb.ID, slReader sqlliveness.Reader,
) *Storage {
	s := &Storage{
		db:       db,
		codec:    codec,
		tableID:  sqlInstancesTableID,
		rowcodec: makeRowCodec(codec),
		slReader: slReader,
	}
	return s
}

// NewStorage creates a new storage struct.
func NewStorage(db *kv.DB, codec keys.SQLCodec, slReader sqlliveness.Reader) *Storage {
	return NewTestingStorage(db, codec, keys.SQLInstancesTableID, slReader)
}

// CreateInstance claims a unique instance identifier for the SQL pod, and
// associates it with its SQL address and session information.
//
// CreateInstance implements the instanceprovider.writer interface.
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
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction deadline to the session expiration to ensure
		// transaction commits before the session expires.
		err := txn.UpdateDeadline(ctx, sessionExpiration)
		if err != nil {
			return err
		}
		instanceIDs, err := s.getAvailableInstanceIDsForRegion(ctx, txn, 1 /* count */)
		if err != nil {
			return err
		}
		if len(instanceIDs) != 1 {
			return errors.AssertionFailedf("len(instanceIDs) must be 1")
		}
		// Regardless of whether the ID is pre-allocated, we will write to it.
		for id := range instanceIDs {
			instanceID = id
			break
		}
		row, err := s.rowcodec.encodeRow(instanceID, addr, sessionID, locality, s.codec, s.tableID)
		if err != nil {
			log.Warningf(ctx, "failed to encode row for instance id %d: %v", instanceID, err)
			return err
		}
		b := txn.NewBatch()
		b.Put(row.Key, row.Value)
		return txn.CommitInBatch(ctx, b)
	})
	if err != nil {
		return base.SQLInstanceID(0), err
	}
	return instanceID, nil
}

// getAvailableInstanceIDsForRegion retrieves a list of instance IDs that are
// available for a given region. The instance IDs retrieved may or may not been
// pre-allocated. When this returns without an error, it is guaranteed that the
// size of the map will be the same as count.
//
// TODO(jaylim-crl): Store current region enum in s once we implement regional
// by row for the sql_instances table.
func (s *Storage) getAvailableInstanceIDsForRegion(
	ctx context.Context, db dbScan, count int,
) (map[base.SQLInstanceID]bool, error) {
	instanceIDs := make(map[base.SQLInstanceID]bool)

	if count == 0 {
		return instanceIDs, nil
	}

	// Read regional rows first.
	rows, err := s.getRegionalInstanceRows(ctx, db)
	if err != nil {
		return nil, err
	}

	// Prioritize all available rows first.
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

	// Use pre-generated regional rows if they are available.
	for i := 0; i < len(rows) && len(instanceIDs) < count; i++ {
		if rows[i].isAvailable() {
			instanceIDs[rows[i].instanceID] = true
			continue
		}

		// If the row has already been used, check if the session is alive.
		// This is beneficial since the rows already belong to the same region.
		// We will only do this after checking all **available** instance IDs.
		sessionAlive, _ := s.slReader.IsAlive(ctx, rows[i].sessionID)
		if !sessionAlive {
			instanceIDs[rows[i].instanceID] = false
		}
	}

	// We are done. All the requested count are already pre-allocated.
	if len(instanceIDs) == count {
		return instanceIDs, nil
	}

	// Not enough pre-allocated regional rows, so we need to retrieve global
	// rows to obtain a new instance IDs.
	rows, err = s.getGlobalInstanceRows(ctx, db)
	if err != nil {
		return nil, err
	}

	// No global rows, so we can be sure that the next ID must be 1 onwards.
	// If we hit this case, len(instanceIDs) must be 0 (i.e. there shouldn't be
	// any regional rows as well).
	if len(rows) == 0 {
		for i := 0; i < count; i++ {
			instanceIDs[base.SQLInstanceID(i+1)] = false
		}
		return instanceIDs, nil
	}

	// Sort instance rows in increasing order of their instance IDs so that
	// active instance IDs are as close to a contiguous sequence. There will
	// definitely be gaps since instance IDs are only chosen from their regional
	// scopes.
	//
	// For example, in the case below, 2 and 3 are unused. When allocating IDs
	// for a third region, we pick 5.
	//   1: us-east1 [USED]
	//   2: us-east1
	//   3: europe-west1
	//   4: europe-west1 [USED]
	sort.SliceStable(rows, func(idx1, idx2 int) bool {
		return rows[idx1].instanceID < rows[idx2].instanceID
	})

	// Initialize prevInstanceID with starter value of 0 as instanceIDs begin
	// from 1.
	prevInstanceID := base.SQLInstanceID(0)
	for i := 0; i < len(rows) && len(instanceIDs) < count; {
		// Check for a gap between adjacent instance IDs indicating the
		// availability of an unused instance ID.
		if rows[i].instanceID-prevInstanceID > 1 {
			instanceIDs[prevInstanceID+1] = false
			prevInstanceID = prevInstanceID + 1
		} else {
			prevInstanceID = rows[i].instanceID
			i++
		}
	}

	remainingCount := count - len(instanceIDs)
	for i := 1; i <= remainingCount; i++ {
		instanceIDs[rows[len(rows)-1].instanceID+base.SQLInstanceID(i)] = false
	}
	return instanceIDs, nil
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
//
// ReleaseInstanceID implements the instanceprovider.writer interface.
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

// StartIDAllocator runs a background task that allocates available instance
// IDs within the sql_instances table.
//
// TODO(jaylim-crl): Add a background job to clean up expired entries. This
// would be similar to the TODO in ReleaseInstanceID.
func (s *Storage) StartIDAllocator(
	ctx context.Context,
	stopper *stop.Stopper,
	ts timeutil.TimeSource,
	sessionExpirationFn func() hlc.Timestamp,
) error {
	return stopper.RunAsyncTask(ctx, "allocate-ids", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		timer := ts.NewTimer()
		defer timer.Stop()

		// Fire timer immediately.
		timer.Reset(0)

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.Ch():
				timer.MarkRead()
				if err := s.generateAvailableInstanceRows(ctx, sessionExpirationFn()); err != nil {
					log.Warningf(ctx, "failed to generate available instance rows: %v", err)
				}
				timer.Reset(AllocateLoopFrequency)
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
// insufficient (i.e. we may not be able to use getAvailableInstanceIDsForRegion).
// One global KV read and write would be sufficient for all regions.
func (s *Storage) generateAvailableInstanceRows(
	ctx context.Context, sessionExpiration hlc.Timestamp,
) error {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction deadline to the session expiration to ensure
		// transaction commits before the session expires.
		err := txn.UpdateDeadline(ctx, sessionExpiration)
		if err != nil {
			return err
		}

		instanceIDs, err := s.getAvailableInstanceIDsForRegion(ctx, txn, preallocatedCount)
		if err != nil {
			return err
		}

		b := txn.NewBatch()
		for instanceID, preallocated := range instanceIDs {
			// Nothing to do if ids have already been pre-allocated.
			if preallocated {
				continue
			}
			row, err := s.rowcodec.encodeRow(
				instanceID, "", sqlliveness.SessionID([]byte{}), roachpb.Locality{}, s.codec, s.tableID,
			)
			if err != nil {
				log.Warningf(ctx, "failed to encode row for instance id %d: %v", instanceID, err)
				return err
			}
			b.Put(row.Key, row.Value)
		}
		return txn.CommitInBatch(ctx, b)
	})
}
