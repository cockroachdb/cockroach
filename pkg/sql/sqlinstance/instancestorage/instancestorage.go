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
// sql_instance system table.
package instancestorage

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Storage implements the storage layer for the sqlinstance subsystem.
type Storage struct {
	codec    keys.SQLCodec
	db       *kv.DB
	tableID  descpb.ID
	slReader sqlliveness.Reader
	rowcodec rowCodec
}

// instancerow encapsulates data for a single row within the system.sql_instances table.
type instancerow struct {
	instanceID base.SQLInstanceID
	addr       string
	sessionID  sqlliveness.SessionID
	timestamp  hlc.Timestamp
}

// NewTestingStorage constructs a new storage with control for the database
// in which the `sql_instances` table should exist.
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

// CreateInstance allocates a unique instance identifier for the SQL pod and
// associates it with its SQL address and session information.
func (s *Storage) CreateInstance(
	ctx context.Context,
	sessionID sqlliveness.SessionID,
	sessionExpiration hlc.Timestamp,
	addr string,
) (instanceID base.SQLInstanceID, _ error) {
	if len(addr) == 0 {
		return base.SQLInstanceID(0), errors.New("no address information for instance")
	}
	if len(sessionID) == 0 {
		return base.SQLInstanceID(0), errors.New("no session information for instance")
	}
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction deadline to the session expiration to
		// ensure transaction commits before the session expires.
		err := txn.UpdateDeadline(ctx, sessionExpiration)
		if err != nil {
			return err
		}
		rows, err := s.getAllInstanceRows(ctx, txn)
		if err != nil {
			return err
		}
		instanceID = s.getAvailableInstanceID(ctx, rows)
		row, err := s.rowcodec.encodeRow(instanceID, addr, sessionID, s.codec, s.tableID)
		if err != nil {
			log.Warningf(ctx, "failed to encode row for instance id %d: %v", instanceID, err)
			return err
		}
		return txn.Put(ctx, row.Key, row.Value)
	})

	if err != nil {
		return base.SQLInstanceID(0), err
	}
	return instanceID, nil
}

// getAvailableInstanceID returns the first available instanceID which can be used
// as a unique identifier for the current SQL pod.
func (s *Storage) getAvailableInstanceID(
	ctx context.Context, rows []instancerow,
) base.SQLInstanceID {
	if len(rows) == 0 {
		// Nothing to do, return starter instance ID 1.
		return base.SQLInstanceID(1)
	}
	// Sort current instances in the system.sql_instances table in the increasing order of their
	// instance IDs. The instance IDs should be a contiguous sequence. If there is any missing ID
	// in the sequence, we can reuse that ID for the current SQL pod.
	// Otherwise, determine the max of all active instance IDs, increment it by one and use that as
	// the instance ID for the current SQL pod.
	sort.SliceStable(rows, func(idx1, idx2 int) bool {
		return rows[idx1].instanceID < rows[idx2].instanceID
	})
	instanceCnt := len(rows)
	// Set instanceID to the max of all active instance IDs + 1
	instanceID := rows[instanceCnt-1].instanceID + 1
	// Initialize prevInstanceID with starter value of 0 as instanceIDs begin
	// from 1.
	prevInstanceID := base.SQLInstanceID(0)
	for i := 0; i < instanceCnt; i++ {
		// Check for a gap between adjacent instance IDs indicating
		// the availability of an unused instance ID.
		if rows[i].instanceID-prevInstanceID > 1 {
			instanceID = prevInstanceID + 1
			break
		}
		// Reuse instance ID if the session is no longer alive. An instance ID
		// could be associated with a dead session if the instance ID cleanup
		// does not occur during SQL pod shutdown such as during an instance panic.
		sessionAlive, _ := s.slReader.IsAlive(ctx, rows[i].sessionID)
		if !sessionAlive {
			instanceID = rows[i].instanceID
			break
		}
		prevInstanceID = rows[i].instanceID
	}
	return instanceID
}

// getInstanceData retrieves the network address for an instance given its instance ID.
func (s *Storage) getInstanceData(
	ctx context.Context, instanceID base.SQLInstanceID,
) (instanceData instancerow, _ error) {
	k := makeInstanceKey(s.codec, s.tableID, instanceID)
	row, err := s.db.Get(ctx, k)
	if err != nil {
		return instancerow{}, errors.Wrapf(err, "could not fetch instance %d", instanceID)
	}
	if row.Value == nil {
		return instancerow{}, sqlinstance.NonExistentInstanceError
	}
	_, addr, sessionID, timestamp, err := s.rowcodec.decodeRow(row)
	if err != nil {
		return instancerow{}, errors.Wrapf(err, "could not decode data for instance %d", instanceID)
	}
	instanceData = instancerow{
		instanceID: instanceID,
		addr:       addr,
		sessionID:  sessionID,
		timestamp:  timestamp,
	}
	return instanceData, nil
}

// getAllInstancesData retrieves instance information on all instances for the tenant.
func (s *Storage) getAllInstancesData(ctx context.Context) (instances []instancerow, err error) {
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		instances, err = s.getAllInstanceRows(ctx, txn)
		return err
	})
	if err != nil {
		return nil, err
	}
	return instances, nil
}

// getAllInstanceRows decodes and returns all instance rows from the system.sql_instances table
// TODO(rima): Add locking mechanism to prevent thrashing at startup in the case where
// multiple instances attempt to initialize their instance IDs simultaneously.
func (s *Storage) getAllInstanceRows(
	ctx context.Context, txn *kv.Txn,
) (instances []instancerow, _ error) {
	start := makeTablePrefix(s.codec, s.tableID)
	end := start.PrefixEnd()
	// Fetch all rows. The expected data size is small, so it should
	// be okay to fetch all rows together.
	const maxRows = 0
	rows, err := txn.Scan(ctx, start, end, maxRows)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		instanceID, addr, sessionID, timestamp, err := s.rowcodec.decodeRow(rows[i])
		if err != nil {
			log.Warningf(ctx, "failed to decode row %v: %v", rows[i].Key, err)
			return nil, err
		}
		curInstance := instancerow{
			instanceID: instanceID,
			addr:       addr,
			sessionID:  sessionID,
			timestamp:  timestamp,
		}
		instances = append(instances, curInstance)
	}
	return instances, nil
}

// ReleaseInstanceID releases an instance ID prior to shutdown of a SQL pod
// The instance ID can be reused by another SQL pod of the same tenant.
func (s *Storage) ReleaseInstanceID(ctx context.Context, id base.SQLInstanceID) error {
	key := makeInstanceKey(s.codec, s.tableID, id)
	if err := s.db.Del(ctx, key); err != nil {
		return errors.Wrapf(err, "could not delete instance %d", id)
	}
	return nil
}
