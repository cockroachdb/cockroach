// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instancestorage package provides API to
// manipulate the sql_instance system table
package instancestorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Storage implements the storage layer for
// the sqlinstance subsystem.
type Storage struct {
	codec    keys.SQLCodec
	stopper  *stop.Stopper
	db       *kv.DB
	tableID  descpb.ID
	rowcodec RowCodec
	mu       struct {
		syncutil.Mutex
		started bool
		// liveInstances caches information on all
		// live instances for a tenant.
		liveInstances *cache.UnorderedCache
	}
}

// NewTestingStorage constructs a new storage with control for the database
// in which the `sql_instances` table should exist.
func NewTestingStorage(
	stopper *stop.Stopper, db *kv.DB, codec keys.SQLCodec, sqlInstancesTableID descpb.ID,
) *Storage {
	s := &Storage{
		stopper: stopper,
		db:      db,
		codec:   codec,
		tableID: sqlInstancesTableID,
	}
	cacheConfig := cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			// Instance data size will be very small and shouldn't
			// require cache eviction.
			return false
		},
	}
	s.mu.liveInstances = cache.NewUnorderedCache(cacheConfig)
	s.rowcodec = makeRowCodec(codec)
	return s
}

// NewStorage creates a new storage struct.
func NewStorage(stopper *stop.Stopper, db *kv.DB, codec keys.SQLCodec) *Storage {
	return NewTestingStorage(stopper, db, codec, keys.SQLInstancesTableID)
}

// Start initiates the instancestorage internals.
// TODO(rima): Add rangefeed functionality
// for keeping cache consistent.
func (s *Storage) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.started {
		return
	}
	s.mu.started = true
}

// CreateInstance allocates a unique Instance identifier for the
// sql pod and associates it with its http address and session
// information.
func (s *Storage) CreateInstance(
	ctx context.Context, sessionID sqlliveness.SessionID, httpAddr string,
) (_ sqlinstance.SQLInstance, err error) {
	if err = s.checkStarted(); err != nil {
		return nil, err
	}
	instance := NewSQLInstance(base.SQLInstanceID(1), httpAddr, sessionID) // Starter value
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		start := s.makeTablePrefix()
		end := start.PrefixEnd()
		const maxRows = 1024 // arbitrary but plenty
		for {
			rows, err := txn.Scan(ctx, start, end, maxRows)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			for i := range rows {
				currentRow, err := s.rowcodec.decodeRow(rows[i])
				if err != nil {
					log.Warningf(ctx, "failed to decode row %s: %v", rows[i].Key.String(), err)
					return err
				}
				// If Instance id is available, reuse it.
				if len(currentRow.httpAddr) == 0 {
					instance.id = currentRow.id
					break
				}
				if currentRow.id >= instance.id {
					instance.id = currentRow.id + 1
				}
			}
			start = rows[len(rows)-1].Key.Next()
		}
		key := s.makeInstanceKey(instance.id)
		value, err := s.rowcodec.encodeRow(instance)
		if err != nil {
			log.Warningf(ctx, "failed to encode row for Instance id %d: %v", instance.id, err)
			return err
		}
		return s.db.Put(ctx, key, &value)
	})
	if err != nil {
		return nil, err
	}
	return instance, nil
}

// GetInstanceAddr retrieves the network address for an Instance
// given its Instance id.
func (s *Storage) GetInstanceAddr(
	ctx context.Context, id base.SQLInstanceID,
) (httpAddr string, err error) {
	if err = s.checkStarted(); err != nil {
		return "", err
	}
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		k := s.makeInstanceKey(id)
		var err error
		var row kv.KeyValue
		var i *Instance
		row, err = s.db.Get(ctx, k)
		if err != nil {
			return err
		}
		if instanceExists := row.Value != nil; !instanceExists {
			return errors.New("non existent Instance")
		}
		i, err = s.rowcodec.decodeRow(row)
		if err != nil {
			return err
		}
		if len(i.httpAddr) == 0 {
			// Instance is no longer active
			s.removeCacheEntry(id)
			return errors.New("inactive Instance")
		}
		s.updateCacheEntry(i)
		httpAddr = i.httpAddr
		return nil
	})
	if err != nil {
		return "", errors.Wrapf(err, "could not fetch Instance %d", id)
	}
	return httpAddr, nil
}

// GetAllInstancesForTenant retrieves Instance information
// on all active Instances for the tenant.
func (s *Storage) GetAllInstancesForTenant(
	ctx context.Context,
) (instances []sqlinstance.SQLInstance, err error) {
	if err = s.checkStarted(); err != nil {
		return nil, err
	}
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		start := s.makeTablePrefix()
		end := start.PrefixEnd()
		const maxRows = 1024 // arbitrary but plenty
		for {
			rows, err := txn.Scan(ctx, start, end, maxRows)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			for i := range rows {
				currentRow, err := s.rowcodec.decodeRow(rows[i])
				if err != nil {
					log.Warningf(ctx, "failed to decode row %s: %v", rows[i].Key.String(), err)
					return err
				}
				if len(currentRow.httpAddr) != 0 {
					// Add active Instance details to cache.
					s.updateCacheEntry(currentRow)
					instances = append(instances, currentRow)
				} else {
					// Remove inactive Instance details from cache.
					s.removeCacheEntry(currentRow.id)
				}
			}
			start = rows[len(rows)-1].Key.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return instances, nil
}

// ReleaseInstanceID releases an Instance id prior to shutdown of a SQL pod
// The Instance id can be reused by another SQL pod of the same tenant.
func (s *Storage) ReleaseInstanceID(ctx context.Context, id base.SQLInstanceID) error {
	if !s.mu.started {
		s.mu.Unlock()
		return sqlinstance.NotStartedError
	}
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		key := s.makeInstanceKey(id)
		value, err := s.rowcodec.encodeEmptyRow()
		if err != nil {
			return err
		}
		return s.db.Put(ctx, key, &value)
	})
	if err != nil {
		return errors.Wrapf(err, "could not release Instance %d", id)
	}
	return nil
}

func (s *Storage) updateCacheEntry(sqlInstance sqlinstance.SQLInstance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mu.started {
		return
	}
	s.mu.liveInstances.Add(sqlInstance.InstanceID(), sqlInstance)
}

func (s *Storage) removeCacheEntry(id base.SQLInstanceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mu.started {
		return
	}
	s.mu.liveInstances.Del(id)
}

func (s *Storage) checkStarted() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mu.started {
		return sqlinstance.NotStartedError
	}
	return nil
}
func (s *Storage) makeTablePrefix() roachpb.Key {
	return s.codec.IndexPrefix(uint32(s.tableID), 1)
}

func (s *Storage) makeInstanceKey(id base.SQLInstanceID) roachpb.Key {
	return keys.MakeFamilyKey(encoding.EncodeVarintAscending(s.makeTablePrefix(), int64(id)), 0)
}
