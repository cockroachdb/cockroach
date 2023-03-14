// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Reader implements the sqlinstance.AddressResolver interface. It uses caching
// backed by rangefeed to cache instance information.
type Reader struct {
	storage  *Storage
	slReader sqlliveness.Reader
	stopper  *stop.Stopper

	// Once initialScanDone is closed, the error (if any) while establishing the
	// rangefeed can be found in initialScanErr.
	initialScanDone chan struct{}
	mu              struct {
		syncutil.Mutex
		cache          instanceCache
		initialScanErr error
	}
}

// NewTestingReader constructs a new Reader with control for the database
// in which the `sql_instances` table should exist.
func NewTestingReader(
	storage *Storage, slReader sqlliveness.Reader, stopper *stop.Stopper,
) *Reader {
	r := &Reader{
		storage:         storage,
		slReader:        slReader,
		initialScanDone: make(chan struct{}),
		stopper:         stopper,
	}
	r.setCache(&emptyInstanceCache{})
	return r
}

// NewReader constructs a new reader for SQL instance data.
func NewReader(storage *Storage, slReader sqlliveness.Reader, stopper *stop.Stopper) *Reader {
	return NewTestingReader(storage, slReader, stopper)
}

// Start initializes the instanceCache for the Reader. The range feed backing
// the cache will run until the stopper stops. If self has a non-zero ID, it
// will be used to initialize the set of instances before the rangefeed catches
// up.
func (r *Reader) Start(ctx context.Context, self sqlinstance.InstanceInfo) {
	r.setCache(&singletonInstanceFeed{
		instance: instancerow{
			region:     self.Region,
			instanceID: self.InstanceID,
			sqlAddr:    self.InstanceSQLAddr,
			rpcAddr:    self.InstanceRPCAddr,
			sessionID:  self.SessionID,
			locality:   self.Locality,
			timestamp:  hlc.Timestamp{}, // intentionally zero
		},
	})
	// Make sure that the reader shuts down gracefully.
	ctx, cancel := r.stopper.WithCancelOnQuiesce(ctx)
	err := r.stopper.RunAsyncTask(ctx, "start-instance-reader", func(ctx context.Context) {
		cache, err := r.storage.newInstanceCache(ctx)
		if err != nil {
			r.setInitialScanDone(err)
			return
		}
		r.setCache(cache)
		r.setInitialScanDone(nil)
	})
	if err != nil {
		cancel()
		r.setInitialScanDone(err)
	}
}

// WaitForStarted will block until the Reader has an initial full snapshot of
// all the instances. If Start hasn't been called, this will block until the
// context is cancelled, or the stopper quiesces.
func (r *Reader) WaitForStarted(ctx context.Context) error {
	select {
	case <-r.initialScanDone:
		return r.initialScanErr()
	case <-r.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable,
			"failed to retrieve initial instance data")
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(),
			"failed to retrieve initial instance data")
	}
}

func (r *Reader) getCache() instanceCache {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.cache
}

func (r *Reader) setCache(feed instanceCache) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.cache = feed
}

func makeInstanceInfo(row instancerow) sqlinstance.InstanceInfo {
	return sqlinstance.InstanceInfo{
		InstanceID:      row.instanceID,
		InstanceRPCAddr: row.rpcAddr,
		InstanceSQLAddr: row.sqlAddr,
		SessionID:       row.sessionID,
		Locality:        row.locality,
	}
}

func makeInstanceInfos(rows []instancerow) []sqlinstance.InstanceInfo {
	ret := make([]sqlinstance.InstanceInfo, len(rows))
	for i := range rows {
		ret[i] = makeInstanceInfo(rows[i])
	}
	return ret
}

// GetAllInstancesUsingTxn reads all instances using the given transaction and returns
// live instances only.
func (r *Reader) GetAllInstancesUsingTxn(
	ctx context.Context, txn *kv.Txn,
) ([]sqlinstance.InstanceInfo, error) {
	decodedRows, err := r.storage.getInstanceRows(ctx, nil /*all regions*/, txn, lock.WaitPolicy_Block)
	if err != nil {
		return nil, err
	}
	filteredRows, err := selectDistinctLiveRows(ctx, r.slReader, decodedRows)
	if err != nil {
		return nil, err
	}
	return makeInstanceInfos(filteredRows), nil
}

// GetInstance implements sqlinstance.AddressResolver interface.
func (r *Reader) GetInstance(
	ctx context.Context, instanceID base.SQLInstanceID,
) (sqlinstance.InstanceInfo, error) {
	if err := r.initialScanErr(); err != nil {
		return sqlinstance.InstanceInfo{}, err
	}
	instance, ok := r.getCache().getInstance(instanceID)
	if !ok {
		return sqlinstance.InstanceInfo{}, sqlinstance.NonExistentInstanceError
	}
	alive, err := r.slReader.IsAlive(ctx, instance.sessionID)
	if err != nil {
		return sqlinstance.InstanceInfo{}, err
	}
	if !alive {
		return sqlinstance.InstanceInfo{}, sqlinstance.NonExistentInstanceError
	}
	instanceInfo := sqlinstance.InstanceInfo{
		InstanceID:      instance.instanceID,
		InstanceRPCAddr: instance.rpcAddr,
		InstanceSQLAddr: instance.sqlAddr,
		SessionID:       instance.sessionID,
		Locality:        instance.locality,
	}
	return instanceInfo, nil
}

// GetAllInstances implements sqlinstance.AddressResolver interface.
// This method does not block as the underlying sqlliveness.Reader
// being used (outside of test environment) is a cached reader which
// does not perform any RPCs in its `isAlive()` calls.
func (r *Reader) GetAllInstances(ctx context.Context) ([]sqlinstance.InstanceInfo, error) {
	if err := r.initialScanErr(); err != nil {
		return nil, err
	}

	liveInstances, err := selectDistinctLiveRows(ctx, r.slReader, r.getCache().listInstances())
	if err != nil {
		return nil, err
	}
	return makeInstanceInfos(liveInstances), nil
}

// selectDistinctLiveRows modifies the given slice in-place and returns
// the selected rows.
func selectDistinctLiveRows(
	ctx context.Context, slReader sqlliveness.Reader, rows []instancerow,
) ([]instancerow, error) {
	// Filter inactive instances.
	{
		truncated := rows[:0]
		for _, row := range rows {
			// Skip instances which are preallocated.
			if row.isAvailable() {
				continue
			}
			isAlive, err := slReader.IsAlive(ctx, row.sessionID)
			if err != nil {
				return nil, err
			}
			if isAlive {
				truncated = append(truncated, row)
			}
		}
		rows = truncated
	}
	sort.Slice(rows, func(idx1, idx2 int) bool {
		if rows[idx1].sqlAddr == rows[idx2].sqlAddr {
			return !rows[idx1].timestamp.Less(rows[idx2].timestamp) // decreasing timestamp order
		}
		return rows[idx1].sqlAddr < rows[idx2].sqlAddr
	})
	// Only provide the latest entry for a given address.
	{
		truncated := rows[:0]
		for i := 0; i < len(rows); i++ {
			if i == 0 || rows[i].sqlAddr != rows[i-1].sqlAddr {
				truncated = append(truncated, rows[i])
			}
		}
		rows = truncated
	}
	return rows, nil
}

func (r *Reader) initialScanErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.initialScanErr
}

func (r *Reader) setInitialScanDone(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Set error before closing done channel.
	r.mu.initialScanErr = err
	close(r.initialScanDone)
}
