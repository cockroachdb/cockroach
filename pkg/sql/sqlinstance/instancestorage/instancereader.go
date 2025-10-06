// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	db       *kv.DB
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
	storage *Storage, slReader sqlliveness.Reader, stopper *stop.Stopper, db *kv.DB,
) *Reader {
	r := &Reader{
		storage:         storage,
		slReader:        slReader,
		initialScanDone: make(chan struct{}),
		stopper:         stopper,
		db:              db,
	}
	r.setCache(&emptyInstanceCache{})
	return r
}

// NewReader constructs a new reader for SQL instance data.
func NewReader(
	storage *Storage, slReader sqlliveness.Reader, stopper *stop.Stopper, db *kv.DB,
) *Reader {
	return NewTestingReader(storage, slReader, stopper, db)
}

// Start initializes the instanceCache for the Reader. The range feed backing
// the cache will run until the stopper stops. If self has a non-zero ID, it
// will be used to initialize the set of instances before the rangefeed catches
// up.
func (r *Reader) Start(ctx context.Context, self sqlinstance.InstanceInfo) {
	r.setCache(&singletonInstanceFeed{
		instance: instancerow{
			region:        self.Region,
			instanceID:    self.InstanceID,
			sqlAddr:       self.InstanceSQLAddr,
			rpcAddr:       self.InstanceRPCAddr,
			sessionID:     self.SessionID,
			locality:      self.Locality,
			binaryVersion: self.BinaryVersion,
			timestamp:     hlc.Timestamp{}, // intentionally zero
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
		BinaryVersion:   row.binaryVersion,
		Region:          row.region,
		IsDraining:      row.isDraining,
	}
}

func makeInstanceInfos(rows []instancerow) []sqlinstance.InstanceInfo {
	ret := make([]sqlinstance.InstanceInfo, len(rows))
	for i := range rows {
		ret[i] = makeInstanceInfo(rows[i])
	}
	return ret
}

// GetAllInstancesNoCache reads all instances directly from the sql_instances
// table, bypassing any caching.
func (r *Reader) GetAllInstancesNoCache(ctx context.Context) ([]sqlinstance.InstanceInfo, error) {
	var instances []sqlinstance.InstanceInfo
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		instances, err = r.GetAllInstancesUsingTxn(ctx, txn)
		return err
	}); err != nil {
		return nil, err
	}
	return instances, nil
}

// GetAllInstancesUsingTxn reads all instances using the given transaction and returns
// live instances only.
func (r *Reader) GetAllInstancesUsingTxn(
	ctx context.Context, txn *kv.Txn,
) ([]sqlinstance.InstanceInfo, error) {
	decodedRows, err := r.storage.getAllInstanceRows(ctx, txn)
	if err != nil {
		return nil, err
	}
	filteredRows, err := selectDistinctLiveRows(ctx, r.slReader, decodedRows)
	if err != nil {
		return nil, err
	}
	return makeInstanceInfos(filteredRows), nil
}

// GetInstance implements sqlinstance.AddressResolver interface. The function
// first tries to find the instance (and validate that it's alive) using the
// instance cache. If it can't be found it the cache, it performs a more
// expensive precise check by directly querying the instances table.
func (r *Reader) GetInstance(
	ctx context.Context, instanceID base.SQLInstanceID,
) (sqlinstance.InstanceInfo, error) {
	if err := r.initialScanErr(); err != nil {
		return sqlinstance.InstanceInfo{}, err
	}
	getNonCached := func(instanceID base.SQLInstanceID) (sqlinstance.InstanceInfo, error) {
		log.Infof(ctx, "getting non-cached version of SQL server %d", instanceID)
		instances, err := r.GetAllInstancesNoCache(ctx)
		if err != nil {
			return sqlinstance.InstanceInfo{}, err
		}
		for i := range instances {
			inst := instances[i]
			if inst.InstanceID == instanceID {
				return inst, nil
			}
		}
		return sqlinstance.InstanceInfo{}, sqlinstance.NonExistentInstanceError
	}

	var err error
	var instance sqlinstance.InstanceInfo
	usedCache := true
	instanceRow, ok := r.getCache().getInstance(instanceID)
	var sessionID sqlliveness.SessionID
	if !ok {
		usedCache = false
		instance, err = getNonCached(instanceID)
		if err != nil {
			return sqlinstance.InstanceInfo{}, err
		}
		sessionID = instance.SessionID
	} else {
		sessionID = instanceRow.sessionID
	}
	alive, err := r.slReader.IsAlive(ctx, sessionID)
	if err != nil {
		return sqlinstance.InstanceInfo{}, err
	}
	if !alive {
		if usedCache {
			// Try again without the cache.
			usedCache = false
			instance, err = getNonCached(instanceID)
			if err != nil {
				return sqlinstance.InstanceInfo{}, err
			}
			alive, err = r.slReader.IsAlive(ctx, instance.SessionID)
			if err != nil {
				return sqlinstance.InstanceInfo{}, err
			}
			if !alive {
				return sqlinstance.InstanceInfo{}, sqlinstance.NonExistentInstanceError
			}
		}
	}
	if !usedCache {
		return instance, nil
	}
	instanceInfo := sqlinstance.InstanceInfo{
		InstanceID:      instanceRow.instanceID,
		InstanceRPCAddr: instanceRow.rpcAddr,
		InstanceSQLAddr: instanceRow.sqlAddr,
		SessionID:       instanceRow.sessionID,
		Locality:        instanceRow.locality,
		BinaryVersion:   instanceRow.binaryVersion,
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
