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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Reader implements the sqlinstance.AddressResolver interface. It uses
// caching backed by rangefeed to cache instance information.
type Reader struct {
	storage         *Storage
	slReader        sqlliveness.Reader
	f               *rangefeed.Factory
	codec           keys.SQLCodec
	tableID         descpb.ID
	clock           *hlc.Clock
	stopper         *stop.Stopper
	rowcodec        rowCodec
	initialScanDone chan struct{}
	mu              struct {
		syncutil.Mutex
		instances  map[base.SQLInstanceID]instancerow
		startError error
		started    bool
	}
}

// NewTestingReader constructs a new Reader with control for the database
// in which the `sql_instances` table should exist.
func NewTestingReader(
	storage *Storage,
	slReader sqlliveness.Reader,
	f *rangefeed.Factory,
	codec keys.SQLCodec,
	tableID descpb.ID,
	clock *hlc.Clock,
	stopper *stop.Stopper,
) *Reader {
	r := &Reader{
		storage:         storage,
		slReader:        slReader,
		f:               f,
		codec:           codec,
		tableID:         tableID,
		clock:           clock,
		rowcodec:        makeRowCodec(codec),
		initialScanDone: make(chan struct{}),
		stopper:         stopper,
	}
	r.mu.instances = make(map[base.SQLInstanceID]instancerow)
	return r
}

// NewReader constructs a new reader for SQL instance data.
func NewReader(
	storage *Storage,
	slReader sqlliveness.Reader,
	f *rangefeed.Factory,
	codec keys.SQLCodec,
	clock *hlc.Clock,
	stopper *stop.Stopper,
) *Reader {
	return NewTestingReader(storage, slReader, f, codec, keys.SQLInstancesTableID, clock, stopper)
}

// Start initializes the rangefeed for the Reader.
func (r *Reader) Start(ctx context.Context) error {
	rf := r.maybeStartRangeFeed(ctx)
	select {
	case <-r.initialScanDone:
		// TODO(rimadeodhar): Avoid blocking on initial
		// scan until first call to read.
		if rf != nil {
			// Add rangefeed to the stopper to ensure it
			// is shutdown correctly.
			r.stopper.AddCloser(rf)
		}
		return r.checkStarted()
	case <-r.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable,
			"failed to retrieve initial instance data")
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(),
			"failed to retrieve initial instance data")
	}
}
func (r *Reader) maybeStartRangeFeed(ctx context.Context) *rangefeed.RangeFeed {
	if r.started() {
		// Nothing to do, return
		return nil
	}
	updateCacheFn := func(
		ctx context.Context, keyVal *roachpb.RangeFeedValue,
	) {
		instanceID, addr, sessionID, locality, timestamp, tombstone, err := r.rowcodec.decodeRow(kv.KeyValue{
			Key:   keyVal.Key,
			Value: &keyVal.Value,
		})
		if err != nil {
			log.Ops.Warningf(ctx, "failed to decode settings row %v: %v", keyVal.Key, err)
			return
		}
		instance := instancerow{
			instanceID: instanceID,
			addr:       addr,
			sessionID:  sessionID,
			timestamp:  timestamp,
			locality:   locality,
		}
		r.updateInstanceMap(instance, tombstone)
	}
	initialScanDoneFn := func(_ context.Context) {
		close(r.initialScanDone)
	}
	initialScanErrFn := func(_ context.Context, err error) (shouldFail bool) {
		if grpcutil.IsAuthError(err) ||
			// This is a hack around the fact that we do not get properly structured
			// errors out of gRPC. See #56208.
			strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
			shouldFail = true
			r.setStartError(err)
			close(r.initialScanDone)
		}
		return shouldFail
	}

	instancesTablePrefix := r.codec.TablePrefix(uint32(r.tableID))
	instancesTableSpan := roachpb.Span{
		Key:    instancesTablePrefix,
		EndKey: instancesTablePrefix.PrefixEnd(),
	}
	rf, err := r.f.RangeFeed(ctx,
		"sql_instances",
		[]roachpb.Span{instancesTableSpan},
		r.clock.Now(),
		updateCacheFn,
		rangefeed.WithInitialScan(initialScanDoneFn),
		rangefeed.WithOnInitialScanError(initialScanErrFn),
		rangefeed.WithRowTimestampInInitialScan(true),
	)
	r.setStarted()
	if err != nil {
		r.setStartError(err)
		close(r.initialScanDone)
		return nil
	}
	return rf
}

// GetInstance implements sqlinstance.AddressResolver interface.
func (r *Reader) GetInstance(
	ctx context.Context, instanceID base.SQLInstanceID,
) (sqlinstance.InstanceInfo, error) {
	if err := r.checkStarted(); err != nil {
		return sqlinstance.InstanceInfo{}, err
	}
	r.mu.Lock()
	instance, ok := r.mu.instances[instanceID]
	r.mu.Unlock()
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
		InstanceID:   instance.instanceID,
		InstanceAddr: instance.addr,
		SessionID:    instance.sessionID,
		Locality:     instance.locality,
	}
	return instanceInfo, nil
}

// GetAllInstances implements sqlinstance.AddressResolver interface.
// This method does not block as the underlying sqlliveness.Reader
// being used (outside of test environment) is a cached reader which
// does not perform any RPCs in its `isAlive()` calls.
func (r *Reader) GetAllInstances(
	ctx context.Context,
) (sqlInstances []sqlinstance.InstanceInfo, _ error) {
	if err := r.checkStarted(); err != nil {
		return nil, err
	}
	liveInstances, err := r.getAllLiveInstances(ctx)
	if err != nil {
		return nil, err
	}
	for _, liveInstance := range liveInstances {
		instanceInfo := sqlinstance.InstanceInfo{
			InstanceID:   liveInstance.instanceID,
			InstanceAddr: liveInstance.addr,
			SessionID:    liveInstance.sessionID,
			Locality:     liveInstance.locality,
		}
		sqlInstances = append(sqlInstances, instanceInfo)
	}
	return sqlInstances, nil
}

func (r *Reader) getAllLiveInstances(ctx context.Context) ([]instancerow, error) {
	rows := r.getAllInstanceRows()
	// Filter inactive instances.
	{
		truncated := rows[:0]
		for _, row := range rows {
			isAlive, err := r.slReader.IsAlive(ctx, row.sessionID)
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
		if rows[idx1].addr == rows[idx2].addr {
			return !rows[idx1].timestamp.Less(rows[idx2].timestamp) // decreasing timestamp order
		}
		return rows[idx1].addr < rows[idx2].addr
	})
	// Only provide the latest entry for a given address.
	{
		truncated := rows[:0]
		for i := 0; i < len(rows); i++ {
			if i == 0 || rows[i].addr != rows[i-1].addr {
				truncated = append(truncated, rows[i])
			}
		}
		rows = truncated
	}
	return rows, nil
}

// getAllInstanceRows returns all instancerow objects contained
// within the map, in an arbitrary order.
func (r *Reader) getAllInstanceRows() (instances []instancerow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, instance := range r.mu.instances {
		instances = append(instances, instance)
	}
	return instances
}

func (r *Reader) updateInstanceMap(instance instancerow, deletionEvent bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if deletionEvent {
		delete(r.mu.instances, instance.instanceID)
		return
	}
	r.mu.instances[instance.instanceID] = instance
}

func (r *Reader) setStarted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.started = true
}

func (r *Reader) started() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.started
}

func (r *Reader) checkStarted() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.mu.started {
		return sqlinstance.NotStartedError
	}
	return r.mu.startError
}

func (r *Reader) setStartError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.startError = err
}
