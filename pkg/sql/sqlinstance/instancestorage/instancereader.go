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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
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
	storage  *Storage
	slReader sqlliveness.Reader
	f        *rangefeed.Factory
	codec    keys.SQLCodec
	clock    *hlc.Clock
	stopper  *stop.Stopper
	rowcodec rowCodec
	// Once initialScanDone is closed, the error (if any) while establishing the
	// rangefeed can be found in initialScanErr.
	initialScanDone chan struct{}
	mu              struct {
		syncutil.Mutex
		instances      map[base.SQLInstanceID]instancerow
		initialScanErr error
	}
}

// NewTestingReader constructs a new Reader with control for the database
// in which the `sql_instances` table should exist.
func NewTestingReader(
	storage *Storage,
	slReader sqlliveness.Reader,
	f *rangefeed.Factory,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	clock *hlc.Clock,
	stopper *stop.Stopper,
) *Reader {
	r := &Reader{
		storage:         storage,
		slReader:        slReader,
		f:               f,
		codec:           codec,
		clock:           clock,
		rowcodec:        makeRowCodec(codec, table),
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
	return NewTestingReader(storage, slReader, f, codec, systemschema.SQLInstancesTable(), clock, stopper)
}

// Start initializes the rangefeed for the Reader. The rangefeed will run until
// the stopper stops. If self has a non-zero ID, it will be used to initialize
// the set of instances before the rangefeed catches up.
func (r *Reader) Start(ctx context.Context, self sqlinstance.InstanceInfo) {
	if self.InstanceID != 0 {
		r.updateInstanceMap(instancerow{
			region:     self.Region,
			instanceID: self.InstanceID,
			sqlAddr:    self.InstanceSQLAddr,
			rpcAddr:    self.InstanceRPCAddr,
			sessionID:  self.SessionID,
			locality:   self.Locality,
			timestamp:  hlc.Timestamp{}, // intentionally zero
		}, false /* deletionEvent */)
	}
	r.startRangeFeed(ctx)
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
	instancesTablePrefix := r.rowcodec.codec.TablePrefix(uint32(r.rowcodec.tableID))
	rows, err := txn.Scan(ctx, instancesTablePrefix, instancesTablePrefix.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}
	decodedRows := make([]instancerow, 0, len(rows))
	for _, row := range rows {
		decodedRow, err := r.rowcodec.decodeRow(row.Key, row.Value)
		if err != nil {
			return nil, err
		}
		decodedRows = append(decodedRows, decodedRow)
	}
	filteredRows, err := selectDistinctLiveRows(ctx, r.slReader, decodedRows)
	if err != nil {
		return nil, err
	}
	return makeInstanceInfos(filteredRows), nil
}

func (r *Reader) startRangeFeed(ctx context.Context) {
	updateCacheFn := func(
		ctx context.Context, keyVal *roachpb.RangeFeedValue,
	) {
		instance, err := r.rowcodec.decodeRow(keyVal.Key, &keyVal.Value)
		if err != nil {
			log.Ops.Warningf(ctx, "failed to decode settings row %v: %v", keyVal.Key, err)
			return
		}
		r.updateInstanceMap(instance, !keyVal.Value.IsPresent())
	}
	initialScanDoneFn := func(_ context.Context) {
		r.setInitialScanErr(nil)
	}
	initialScanErrFn := func(_ context.Context, err error) (shouldFail bool) {
		if grpcutil.IsAuthError(err) ||
			// This is a hack around the fact that we do not get properly structured
			// errors out of gRPC. See #56208.
			strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
			shouldFail = true
			r.setInitialScanErr(err)
		}
		return shouldFail
	}

	instancesTablePrefix := r.rowcodec.makeIndexPrefix()
	instancesTableSpan := roachpb.Span{
		Key:    instancesTablePrefix,
		EndKey: instancesTablePrefix.PrefixEnd(),
	}
	rf, err := r.f.RangeFeed(ctx,
		"sql_instances",
		[]roachpb.Span{instancesTableSpan},
		r.clock.Now(),
		updateCacheFn,
		rangefeed.WithSystemTablePriority(),
		rangefeed.WithInitialScan(initialScanDoneFn),
		rangefeed.WithOnInitialScanError(initialScanErrFn),
		rangefeed.WithRowTimestampInInitialScan(true),
	)
	if err != nil {
		r.setInitialScanErr(err)
		return
	}
	r.stopper.AddCloser(rf)
}

// GetInstance implements sqlinstance.AddressResolver interface.
func (r *Reader) GetInstance(
	ctx context.Context, instanceID base.SQLInstanceID,
) (sqlinstance.InstanceInfo, error) {
	if err := r.initialScanErr(); err != nil {
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
	liveInstances, err := selectDistinctLiveRows(ctx, r.slReader, r.getAllInstanceRows())
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

// getAllInstanceRows returns all instancerow objects contained within the map,
// in an arbitrary order.
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

func (r *Reader) initialScanErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.initialScanErr
}

func (r *Reader) setInitialScanErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Set error before closing done channel.
	r.mu.initialScanErr = err
	close(r.initialScanDone)
}
