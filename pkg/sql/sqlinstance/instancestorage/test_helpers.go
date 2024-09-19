// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package instancestorage provides a mock implementation
// of instance storage for testing purposes.
package instancestorage

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// FakeStorage implements the instanceprovider.storage interface.
type FakeStorage struct {
	mu struct {
		syncutil.Mutex
		instances     map[base.SQLInstanceID]sqlinstance.InstanceInfo
		instanceIDCtr base.SQLInstanceID
	}
}

// NewFakeStorage creates a new FakeStorage.
func NewFakeStorage() *FakeStorage {
	f := &FakeStorage{}
	f.mu.instances = make(map[base.SQLInstanceID]sqlinstance.InstanceInfo)
	f.mu.instanceIDCtr = base.SQLInstanceID(1)
	return f
}

// CreateInstance implements the instanceprovider.writer interface.
func (f *FakeStorage) CreateInstance(
	ctx context.Context,
	sessionID sqlliveness.SessionID,
	sessionExpiration hlc.Timestamp,
	rpcAddr string,
	sqlAddr string,
	locality roachpb.Locality,
) (base.SQLInstanceID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := sqlinstance.InstanceInfo{
		InstanceID:      f.mu.instanceIDCtr,
		InstanceRPCAddr: rpcAddr,
		InstanceSQLAddr: sqlAddr,
		SessionID:       sessionID,
		Locality:        locality,
	}
	f.mu.instances[f.mu.instanceIDCtr] = i
	f.mu.instanceIDCtr++
	return i.InstanceID, nil
}

// ReleaseInstanceID implements the instanceprovider.writer interface.
func (f *FakeStorage) ReleaseInstanceID(_ context.Context, id base.SQLInstanceID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.mu.instances, id)
	return nil
}

// CreateInstanceDataForTest creates a new entry in the sql_instances system
// table for testing purposes.
func (s *Storage) CreateInstanceDataForTest(
	ctx context.Context,
	region []byte,
	instanceID base.SQLInstanceID,
	rpcAddr string,
	sqlAddr string,
	sessionID sqlliveness.SessionID,
	sessionExpiration hlc.Timestamp,
	locality roachpb.Locality,
	binaryVersion roachpb.Version,
	encodeIsDraining bool,
	isDraining bool,
) error {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction deadline to the session expiration to ensure
		// transaction commits before the session expires.
		err := txn.UpdateDeadline(ctx, sessionExpiration)
		if err != nil {
			return err
		}

		key := s.rowCodec.encodeKey(region, instanceID)

		value, err := s.rowCodec.encodeValue(rpcAddr, sqlAddr,
			sessionID, locality, binaryVersion,
			true /* encodeIsDraining */, isDraining)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		b.Put(key, value)
		return txn.CommitInBatch(ctx, b)
	})
}

// GetInstanceDataForTest returns instance data directly from raw storage for
// testing purposes.
func (s *Storage) GetInstanceDataForTest(
	ctx context.Context, region []byte, instanceID base.SQLInstanceID,
) (sqlinstance.InstanceInfo, error) {
	k := s.rowCodec.encodeKey(region, instanceID)
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	row, err := s.db.Get(ctx, k)
	if err != nil {
		return sqlinstance.InstanceInfo{}, errors.Wrapf(err, "could not fetch instance %d", instanceID)
	}
	if row.Value == nil {
		return sqlinstance.InstanceInfo{}, sqlinstance.NonExistentInstanceError
	}
	rpcAddr, sqlAddr, sessionID, locality, binaryVersion, isDraining, _, err := s.rowCodec.decodeValue(*row.Value)
	if err != nil {
		return sqlinstance.InstanceInfo{}, errors.Wrapf(err, "could not decode data for instance %d", instanceID)
	}
	instanceInfo := sqlinstance.InstanceInfo{
		InstanceID:      instanceID,
		InstanceRPCAddr: rpcAddr,
		InstanceSQLAddr: sqlAddr,
		SessionID:       sessionID,
		Locality:        locality,
		BinaryVersion:   binaryVersion,
		IsDraining:      isDraining,
	}
	return instanceInfo, nil
}

// GetAllInstancesDataForTest returns all instance data from raw storage for
// testing purposes.
func (s *Storage) GetAllInstancesDataForTest(
	ctx context.Context,
) ([]sqlinstance.InstanceInfo, error) {
	var rows []instancerow
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		rows, err = s.getInstanceRows(ctx, nil /*global*/, txn, lock.WaitPolicy_Block)
		return err
	}); err != nil {
		return nil, err
	}
	return makeInstanceInfos(rows), nil
}

// SortInstances sorts instances by their id.
func SortInstances(instances []sqlinstance.InstanceInfo) {
	sort.Slice(instances, func(idx1, idx2 int) bool {
		return instances[idx1].InstanceID < instances[idx2].InstanceID
	})
}
