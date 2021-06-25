// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instancestorage provides a mock implementation
// of instance storage for testing purposes.
package instancestorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// FakeStorage implements the instanceprovider.storage interface.
type FakeStorage struct {
	mu struct {
		syncutil.Mutex
		instances     map[base.SQLInstanceID]sqlinstance.InstanceInfo
		instanceIDCtr base.SQLInstanceID
		started       bool
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
	_ context.Context, sessionID sqlliveness.SessionID, _ hlc.Timestamp, addr string,
) (base.SQLInstanceID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := sqlinstance.InstanceInfo{
		InstanceID:   f.mu.instanceIDCtr,
		InstanceAddr: addr,
		SessionID:    sessionID,
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

// GetInstanceDataForTest returns instance data directly from raw storage
// for testing purposes.
func (s *Storage) GetInstanceDataForTest(
	ctx context.Context, instanceID base.SQLInstanceID,
) (sqlinstance.InstanceInfo, error) {
	i, err := s.getInstanceData(ctx, instanceID)
	if err != nil {
		return sqlinstance.InstanceInfo{}, err
	}
	instanceInfo := sqlinstance.InstanceInfo{
		InstanceID:   i.instanceID,
		InstanceAddr: i.addr,
		SessionID:    i.sessionID,
	}
	return instanceInfo, nil
}

// GetAllInstancesDataForTest returns all instance data from raw storage
// for testing purposes.
func (s *Storage) GetAllInstancesDataForTest(
	ctx context.Context,
) (instances []sqlinstance.InstanceInfo, _ error) {
	rows, err := s.getAllInstancesData(ctx)
	if err != nil {
		return nil, err
	}
	for _, instance := range rows {
		instanceInfo := sqlinstance.InstanceInfo{
			InstanceID:   instance.instanceID,
			InstanceAddr: instance.addr,
			SessionID:    instance.sessionID,
		}
		instances = append(instances, instanceInfo)
	}
	return instances, nil
}
