// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instancestorage provides a mock implementation
// of instance storage for testing purposes
package instancestorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// FakeStorage implements the instanceprovider.storage interface.
type FakeStorage struct {
	mu struct {
		syncutil.Mutex
		instances     map[base.SQLInstanceID]sqlinstance.SQLInstance
		instanceIDCtr base.SQLInstanceID
		started       bool
	}
}

// NewFakeStorage creates a new FakeStorage.
func NewFakeStorage() *FakeStorage {
	f := &FakeStorage{}
	f.mu.instances = make(map[base.SQLInstanceID]sqlinstance.SQLInstance)
	f.mu.instanceIDCtr = base.SQLInstanceID(1)
	return f
}

// Start implements the instanceprovider.storage interface
func (f *FakeStorage) Start() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.started {
		return
	}
	f.mu.started = true
}

// CreateInstance implements the instanceprovider.storage interface.
func (f *FakeStorage) CreateInstance(
	ctx context.Context, sessionID sqlliveness.SessionID, httpAddr string,
) (sqlinstance.SQLInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := &Instance{
		id:        f.mu.instanceIDCtr,
		httpAddr:  httpAddr,
		sessionID: sessionID,
	}
	f.mu.instances[f.mu.instanceIDCtr] = i
	return i, nil
}

// ReleaseInstanceID implements the instanceprovider.storage interface.
func (f *FakeStorage) ReleaseInstanceID(_ context.Context, id base.SQLInstanceID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.mu.instances, id)
	return nil
}

// GetAllInstancesForTenant implements the instanceprovider.storage interface
func (f *FakeStorage) GetAllInstancesForTenant(
	_ context.Context,
) (instances []sqlinstance.SQLInstance, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, i := range f.mu.instances {
		instances = append(instances, i)
	}
	return instances, nil
}

// GetInstanceAddr implements the instanceprovider.storage interface
func (f *FakeStorage) GetInstanceAddr(
	_ context.Context, id base.SQLInstanceID,
) (httpAddr string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.mu.instances[id]
	if !ok {
		return "", errors.New("non existent instance")
	}
	return i.InstanceAddr(), nil
}
