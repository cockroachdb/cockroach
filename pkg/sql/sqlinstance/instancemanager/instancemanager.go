// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instance provides implementation to initialize unique
// instance ids for SQL pods and associate them with network
// addresses

package instancemanager

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SQLInstance implements the SQLInstance interface
// defined in the sqlinstance package
type SQLInstance struct {
	id base.SQLInstanceID
}

// InstanceID returns the instance ID
// associated with the SQL server
func (s *SQLInstance) InstanceID() base.SQLInstanceID {
	return s.id
}

// CacheSize governs the size of the cache used to
// cache sql instance address mappings
var CacheSize = settings.RegisterIntSetting(
	"server.sqlinstance.storage_instance_cache_size",
	"number of instance-address entries to store in the LRU",
	512)

type instancemanager struct {
	settings *cluster.Settings
	stopper  *stop.Stopper
	mu       struct {
		syncutil.Mutex
		activeInstances *cache.UnorderedCache
		instanceIDCtr   int
	}
}

// NewSQLInstanceManager creates a new instance manager for managing SQL instances
func NewSQLInstanceManager(
	settings *cluster.Settings, stopper *stop.Stopper,
) sqlinstance.InstanceManager {
	i := &instancemanager{
		settings: settings,
		stopper:  stopper,
	}
	cacheConfig := cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > int(CacheSize.Get(&settings.SV))
		},
	}
	i.mu.activeInstances = cache.NewUnorderedCache(cacheConfig)
	i.mu.instanceIDCtr = 10001
	return i
}

// CreateInstance creates a new SQL instance id
func (i *instancemanager) CreateInstance(_ sqlliveness.SessionID) (sqlinstance.SQLInstance, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	newInstanceID := SQLInstance{
		id: base.SQLInstanceID(i.mu.instanceIDCtr),
	}

	// TODO(rima): Add logic to init instance id using sqlinstance system table
	return &newInstanceID, nil
}

// SetInstanceAddr sets the http address within the sqlinstance subsystem for
// a given SQL instance id
func (i *instancemanager) SetInstanceAddr(id base.SQLInstanceID, httpAddr string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.mu.activeInstances.Add(id, httpAddr)
	// TODO(rima): Add logic to update sqlinstance system table with the http addr
}

// ShutdownInstance will release the instance id with the sqlinstance system table
// and shutdown the SQL server
func (i *instancemanager) ShutdownInstance(ctx context.Context, _ sqlliveness.SessionID) {
	// TODO(rima): Add logic to update entry in sqlinstance system table
	i.stopper.Stop(ctx)
}

// GetInstanceAddr return the http address for a SQL instance id
func (i *instancemanager) GetInstanceAddr(id base.SQLInstanceID) (httpAddr string, _ error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if addr, ok := i.mu.activeInstances.Get(id); ok {
		addr := addr.(string)
		return addr, nil
	}
	// TODO (rima): Add logic to read from sqlinstance system table and add
	// to cache
	return "", fmt.Errorf("non existent instance id %d", id)
}

// TODO(davidh): Placeholder. Reconcile w/ Rima's work once she's done
func (i *instancemanager) GetAllInstancesForTenant() ([]sqlinstance.SQLInstance, error) {
	return nil, nil
}
