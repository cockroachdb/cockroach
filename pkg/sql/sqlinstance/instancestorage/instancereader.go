// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/errors"
)

// Reader implements the sqlinstance.AddressResolver interface.
// TODO(rima): Add caching backed by rangefeed to Reader.
type Reader struct {
	storage  *Storage
	slReader sqlliveness.Reader
}

// NewReader constructs a new reader for SQL instance data.
func NewReader(storage *Storage, slReader sqlliveness.Reader) *Reader {
	return &Reader{
		storage:  storage,
		slReader: slReader,
	}
}

// GetInstanceAddr returns the network address of the SQL instance
// if the SQL instance is live.
func (r *Reader) GetInstanceAddr(
	ctx context.Context, instanceID base.SQLInstanceID,
) (addr string, _ error) {
	instanceData, err := r.storage.getInstanceData(ctx, instanceID)
	if err != nil {
		return "", err
	}
	sessionAlive, err := r.slReader.IsAlive(ctx, instanceData.sessionID)
	if err != nil {
		return "", err
	}
	if !sessionAlive {
		return "", errors.Newf("inactive instance id %d", instanceID)
	}
	return instanceData.addr, nil
}

// GetAllInstances returns a list of all the active instances for the tenant.
func (r *Reader) GetAllInstances(
	ctx context.Context,
) (instances []sqlinstance.InstanceInfo, _ error) {
	instanceRows, err := r.storage.getAllInstancesData(ctx)
	if err != nil {
		return nil, err
	}
	instanceRows = r.filterInactiveInstances(ctx, instanceRows)
	if len(instanceRows) == 0 {
		return nil, nil
	}
	for _, instanceRow := range instanceRows {
		instances = append(instances, *sqlinstance.NewSQLInstanceInfo(instanceRow.instanceID, instanceRow.addr, instanceRow.sessionID))
	}
	return instances, nil
}

func (r *Reader) filterInactiveInstances(
	ctx context.Context, instanceRows []instancerow,
) (instances []instancerow) {
	if len(instanceRows) == 0 {
		return instanceRows
	}
	idsToFilter := r.getInstanceIDsToFilter(ctx, instanceRows)
	for _, instanceRow := range instanceRows {
		if _, inactiveInstance := idsToFilter[instanceRow.instanceID]; !inactiveInstance {
			instances = append(instances, instanceRow)
		}
	}
	return instances
}

// getInstanceIDsToFilter returns the list of instance IDs to filter based on their liveness.
func (r *Reader) getInstanceIDsToFilter(
	ctx context.Context, instanceRows []instancerow,
) map[base.SQLInstanceID]struct{} {
	idsToFilter := make(map[base.SQLInstanceID]struct{})
	addrToInstanceMap := make(map[string]instancerow)
	for _, instanceRow := range instanceRows {
		sessionAlive, _ := r.slReader.IsAlive(ctx, instanceRow.sessionID)
		if !sessionAlive {
			idsToFilter[instanceRow.instanceID] = struct{}{}
		}
		// Check and remove duplicate address <-> instance mappings
		// which could happen when an old, unused instance row hasn't
		// been cleaned up prior to SQL pod shutdown.
		dupInstance, ok := addrToInstanceMap[instanceRow.addr]
		if ok {
			// If we already have an address <-> instance mapping
			// for given address, mark the older instance row id
			// for removal and update address <-> instance mapping
			// with the latest instance information.
			if dupInstance.timestamp.Less(instanceRow.timestamp) {
				idsToFilter[dupInstance.instanceID] = struct{}{}
				addrToInstanceMap[instanceRow.addr] = instanceRow
			} else {
				idsToFilter[instanceRow.instanceID] = struct{}{}
			}
		} else {
			// Add an address to instance mapping
			addrToInstanceMap[instanceRow.addr] = instanceRow
		}
	}
	return idsToFilter
}
