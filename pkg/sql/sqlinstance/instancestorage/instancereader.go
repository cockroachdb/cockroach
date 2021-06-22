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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
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

// GetInstanceAddr implements sqlinstance.AddressResolver interface.
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
		return "", sqlinstance.NonExistentInstanceError
	}
	return instanceData.addr, nil
}

// GetAllInstances implements sqlinstance.AddressResolver interface.
func (r *Reader) GetAllInstances(
	ctx context.Context,
) (instances []sqlinstance.InstanceInfo, _ error) {
	instanceRows, err := r.storage.getAllInstancesData(ctx)
	if err != nil {
		return nil, err
	}
	instanceRows, err = r.filterInactiveInstances(ctx, instanceRows)
	if err != nil {
		return nil, err
	}
	for _, instanceRow := range instanceRows {
		instanceInfo := sqlinstance.InstanceInfo{
			InstanceID:   instanceRow.instanceID,
			InstanceAddr: instanceRow.addr,
			SessionID:    instanceRow.sessionID,
		}
		instances = append(instances, instanceInfo)
	}
	return instances, nil
}

func (r *Reader) filterInactiveInstances(
	ctx context.Context, rows []instancerow,
) ([]instancerow, error) {
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
