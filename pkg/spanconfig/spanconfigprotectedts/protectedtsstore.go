// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigprotectedts

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type storeKey struct {
	tenantID  roachpb.TenantID
	descID    descpb.ID
	isCluster bool
}

type storeValue struct {
	protectedTimestamp hlc.Timestamp
	frontierTimestamp  hlc.Timestamp
}

type Store struct {
	mu struct {
		syncutil.RWMutex
		protections map[storeKey]map[uuid.UUID]storeValue
	}
}

var _ ProtectedTimestampStore = &Store{}

// NewStore instantiates a protectedts store.
func NewStore() *Store {
	s := &Store{}
	s.mu.protections = make(map[storeKey]map[uuid.UUID]storeValue)
	return s
}

func (s *Store) GetPTSRecordsForTableAsOf(
	id descpb.ID, parentID descpb.ID, asOf hlc.Timestamp,
) []roachpb.ProtectedTimestampRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]roachpb.ProtectedTimestampRecord, 0)

	addRecordsForStoreKey := func(key storeKey) {
		if r, ok := s.mu.protections[key]; ok {
			for _, v := range r {
				if v.frontierTimestamp.LessEq(asOf) {
					res = append(res, roachpb.ProtectedTimestampRecord{Timestamp: v.protectedTimestamp})
				}
			}
		}
	}

	// Check if there are any protections at the cluster level.
	addRecordsForStoreKey(storeKey{isCluster: true})
	// Check if there are any protections on the parent.
	addRecordsForStoreKey(storeKey{descID: parentID})
	// Check if there are any protections on the table.
	addRecordsForStoreKey(storeKey{descID: id})
	return res
}

func (s *Store) Apply(frontierTS hlc.Timestamp, updates ...ProtectedTimestampUpdate) {
	s.applyInternal(frontierTS, updates...)
}

func (s *Store) applyInternal(frontierTS hlc.Timestamp, updates ...ProtectedTimestampUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, update := range updates {
		switch t := update.Target.Union.(type) {
		case *ptpb.Target_Cluster:
			key := storeKey{isCluster: true}
			s.mu.protections[key][update.RecordID] = storeValue{
				protectedTimestamp: update.Timestamp,
				frontierTimestamp:  frontierTS,
			}
		case *ptpb.Target_Tenants:
			for _, tenantID := range t.Tenants.IDs {
				key := storeKey{tenantID: tenantID}
				s.mu.protections[key][update.RecordID] = storeValue{
					protectedTimestamp: update.Timestamp,
					frontierTimestamp:  frontierTS,
				}
			}
		case *ptpb.Target_SchemaObjects:
			for _, descID := range t.SchemaObjects.IDs {
				key := storeKey{descID: descID}
				s.mu.protections[key][update.RecordID] = storeValue{
					protectedTimestamp: update.Timestamp,
					frontierTimestamp:  frontierTS,
				}
			}
		}
	}
}
