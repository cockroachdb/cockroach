// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// nodeTombstoneStorage maintains a local (i.e. unreplicated)
// registry of which nodes were permanently removed from the cluster.
type nodeTombstoneStorage struct {
	engs []storage.Engine
	mu   struct {
		syncutil.RWMutex
		// cache contains both positive and negative hits. Positive hits
		// unconditionally override negative hits.
		cache map[roachpb.NodeID]time.Time
	}
}

func (s *nodeTombstoneStorage) key(nodeID roachpb.NodeID) roachpb.Key {
	return keys.StoreNodeTombstoneKey(nodeID)
}

// IsDecommissioned returns when (in UTC) a node was decommissioned
// (i.e. was permanently removed from the cluster). If not removed,
// returns the zero time.
//
// Errors are not returned during normal operation.
func (s *nodeTombstoneStorage) IsDecommissioned(
	ctx context.Context, nodeID roachpb.NodeID,
) (time.Time, error) {
	s.mu.RLock()
	ts, ok := s.mu.cache[nodeID]
	s.mu.RUnlock()
	if ok {
		// Cache hit.
		return ts, nil
	}

	// No cache hit.
	k := s.key(nodeID)
	for _, eng := range s.engs {
		v, _, err := storage.MVCCGet(ctx, eng, k, hlc.Timestamp{}, storage.MVCCGetOptions{})
		if err != nil {
			return time.Time{}, err
		}
		if v == nil {
			// Not found.
			continue
		}
		var tsp hlc.Timestamp
		if err := v.GetProto(&tsp); err != nil {
			return time.Time{}, err
		}
		// Found, offer to cache and return.
		ts := timeutil.Unix(0, tsp.WallTime).UTC()
		s.maybeAddCached(nodeID, ts)
		return ts, nil
	}
	// Not found, add a negative cache hit.
	s.maybeAddCached(nodeID, time.Time{})
	return time.Time{}, nil
}

func (s *nodeTombstoneStorage) maybeAddCached(nodeID roachpb.NodeID, ts time.Time) (updated bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldTS, ok := s.mu.cache[nodeID]; !ok || oldTS.IsZero() {
		if s.mu.cache == nil {
			s.mu.cache = map[roachpb.NodeID]time.Time{}
		}
		s.mu.cache[nodeID] = ts
		return true // updated
	}
	return false // not updated
}

// SetDecommissioned marks a node as permanently removed
// from the cluster at the supplied approximate timestamp.
//
// Once a node is recorded as permanently removed, future
// invocations are ignored.
//
// Errors are not returned during normal operation.
func (s *nodeTombstoneStorage) SetDecommissioned(
	ctx context.Context, nodeID roachpb.NodeID, ts time.Time,
) error {
	if len(s.engs) == 0 {
		return errors.New("no engines configured for nodeTombstoneStorage")
	}
	if ts.IsZero() {
		return errors.New("can't mark as decommissioned at timestamp zero")
	}
	if !s.maybeAddCached(nodeID, ts.UTC()) {
		// The cache already had a positive hit for this node, so don't do anything.
		return nil
	}

	// We've populated the cache, now write through to disk.
	k := s.key(nodeID)
	for _, eng := range s.engs {
		// Read the store ident before trying to write to this
		// engine. An engine that is not bootstrapped should not be
		// written to, as we check (in InitEngine) that it is empty.
		//
		// One initialized engine is always available when this method
		// is called, so we're still persisting on at least one engine.
		if _, err := kvserver.ReadStoreIdent(ctx, eng); err != nil {
			if errors.Is(err, &kvserver.NotBootstrappedError{}) {
				continue
			}
			return err
		}
		var v roachpb.Value
		if err := v.SetProto(&hlc.Timestamp{WallTime: ts.UnixNano()}); err != nil {
			return err
		}

		if err := storage.MVCCPut(
			ctx, eng, nil /* MVCCStats */, k, hlc.Timestamp{}, v, nil, /* txn */
		); err != nil {
			return err
		}
	}
	return nil
}
