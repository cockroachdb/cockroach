// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigptsreader

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// adapter implements the spanconfig.ProtectedTSReader interface and is intended
// as a bridge between the old and new protected timestamp subsystems in KV.
//
// V1 of the protected timestamp subsystem only allowed protections to be set
// over spans belonging to the system tenant. These protections were cached by
// each node in the cluster by ptcache.Cache. V2 of the subsystem allows
// protections to be set on all spans (including secondary tenant spans) and are
// cached in the spanconfig.Store maintained by the spanconfig.KVSubscriber. In
// release 22.1 both the old and new subsystem co-exist, and as such,
// protections that apply to system tenant spans may be present in either the
// ptcache.Cache or spanconfig.KVSubscriber. This adapter struct encapsulates
// protected timestamp information from both these sources behind a single
// interface.
//
// TODO(arul): In 22.2, we would have completely migrated away from the old
//
//	subsystem, and we'd be able to get rid of this interface.
type adapter struct {
	cache        protectedts.Cache
	kvSubscriber spanconfig.KVSubscriber
	s            *cluster.Settings
}

var _ spanconfig.ProtectedTSReader = &adapter{}

// NewAdapter returns an adapter that implements spanconfig.ProtectedTSReader.
func NewAdapter(
	cache protectedts.Cache, kvSubscriber spanconfig.KVSubscriber, s *cluster.Settings,
) spanconfig.ProtectedTSReader {
	return &adapter{
		cache:        cache,
		kvSubscriber: kvSubscriber,
		s:            s,
	}
}

// GetProtectionTimestamps is part of the spanconfig.ProtectedTSReader
// interface.
func (a *adapter) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, err error) {
	subscriberTimestamps, subscriberFreshness, err := a.kvSubscriber.GetProtectionTimestamps(ctx, sp)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	return subscriberTimestamps, subscriberFreshness, nil
}

// TestingRefreshPTSState refreshes the in-memory protected timestamp state to
// at least asOf.
func TestingRefreshPTSState(
	ctx context.Context, protectedTSReader spanconfig.ProtectedTSReader, asOf hlc.Timestamp,
) error {
	a, ok := protectedTSReader.(*adapter)
	if !ok {
		return errors.AssertionFailedf("could not convert protectedTSReader to adapter")
	}
	// First refresh the cache past asOf.
	if err := a.cache.Refresh(ctx, asOf); err != nil {
		return err
	}

	// Now ensure the KVSubscriber is fresh enough.
	return retry.ForDuration(200*time.Second, func() error {
		_, fresh, err := a.GetProtectionTimestamps(ctx, keys.EverythingSpan)
		if err != nil {
			return err
		}
		if fresh.Less(asOf) {
			return errors.AssertionFailedf("KVSubscriber fresh as of %s; not caught up to %s", fresh, asOf)
		}
		return nil
	})
}
