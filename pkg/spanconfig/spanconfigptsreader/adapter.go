// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigptsreader

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
//  subsystem, and we'd be able to get rid of this interface.
type adapter struct {
	cache        protectedts.Cache
	kvSubscriber spanconfig.KVSubscriber
}

var _ spanconfig.ProtectedTSReader = &adapter{}

// NewAdapter returns an adapter that implements spanconfig.ProtectedTSReader.
func NewAdapter(
	cache protectedts.Cache, kvSubscriber spanconfig.KVSubscriber,
) spanconfig.ProtectedTSReader {
	return &adapter{
		cache:        cache,
		kvSubscriber: kvSubscriber,
	}
}

// GetProtectionTimestamps is part of the spanconfig.ProtectedTSReader
// interface.
func (a *adapter) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, err error) {
	cacheTimestamps, cacheFreshness, err := a.cache.GetProtectionTimestamps(ctx, sp)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	subscriberTimestamps, subscriberFreshness, err := a.kvSubscriber.GetProtectionTimestamps(ctx, sp)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	// The freshness of the adapter is the minimum freshness of the Cache and
	// KVSubscriber.
	subscriberFreshness.Backward(cacheFreshness)
	return append(subscriberTimestamps, cacheTimestamps...), subscriberFreshness, nil
}

// TestingRefreshPTSState refreshes the in-memory protected timestamp state to
// at least asOf.
func TestingRefreshPTSState(
	ctx context.Context,
	t *testing.T,
	protectedTSReader spanconfig.ProtectedTSReader,
	asOf hlc.Timestamp,
) error {
	a, ok := protectedTSReader.(*adapter)
	if !ok {
		return errors.AssertionFailedf("could not convert protectedTSReader to adapter")
	}
	testutils.SucceedsSoon(t, func() error {
		_, fresh, err := a.GetProtectionTimestamps(ctx, roachpb.Span{})
		if err != nil {
			return err
		}
		if fresh.Less(asOf) {
			return errors.AssertionFailedf("KVSubscriber fresh as of %s; not caught up to %s", fresh, asOf)
		}
		return nil
	})
	return a.cache.Refresh(ctx, asOf)
}
