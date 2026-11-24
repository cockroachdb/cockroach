// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigptsreader

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// adapter implements the spanconfig.ProtectedTSReader interface by delegating
// to the KVSubscriber.
//
// TODO(david): This adapter is now just a passthrough to the KVSubscriber,
// which already implements ProtectedTSReader. We should remove this adapter
// and use KVSubscriber directly.
type adapter struct {
	kvSubscriber spanconfig.KVSubscriber
	s            *cluster.Settings
}

var _ spanconfig.ProtectedTSReader = &adapter{}

// NewAdapter returns an adapter that implements spanconfig.ProtectedTSReader.
func NewAdapter(
	kvSubscriber spanconfig.KVSubscriber, s *cluster.Settings,
) spanconfig.ProtectedTSReader {
	return &adapter{
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

	// Ensure the KVSubscriber is fresh enough.
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
