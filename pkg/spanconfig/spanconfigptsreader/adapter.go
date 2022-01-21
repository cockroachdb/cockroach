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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptcache"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// adapter implements the spanconfig.ProtectedTSReader interface and is intended
// as a bridge between the old and new protected timestamp subsystems in KV.
//
// V1 of the protected timestamp subsystem only allowed protections to be set
// over spans belonging to the system tenant. These protections were stored in
// memory by ptcache.Cache. V2 of the subsystem allows protections to be set
// on all spans and are stored in the spanconfig.Store maintained by the
// spanconfig.KVSubscriber. In release 22.1 both the old and new subsystem
// co-exist, and as such, protections that apply to system tenant spans may be
// present in either the ptcache.Cache or spanconfig.KVSubscriber. This adapter
// interface encapsulates protected timestamp information from both these
// sources behind a single interface.
//
// TODO(arul): In 22.2, we would have completely migrated away from the old
//  subsystem, and we'd be able to get rid of this interface.
//
// TODO(arul): Embed the KVSubscriber here as well and actually encapsulate PTS
// information from both these sources as described above; This'll happen once
// we make the KVSubscriber implement the spanconfig.ProtectedTSReader
// interface.
type adapter struct {
	*ptcache.Cache
}

var _ spanconfig.ProtectedTSReader = &adapter{}

// NewAdapter returns an adapter that implements spanconfig.ProtectedTSReader.
func NewAdapter(cache *ptcache.Cache) spanconfig.ProtectedTSReader {
	return &adapter{
		Cache: cache,
	}
}

// TestingRefreshPTSState refreshes the in-memory protected timestamp state to
// at least asOf.
// TODO(arul): Once we wrap the KVSubscriber in this adapter interface, we'll
// need to ensure that the subscriber is at-least as caught up as the supplied
// asOf timestamp as well.
func TestingRefreshPTSState(
	ctx context.Context, protectedTSReader spanconfig.ProtectedTSReader, asOf hlc.Timestamp,
) error {
	a, ok := protectedTSReader.(*adapter)
	if !ok {
		return errors.AssertionFailedf("could not convert protectedts.Provider to ptprovider.Provider")
	}
	return a.Cache.Refresh(ctx, asOf)
}
