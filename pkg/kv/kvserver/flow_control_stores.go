// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// StoresFlowControlHandles is a concrete implementation of
// kvflowcontrol.Handles, backed by a set of Stores.
type StoresFlowControlHandles Stores

var _ kvflowcontrol.Handles = &StoresFlowControlHandles{}

// MakeStoresFlowControlHandles returns a new StoresFlowControlHandles instance.
func MakeStoresFlowControlHandles(stores *Stores) *StoresFlowControlHandles {
	return (*StoresFlowControlHandles)(stores)
}

// Lookup is part of the kvflowcontrol.Handles interface.
func (sh *StoresFlowControlHandles) Lookup(
	rangeID roachpb.RangeID,
) (handle kvflowcontrol.Handle, found bool) {
	ls := (*Stores)(sh)
	if err := ls.VisitStores(func(s *Store) error {
		if h, ok := makeStoreFlowControlHandles(s).Lookup(rangeID); ok {
			handle = h
			found = true
		}
		return nil
	}); err != nil {
		ctx := ls.AnnotateCtx(context.Background())
		log.Errorf(ctx, "unexpected error: %s", err)
		return nil, false
	}
	return handle, found
}

// storeFlowControlHandles is a concrete implementation of
// kvflowcontrol.Handles, backed by a single Store.
type storeFlowControlHandles Store

var _ kvflowcontrol.Handles = &storeFlowControlHandles{}

// makeStoreFlowControlHandles returns a new storeFlowControlHandles instance.
func makeStoreFlowControlHandles(store *Store) *storeFlowControlHandles {
	return (*storeFlowControlHandles)(store)
}

// Lookup is part of the kvflowcontrol.Handles interface.
func (sh *storeFlowControlHandles) Lookup(
	rangeID roachpb.RangeID,
) (_ kvflowcontrol.Handle, found bool) {

	s := (*Store)(sh)
	repl := s.GetReplicaIfExists(rangeID)
	if repl == nil {
		return nil, false
	}

	repl.mu.Lock()
	defer repl.mu.Unlock()
	return nil, false // TODO(irfansharif): Fill this in.
}
