// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tristan Rice (rice@fn.lc)

package storage

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// InternalStoresServer implements the storage parts of the
// roachpb.InternalStoresServer
// interface.
type InternalStoresServer struct {
	descriptor *roachpb.NodeDescriptor
	stores     *Stores
}

var _ roachpb.InternalStoresServer = InternalStoresServer{}

// MakeInternalStoresServer returns a new instance of InternalStoresServer.
func MakeInternalStoresServer(
	descriptor *roachpb.NodeDescriptor, stores *Stores,
) InternalStoresServer {
	return InternalStoresServer{descriptor, stores}
}

func (is InternalStoresServer) execStoreCommand(
	h roachpb.StoreRequestHeader, f func(*Store) error,
) error {
	if h.NodeID != is.descriptor.NodeID {
		return errors.Errorf("request for NodeID %d cannot be served by NodeID %d",
			h.NodeID, is.descriptor.NodeID)
	}
	store, err := is.stores.GetStore(h.StoreID)
	if err != nil {
		return err
	}
	return f(store)
}

// PollFrozen implements the roachpb.InternalStoresServer interface.
func (is InternalStoresServer) PollFrozen(
	ctx context.Context, args *roachpb.PollFrozenRequest,
) (*roachpb.PollFrozenResponse, error) {
	resp := &roachpb.PollFrozenResponse{}
	err := is.execStoreCommand(args.StoreRequestHeader,
		func(s *Store) error {
			resp.Results = s.FrozenStatus(args.CollectFrozen)
			return nil
		})
	return resp, err
}

// Reserve implements the roachpb.InternalStoresServer interface.
func (is InternalStoresServer) Reserve(
	ctx context.Context, req *roachpb.ReservationRequest,
) (*roachpb.ReservationResponse, error) {
	resp := &roachpb.ReservationResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader,
		func(s *Store) error {
			*resp = s.Reserve(ctx, *req)
			return nil
		})
	return resp, err
}
