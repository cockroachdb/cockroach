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
	"bytes"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

// Server implements the storage parts of the StoresServer interface.
type Server struct {
	descriptor *roachpb.NodeDescriptor
	stores     *Stores
}

var _ StoresServer = Server{}

// MakeServer returns a new instance of Server.
func MakeServer(
	descriptor *roachpb.NodeDescriptor, stores *Stores,
) Server {
	return Server{descriptor, stores}
}

func (is Server) execStoreCommand(
	h StoreRequestHeader, f func(*Store) error,
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

// PollFrozen implements the StoresServer interface.
func (is Server) PollFrozen(
	ctx context.Context, args *PollFrozenRequest,
) (*PollFrozenResponse, error) {
	resp := &PollFrozenResponse{}
	err := is.execStoreCommand(args.StoreRequestHeader,
		func(s *Store) error {
			resp.Results = s.FrozenStatus(args.CollectFrozen)
			return nil
		})
	return resp, err
}

// Reserve implements the StoresServer interface.
func (is Server) Reserve(
	ctx context.Context, req *ReservationRequest,
) (*ReservationResponse, error) {
	resp := &ReservationResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader,
		func(s *Store) error {
			*resp = s.Reserve(ctx, *req)
			return nil
		})
	return resp, err
}

// CollectChecksum implements the StoresServer interface.
func (is Server) CollectChecksum(
	ctx context.Context, req *CollectChecksumRequest,
) (*CollectChecksumResponse, error) {
	resp := &CollectChecksumResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader,
		func(s *Store) error {
			r, err := s.GetReplica(req.RangeID)
			if err != nil {
				return err
			}
			c, err := r.getChecksum(ctx, req.ChecksumID)
			if err != nil {
				return err
			}
			resp.Checksum = c.checksum
			if bytes.Equal(req.Checksum, c.checksum) {
				return nil
			}
			log.Errorf(ctx, "consistency check failed on range ID %s: expected checksum %x, got %x",
				req.RangeID, req.Checksum, c.checksum)
			if c.snapshot == nil {
				return nil
			}
			resp.Snapshot = c.snapshot
			if !r.store.ctx.ConsistencyCheckPanicOnFailure {
				return nil
			}
			if p := r.store.ctx.TestingKnobs.BadChecksumPanic; p != nil {
				p()
			} else {
				// We use a goroutine and a sleep here to give gRPC a chance to get the
				// response back to the initiator of the check. The upside is that the
				// initiator can log a diff and not have its goroutine time out.
				go func() {
					time.Sleep(10 * time.Second)
					panic("committing suicide due to failed consistency check")
				}()
			}
			return nil
		})
	return resp, err
}
