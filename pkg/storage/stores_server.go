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

package storage

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Server implements PerReplicaServer.
type Server struct {
	stores *Stores
}

var _ PerReplicaServer = Server{}

// MakeServer returns a new instance of Server.
func MakeServer(descriptor *roachpb.NodeDescriptor, stores *Stores) Server {
	return Server{stores}
}

func (is Server) execStoreCommand(h StoreRequestHeader, f func(*Store) error) error {
	store, err := is.stores.GetStore(h.StoreID)
	if err != nil {
		return err
	}
	return f(store)
}

// CollectChecksum implements PerReplicaServer.
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
			defer r.Unref()
			c, err := r.getChecksum(ctx, req.ChecksumID)
			if err != nil {
				return err
			}
			ccr := c.CollectChecksumResponse
			if !bytes.Equal(req.Checksum, ccr.Checksum) {
				// If this check is false, then this request is the replica carrying out
				// the consistency check. The message is spurious, but we want to leave the
				// snapshot (if present) intact.
				if len(req.Checksum) > 0 {
					log.Errorf(ctx, "consistency check failed on range r%d: expected checksum %x, got %x",
						req.RangeID, req.Checksum, ccr.Checksum)
					// Leave resp.Snapshot alone so that the caller will receive what's
					// in it (if anything).
				}
			} else {
				ccr.Snapshot = nil
			}
			resp = &ccr
			return nil
		})
	return resp, err
}
