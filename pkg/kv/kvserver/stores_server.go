// Copyright 2016 The Cockroach Authors.
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
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/redact"
)

// Server implements PerReplicaServer.
type Server struct {
	stores *Stores
}

var _ PerReplicaServer = Server{}
var _ PerStoreServer = Server{}

// MakeServer returns a new instance of Server.
func MakeServer(descriptor *roachpb.NodeDescriptor, stores *Stores) Server {
	return Server{stores}
}

func (is Server) execStoreCommand(
	ctx context.Context, h StoreRequestHeader, f func(context.Context, *Store) error,
) error {
	store, err := is.stores.GetStore(h.StoreID)
	if err != nil {
		return err
	}
	// NB: we use a task here to prevent errant RPCs that arrive after stopper shutdown from
	// causing crashes. See #56085 for an example of such a crash.
	return store.stopper.RunTaskWithErr(ctx, "store command", func(ctx context.Context) error {
		return f(ctx, store)
	})
}

// CollectChecksum implements PerReplicaServer.
func (is Server) CollectChecksum(
	ctx context.Context, req *CollectChecksumRequest,
) (*CollectChecksumResponse, error) {
	resp := &CollectChecksumResponse{}
	err := is.execStoreCommand(ctx, req.StoreRequestHeader,
		func(ctx context.Context, s *Store) error {
			r, err := s.GetReplica(req.RangeID)
			if err != nil {
				return err
			}
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
						req.RangeID, redact.Safe(req.Checksum), redact.Safe(ccr.Checksum))
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

// WaitForApplication implements PerReplicaServer.
//
// It is the caller's responsibility to cancel or set a timeout on the context.
// If the context is never canceled, WaitForApplication will retry forever.
func (is Server) WaitForApplication(
	ctx context.Context, req *WaitForApplicationRequest,
) (*WaitForApplicationResponse, error) {
	resp := &WaitForApplicationResponse{}
	err := is.execStoreCommand(ctx, req.StoreRequestHeader, func(ctx context.Context, s *Store) error {
		// TODO(benesch): Once Replica changefeeds land, see if we can implement
		// this request handler without polling.
		retryOpts := retry.Options{InitialBackoff: 10 * time.Millisecond}
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Long-lived references to replicas are frowned upon, so re-fetch the
			// replica on every turn of the loop.
			repl, err := s.GetReplica(req.RangeID)
			if err != nil {
				return err
			}
			repl.mu.RLock()
			leaseAppliedIndex := repl.mu.state.LeaseAppliedIndex
			repl.mu.RUnlock()
			if leaseAppliedIndex >= req.LeaseIndex {
				// For performance reasons, we don't sync to disk when
				// applying raft commands. This means that if a node restarts
				// after applying but before the next sync, its
				// LeaseAppliedIndex could temporarily regress (until it
				// reapplies its latest raft log entries).
				//
				// Merging relies on the monotonicity of the log applied
				// index, so before returning ensure that rocksdb has synced
				// everything up to this point to disk.
				//
				// https://github.com/cockroachdb/cockroach/issues/33120
				return storage.WriteSyncNoop(ctx, s.engine)
			}
		}
		if ctx.Err() == nil {
			log.Fatal(ctx, "infinite retry loop exited but context has no error")
		}
		return ctx.Err()
	})
	return resp, err
}

// WaitForReplicaInit implements PerReplicaServer.
//
// It is the caller's responsibility to cancel or set a timeout on the context.
// If the context is never canceled, WaitForReplicaInit will retry forever.
func (is Server) WaitForReplicaInit(
	ctx context.Context, req *WaitForReplicaInitRequest,
) (*WaitForReplicaInitResponse, error) {
	resp := &WaitForReplicaInitResponse{}
	err := is.execStoreCommand(ctx, req.StoreRequestHeader, func(ctx context.Context, s *Store) error {
		retryOpts := retry.Options{InitialBackoff: 10 * time.Millisecond}
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Long-lived references to replicas are frowned upon, so re-fetch the
			// replica on every turn of the loop.
			if repl, err := s.GetReplica(req.RangeID); err == nil && repl.IsInitialized() {
				return nil
			}
		}
		if ctx.Err() == nil {
			log.Fatal(ctx, "infinite retry loop exited but context has no error")
		}
		return ctx.Err()
	})
	return resp, err
}

// CompactEngineSpan implements PerStoreServer. It blocks until the compaction
// is done, so it can be a long-lived RPC.
func (is Server) CompactEngineSpan(
	ctx context.Context, req *CompactEngineSpanRequest,
) (*CompactEngineSpanResponse, error) {
	resp := &CompactEngineSpanResponse{}
	err := is.execStoreCommand(ctx, req.StoreRequestHeader,
		func(ctx context.Context, s *Store) error {
			return s.Engine().CompactRange(req.Span.Key, req.Span.EndKey, true /* forceBottommost */)
		})
	return resp, err
}
