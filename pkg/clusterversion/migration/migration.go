// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migration contains mechanisms for advancing the pkg/clusterversion of
// a cluster. These clusterversions serve a dual purpose: first to act as a gate
// for new features and second to provide a low-water mark for backward
// compatiblity between the binaries of nodes in a cluster.
//
// To upgrade from X to X+1 call UpgradeCluster on a *Coordinator (X here is the
// versions as ordered by the VersionKey, so in practice this looks like an
// upgrade from vA.B.C to vA.B.C+1 or from vA.B.C to vA.B+1.0). This first
// updates the version on each node in the cluster such taht no `IsActive(X+1)`
// check will ever return false again on any node in this cluster. After this
// finishes, the version's idempotent migration hook is run (if there is one).
// This hook can and often will depend on the preceding `IsActive(X+1)`
// guarantee.
//
// The `IsActive(X+1)` guarantee is provided as follows. First the new version
// is written to replicated kv storage, so that all new nodes start with at
// least this version. Then it uses a synchronous rpc to each existing node to
// update the version on that node, which is persisted to all stores before
// returning. If any nodes are unavailable, this blocks until they're back or
// completely decomissioned. WIP what happens if a node starts with stores that
// don't have the same version?
package migration

import (
	"context"
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type Server struct {
	engs []engine.Engine
	mu   struct {
		syncutil.Mutex
		current clusterversion.ClusterVersion
	}
}

func NewServer(
	bootVersion clusterversion.ClusterVersion, engs []engine.Engine,
) (*Server, clusterversion.Handle) {
	s := &Server{
		engs: engs,
	}
	s.mu.current = bootVersion
	return s, s
}

var _ ClusterVersionServer = (*Server)(nil)
var _ clusterversion.Handle = (*Server)(nil)

// IsActive implements the Handle interface.
func (s *Server) IsActive(ctx context.Context, feature clusterversion.VersionKey) bool {
	return s.ActiveVersion(ctx).IsActive(feature)
}

// ActiveVersion implements the Handle interface.
func (s *Server) ActiveVersion(ctx context.Context) clusterversion.ClusterVersion {
	ver := s.ActiveVersionOrEmpty(ctx)
	if ver == (clusterversion.ClusterVersion{}) {
		log.Fatalf(ctx, "version not initialized")
	}
	return ver
}

// ActiveVersionOrEmpty implements the Handle interface.
func (s *Server) ActiveVersionOrEmpty(_ context.Context) clusterversion.ClusterVersion {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.current
}

// BinaryVersion implements the Handle interface.
func (s *Server) BinaryVersion() roachpb.Version {
	panic(`WIP refactor this out of cluster.Settings`)
}

// BinaryMinSupportedVersion implements the Handle interface.
func (s *Server) BinaryMinSupportedVersion() roachpb.Version {
	panic(`WIP refactor this out of cluster.Settings`)
}

// ForwardNodeVersion implements the ClusterVersionServer interface.
func (s *Server) ForwardNodeVersion(
	ctx context.Context, req *ForwardNodeVersionRequest,
) (*ForwardNodeVersionResponse, error) {
	err := s.forwardVersion(ctx, req.Version)
	if err != nil {
		resErr := errors.EncodeError(ctx, err)
		return &ForwardNodeVersionResponse{Err: &resErr}, nil
	}
	return &ForwardNodeVersionResponse{}, nil
}

func (s *Server) forwardVersion(ctx context.Context, target clusterversion.ClusterVersion) error {
	// WIP: Validation.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.current.Less(target.Version) {
		s.mu.current = target
		if err := storage.WriteClusterVersionToEngines(ctx, s.engs, target); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) NewCoordinator(
	db *client.DB, rpcCtx *rpc.Context, gossip *gossip.Gossip,
) *Coordinator {
	return &Coordinator{s: s, db: db, rpcCtx: rpcCtx, gossip: gossip}
}

type Coordinator struct {
	s      *Server
	db     *client.DB
	rpcCtx *rpc.Context
	gossip *gossip.Gossip
}

func (c *Coordinator) UpgradeCluster(
	ctx context.Context, target clusterversion.ClusterVersion, progressFn func(string),
) error {
	// WIP this is an awkward way to step through versions, expose something in
	// the clusterversion pkg to make this less brittle
	for key := clusterversion.Version19_1; ; key++ {
		v, hookFn := clusterversion.VersionAndHookByKey(key)
		if v.Less(target.Version) {
			continue
		}
		if hookFn != nil {
			progressFn(fmt.Sprintf(`running migration hook for %s`, v))
			if err := hookFn(ctx, nil /* WIP */); err != nil {
				return err
			}
		}
		progressFn(fmt.Sprintf(`activating %s on all nodes`, v))
		if err := c.communicateNewVersion(ctx, clusterversion.ClusterVersion{Version: v}); err != nil {
			return err
		}
		// WIP don't use == here, need to generate a proto one
		if v == target.Version {
			break
		}
	}
	return nil
}

// communicateNewVersion migrates this cluster to activate the target features.
func (c *Coordinator) communicateNewVersion(
	ctx context.Context, target clusterversion.ClusterVersion,
) error {
	if err := updateClusterMinimum(ctx, c.db, target); err != nil {
		return err
	}

	// WIP: Get every non-decommissioned node. Can we do this transactionally with
	// the updateClusterMinimum call above?
	var nodeIDs []roachpb.NodeID

	g := ctxgroup.WithContext(ctx)
	for i := range nodeIDs {
		nodeID := nodeIDs[i]
		g.GoCtx(func(ctx context.Context) error {
			// WIP: How should retries work?
			//
			// WIP: If something gets stuck, we need to be able to push the status
			// back up to the job so the user can see what's blocking the upgrade.
			address, err := c.gossip.GetNodeIDAddress(nodeID)
			if err != nil {
				return err
			}
			conn, err := c.rpcCtx.GRPCDialNode(address.String(), nodeID, rpc.SystemClass).Connect(ctx)
			if err != nil {
				return err
			}
			client := NewClusterVersionClient(conn)
			// WIP: What sort of cleanup is needed for conn and client?
			res, err := client.ForwardNodeVersion(ctx, &ForwardNodeVersionRequest{Version: target})
			if err != nil {
				return err
			}
			if res.Err != nil {
				return errors.DecodeError(ctx, *res.Err)
			}
			return nil
		})
	}
	return g.Wait()
}

func updateClusterMinimum(ctx context.Context, db *client.DB, version clusterversion.ClusterVersion) error {
	err := db.Put(ctx, keys.ClusterVersionCurrent, version.Version.String())
	return errors.Wrapf(err, "persisting new cluster version: %s", version.Version)
}
