// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// migrationServer is an implementation of the Migration service. The RPCs here
// are used to power the migrations infrastructure in pkg/migrations.
type migrationServer struct {
	server *Server

	// We use this mutex to serialize attempts to bump the cluster version.
	syncutil.Mutex
}

var _ serverpb.MigrationServer = &migrationServer{}

// ValidateTargetClusterVersion implements the MigrationServer interface.
// It's used to verify that we're running a binary that's able to support the
// given cluster version.
func (m *migrationServer) ValidateTargetClusterVersion(
	ctx context.Context, req *serverpb.ValidateTargetClusterVersionRequest,
) (*serverpb.ValidateTargetClusterVersionResponse, error) {
	targetCV := req.ClusterVersion
	versionSetting := m.server.ClusterSettings().Version

	// We're validating the following:
	//
	//   node's minimum supported version <= target version <= node's binary version
	if targetCV.Less(versionSetting.BinaryMinSupportedVersion()) {
		msg := fmt.Sprintf("target cluster version %s less than binary's min supported version %s",
			targetCV, versionSetting.BinaryMinSupportedVersion())
		log.Warningf(ctx, "%s", msg)
		return nil, errors.Newf("%s", redact.Safe(msg))
	}

	if versionSetting.BinaryVersion().Less(targetCV.Version) {
		msg := fmt.Sprintf("binary version %s less than target cluster version %s",
			versionSetting.BinaryVersion(), targetCV)
		log.Warningf(ctx, "%s", msg)
		return nil, errors.Newf("%s", redact.Safe(msg))
	}

	resp := &serverpb.ValidateTargetClusterVersionResponse{}
	return resp, nil
}

// BumpClusterVersion implements the MigrationServer interface. It's used to
// inform us of a cluster version bump. Here we're responsible for durably
// persisting the cluster version and enabling the corresponding version gates.
func (m *migrationServer) BumpClusterVersion(
	ctx context.Context, req *serverpb.BumpClusterVersionRequest,
) (*serverpb.BumpClusterVersionResponse, error) {
	m.Lock()
	defer m.Unlock()

	versionSetting := m.server.ClusterSettings().Version
	prevCV, err := kvserver.SynthesizeClusterVersionFromEngines(
		ctx, m.server.engines, versionSetting.BinaryVersion(),
		versionSetting.BinaryMinSupportedVersion(),
	)
	if err != nil {
		return nil, err
	}

	newCV := *req.ClusterVersion

	if err := func() error {
		if !prevCV.Less(newCV.Version) {
			// Nothing to do.
			return nil
		}

		// TODO(irfansharif): We should probably capture this pattern of
		// "persist the cluster version first" and only then bump the
		// version setting in a better way.

		// Whenever the version changes, we want to persist that update to
		// wherever the CRDB process retrieved the initial version from
		// (typically a collection of storage.Engines).
		if err := kvserver.WriteClusterVersionToEngines(ctx, m.server.engines, newCV); err != nil {
			return err
		}

		// We bump the local version gate here.
		//
		// NB: On 21.1 nodes we no longer use gossip to propagate cluster
		// version bumps. We'll still disseminate it through gossip, but the
		// actual (local) setting update happens here.
		//
		// TODO(irfansharif): We should stop disseminating cluster version
		// bumps through gossip after 21.1 is cut. There will be no one
		// listening in on it.
		if err := m.server.ClusterSettings().Version.SetActiveVersion(ctx, newCV); err != nil {
			return err
		}
		log.Infof(ctx, "active cluster version setting is now %s (up from %s)", newCV, prevCV)
		return nil
	}(); err != nil {
		return nil, err
	}

	resp := &serverpb.BumpClusterVersionResponse{}
	return resp, nil
}
