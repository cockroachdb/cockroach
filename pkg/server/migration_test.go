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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestValidateTargetClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := func(major, minor int32) roachpb.Version {
		return roachpb.Version{Major: major, Minor: minor}
	}
	cv := func(major, minor int32) clusterversion.ClusterVersion {
		return clusterversion.ClusterVersion{Version: v(major, minor)}
	}

	var tests = []struct {
		binaryVersion             roachpb.Version
		binaryMinSupportedVersion roachpb.Version
		targetClusterVersion      clusterversion.ClusterVersion
		expErrMatch               string // empty if expecting a nil error
	}{
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(20, 1),
			expErrMatch:               "",
		},
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(20, 2),
			expErrMatch:               "",
		},
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(21, 1),
			expErrMatch:               "binary version.*less than target cluster version",
		},
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(19, 2),
			expErrMatch:               "target cluster version.*less than binary's min supported version",
		},
	}

	//   node's minimum supported version <= target version <= node's binary version

	for i, test := range tests {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			test.binaryVersion,
			test.binaryMinSupportedVersion,
			false, /* initializeVersion */
		)

		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Server: &TestingKnobs{
					BinaryVersionOverride: test.binaryVersion,
				},
			},
		})

		migrationServer := s.MigrationServer().(*migrationServer)
		req := &serverpb.ValidateTargetClusterVersionRequest{
			ClusterVersion: &test.targetClusterVersion,
		}
		_, err := migrationServer.ValidateTargetClusterVersion(context.Background(), req)
		if !testutils.IsError(err, test.expErrMatch) {
			t.Fatalf("test %d: got error %s, wanted error matching '%s'", i, err, test.expErrMatch)
		}

		s.Stopper().Stop(context.Background())
	}
}
