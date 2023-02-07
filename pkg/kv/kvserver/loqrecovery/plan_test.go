// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestVersionBelowMinIsRejected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	belowMin := clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey)
	belowMin.Major -= 1

	replicaInfo := infoWithVersion(belowMin)

	_, _, err := PlanReplicas(ctx, replicaInfo, nil, nil, uuid.DefaultGenerator)
	require.ErrorContains(t, err, "version is too old", "version check failed")
}

func TestVersionAboveCurrentIsRejected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	belowCurrent := clusterversion.ByKey(clusterversion.BinaryVersionKey)
	belowCurrent.Minor += 1

	replicaInfo := infoWithVersion(belowCurrent)

	_, _, err := PlanReplicas(ctx, replicaInfo, nil, nil, uuid.DefaultGenerator)
	require.ErrorContains(t, err, "version is too new", "version check failed")
}

func TestVersionInValidRangeArePreserved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	current := clusterversion.ByKey(clusterversion.BinaryVersionKey)
	current.Patch += 1

	replicaInfo := infoWithVersion(current)

	plan, _, err := PlanReplicas(ctx, replicaInfo, nil, nil, uuid.DefaultGenerator)
	require.NoError(t, err, "good version is rejected")
	require.Equal(t, current, plan.Version, "plan version was not preserved")
}

// infoWithVersion creates a skeleton info that passes all checks beside version.
func infoWithVersion(v roachpb.Version) loqrecoverypb.ClusterReplicaInfo {
	return loqrecoverypb.ClusterReplicaInfo{
		ClusterID: uuid.FastMakeV4().String(),
		Version:   v,
	}
}
