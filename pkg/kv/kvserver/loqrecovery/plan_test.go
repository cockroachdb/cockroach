// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

func TestVersionIsPreserved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	current := clusterversion.Latest.Version()
	current.Patch += 1

	replicaInfo := infoWithVersion(current)

	plan, _, err := PlanReplicas(ctx, replicaInfo, nil, nil, uuid.DefaultGenerator)
	require.NoError(t, err, "good version is rejected")
	require.Equal(t, current, plan.Version, "plan version was not preserved")
}

// infoWithVersion creates a skeleton info that passes all checks beside version.
func infoWithVersion(v roachpb.Version) loqrecoverypb.ClusterReplicaInfo {
	return loqrecoverypb.ClusterReplicaInfo{
		ClusterID: uuid.MakeV4().String(),
		Version:   v,
	}
}
