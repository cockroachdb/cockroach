// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterversion

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// Test that the OnChange callback is called for the cluster version with the
// right argument.
func TestClusterVersionOnChange(t *testing.T) {
	ctx := context.Background()
	var sv settings.Values

	cvs := &clusterVersionSetting{}
	cvs.VersionSetting = settings.MakeVersionSetting(cvs)
	settings.RegisterVersionSetting(
		settings.ApplicationLevel,
		"dummy version key",
		"test description",
		&cvs.VersionSetting)

	handle := newHandleImpl(cvs, &sv, binaryVersion, binaryMinSupportedVersion)
	newCV := ClusterVersion{
		Version: roachpb.Version{
			Major:    1,
			Minor:    2,
			Patch:    3,
			Internal: 4,
		},
	}
	encoded, err := protoutil.Marshal(&newCV)
	require.NoError(t, err)

	var capturedV ClusterVersion
	handle.SetOnChange(func(ctx context.Context, newVersion ClusterVersion) {
		capturedV = newVersion
	})
	cvs.SetInternal(ctx, &sv, encoded)
	require.Equal(t, newCV, capturedV)
}
