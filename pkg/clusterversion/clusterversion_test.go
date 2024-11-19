// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
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

	handle := newHandleImpl(cvs, &sv, Latest.Version(), MinSupported.Version())
	newCV := ClusterVersion{
		Version: roachpb.Version{
			Major:    1,
			Minor:    2,
			Patch:    3,
			Internal: 4,
		},
	}
	var capturedV ClusterVersion
	handle.SetOnChange(func(ctx context.Context, newVersion ClusterVersion) {
		capturedV = newVersion
	})
	cvs.SetInternal(ctx, &sv, newCV)
	require.Equal(t, newCV, capturedV)
}
