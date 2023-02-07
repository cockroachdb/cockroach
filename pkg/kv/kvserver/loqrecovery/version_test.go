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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestVersionCompatibility(t *testing.T) {
	for _, d := range []struct {
		name         string
		version      roachpb.Version
		minSupported roachpb.Version
		current      roachpb.Version
		err          bool
		isV1         bool
	}{
		{
			name: "current version",
			version: roachpb.Version{
				Major: 23,
				Minor: 1,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major: 23,
				Minor: 1,
			},
		},
		{
			name: "v1",
			version: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major: 23,
				Minor: 2,
			},
			isV1: true,
		},
		{
			name: "in between",
			version: roachpb.Version{
				Major: 23,
				Minor: 1,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major: 23,
				Minor: 2,
			},
		},
		{
			name: "too old",
			version: roachpb.Version{
				Major: 22,
				Minor: 1,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major: 23,
				Minor: 1,
			},
			err: true,
		},
		{
			name: "dev build with release data file",
			version: roachpb.Version{
				Major:    22,
				Minor:    2,
				Internal: 50,
			},
			minSupported: roachpb.Version{
				Major: 22 + clusterversion.DevOffset,
				Minor: 2,
			},
			current: roachpb.Version{
				Major:    22 + clusterversion.DevOffset,
				Minor:    2,
				Internal: 50,
			},
			err: true,
		},
		{
			name: "dev build with dev data file",
			version: roachpb.Version{
				Major:    22 + clusterversion.DevOffset,
				Minor:    2,
				Internal: 50,
			},
			minSupported: roachpb.Version{
				Major: 22 + clusterversion.DevOffset,
				Minor: 2,
			},
			current: roachpb.Version{
				Major:    22 + clusterversion.DevOffset,
				Minor:    2,
				Internal: 50,
			},
		},
		{
			name: "upgrade from v1",
			version: roachpb.Version{
				Major:    22,
				Minor:    2,
				Internal: 1,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major:    22,
				Minor:    2,
				Internal: 50,
			},
		},
	} {
		t.Run(d.name, func(t *testing.T) {
			err := checkVersionAllowedImpl(d.version, d.minSupported, d.current)
			if d.err {
				require.Error(t, err, "version check must fail")
			} else {
				require.NoError(t, err, "version check must pass")
			}
		})
	}
}
