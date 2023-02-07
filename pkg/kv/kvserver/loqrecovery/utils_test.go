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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestPatchVersionCompatibility(t *testing.T) {
	for _, d := range []struct {
		name         string
		version      roachpb.Version
		minSupported roachpb.Version
		current      roachpb.Version
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
			name: "data from previous patch",
			version: roachpb.Version{
				Major: 23,
				Minor: 1,
				Patch: 1,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major: 23,
				Minor: 1,
				Patch: 2,
			},
		},
		{
			name: "data from future patch",
			version: roachpb.Version{
				Major: 23,
				Minor: 1,
				Patch: 2,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major: 23,
				Minor: 1,
				Patch: 1,
			},
		},
		{
			name: "data from previous release patch",
			version: roachpb.Version{
				Major: 22,
				Minor: 2,
				Patch: 7,
			},
			minSupported: roachpb.Version{
				Major: 22,
				Minor: 2,
			},
			current: roachpb.Version{
				Major: 23,
				Minor: 1,
				Patch: 2,
			},
		},
	} {
		t.Run(d.name, func(t *testing.T) {
			err := checkDataVersionIsAllowed(d.version, d.minSupported, d.current)
			require.NoError(t, err, "version check must pass")
		})
	}
}
