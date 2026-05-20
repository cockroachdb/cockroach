// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkmerge"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCheckRequiredInstancesAvailable verifies that the helper detects
// when the available instance set is missing one or more SQL instances
// referenced by an input SST URI.
func TestCheckRequiredInstancesAvailable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name                string
		ssts                []execinfrapb.BulkMergeSpec_SST
		availableInstances  []base.SQLInstanceID
		expectedUnavailable []base.SQLInstanceID
	}{
		{
			name:               "empty ssts returns nil",
			ssts:               nil,
			availableInstances: nil,
		},
		{
			name: "plan covers required instance",
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{URI: "nodelocal://1/job/123/map/file1.sst"},
			},
			availableInstances: []base.SQLInstanceID{1, 2},
		},
		{
			name: "plan missing one required instance",
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{URI: "nodelocal://2/job/123/map/file1.sst"},
			},
			availableInstances:  []base.SQLInstanceID{1, 3},
			expectedUnavailable: []base.SQLInstanceID{2},
		},
		{
			name: "plan missing multiple required instances",
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{URI: "nodelocal://2/job/123/map/file1.sst"},
				{URI: "nodelocal://4/job/123/map/file2.sst"},
				{URI: "nodelocal://1/job/123/map/file3.sst"},
			},
			availableInstances:  []base.SQLInstanceID{1},
			expectedUnavailable: []base.SQLInstanceID{2, 4},
		},
		{
			name: "non-nodelocal uris ignored",
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{URI: "s3://bucket/file1.sst"},
				{URI: "gs://bucket/file2.sst"},
				{URI: ""},
			},
			availableInstances: nil,
		},
		{
			name: "instance 0 (self) ignored",
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{URI: "nodelocal://0/job/123/map/file1.sst"},
			},
			availableInstances: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := bulkmerge.CheckRequiredInstancesAvailable(tc.ssts, tc.availableInstances)
			if len(tc.expectedUnavailable) == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var unavailErr *bulkmerge.InstanceUnavailableError
			require.True(t, errors.As(err, &unavailErr),
				"expected InstanceUnavailableError, got: %v", err)
			require.ElementsMatch(t, tc.expectedUnavailable, unavailErr.UnavailableInstances)
			require.True(t, jobs.IsPermanentJobError(err),
				"InstanceUnavailableError must be a permanent job error")
		})
	}
}
