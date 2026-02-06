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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCheckRequiredInstancesAvailable tests the instance availability
// validation logic used before starting a distributed merge.
func TestCheckRequiredInstancesAvailable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("empty sst list returns nil", func(t *testing.T) {
		err := bulkmerge.CheckRequiredInstancesAvailable(nil, nil)
		require.NoError(t, err)
	})

	t.Run("all instances available returns nil", func(t *testing.T) {
		// Create SSTs with URIs for instance 1.
		ssts := []execinfrapb.BulkMergeSpec_SST{
			{URI: "nodelocal://1/job/123/map/file1.sst"},
			{URI: "nodelocal://1/job/123/map/file2.sst"},
		}

		// Create available instances list including instance 1.
		availableInstances := []sqlinstance.InstanceInfo{
			{InstanceID: base.SQLInstanceID(1), SessionID: sqlliveness.SessionID("session1")},
			{InstanceID: base.SQLInstanceID(2), SessionID: sqlliveness.SessionID("session2")},
		}

		err := bulkmerge.CheckRequiredInstancesAvailable(ssts, availableInstances)
		require.NoError(t, err)
	})

	t.Run("missing instance returns InstanceUnavailableError", func(t *testing.T) {
		// Create SSTs with URIs for instance 2 (which is not available).
		ssts := []execinfrapb.BulkMergeSpec_SST{
			{URI: "nodelocal://2/job/123/map/file1.sst"},
		}

		// Create available instances list NOT including instance 2.
		availableInstances := []sqlinstance.InstanceInfo{
			{InstanceID: base.SQLInstanceID(1), SessionID: sqlliveness.SessionID("session1")},
		}

		err := bulkmerge.CheckRequiredInstancesAvailable(ssts, availableInstances)
		require.Error(t, err)

		var unavailErr *bulkmerge.InstanceUnavailableError
		require.True(t, errors.As(err, &unavailErr), "expected InstanceUnavailableError, got: %v", err)
		require.Contains(t, err.Error(), "which are unavailable")
		require.Equal(t, []base.SQLInstanceID{2}, unavailErr.UnavailableInstances)

		// The error should be marked as a permanent job error so the job
		// registry does not retry after the internal backoff loop has been
		// exhausted.
		require.True(t, jobs.IsPermanentJobError(err),
			"InstanceUnavailableError should be a permanent job error")
	})

	t.Run("non-nodelocal URIs are ignored", func(t *testing.T) {
		// Create SSTs with non-nodelocal URIs.
		ssts := []execinfrapb.BulkMergeSpec_SST{
			{URI: "s3://bucket/job/123/map/file1.sst"},
			{URI: "gs://bucket/job/123/map/file2.sst"},
			{URI: ""}, // empty URI
		}

		// No instances available, but it should not matter.
		availableInstances := []sqlinstance.InstanceInfo{}

		err := bulkmerge.CheckRequiredInstancesAvailable(ssts, availableInstances)
		require.NoError(t, err)
	})

	t.Run("instance 0 (self) is ignored", func(t *testing.T) {
		// Create SSTs with instance 0 (self) URI.
		ssts := []execinfrapb.BulkMergeSpec_SST{
			{URI: "nodelocal://0/job/123/map/file1.sst"},
		}

		// No instances in list.
		availableInstances := []sqlinstance.InstanceInfo{}

		err := bulkmerge.CheckRequiredInstancesAvailable(ssts, availableInstances)
		require.NoError(t, err)
	})
}
