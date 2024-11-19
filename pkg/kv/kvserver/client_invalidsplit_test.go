// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSplitAtInvalidTenantPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// badKey is the tenant prefix followed by a "large" byte that indicates
	// that it should be followed by a separate uvarint encoded key (which is
	// not there).
	//
	// See: https://github.com/cockroachdb/cockroach/issues/104796
	var badKey = append([]byte{'\xfe'}, '\xfd')
	_, _, err := keys.DecodeTenantPrefix(badKey)
	t.Log(err)
	require.Error(t, err)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	_, _, err = tc.SplitRange(badKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), `checking for valid tenantID`)
}
