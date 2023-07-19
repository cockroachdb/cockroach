// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_api_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestHealthAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)
	ts := s.(*server.TestServer)

	// We need to retry because the node ID isn't set until after
	// bootstrapping.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.HealthResponse
		return srvtestutils.GetAdminJSONProto(s, "health", &resp)
	})

	// Make the SQL listener appear unavailable. Verify that health fails after that.
	ts.TestingSetReady(false)
	var resp serverpb.HealthResponse
	err := srvtestutils.GetAdminJSONProto(s, "health?ready=1", &resp)
	if err == nil {
		t.Error("server appears ready even though SQL listener is not")
	}
	ts.TestingSetReady(true)
	err = srvtestutils.GetAdminJSONProto(s, "health?ready=1", &resp)
	if err != nil {
		t.Errorf("server not ready after SQL listener is ready again: %v", err)
	}

	// Expire this node's liveness record by pausing heartbeats and advancing the
	// server's clock.
	nl := ts.NodeLiveness().(*liveness.NodeLiveness)
	defer nl.PauseAllHeartbeatsForTest()()
	self, ok := nl.Self()
	assert.True(t, ok)
	s.Clock().Update(self.Expiration.ToTimestamp().Add(1, 0).UnsafeToClockTimestamp())

	testutils.SucceedsSoon(t, func() error {
		err := srvtestutils.GetAdminJSONProto(s, "health?ready=1", &resp)
		if err == nil {
			return errors.New("health OK, still waiting for unhealth")
		}

		t.Logf("observed error: %v", err)
		if !testutils.IsError(err, `(?s)503 Service Unavailable.*"error": "node is not healthy"`) {
			return err
		}
		return nil
	})

	// After the node reports an error with `?ready=1`, the health
	// endpoint must still succeed without error when `?ready=1` is not specified.
	if err := srvtestutils.GetAdminJSONProto(s, "health", &resp); err != nil {
		t.Fatal(err)
	}
}

func TestLivenessAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	startTime := tc.Server(0).Clock().PhysicalNow()

	// We need to retry because the gossiping of liveness status is an
	// asynchronous process.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.LivenessResponse
		if err := serverutils.GetJSONProto(tc.Server(0), "/_admin/v1/liveness", &resp); err != nil {
			return err
		}
		if a, e := len(resp.Livenesses), tc.NumServers(); a != e {
			return errors.Errorf("found %d liveness records, wanted %d", a, e)
		}
		livenessMap := make(map[roachpb.NodeID]livenesspb.Liveness)
		for _, l := range resp.Livenesses {
			livenessMap[l.NodeID] = l
		}
		for i := 0; i < tc.NumServers(); i++ {
			s := tc.Server(i)
			sl, ok := livenessMap[s.NodeID()]
			if !ok {
				return errors.Errorf("found no liveness record for node %d", s.NodeID())
			}
			if sl.Expiration.WallTime < startTime {
				return errors.Errorf(
					"expected node %d liveness to expire in future (after %d), expiration was %d",
					s.NodeID(),
					startTime,
					sl.Expiration,
				)
			}
			status, ok := resp.Statuses[s.NodeID()]
			if !ok {
				return errors.Errorf("found no liveness status for node %d", s.NodeID())
			}
			if a, e := status, livenesspb.NodeLivenessStatus_LIVE; a != e {
				return errors.Errorf(
					"liveness status for node %s was %s, wanted %s", s.NodeID(), a, e,
				)
			}
		}
		return nil
	})
}
