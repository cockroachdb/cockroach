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
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestStatusGossipJson ensures that the output response for the full gossip
// info contains the required fields.
func TestStatusGossipJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	var data gossip.InfoStatus
	if err := srvtestutils.GetStatusJSONProto(s, "gossip/local", &data); err != nil {
		t.Fatal(err)
	}
	if _, ok := data.Infos["first-range"]; !ok {
		t.Errorf("no first-range info returned: %v", data)
	}
	if _, ok := data.Infos["cluster-id"]; !ok {
		t.Errorf("no clusterID info returned: %v", data)
	}
	if _, ok := data.Infos["node:1"]; !ok {
		t.Errorf("no node 1 info returned: %v", data)
	}
}
