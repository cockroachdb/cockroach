// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package telemetryccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1min under race")
	skip.UnderDeadlock(t, "takes >1min under deadlock")

	sqltestutils.TelemetryTest(
		t,
		[]base.TestServerArgs{
			{
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "region", Value: "us-east-1"}},
				},
			},
			{
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "region", Value: "ca-central-1"}},
				},
			},
			{
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "region", Value: "ap-southeast-2"}},
				},
			},
		},
		false, /* testTenant */
	)
}
