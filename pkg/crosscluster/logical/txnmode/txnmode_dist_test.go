// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuildTxnReplicationPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name               string
		gatewayID          base.SQLInstanceID
		applierInstanceIDs []base.SQLInstanceID
	}{
		{
			name:               "single node",
			gatewayID:          1,
			applierInstanceIDs: []base.SQLInstanceID{1},
		},
		{
			name:               "three nodes",
			gatewayID:          1,
			applierInstanceIDs: []base.SQLInstanceID{1, 2, 3},
		},
		{
			name:               "five nodes gateway not in appliers",
			gatewayID:          1,
			applierInstanceIDs: []base.SQLInstanceID{2, 3, 4, 5, 6},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			n := len(tc.applierInstanceIDs)

			infra := physicalplan.NewPhysicalInfrastructure(
				uuid.MakeV4(), tc.gatewayID,
			)
			plan := &sql.PhysicalPlan{
				PhysicalPlan: physicalplan.MakePhysicalPlan(infra),
			}

			spec := execinfrapb.TxnLDRCoordinatorSpec{
				JobID: 123,
			}

			err := buildTxnReplicationPlan(ctx, plan, tc.applierInstanceIDs, spec)
			require.NoError(t, err)

			// Count processors by type.
			var coordinators, appliers, depResolvers, noops int
			for _, p := range plan.Processors {
				switch {
				case p.Spec.Core.TxnLdrCoordinator != nil:
					coordinators++
				case p.Spec.Core.TxnLdrApplier != nil:
					appliers++
				case p.Spec.Core.TxnLdrDepResolver != nil:
					depResolvers++
				case p.Spec.Core.Noop != nil:
					noops++
				}
			}

			require.Equal(t, 1, coordinators, "expected 1 coordinator")
			require.Equal(t, n, appliers, "expected %d appliers", n)
			require.Equal(t, n, depResolvers, "expected %d dep resolvers", n)
			require.Equal(t, 1, noops, "expected 1 noop")
			require.Len(t, plan.Processors, 2*n+2, "total processors")

			// Coordinator and noop are on the gateway.
			for _, p := range plan.Processors {
				if p.Spec.Core.TxnLdrCoordinator != nil {
					require.Equal(t, tc.gatewayID, p.SQLInstanceID,
						"coordinator should be on gateway")
				}
				if p.Spec.Core.Noop != nil {
					require.Equal(t, tc.gatewayID, p.SQLInstanceID,
						"noop should be on gateway")
				}
			}

			// Applier and dep resolver co-location: for each instance,
			// there must be exactly one applier and one dep resolver.
			applierInstances := make([]base.SQLInstanceID, 0, n)
			depResolverInstances := make([]base.SQLInstanceID, 0, n)
			for _, p := range plan.Processors {
				if p.Spec.Core.TxnLdrApplier != nil {
					applierInstances = append(applierInstances, p.SQLInstanceID)
					require.Equal(t, int32(p.SQLInstanceID),
						p.Spec.Core.TxnLdrApplier.ApplierID,
						"applier ID must match instance ID")
				}
				if p.Spec.Core.TxnLdrDepResolver != nil {
					depResolverInstances = append(depResolverInstances, p.SQLInstanceID)
					require.Equal(t, int32(p.SQLInstanceID),
						p.Spec.Core.TxnLdrDepResolver.ApplierID,
						"dep resolver applier ID must match instance ID")
				}
			}

			sort.Slice(applierInstances, func(i, j int) bool {
				return applierInstances[i] < applierInstances[j]
			})
			sort.Slice(depResolverInstances, func(i, j int) bool {
				return depResolverInstances[i] < depResolverInstances[j]
			})
			require.Equal(t, applierInstances, depResolverInstances,
				"applier and dep resolver instances must match")

			// Stream count: N (coord→applier) + N² (applier→dep resolver)
			// + N (dep resolver→noop, added by AddSingleGroupStage).
			expectedStreams := n + n*n + n
			require.Len(t, plan.Streams, expectedStreams,
				"expected %d streams", expectedStreams)
		})
	}
}
