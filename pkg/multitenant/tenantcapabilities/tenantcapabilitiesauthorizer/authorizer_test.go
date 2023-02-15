// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiesauthorizer

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiestestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// TestDataDriven runs datadriven tests against the Authorizer interface. The
// syntax is as follows:
//
// "update-state": updates the underlying global tenant capability state.
// Example:
//
//		upsert ten=10 can_admin_split=true
//	 ----
//	 ok
//
//		delete ten=15
//		----
//	 ok
//
// "has-capability-for-batch": performs a capability check, given a tenant and
// batch request declaration. Example:
//
//	has-capability-for-batch ten=10 cmds=(split)
//	----
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		mockReader := mockReader(make(map[roachpb.TenantID]tenantcapabilitiespb.TenantCapabilities))
		authorizer := New(cluster.MakeTestingClusterSettings(), nil /* TestingKnobs */)
		authorizer.BindReader(mockReader)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tenID := tenantcapabilitiestestutils.GetTenantID(t, d)
			switch d.Cmd {
			case "upsert":
				update, err := tenantcapabilitiestestutils.ParseTenantCapabilityUpsert(t, d)
				if err != nil {
					return err.Error()
				}
				mockReader.updateState([]*tenantcapabilities.Update{update})
			case "delete":
				update := tenantcapabilitiestestutils.ParseTenantCapabilityDelete(t, d)
				mockReader.updateState([]*tenantcapabilities.Update{update})
			case "has-capability-for-batch":
				ba := tenantcapabilitiestestutils.ParseBatchRequests(t, d)
				err := authorizer.HasCapabilityForBatch(context.Background(), tenID, &ba)
				if err == nil {
					return "ok"
				}
				return err.Error()
			case "has-node-status-capability":
				err := authorizer.HasNodeStatusCapability(context.Background(), tenID)
				if err == nil {
					return "ok"
				}
				return err.Error()
			case "has-tsdb-query-capability":
				err := authorizer.HasTSDBQueryCapability(context.Background(), tenID)
				if err == nil {
					return "ok"
				}
				return err.Error()
			default:
				return fmt.Sprintf("unknown command %s", d.Cmd)
			}
			return "ok"
		})
	})
}

type mockReader map[roachpb.TenantID]tenantcapabilitiespb.TenantCapabilities

func (m mockReader) updateState(updates []*tenantcapabilities.Update) {
	for _, update := range updates {
		if update.Deleted {
			delete(m, update.TenantID)
		} else {
			m[update.TenantID] = update.TenantCapabilities
		}
	}
}

// GetCapabilities implements the tenantcapabilities.Reader interface.
func (m mockReader) GetCapabilities(
	id roachpb.TenantID,
) (tenantcapabilitiespb.TenantCapabilities, bool) {
	cp, found := m[id]
	return cp, found
}
