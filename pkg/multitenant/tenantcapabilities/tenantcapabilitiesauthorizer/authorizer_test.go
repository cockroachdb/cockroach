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
//	update-state
//	upsert {ten=10}:{CanAdminSplit=true}
//	delete {ten=15}
//	----
//
// "has-capability-for-batch": performs a capability check, given a tenant and
// batch request declaration. Example:
//
//	has-capability-for-batch
//	{ten=10}
//	split
//	----
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		mockReader := mockReader(make(map[roachpb.TenantID]tenantcapabilitiespb.TenantCapabilities))
		authorizer := New()
		authorizer.BindReader(context.Background(), mockReader)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "update-state":
				updates := tenantcapabilitiestestutils.ParseTenantCapabilityUpdateStateArguments(t, d.Input)
				mockReader.updateState(updates)

			case "has-capability-for-batch":
				tenID, ba := tenantcapabilitiestestutils.ParseBatchRequestString(t, d.Input)
				err := authorizer.HasCapabilityForBatch(context.Background(), tenID, &ba)
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

func (m mockReader) updateState(updates []tenantcapabilities.Update) {
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
