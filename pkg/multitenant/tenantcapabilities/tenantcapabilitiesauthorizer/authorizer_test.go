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

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiestestutils"
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
		authorizer := New()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "update-state":
				updates := tenantcapabilitiestestutils.ParseTenantCapabilityUpdateStateArguments(t, d.Input)
				err := authorizer.Apply(updates)
				if err != nil {
					return fmt.Sprintf("error: %v", err.Error())
				}

			case "has-capability-for-batch":
				tenID, ba := tenantcapabilitiestestutils.ParseBatchRequestString(t, d.Input)
				hasCapability := authorizer.HasCapabilityForBatch(context.Background(), tenID, &ba)
				return fmt.Sprintf("%t", hasCapability)

			default:
				return fmt.Sprintf("unknown command %s", d.Cmd)
			}
			return "ok"
		})
	})
}
