// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitiesauthorizer

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiestestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDriven runs datadriven tests against the Authorizer interface. The
// syntax is as follows:
//
// "update-state": updates the underlying global tenant capability state.
// Example:
//
// upsert ten=10 can_admin_split=true
// ----
// ok
//
// delete ten=15
// ----
// ok
//
// "has-capability-for-batch": performs a capability check, given a tenant and
// batch request declaration. Example:
//
// has-capability-for-batch ten=10 cmds=(split)
// ----
// ok
//
// "has-node-status-capability": performas a capability check to be able to
// retrieve node status metadata. Example:
//
// has-node-status-capability ten=11
// ----
// ok
//
// "has-tsdb-query-capability": performas a capability check to be able to
// make TSDB queries. Example:
//
// has-tsdb-query-capability ten=11
// ----
// ok
//
// "set-bool-cluster-setting": overrides the specified boolean cluster setting
// to the given value. Currently, only the authorizerEnabled cluster setting is
// supported.
//
// set-bool-cluster-setting name=tenant_capabilities.authorizer.enabled value=false
// ----
// ok
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		clusterSettings := cluster.MakeTestingClusterSettings()
		ctx := context.Background()
		mockReader := mockReader(make(map[roachpb.TenantID]*tenantcapabilities.Entry))
		authorizer := New(clusterSettings, nil /* TestingKnobs */)
		authorizer.BindReader(mockReader)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var tenID roachpb.TenantID
			if d.HasArg("ten") {
				tenID = tenantcapabilitiestestutils.GetTenantID(t, d)
			}
			switch d.Cmd {
			case "upsert":
				entry, err := tenantcapabilitiestestutils.ParseTenantCapabilityUpsert(t, d)
				if err != nil {
					return err.Error()
				}
				mockReader.updateState([]*tenantcapabilities.Update{
					{Entry: entry},
				})
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
			case "has-tsdb-all-capability":
				err := authorizer.HasTSDBAllMetricsCapability(context.Background(), tenID)
				if err == nil {
					return "ok"
				}
				return err.Error()
			case "set-authorizer-mode":
				var valStr string
				d.ScanArgs(t, "value", &valStr)
				val, ok := authorizerMode.ParseEnum(valStr)
				if !ok {
					t.Fatalf("unknown authorizer mode %s", valStr)
				}
				authorizerMode.Override(ctx, &clusterSettings.SV, authorizerModeType(val))
			case "is-exempt-from-rate-limiting":
				return fmt.Sprintf("%t", authorizer.IsExemptFromRateLimiting(context.Background(), tenID))
			default:
				return fmt.Sprintf("unknown command %s", d.Cmd)
			}
			return "ok"
		})
	})
}

type mockReader map[roachpb.TenantID]*tenantcapabilities.Entry

var _ tenantcapabilities.Reader = mockReader{}

func (m mockReader) updateState(updates []*tenantcapabilities.Update) {
	for _, update := range updates {
		if update.Deleted {
			delete(m, update.TenantID)
		} else {
			m[update.TenantID] = &update.Entry
		}
	}
}

var unused = make(<-chan struct{})

// GetInfo implements the tenantcapabilities.Reader interface.
func (m mockReader) GetInfo(id roachpb.TenantID) (tenantcapabilities.Entry, <-chan struct{}, bool) {
	entry, found := m[id]
	if found {
		return *entry, unused, found
	}
	return tenantcapabilities.Entry{}, unused, found
}

// GetCapabilities implements the tenantcapabilities.Reader interface.
func (m mockReader) GetCapabilities(
	id roachpb.TenantID,
) (*tenantcapabilitiespb.TenantCapabilities, bool) {
	entry, found := m[id]
	return entry.TenantCapabilities, found
}

// GetGlobalCapabilityState implements the tenantcapabilities.Reader interface.
func (m mockReader) GetGlobalCapabilityState() map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities {
	ret := make(map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities, len(m))
	for id, entry := range m {
		ret[id] = entry.TenantCapabilities
	}
	return ret
}

func TestAllBatchCapsAreBoolean(t *testing.T) {
	checkCap := func(t *testing.T, capID tenantcapabilities.ID) {
		if capID >= tenantcapabilities.MaxCapabilityID {
			// One of the special values.
			return
		}
		caps := &tenantcapabilitiespb.TenantCapabilities{}
		var v *tenantcapabilities.BoolValue
		require.Implements(t, v, tenantcapabilities.MustGetValueByID(caps, capID))
	}

	for m, mc := range reqMethodToCap {
		if mc.capFn != nil {
			switch m {
			case kvpb.EndTxn:
				// Handled below.
			default:
				t.Fatalf("unexpected capability function for %s", m)
			}
		} else {
			checkCap(t, mc.capID)
		}
	}

	{
		const method = kvpb.EndTxn
		mc := reqMethodToCap[method]
		capIDs := []tenantcapabilities.ID{
			mc.get(&kvpb.EndTxnRequest{}),
			mc.get(&kvpb.EndTxnRequest{Prepare: true}),
		}
		for _, capID := range capIDs {
			checkCap(t, capID)
		}
	}
}

func TestAllBatchRequestTypesHaveAssociatedCaps(t *testing.T) {
	for req := kvpb.Method(0); req < kvpb.NumMethods; req++ {
		_, ok := reqMethodToCap[req]
		if !ok {
			t.Errorf("no capability associated with request type %s", req)
		}
	}
}
