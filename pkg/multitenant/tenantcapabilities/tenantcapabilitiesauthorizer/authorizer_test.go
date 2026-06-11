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
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiestestutils"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
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
	checkCap := func(t *testing.T, capID tenantcapabilitiespb.ID) {
		if capID >= tenantcapabilitiespb.MaxCapabilityID {
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
		capIDs := []tenantcapabilitiespb.ID{
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

// TestEndTxnWithCommitTriggerRequiresSystemTenant is a regression test for
// VULM-477. A non-prepare EndTxn carries no capability requirement, but an
// EndTxn may also carry an InternalCommitTrigger. Commit triggers drive
// privileged range operations (split/merge, replica changes, sticky bit
// modifications, node-liveness gossip) and are intended for internal use only.
// A secondary tenant that attaches one to an otherwise-ordinary EndTxn must not
// have it honored: the trigger executes in batcheval with little to no
// validation of its contents against the tenant's keyspace, so allowing it lets
// a tenant escape its capability set.
//
// Each batch mirrors the exploit: a write to anchor the transaction followed by
// a committing EndTxn carrying a trigger. The authorization layer must reject it
// for a secondary tenant regardless of the trigger's contents.
func TestEndTxnWithCommitTriggerRequiresSystemTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	clusterSettings := cluster.MakeTestingClusterSettings()
	reader := mockReader(make(map[roachpb.TenantID]*tenantcapabilities.Entry))
	authorizer := New(clusterSettings, nil /* knobs */)
	authorizer.BindReader(reader)

	tenID := roachpb.MustMakeTenantID(10)
	reader.updateState([]*tenantcapabilities.Update{
		{Entry: tenantcapabilities.Entry{
			TenantID:    tenID,
			ServiceMode: mtinfopb.ServiceModeExternal,
		}},
	})

	batchWithTrigger := func(ct *roachpb.InternalCommitTrigger) *kvpb.BatchRequest {
		ba := &kvpb.BatchRequest{}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})
		ba.Add(&kvpb.EndTxnRequest{Commit: true, InternalCommitTrigger: ct})
		return ba
	}

	// Every kind of commit trigger must be rejected for a secondary tenant. The
	// "empty" case matters most: the authorizer keys off InternalCommitTrigger !=
	// nil, not off any populated sub-trigger, and an empty trigger that reached
	// batcheval would crash the node (RunCommitTrigger fatals on an unrecognized
	// trigger). It is also the exact boundary a future refactor could regress.
	triggers := []struct {
		name    string
		trigger *roachpb.InternalCommitTrigger
	}{
		{"empty", &roachpb.InternalCommitTrigger{}},
		{"sticky-bit", &roachpb.InternalCommitTrigger{StickyBitTrigger: &roachpb.StickyBitTrigger{}}},
		{"split", &roachpb.InternalCommitTrigger{SplitTrigger: &roachpb.SplitTrigger{}}},
		{"merge", &roachpb.InternalCommitTrigger{MergeTrigger: &roachpb.MergeTrigger{}}},
		{"change-replicas", &roachpb.InternalCommitTrigger{ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{}}},
		{"modified-span", &roachpb.InternalCommitTrigger{ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{}}},
	}
	for _, tc := range triggers {
		t.Run(tc.name, func(t *testing.T) {
			ba := batchWithTrigger(tc.trigger)
			// The system tenant may always use commit triggers.
			require.NoError(t, authorizer.HasCapabilityForBatch(ctx, roachpb.SystemTenantID, ba))
			// A secondary tenant must be denied.
			require.Error(t, authorizer.HasCapabilityForBatch(ctx, tenID, ba),
				"secondary tenant was allowed to attach a commit trigger")
		})
	}

	// A committing EndTxn without a trigger must remain allowed for a secondary
	// tenant: the fix must not over-reject ordinary commits.
	t.Run("no-trigger-allowed", func(t *testing.T) {
		ba := &kvpb.BatchRequest{}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})
		require.NoError(t, authorizer.HasCapabilityForBatch(ctx, tenID, ba))
	})

	// The rejection is mode-independent. In particular it must hold under
	// allow-all, which otherwise waves through every capability check, and under
	// the pre-v23.1 (v222) mode that getMode selects transiently during tenant
	// startup before the capability reader is populated.
	for _, mode := range []struct {
		name string
		mode authorizerModeType
	}{
		{"on", authorizerModeOn},
		{"allow-all", authorizerModeAllowAll},
		{"v222", authorizerModeV222},
	} {
		t.Run("mode="+mode.name, func(t *testing.T) {
			authorizerMode.Override(ctx, &clusterSettings.SV, mode.mode)
			defer authorizerMode.Override(ctx, &clusterSettings.SV, authorizerModeOn)
			ba := batchWithTrigger(&roachpb.InternalCommitTrigger{
				ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{},
			})
			require.Errorf(t, authorizer.HasCapabilityForBatch(ctx, tenID, ba),
				"secondary tenant trigger allowed under %s mode", mode.name)
		})
	}
}
