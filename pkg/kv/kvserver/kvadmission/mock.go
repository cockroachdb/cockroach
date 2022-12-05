package kvadmission

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/pebble"
)

type mockKVAdmissionController struct{}

var _ Controller = &mockKVAdmissionController{}

func TestingMockKVAdmissionController() Controller {
	return &mockKVAdmissionController{}
}

func (m *mockKVAdmissionController) AdmitKVWork(
	context.Context, roachpb.TenantID, *roachpb.BatchRequest,
) (Handle, error) {
	return Handle{}, nil
}

func (m *mockKVAdmissionController) AdmittedKVWorkDone(Handle, *StoreWriteBytes) {
}

func (m *mockKVAdmissionController) AdmitRangefeedRequest(
	roachpb.TenantID, *roachpb.RangeFeedRequest,
) *admission.Pacer {
	return nil
}

func (m *mockKVAdmissionController) SetTenantWeightProvider(TenantWeightProvider, *stop.Stopper) {
}

func (m *mockKVAdmissionController) SnapshotIngested(roachpb.StoreID, pebble.IngestOperationStats) {
}

func (m *mockKVAdmissionController) FollowerStoreWriteBytes(
	roachpb.StoreID, FollowerStoreWriteBytes,
) {
}

func (m *mockKVAdmissionController) ReturnWriteBurstQuotaLocally(
	context.Context, *kvserverpb.RaftMessageRequest,
) {
}

func (m *mockKVAdmissionController) BelowRaftAdmission(
	context.Context,
	roachpb.TenantID,
	roachpb.NodeID,
	roachpb.StoreID,
	kvserverpb.RaftAdmissionMeta,
	int64,
	bool,
	admission.TermIndexTuple,
	roachpb.RangeID,
) {
}

func (m *mockKVAdmissionController) GetQuotaToReturnRemotely(
	context.Context, roachpb.NodeID,
) (
	_ map[admission.RangeStoreTuple]map[admissionpb.WorkPriority]admission.TermIndexTuple,
	ok bool,
) {
	return nil, false
}

func (m *mockKVAdmissionController) TrackQuotaToReturn(
	context.Context,
	admission.RangeStoreTuple,
	admissionpb.WorkPriority,
	roachpb.NodeID,
	admission.TermIndexTuple,
) {
}
