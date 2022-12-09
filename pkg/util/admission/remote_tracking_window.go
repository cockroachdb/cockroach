package admission

import (
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// CLIENT SIDE
// AdmitKVWork()
// - calls RemoteGrantCoordinator.WaitForCapacity

// AdmittedKVWorkDone()
// if absorbed_handle is non-null
// - calls DeductCapacity and TrackOversubscribed

// BatchAbsorbedRequest() received
// - RestoreCapacity(TrackOversubscribed.Absorbed(UUID))

// RemoteGrantCoordinator (what you called IOGrantCoordinator) has an internal capacity. There are a map of these
// map[storeID]RemoteGrantCoordinator.
type RemoteGrantCoordinator interface {
	// WaitForCapacity is called pre-admission request. Blocks
	// if there is no capacity left, or returns immediately if there is. If it is
	// blocked and later receives capacity (through RestoreCapacity). It will
	// choose fairly between tenants and then in priority order between requests
	WaitForCapacity(info tenantInfo, priority admissionpb.WorkPriority) bool

	// DeductCapacity is called after BatchResponse is received if the absorption handle is non-null.
	// calls TrackOversubscribed.Waiting in addition to
	DeductCapacity(info tenantInfo, count int64)

	// RestoreCapacity is called once an AbsorptionResponse is received
	RestoreCapacity(info tenantInfo, count int64)
}

// TrackOversubscribed is basically a map[UUID](tenantInfo, int64)
type TrackOversubscribed interface {
	Waiting(id uuid.UUID, info tenantInfo, count int64)
	Absorbed(id uuid.UUID) (tenantInfo, int64)
}

// SERVER SIDE

// In the appropriate cmd_* methods
// - call work_queue.TryAdmit()
// -   if success, sets absorbed = null
// -   if fails, calls AdmitWithCallback
//      - once Admit returns, the callback sends a BatchAbsorbed response

/*
type WorkQueue interface {
	// AdmitWithCallback is basically the same as Admit but never blocks, instead
	// it returns a bool whether blocking would occur and if it does block calls
	// the callback once it is completed.
	AdmitWithCallback(info WorkInfo, callback func()) bool
}
*/
