package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortcache"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	opentracing "github.com/opentracing/opentracing-go"
)

// ReplicaEvalContextI .
type ReplicaEvalContextI interface {
	ClusterSettings() *cluster.Settings
	TxnSpanGCThreshold() (hlc.Timestamp, error)
	GCThreshold() (hlc.Timestamp, error)
	Desc() (*roachpb.RangeDescriptor, error)
	//StoreTestingKnobs() StoreTestingKnobs
	ContainsKey(roachpb.Key) (bool, error)
	AbortCache() *abortcache.Instance
	RangeID() roachpb.RangeID
	FirstIndex() (uint64, error)
	Term(uint64) (uint64, error)
	GetLease() (roachpb.Lease, *roachpb.Lease, error)
	Tracer() opentracing.Tracer
	GetMVCCStats() (enginepb.MVCCStats, error)
	GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error)
	Engine() engine.Engine
	IsFirstRange() bool

	// Used from storageccl
	NodeID() roachpb.NodeID
	DB() *client.DB

	MakeReplicaStateLoader() stateloader.Instance
	//pushTxnQueue() *pushTxnQueue
}

// CommandArgs contains all the arguments to a command.
// TODO(bdarnell): consider merging with storagebase.FilterArgs (which
// would probably require removing the EvalCtx field due to import order
// constraints).
type CommandArgs struct {
	EvalCtx ReplicaEvalContextI
	Header  roachpb.Header
	Args    roachpb.Request

	// If MaxKeys is non-zero, span requests should limit themselves to
	// that many keys. Commands using this feature should also set
	// NumKeys and ResumeSpan in their responses.
	MaxKeys int64

	// *Stats should be mutated to reflect any writes made by the command.
	Stats *enginepb.MVCCStats
}

// IntentsWithArg groups a slice of intents with the request which encountered them.
type IntentsWithArg struct {
	Args    roachpb.Request
	Intents []roachpb.Intent
}
