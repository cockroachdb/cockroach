package kvqueue

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TestingKnobs represents testing knobs for KV queues.
type TestingKnobs struct {
	// DisableLastProcessedCheck disables checking on replica queue last processed times.
	DisableLastProcessedCheck bool
	// SplitQueuePurgatoryChan allows a test to control the channel used to
	// trigger split queue purgatory processing.
	SplitQueuePurgatoryChan <-chan time.Time
	// RaftSnapshotQueueSkipReplica causes the raft snapshot queue to skip sending
	// a snapshot to a follower replica.
	RaftSnapshotQueueSkipReplica func() bool
	// ConsistencyQueueResultHook is an interceptor for the result of consistency
	// queue operations.
	ConsistencyQueueResultHook func(response roachpb.CheckConsistencyResponse)
	// DisableReplicaRebalancing disables rebalancing of replicas but otherwise
	// leaves the replicate queue operational.
	DisableReplicaRebalancing bool
}
