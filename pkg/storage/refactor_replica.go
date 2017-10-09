package storage

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Store .
type Store struct{}

// Replica .
type Replica struct{}

// This is an exploration of the "life of a request" from `(*Store).Send` onward
// with the goal of identifying the different components of the request
// lifecycle ("stages").
//
// What's hopefully clear from the exposition below is that the process of
// obtaining a BatchResponse (or an error) from a BatchRequest is
// straightforward. Yet, it is not a trivial exercise to "grok" this path from
// our current codebase. Additionally, we're not doing a good job separating the
// general tasks associated to this, which are (for writes):
//
// 1. "Replica frontend": query and propose to the:
// 2. "Replica backend": the actual state machine.
//
// No attempt has been made here to untangle 2). That's much more complicated
// since a lot of the logic is actually in Store, and there are various
// callbacks between Store and Replica. But the frontend 1) seems fairly
// straightforward.
//
// The below is pseudocode. In particular, these various "stages" wouldn't be
// interchangeable in practice, and none of the types make sense. The idea is
// that I wanted to avoid having to nest six levels deep but to basically have
// this call structure:
//
// ```
// func Stage1(data Data) {
//   nextData := Preprocess(data)
//   prevReturn := Stage2(nextData)
//   return Postprocess(prevReturn)
// }
// ```
//
// but again, consider that a presentational device only.

// Send .
func (s *Store) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	return Nest(
		AnnotateCtxStage,        // augment ctx
		SetActiveTimestampStage, // set ba.Timestamp
		HLCStage,                // hlc.Update
		ObservedTimestampStage,  // adjust ba.MaxTimestamp, on return populate br.Txn.MaxOffsets
		// Retrying as necessary, handle WriteIntentError etc from the wrapped ReplicaSendStage.
		// Internally concerns the intent resolver, the pushTxnQueue, and others. Deserves
		// an "internal refactor" as well.
		ConflictResolutionStage,
		ReplicaSendStage, // *Replica.Send
	)(ctx, ba)
}

// Send .
func (r *Replica) Send(ctx context.Context, ba roachpb.BatchRequest) {
	Nest(
		AnnotateCtxStage,
		DispatchStage, // delegate to one of the HandleX below
	)
}

// HandleReadOnlyRequest .
func (r *Replica) HandleReadOnlyRequest(Input) Output {
	return Nest(
		LeaseStage,          // verify and/or acquire lease
		CommandQueueStage,   // guard the stages below in CommandQueue
		TimestampCacheStage, // bump write timestamp, populate tsCache on return
		ReadLockStage,       // hold readOnlyCmdMu over EvalStage
		EvalStage,           // evaluate the batch, creating a proposal
		ResolveIntentsStage, // hand found intents to intentResolver
	)
}

// HandleAdminRequest .
func (r *Replica) HandleAdminRequest(Input) Output {
	return Nest(
		LeaseStage,
		EvalStage,
	)
}

// HandleWriteBatch .
func (r *Replica) HandleWriteBatch(Input) Output {
	return Nest(
		AmbiguityStage, // try to terminate AmbiguousCommitError, if possible
		LeaseStage,     // NB: current code has this in CommandQueueStage (bad idea?!)
		CommandQueueStage,
		TimestampCacheStage,
		EvalStage,
		QuotaStage,          // acquire and release proposal quota
		LeaseIndexStage,     // assign new LeaseAppliedIndex while proposal applies at past index
		InflightStage,       // put proposal into pendingCmds, put it into Raft, wait for result
		ResolveIntentsStage, // hand committed/aborted intents to intentResolver
	)
}
