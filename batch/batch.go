package batch

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"golang.org/x/net/context"

	gogoproto "github.com/gogo/protobuf/proto"
)

// UpdateForBatch updates the first argument (the header of a request contained
// in a batch) from the second one (the batch header), returning an error when
// inconsistencies are found.
// It is checked that the individual call does not have a UserPriority
// or Txn set that differs from the batch's.
func UpdateForBatch(args proto.Request, bHeader proto.RequestHeader) error {
	// Disallow transaction, user and priority on individual calls, unless
	// equal.
	aHeader := args.Header()
	if aPrio := aHeader.GetUserPriority(); aPrio != proto.Default_RequestHeader_UserPriority && aPrio != bHeader.GetUserPriority() {
		return util.Errorf("conflicting user priority on call in batch")
	}
	aHeader.UserPriority = bHeader.UserPriority
	// Only allow individual transactions on the requests of a batch if
	// - the batch is non-transactional,
	// - the individual transaction does not write intents, and
	// - the individual transaction is initialized.
	// The main usage of this is to allow mass-resolution of intents, which
	// entails sending a non-txn batch of transactional InternalResolveIntent.
	if aHeader.Txn != nil && !aHeader.Txn.Equal(bHeader.Txn) {
		if len(aHeader.Txn.ID) == 0 || proto.IsTransactionWrite(args) || bHeader.Txn != nil {
			return util.Errorf("conflicting transaction in transactional batch")
		}
	} else {
		aHeader.Txn = bHeader.Txn
	}
	return nil
}

// MaybeWrap wraps the given argument in a batch, unless it is already one.
func MaybeWrap(args proto.Request) (*proto.BatchRequest, func(proto.Response) proto.Response) {
	if bArgs, ok := args.(*proto.BatchRequest); ok {
		return bArgs, func(a proto.Response) proto.Response { return a }
	}
	bArgs := &proto.BatchRequest{}
	bArgs.RequestHeader = *(gogoproto.Clone(args.Header()).(*proto.RequestHeader))
	if !proto.IsRange(args) {
		// TODO(tschottdorf): this is only here because BatchRequest is
		// marked as a `range` operation. This has side effects such as
		// creating unneccessary intents at TxnCoordSender.
		// TODO(tschottdorf): remove
		// bArgs.RequestHeader.EndKey = bArgs.RequestHeader.Key.Next()
	}
	bArgs.Add(args)
	return bArgs, func(reply proto.Response) proto.Response {
		bReply, ok := reply.(*proto.BatchResponse)
		if !ok {
			// Request likely never sent, but caught a local error.
			return reply
		}
		var unwrappedReply proto.Response
		if len(bReply.Responses) == 0 {
			unwrappedReply = args.CreateReply()
		} else {
			unwrappedReply = bReply.Responses[0].GetValue().(proto.Response)
		}
		// The ReplyTxn is propagated from one response to the next request,
		// and we adopt the mechanism that whenever the Txn changes, it needs
		// to be set in the reply, for example to ratched up the transaction
		// timestamp on writes when necessary.
		// This is internally necessary to sequentially execute the batch,
		// so it makes some sense to take the burden of updating the Txn
		// from TxnCoordSender - it will only need to act on retries/aborts
		// in the future.
		unwrappedReply.Header().Txn = bReply.Txn
		return unwrappedReply
	}
}

// MaybeWrapCall returns a new call which wraps the original Args and Reply
// in a batch, if necessary.
func MaybeWrapCall(call proto.Call) (proto.Call, func(proto.Call) proto.Call) {
	var unwrap func(proto.Response) proto.Response
	call.Args, unwrap = MaybeWrap(call.Args)
	newUnwrap := func(origReply proto.Response) func(proto.Call) proto.Call {
		return func(newCall proto.Call) proto.Call {
			origReply.Reset()
			gogoproto.Merge(origReply, unwrap(newCall.Reply))
			*origReply.Header() = *newCall.Reply.Header()
			newCall.Reply = origReply
			assertIntegrity(origReply.Header(), newCall.Reply.Header())
			return newCall
		}
	}(call.Reply)
	call.Reply = call.Args.CreateReply()
	return call, newUnwrap
}

// Unroll unrolls a batched command and sends the individual requests
// sequentially. It's for testing code.
// TODO(tschottdorf): move to according location once it's clear who needs to
// use this except for retryableLocalSender.
func Unroll(ctx context.Context, sender client.Sender, batchArgs *proto.BatchRequest, batchReply *proto.BatchResponse) {
	// Prepare the calls by unrolling the batch. If the batchReply is
	// pre-initialized with replies, use those; otherwise create replies
	// as needed.
	for _, arg := range batchArgs.Requests {
		if err := UpdateForBatch(arg.GetValue().(proto.Request), batchArgs.RequestHeader); err != nil {
			batchReply.Header().SetGoError(err)
			return
		}
	}

	batchReply.Txn = batchArgs.Txn
	for i := range batchArgs.Requests {
		args := batchArgs.Requests[i].GetValue().(proto.Request)
		call := proto.Call{Args: args}
		// Create a reply from the method type and add to batch response.
		if i >= len(batchReply.Responses) {
			call.Reply = args.CreateReply()
			batchReply.Add(call.Reply)
		} else {
			call.Reply = batchReply.Responses[i].GetValue().(proto.Response)
		}
		sender.Send(ctx, call)
		// Amalgamate transaction updates and propagate first error, if applicable.
		if batchReply.Txn != nil {
			batchReply.Txn.Update(call.Reply.Header().Txn)
		}
		if call.Reply.Header().Error != nil {
			batchReply.Error = call.Reply.Header().Error
			return
		}
	}
}

func assertIntegrity(iHeader, bHeader *proto.ResponseHeader) {
	if (iHeader.Txn == nil) != (bHeader.Txn == nil) {
		panic(fmt.Sprintf("%s != %s", iHeader.Txn, bHeader.Txn))
	}
	if iHeader.Txn == nil {
		// Both are nil.
		return
	}
	if iHeader.Txn.Timestamp != bHeader.Txn.Timestamp {
		panic(fmt.Sprintf("%s != %s", iHeader.Txn, bHeader.Txn))
	}
}

// KeyRange returns a key range which contains all keys in the Batch.
// In particular, this resolves local addressing.
func KeyRange(br *proto.BatchRequest) (proto.Key, proto.Key) {
	if len(br.Requests) == 0 {
		panic("KeyRange called on empty BatchRequest")
	}
	from := proto.KeyMax
	to := proto.KeyMin
	for _, arg := range br.Requests {
		req := arg.GetValue().(proto.Request)
		if req.Method() == proto.Noop {
			continue
		}
		h := req.Header()
		key := keys.KeyAddress(h.Key)
		if key.Less(keys.KeyAddress(from)) {
			// Key is smaller than `from`.
			from = key
		}
		if keys.KeyAddress(to).Less(key) {
			// Key is larger than `to`.
			to = key.Next()
		}
		if endKey := keys.KeyAddress(h.EndKey); keys.KeyAddress(to).Less(endKey) {
			// EndKey is larger than `to`.
			to = endKey
		}
	}
	return from, to
}

// Short gives a brief summary of the contained requests and keys in the batch.
func Short(br *proto.BatchRequest) string {
	var str []string
	for _, arg := range br.Requests {
		req := arg.GetValue().(proto.Request)
		h := req.Header()
		str = append(str, fmt.Sprintf("%T [%s,%s)", req, h.Key, h.EndKey))
	}
	from, to := KeyRange(br)
	return fmt.Sprintf("[%s,%s): ", from, to) + strings.Join(str, ", ")
}

// Sender is a new incarnation of client.Sender which only supports batches
// and uses a request-response pattern.
type Sender interface {
	Send(context.Context, *proto.BatchRequest) (*proto.BatchResponse, error)
}

// SenderFn is a function that implements a Sender.
type SenderFn func(context.Context, *proto.BatchRequest) (*proto.BatchResponse, error)

// A ChunkingSender sends batches, subdividing them appropriately.
type ChunkingSender struct {
	f SenderFn
}

// NewChunkingSender returns a new chunking sender which sends through the supplied
// SenderFn.
func NewChunkingSender(f SenderFn) Sender {
	return &ChunkingSender{f: f}
}

// Send implements Sender.
func (cs *ChunkingSender) Send(ctx context.Context, batchArgs *proto.BatchRequest) (*proto.BatchResponse, error) {
	var argChunks []*proto.BatchRequest
	if len(batchArgs.Requests) < 1 {
		panic("empty batchArgs")
	}
	// TODO(tschottdorf): only cuts an EndTransaction request off. Also need
	// to untangle reverse/forward, txn/non-txn, ...
	// We actually don't want to do this for single-range requests. Whether it
	// is one or not is unknown right now (you can only find out after you've
	// sent to the Range/looked up a descriptor that suggests that you're
	// multi-range. In those cases, should return an error so that we split and
	// retry once the chunk which contains EndTransaction (i.e. the last one).
	if etArgs, ok := proto.GetArg(batchArgs, proto.EndTransaction); ok &&
		len(batchArgs.Requests) > 1 {
		firstChunk := *batchArgs // shallow copy so that we get to manipulate .Requests
		etChunk := &proto.BatchRequest{}
		etChunk.Add(etArgs)
		etChunk.RequestHeader = *gogoproto.Clone(&batchArgs.RequestHeader).(*proto.RequestHeader)
		firstChunk.Requests = batchArgs.Requests[:len(batchArgs.Requests)-1]
		argChunks = append(argChunks, &firstChunk, etChunk)
	} else {
		argChunks = append(argChunks, batchArgs)
	}
	var rplChunks []*proto.BatchResponse
	// TODO(tschottdorf): propagate reply header to next request.
	for len(argChunks) > 0 {
		batchArgs, argChunks = argChunks[0], argChunks[1:]
		rpl, err := cs.f(ctx, batchArgs)
		if err != nil {
			return nil, err
		}
		rplChunks = append(rplChunks, rpl)
	}
	return fuseReplyChunks(rplChunks)
}

func fuseReplyChunks(rplChunks []*proto.BatchResponse) (*proto.BatchResponse, error) {
	if len(rplChunks) == 0 {
		panic("no responses given")
	}
	reply := rplChunks[0]
	for _, rpl := range rplChunks[1:] {
		reply.Responses = append(reply.Responses, rpl.Responses...)
	}
	reply.ResponseHeader = rplChunks[len(rplChunks)-1].ResponseHeader
	return reply, nil
}
