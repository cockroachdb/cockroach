// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// KeyValue represents a single key/value pair. This is similar to
// roachpb.KeyValue except that the value may be nil. The timestamp
// in the value will be populated with the MVCC timestamp at which this
// value was read if this struct was produced by a GetRequest or
// ScanRequest which uses the KEY_VALUES ScanFormat. Values created from
// a ScanRequest which uses the BATCH_RESPONSE ScanFormat will contain a
// zero Timestamp.
type KeyValue struct {
	Key   roachpb.Key
	Value *roachpb.Value
}

func (kv *KeyValue) String() string {
	return kv.Key.String() + "=" + kv.PrettyValue()
}

// Exists returns true iff the value exists.
func (kv *KeyValue) Exists() bool {
	return kv.Value != nil
}

// PrettyValue returns a human-readable version of the value as a string.
func (kv *KeyValue) PrettyValue() string {
	if kv.Value == nil {
		return "nil"
	}
	switch kv.Value.GetTag() {
	case roachpb.ValueType_INT:
		v, err := kv.Value.GetInt()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("%d", v)
	case roachpb.ValueType_FLOAT:
		v, err := kv.Value.GetFloat()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("%v", v)
	case roachpb.ValueType_BYTES:
		v, err := kv.Value.GetBytes()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("%q", v)
	case roachpb.ValueType_TIME:
		v, err := kv.Value.GetTime()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return v.String()
	}
	return fmt.Sprintf("%x", kv.Value.RawBytes)
}

// ValueBytes returns the value as a byte slice. This method will panic if the
// value's type is not a byte slice.
func (kv *KeyValue) ValueBytes() []byte {
	if kv.Value == nil {
		return nil
	}
	bytes, err := kv.Value.GetBytes()
	if err != nil {
		panic(err)
	}
	return bytes
}

// ValueInt returns the value decoded as an int64. This method will panic if
// the value cannot be decoded as an int64.
func (kv *KeyValue) ValueInt() int64 {
	if kv.Value == nil {
		return 0
	}
	i, err := kv.Value.GetInt()
	if err != nil {
		panic(err)
	}
	return i
}

// ValueProto parses the byte slice value into msg.
func (kv *KeyValue) ValueProto(msg protoutil.Message) error {
	if kv.Value == nil {
		msg.Reset()
		return nil
	}
	return kv.Value.GetProto(msg)
}

// Result holds the result for a single DB or Txn operation (e.g. Get, Put,
// etc).
type Result struct {
	calls int
	// Err contains any error encountered when performing the operation.
	Err error
	// Rows contains the key/value pairs for the operation. The number of rows
	// returned varies by operation. For Get, Put, CPut, Inc and Del the number
	// of rows returned is the number of keys operated on. For Scan the number of
	// rows returned is the number or rows matching the scan capped by the
	// maxRows parameter and other options. For DelRange Rows is nil.
	Rows []KeyValue

	// Keys is set by some operations instead of returning the rows themselves.
	Keys []roachpb.Key

	// ResumeSpan is the the span to be used on the next operation in a
	// sequence of operations. It is returned whenever an operation over a
	// span of keys is bounded and the operation returns before completely
	// running over the span. It allows the operation to be called again with
	// a new shorter span of keys. A nil span is set when the operation has
	// successfully completed running through the span.
	ResumeSpan *roachpb.Span
	// When ResumeSpan is populated, this specifies the reason why the operation
	// wasn't completed and needs to be resumed.
	ResumeReason roachpb.ResponseHeader_ResumeReason

	// RangeInfos contains information about the replicas that produced this
	// result.
	// This is only populated if Err == nil and if ReturnRangeInfo has been set on
	// the request.
	RangeInfos []roachpb.RangeInfo
}

// ResumeSpanAsValue returns the resume span as a value if one is set,
// or an empty span if one is not set.
func (r *Result) ResumeSpanAsValue() roachpb.Span {
	if r.ResumeSpan == nil {
		return roachpb.Span{}
	}
	return *r.ResumeSpan
}

func (r Result) String() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	var buf strings.Builder
	for i := range r.Rows {
		if i > 0 {
			buf.WriteString("\n")
		}
		fmt.Fprintf(&buf, "%d: %s", i, &r.Rows[i])
	}
	return buf.String()
}

// DBContext contains configuration parameters for DB.
type DBContext struct {
	// UserPriority is the default user priority to set on API calls. If
	// userPriority is set to any value except 1 in call arguments, this
	// value is ignored.
	UserPriority roachpb.UserPriority
	// NodeID provides the node ID for setting the gateway node and avoiding
	// clock uncertainty for root transactions started at the gateway.
	NodeID *base.SQLIDContainer
	// Stopper is used for async tasks.
	Stopper *stop.Stopper
}

// DefaultDBContext returns (a copy of) the default options for
// NewDBWithContext.
func DefaultDBContext() DBContext {
	var c base.NodeIDContainer
	return DBContext{
		UserPriority: roachpb.NormalUserPriority,
		// TODO(tbg): this is ugly. Force callers to pass in an SQLIDContainer.
		NodeID:  base.NewSQLIDContainer(0, &c, true /* exposed */),
		Stopper: stop.NewStopper(),
	}
}

// CrossRangeTxnWrapperSender is a Sender whose purpose is to wrap
// non-transactional requests that span ranges into a transaction so they can
// execute atomically.
//
// TODO(andrei, bdarnell): This is a wart. Our semantics are that batches are
// atomic, but there's only historical reason for that. We should disallow
// non-transactional batches and scans, forcing people to use transactions
// instead. And then this Sender can go away.
type CrossRangeTxnWrapperSender struct {
	db      *DB
	wrapped Sender
}

var _ Sender = &CrossRangeTxnWrapperSender{}

// Send implements the Sender interface.
func (s *CrossRangeTxnWrapperSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if ba.Txn != nil {
		log.Fatalf(ctx, "CrossRangeTxnWrapperSender can't handle transactional requests")
	}

	br, pErr := s.wrapped.Send(ctx, ba)
	if _, ok := pErr.GetDetail().(*roachpb.OpRequiresTxnError); !ok {
		return br, pErr
	}

	err := s.db.Txn(ctx, func(ctx context.Context, txn *Txn) error {
		txn.SetDebugName("auto-wrap")
		b := txn.NewBatch()
		b.Header = ba.Header
		for _, arg := range ba.Requests {
			req := arg.GetInner().ShallowCopy()
			b.AddRawRequest(req)
		}
		err := txn.CommitInBatch(ctx, b)
		br = b.RawResponse()
		return err
	})
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	br.Txn = nil // hide the evidence
	return br, nil
}

// Wrapped returns the wrapped sender.
func (s *CrossRangeTxnWrapperSender) Wrapped() Sender {
	return s.wrapped
}

// DB is a database handle to a single cockroach cluster. A DB is safe for
// concurrent use by multiple goroutines.
type DB struct {
	log.AmbientContext

	factory TxnSenderFactory
	clock   *hlc.Clock
	ctx     DBContext
	// crs is the sender used for non-transactional requests.
	crs CrossRangeTxnWrapperSender
}

// NonTransactionalSender returns a Sender that can be used for sending
// non-transactional requests. The Sender is capable of transparently wrapping
// non-transactional requests that span ranges in transactions.
//
// The Sender returned should not be used for sending transactional requests -
// it bypasses the TxnCoordSender. Use db.Txn() or db.NewTxn() for transactions.
func (db *DB) NonTransactionalSender() Sender {
	return &db.crs
}

// GetFactory returns the DB's TxnSenderFactory.
func (db *DB) GetFactory() TxnSenderFactory {
	return db.factory
}

// Clock returns the DB's hlc.Clock.
func (db *DB) Clock() *hlc.Clock {
	return db.clock
}

// NewDB returns a new DB.
func NewDB(actx log.AmbientContext, factory TxnSenderFactory, clock *hlc.Clock) *DB {
	return NewDBWithContext(actx, factory, clock, DefaultDBContext())
}

// NewDBWithContext returns a new DB with the given parameters.
func NewDBWithContext(
	actx log.AmbientContext, factory TxnSenderFactory, clock *hlc.Clock, ctx DBContext,
) *DB {
	if actx.Tracer == nil {
		panic("no tracer set in AmbientCtx")
	}
	db := &DB{
		AmbientContext: actx,
		factory:        factory,
		clock:          clock,
		ctx:            ctx,
		crs: CrossRangeTxnWrapperSender{
			wrapped: factory.NonTransactionalSender(),
		},
	}
	db.crs.db = db
	return db
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error. It is not considered an error for the key not to exist.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (db *DB) Get(ctx context.Context, key interface{}) (KeyValue, error) {
	b := &Batch{}
	b.Get(key)
	return getOneRow(db.Run(ctx, b), b)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message. If the key doesn't exist, the proto will simply be reset.
//
// key can be either a byte slice or a string.
func (db *DB) GetProto(ctx context.Context, key interface{}, msg protoutil.Message) error {
	_, err := db.GetProtoTs(ctx, key, msg)
	return err
}

// GetProtoTs retrieves the value for a key and decodes the result as a proto
// message. It additionally returns the timestamp at which the key was read.
// If the key doesn't exist, the proto will simply be reset and a zero timestamp
// will be returned. A zero timestamp will also be returned if unmarshaling
// fails.
//
// key can be either a byte slice or a string.
func (db *DB) GetProtoTs(
	ctx context.Context, key interface{}, msg protoutil.Message,
) (hlc.Timestamp, error) {
	r, err := db.Get(ctx, key)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if err := r.ValueProto(msg); err != nil || r.Value == nil {
		return hlc.Timestamp{}, err
	}
	return r.Value.Timestamp, nil
}

// Put sets the value for a key.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
func (db *DB) Put(ctx context.Context, key, value interface{}) error {
	b := &Batch{}
	b.Put(key, value)
	return getOneErr(db.Run(ctx, b), b)
}

// PutInline sets the value for a key, but does not maintain
// multi-version values. The most recent value is always overwritten.
// Inline values cannot be mutated transactionally and should be used
// with caution.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
func (db *DB) PutInline(ctx context.Context, key, value interface{}) error {
	b := &Batch{}
	b.PutInline(key, value)
	return getOneErr(db.Run(ctx, b), b)
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue. Note that this must be an interface{}(nil), not a
// typed nil value (e.g. []byte(nil)).
//
// Returns an error if the existing value is not equal to expValue.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
func (db *DB) CPut(ctx context.Context, key, value interface{}, expValue *roachpb.Value) error {
	b := &Batch{}
	b.CPut(key, value, expValue)
	return getOneErr(db.Run(ctx, b), b)
}

// InitPut sets the first value for a key to value. A ConditionFailedError is
// reported if a value already exists for the key and it's not equal to the
// value passed in. If failOnTombstones is set to true, tombstones count as
// mismatched values and will cause a ConditionFailedError.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc). It is illegal to
// set value to nil.
func (db *DB) InitPut(ctx context.Context, key, value interface{}, failOnTombstones bool) error {
	b := &Batch{}
	b.InitPut(key, value, failOnTombstones)
	return getOneErr(db.Run(ctx, b), b)
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// key can be either a byte slice or a string.
func (db *DB) Inc(ctx context.Context, key interface{}, value int64) (KeyValue, error) {
	b := &Batch{}
	b.Inc(key, value)
	return getOneRow(db.Run(ctx, b), b)
}

func (db *DB) scan(
	ctx context.Context,
	begin, end interface{},
	maxRows int64,
	isReverse bool,
	forUpdate bool,
	readConsistency roachpb.ReadConsistencyType,
) ([]KeyValue, error) {
	b := &Batch{}
	b.Header.ReadConsistency = readConsistency
	if maxRows > 0 {
		b.Header.MaxSpanRequestKeys = maxRows
	}
	b.scan(begin, end, isReverse, forUpdate)
	r, err := getOneResult(db.Run(ctx, b), b)
	return r.Rows, err
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (db *DB) Scan(ctx context.Context, begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	return db.scan(ctx, begin, end, maxRows, false /* isReverse */, false /* forUpdate */, roachpb.CONSISTENT)
}

// ScanForUpdate retrieves the rows between begin (inclusive) and end
// (exclusive) in ascending order. Unreplicated, exclusive locks are
// acquired on each of the returned keys.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (db *DB) ScanForUpdate(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return db.scan(ctx, begin, end, maxRows, false /* isReverse */, true /* forUpdate */, roachpb.CONSISTENT)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (db *DB) ReverseScan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return db.scan(ctx, begin, end, maxRows, true /* isReverse */, false /* forUpdate */, roachpb.CONSISTENT)
}

// ReverseScanForUpdate retrieves the rows between begin (inclusive) and end
// (exclusive) in descending order. Unreplicated, exclusive locks are acquired
// on each of the returned keys.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (db *DB) ReverseScanForUpdate(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return db.scan(ctx, begin, end, maxRows, true /* isReverse */, true /* forUpdate */, roachpb.CONSISTENT)
}

// Del deletes one or more keys.
//
// key can be either a byte slice or a string.
func (db *DB) Del(ctx context.Context, keys ...interface{}) error {
	b := &Batch{}
	b.Del(keys...)
	return getOneErr(db.Run(ctx, b), b)
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// TODO(pmattis): Perhaps the result should return which rows were deleted.
//
// key can be either a byte slice or a string.
func (db *DB) DelRange(ctx context.Context, begin, end interface{}) error {
	b := &Batch{}
	b.DelRange(begin, end, false)
	return getOneErr(db.Run(ctx, b), b)
}

// AdminMerge merges the range containing key and the subsequent range. After
// the merge operation is complete, the range containing key will contain all of
// the key/value pairs of the subsequent range and the subsequent range will no
// longer exist. Neither range may contain learner replicas, if one does, an
// error is returned.
//
// key can be either a byte slice or a string.
func (db *DB) AdminMerge(ctx context.Context, key interface{}) error {
	b := &Batch{}
	b.adminMerge(key)
	return getOneErr(db.Run(ctx, b), b)
}

// AdminSplit splits the range at splitkey.
//
// spanKey is a key within the range that should be split, and splitKey is the
// key at which that range should be split. splitKey is not used exactly as
// provided--it is first mutated by keys.EnsureSafeSplitKey. Accounting for
// this mutation sometimes requires constructing a key that falls in a
// different range, hence the separation between spanKey and splitKey. See
// #16008 for details, and #16344 for the tracking issue to clean this mess up
// properly.
//
// expirationTime is the timestamp when the split expires and is eligible for
// automatic merging by the merge queue. To specify that a split should
// immediately be eligible for automatic merging, set expirationTime to
// hlc.Timestamp{} (I.E. the zero timestamp). To specify that a split should
// never be eligible, set expirationTime to hlc.MaxTimestamp.
//
// The keys can be either byte slices or a strings.
func (db *DB) AdminSplit(
	ctx context.Context, spanKey, splitKey interface{}, expirationTime hlc.Timestamp,
) error {
	b := &Batch{}
	b.adminSplit(spanKey, splitKey, expirationTime)
	return getOneErr(db.Run(ctx, b), b)
}

// SplitAndScatter is a helper that wraps AdminSplit + AdminScatter.
func (db *DB) SplitAndScatter(
	ctx context.Context, key roachpb.Key, expirationTime hlc.Timestamp,
) error {
	if err := db.AdminSplit(ctx, key, key, expirationTime); err != nil {
		return err
	}
	scatterReq := &roachpb.AdminScatterRequest{
		RequestHeader:   roachpb.RequestHeaderFromSpan(roachpb.Span{Key: key, EndKey: key.Next()}),
		RandomizeLeases: true,
	}
	if _, pErr := SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
		return pErr.GoError()
	}
	return nil
}

// AdminUnsplit removes the sticky bit of the range specified by splitKey.
//
// splitKey is the start key of the range whose sticky bit should be removed.
//
// If splitKey is not the start key of a range, then this method will throw an
// error. If the range specified by splitKey does not have a sticky bit set,
// then this method will not throw an error and is a no-op.
func (db *DB) AdminUnsplit(ctx context.Context, splitKey interface{}) error {
	b := &Batch{}
	b.adminUnsplit(splitKey)
	return getOneErr(db.Run(ctx, b), b)
}

// AdminTransferLease transfers the lease for the range containing key to the
// specified target. The target replica for the lease transfer must be one of
// the existing replicas of the range.
//
// key can be either a byte slice or a string.
//
// When this method returns, it's guaranteed that the old lease holder has
// applied the new lease, but that's about it. It's not guaranteed that the new
// lease holder has applied it (so it might not know immediately that it is the
// new lease holder).
func (db *DB) AdminTransferLease(
	ctx context.Context, key interface{}, target roachpb.StoreID,
) error {
	b := &Batch{}
	b.adminTransferLease(key, target)
	return getOneErr(db.Run(ctx, b), b)
}

// AdminChangeReplicas adds or removes a set of replicas for a range.
func (db *DB) AdminChangeReplicas(
	ctx context.Context,
	key interface{},
	expDesc roachpb.RangeDescriptor,
	chgs []roachpb.ReplicationChange,
) (*roachpb.RangeDescriptor, error) {
	b := &Batch{}
	b.adminChangeReplicas(key, expDesc, chgs)
	if err := getOneErr(db.Run(ctx, b), b); err != nil {
		return nil, err
	}
	responses := b.response.Responses
	if len(responses) == 0 {
		return nil, errors.Errorf("unexpected empty responses for AdminChangeReplicas")
	}
	resp, ok := responses[0].GetInner().(*roachpb.AdminChangeReplicasResponse)
	if !ok {
		return nil, errors.Errorf("unexpected response of type %T for AdminChangeReplicas",
			responses[0].GetInner())
	}
	desc := resp.Desc
	return &desc, nil
}

// AdminRelocateRange relocates the replicas for a range onto the specified
// list of stores.
func (db *DB) AdminRelocateRange(
	ctx context.Context, key interface{}, targets []roachpb.ReplicationTarget,
) error {
	b := &Batch{}
	b.adminRelocateRange(key, targets)
	return getOneErr(db.Run(ctx, b), b)
}

// WriteBatch applies the operations encoded in a BatchRepr, which is the
// serialized form of a RocksDB Batch. The command cannot span Ranges and must
// be run on an empty keyrange.
func (db *DB) WriteBatch(ctx context.Context, begin, end interface{}, data []byte) error {
	b := &Batch{}
	b.writeBatch(begin, end, data)
	return getOneErr(db.Run(ctx, b), b)
}

// AddSSTable links a file into the RocksDB log-structured merge-tree. Existing
// data in the range is cleared.
func (db *DB) AddSSTable(
	ctx context.Context,
	begin, end interface{},
	data []byte,
	disallowShadowing bool,
	stats *enginepb.MVCCStats,
	ingestAsWrites bool,
) error {
	b := &Batch{}
	b.addSSTable(begin, end, data, disallowShadowing, stats, ingestAsWrites)
	return getOneErr(db.Run(ctx, b), b)
}

// sendAndFill is a helper which sends the given batch and fills its results,
// returning the appropriate error which is either from the first failing call,
// or an "internal" error.
func sendAndFill(ctx context.Context, send SenderFunc, b *Batch) error {
	// Errors here will be attached to the results, so we will get them from
	// the call to fillResults in the regular case in which an individual call
	// fails. But send() also returns its own errors, so there's some dancing
	// here to do because we want to run fillResults() so that the individual
	// result gets initialized with an error from the corresponding call.
	var ba roachpb.BatchRequest
	ba.Requests = b.reqs
	ba.Header = b.Header
	b.response, b.pErr = send(ctx, ba)
	b.fillResults(ctx)
	if b.pErr == nil {
		b.pErr = roachpb.NewError(b.resultErr())
	}
	return b.pErr.GoError()
}

// Run executes the operations queued up within a batch. Before executing any
// of the operations the batch is first checked to see if there were any errors
// during its construction (e.g. failure to marshal a proto message).
//
// The operations within a batch are run in parallel and the order is
// non-deterministic. It is an unspecified behavior to modify and retrieve the
// same key within a batch.
//
// Upon completion, Batch.Results will contain the results for each
// operation. The order of the results matches the order the operations were
// added to the batch.
func (db *DB) Run(ctx context.Context, b *Batch) error {
	if err := b.prepare(); err != nil {
		return err
	}
	return sendAndFill(ctx, db.send, b)
}

// NewTxn creates a new RootTxn.
func (db *DB) NewTxn(ctx context.Context, debugName string) *Txn {
	// Observed timestamps don't work with multi-tenancy. See:
	//
	// https://github.com/cockroachdb/cockroach/issues/48008
	nodeID, _ := db.ctx.NodeID.OptionalNodeID() // zero if not available
	txn := NewTxn(ctx, db, nodeID)
	txn.SetDebugName(debugName)
	return txn
}

// Txn executes retryable in the context of a distributed transaction. The
// transaction is automatically aborted if retryable returns any error aside
// from recoverable internal errors, and is automatically committed
// otherwise. The retryable function should have no side effects which could
// cause problems in the event it must be run more than once.
func (db *DB) Txn(ctx context.Context, retryable func(context.Context, *Txn) error) error {
	// TODO(radu): we should open a tracing Span here (we need to figure out how
	// to use the correct tracer).

	// Observed timestamps don't work with multi-tenancy. See:
	//
	// https://github.com/cockroachdb/cockroach/issues/48008
	nodeID, _ := db.ctx.NodeID.OptionalNodeID() // zero if not available
	txn := NewTxn(ctx, db, nodeID)
	txn.SetDebugName("unnamed")
	err := txn.exec(ctx, func(ctx context.Context, txn *Txn) error {
		return retryable(ctx, txn)
	})
	if err != nil {
		txn.CleanupOnError(ctx, err)
	}
	// Terminate TransactionRetryWithProtoRefreshError here, so it doesn't cause a higher-level
	// txn to be retried. We don't do this in any of the other functions in DB; I
	// guess we should.
	if errors.HasType(err, (*roachpb.TransactionRetryWithProtoRefreshError)(nil)) {
		return errors.Wrapf(err, "terminated retryable error")
	}
	return err
}

// send runs the specified calls synchronously in a single batch and returns
// any errors. Returns (nil, nil) for an empty batch.
func (db *DB) send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return db.sendUsingSender(ctx, ba, db.NonTransactionalSender())
}

// sendUsingSender uses the specified sender to send the batch request.
func (db *DB) sendUsingSender(
	ctx context.Context, ba roachpb.BatchRequest, sender Sender,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) == 0 {
		return nil, nil
	}
	if err := ba.ReadConsistency.SupportsBatch(ba); err != nil {
		return nil, roachpb.NewError(err)
	}
	if ba.UserPriority == 0 && db.ctx.UserPriority != 1 {
		ba.UserPriority = db.ctx.UserPriority
	}

	tracing.AnnotateTrace()
	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		if log.V(1) {
			log.Infof(ctx, "failed batch: %s", pErr)
		}
		return nil, pErr
	}
	return br, nil
}

// getOneErr returns the error for a single-request Batch that was run.
// runErr is the error returned by Run, b is the Batch that was passed to Run.
func getOneErr(runErr error, b *Batch) error {
	if runErr != nil && len(b.Results) > 0 {
		return b.Results[0].Err
	}
	return runErr
}

// getOneResult returns the result for a single-request Batch that was run.
// runErr is the error returned by Run, b is the Batch that was passed to Run.
func getOneResult(runErr error, b *Batch) (Result, error) {
	if runErr != nil {
		if len(b.Results) > 0 {
			return b.Results[0], b.Results[0].Err
		}
		return Result{Err: runErr}, runErr
	}
	res := b.Results[0]
	if res.Err != nil {
		panic("run succeeded even through the result has an error")
	}
	return res, nil
}

// getOneRow returns the first row for a single-request Batch that was run.
// runErr is the error returned by Run, b is the Batch that was passed to Run.
func getOneRow(runErr error, b *Batch) (KeyValue, error) {
	res, err := getOneResult(runErr, b)
	if err != nil {
		return KeyValue{}, err
	}
	return res.Rows[0], nil
}

// IncrementValRetryable increments a key's value by a specified amount and
// returns the new value.
//
// It performs the increment as a retryable non-transactional increment. The key
// might be incremented multiple times because of the retries.
func IncrementValRetryable(ctx context.Context, db *DB, key roachpb.Key, inc int64) (int64, error) {
	var err error
	var res KeyValue
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		res, err = db.Inc(ctx, key, inc)
		if errors.HasType(err, (*roachpb.UnhandledRetryableError)(nil)) ||
			errors.HasType(err, (*roachpb.AmbiguousResultError)(nil)) {
			continue
		}
		break
	}
	return res.ValueInt(), err
}
