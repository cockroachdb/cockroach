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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// asyncRollbackTimeout is the context timeout during rollback() for a client
// who has already disconnected. This is needed to asynchronously clean up the
// client's intents and txn record. If the intent resolver has spare async task
// capacity, this timeout only needs to be long enough for the EndTxn request to
// make it through Raft, but if the cleanup task is synchronous (to backpressure
// clients) then cleanup will be abandoned when the timeout expires.
//
// We generally want to clean up if possible, so we set it high at 1 minute. If
// the transaction is very large or cleanup is very costly (e.g. hits a slow
// path for some reason), and the async pool is full (i.e. the system is
// under load), then it makes sense to abandon the cleanup before too long.
const asyncRollbackTimeout = time.Minute

// Txn is an in-progress distributed database transaction. A Txn is safe for
// concurrent use by multiple goroutines.
type Txn struct {
	db *DB

	// typ indicates the type of transaction.
	typ TxnType

	// gatewayNodeID, if != 0, is the ID of the node on whose behalf this
	// transaction is running. Normally this is the current node, but in the case
	// of Txns created on remote nodes by DistSQL this will be the gateway.
	// It will be attached to all requests sent through this transaction.
	gatewayNodeID roachpb.NodeID

	// The following fields are not safe for concurrent modification.
	// They should be set before operating on the transaction.

	// commitTriggers are run upon successful commit.
	commitTriggers []func(ctx context.Context)
	// systemConfigTrigger is set to true when modifying keys from the SystemConfig
	// span. This sets the SystemConfigTrigger on EndTxnRequest.
	systemConfigTrigger bool

	// mu holds fields that need to be synchronized for concurrent request execution.
	mu struct {
		syncutil.Mutex
		ID           uuid.UUID
		debugName    string
		userPriority roachpb.UserPriority

		// previousIDs holds the set of all previous IDs that the Txn's Proto has
		// had across transaction aborts. This allows us to determine if a given
		// response was meant for any incarnation of this transaction. This is
		// useful for catching retriable errors that have escaped inner
		// transactions, so that they don't cause a retry of an outer transaction.
		previousIDs map[uuid.UUID]struct{}

		// sender is a stateful sender for use with transactions (usually a
		// TxnCoordSender). A new sender is created on transaction restarts (not
		// retries).
		sender TxnSender

		// The txn has to be committed by this deadline. A nil value indicates no
		// deadline.
		deadline *hlc.Timestamp
	}

	// admissionHeader is used for admission control for work done in this
	// transaction. Only certain paths initialize this properly, and the
	// remaining just use the zero value. The set of code paths that initialize
	// this are expected to expand over time.
	admissionHeader roachpb.AdmissionHeader
}

// NewTxn returns a new RootTxn.
// Note: for SQL usage, prefer NewTxnWithSteppingEnabled() below.
//
// If the transaction is used to send any operations, CommitOrCleanup() or
// CleanupOnError() should eventually be called to commit/rollback the
// transaction (including stopping the heartbeat loop).
//
// gatewayNodeID: If != 0, this is the ID of the node on whose behalf this
//   transaction is running. Normally this is the current node, but in the case
//   of Txns created on remote nodes by DistSQL this will be the gateway.
//   If 0 is passed, then no value is going to be filled in the batches sent
//   through this txn. This will have the effect that the DistSender will fill
//   in the batch with the current node's ID.
//   If the gatewayNodeID is set and this is a root transaction, we optimize
//   away any clock uncertainty for our own node, as our clock is accessible.
//
// See also db.NewTxn().
func NewTxn(ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID) *Txn {
	if db == nil {
		panic(errors.WithContextTags(
			errors.AssertionFailedf("attempting to create txn with nil db"), ctx))
	}

	now := db.clock.NowAsClockTimestamp()
	kvTxn := roachpb.MakeTransaction(
		"unnamed",
		nil, // baseKey
		roachpb.NormalUserPriority,
		now.ToTimestamp(),
		db.clock.MaxOffset().Nanoseconds(),
	)

	return NewTxnFromProto(ctx, db, gatewayNodeID, now, RootTxn, &kvTxn)
}

// NewTxnWithSteppingEnabled is like NewTxn but suitable for use by SQL. Note
// that this initializes Txn.admissionHeader to specify that the source is
// FROM_SQL.
func NewTxnWithSteppingEnabled(ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID) *Txn {
	txn := NewTxn(ctx, db, gatewayNodeID)
	txn.admissionHeader = roachpb.AdmissionHeader{
		Priority:   int32(admission.NormalPri),
		CreateTime: timeutil.Now().UnixNano(),
		Source:     roachpb.AdmissionHeader_FROM_SQL,
	}
	_ = txn.ConfigureStepping(ctx, SteppingEnabled)
	return txn
}

// NewTxnFromProto is like NewTxn but assumes the Transaction object is already initialized.
// Do not use this directly; use NewTxn() instead.
// This function exists for testing only.
func NewTxnFromProto(
	ctx context.Context,
	db *DB,
	gatewayNodeID roachpb.NodeID,
	now hlc.ClockTimestamp,
	typ TxnType,
	proto *roachpb.Transaction,
) *Txn {
	// Ensure the gateway node ID is marked as free from clock offset.
	if gatewayNodeID != 0 && typ == RootTxn {
		proto.UpdateObservedTimestamp(gatewayNodeID, now)
	}

	txn := &Txn{db: db, typ: typ, gatewayNodeID: gatewayNodeID}
	txn.mu.ID = proto.ID
	txn.mu.userPriority = roachpb.NormalUserPriority
	txn.mu.sender = db.factory.RootTransactionalSender(proto, txn.mu.userPriority)
	return txn
}

// NewLeafTxn instantiates a new leaf transaction.
func NewLeafTxn(
	ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID, tis *roachpb.LeafTxnInputState,
) *Txn {
	if db == nil {
		panic(errors.WithContextTags(
			errors.AssertionFailedf("attempting to create leaf txn with nil db for Transaction: %s", tis.Txn), ctx))
	}
	if tis.Txn.Status != roachpb.PENDING {
		panic(errors.WithContextTags(
			errors.AssertionFailedf("can't create leaf txn with non-PENDING proto: %s", tis.Txn), ctx))
	}
	tis.Txn.AssertInitialized(ctx)
	txn := &Txn{db: db, typ: LeafTxn, gatewayNodeID: gatewayNodeID}
	txn.mu.ID = tis.Txn.ID
	txn.mu.userPriority = roachpb.NormalUserPriority
	txn.mu.sender = db.factory.LeafTransactionalSender(tis)
	return txn
}

// DB returns a transaction's DB.
func (txn *Txn) DB() *DB {
	return txn.db
}

// Sender returns a transaction's TxnSender.
func (txn *Txn) Sender() TxnSender {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender
}

// ID returns the current ID of the transaction.
func (txn *Txn) ID() uuid.UUID {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.ID
}

// Epoch exports the txn's epoch.
func (txn *Txn) Epoch() enginepb.TxnEpoch {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.Epoch()
}

// status returns the txn proto status field.
func (txn *Txn) status() roachpb.TransactionStatus {
	return txn.mu.sender.TxnStatus()
}

// IsCommitted returns true iff the transaction has the committed status.
func (txn *Txn) IsCommitted() bool {
	return txn.status() == roachpb.COMMITTED
}

// IsOpen returns true iff the transaction is in the open state where
// it can accept further commands.
func (txn *Txn) IsOpen() bool {
	return txn.status() == roachpb.PENDING
}

// SetUserPriority sets the transaction's user priority. Transactions default to
// normal user priority. The user priority must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetUserPriority(userPriority roachpb.UserPriority) error {
	if txn.typ != RootTxn {
		panic(errors.AssertionFailedf("SetUserPriority() called on leaf txn"))
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.userPriority == userPriority {
		return nil
	}

	if userPriority < roachpb.MinUserPriority || userPriority > roachpb.MaxUserPriority {
		return errors.AssertionFailedf("the given user priority %f is out of the allowed range [%f, %f]",
			userPriority, roachpb.MinUserPriority, roachpb.MaxUserPriority)
	}

	txn.mu.userPriority = userPriority
	return txn.mu.sender.SetUserPriority(userPriority)
}

// TestingSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) TestingSetPriority(priority enginepb.TxnPriority) {
	txn.mu.Lock()
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.mu.userPriority = roachpb.UserPriority(-priority)
	if err := txn.mu.sender.SetUserPriority(txn.mu.userPriority); err != nil {
		log.Fatalf(context.TODO(), "%+v", err)
	}
	txn.mu.Unlock()
}

// UserPriority returns the transaction's user priority.
func (txn *Txn) UserPriority() roachpb.UserPriority {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.userPriority
}

// SetDebugName sets the debug name associated with the transaction which will
// appear in log files and the web UI.
func (txn *Txn) SetDebugName(name string) {
	if txn.typ != RootTxn {
		panic(errors.AssertionFailedf("SetDebugName() called on leaf txn"))
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.mu.sender.SetDebugName(name)
	txn.mu.debugName = name
}

// DebugName returns the debug name associated with the transaction.
func (txn *Txn) DebugName() string {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.debugNameLocked()
}

func (txn *Txn) debugNameLocked() string {
	return fmt.Sprintf("%s (id: %s)", txn.mu.debugName, txn.mu.ID)
}

// String returns a string version of this transaction.
func (txn *Txn) String() string {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.String()
}

// ReadTimestamp returns the transaction's current read timestamp.
// Note a transaction can be internally pushed forward in time before
// committing so this is not guaranteed to be the commit timestamp.
// Use CommitTimestamp() when needed.
func (txn *Txn) ReadTimestamp() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.readTimestampLocked()
}

func (txn *Txn) readTimestampLocked() hlc.Timestamp {
	return txn.mu.sender.ReadTimestamp()
}

// CommitTimestamp returns the transaction's start timestamp.
// The start timestamp can get pushed but the use of this
// method will guarantee that if a timestamp push is needed
// the commit will fail with a retryable error.
func (txn *Txn) CommitTimestamp() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.CommitTimestamp()
}

// ProvisionalCommitTimestamp returns the transaction's provisional
// commit timestamp. This can evolve throughout a txn's lifecycle. See
// the comment on the WriteTimestamp field of TxnMeta for details.
func (txn *Txn) ProvisionalCommitTimestamp() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.ProvisionalCommitTimestamp()
}

// RequiredFrontier returns the largest timestamp at which the transaction may
// read values when performing a read-only operation.
func (txn *Txn) RequiredFrontier() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.RequiredFrontier()
}

// SetSystemConfigTrigger sets the system db trigger to true on this transaction.
// This will impact the EndTxnRequest. Note that this method takes a boolean
// argument indicating whether this transaction is intended for the system
// tenant. Only transactions for the system tenant need to set the system config
// trigger which is used to gossip updates to the system config to KV servers.
// The KV servers need access to an up-to-date system config in order to
// determine split points and zone configurations.
func (txn *Txn) SetSystemConfigTrigger(forSystemTenant bool) error {
	if txn.typ != RootTxn {
		return errors.AssertionFailedf("SetSystemConfigTrigger() called on leaf txn")
	}
	if !forSystemTenant {
		return nil
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	if err := txn.mu.sender.AnchorOnSystemConfigRange(); err != nil {
		return err
	}
	txn.systemConfigTrigger = true
	return nil
}

// DisablePipelining instructs the transaction not to pipeline requests. It
// should rarely be necessary to call this method.
//
// DisablePipelining must be called before any operations are performed on the
// transaction.
func (txn *Txn) DisablePipelining() error {
	if txn.typ != RootTxn {
		return errors.AssertionFailedf("DisablePipelining() called on leaf txn")
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.DisablePipelining()
}

// NewBatch creates and returns a new empty batch object for use with the Txn.
func (txn *Txn) NewBatch() *Batch {
	return &Batch{txn: txn, AdmissionHeader: txn.admissionHeader}
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error. It is not considered an error for the key to not exist.
//
//   r, err := txn.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) Get(ctx context.Context, key interface{}) (KeyValue, error) {
	b := txn.NewBatch()
	b.Get(key)
	return getOneRow(txn.Run(ctx, b), b)
}

// GetForUpdate retrieves the value for a key, returning the retrieved key/value
// or an error. An unreplicated, exclusive lock is acquired on the key, if it
// exists. It is not considered an error for the key to not exist.
//
//   r, err := txn.GetForUpdate("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) GetForUpdate(ctx context.Context, key interface{}) (KeyValue, error) {
	b := txn.NewBatch()
	b.GetForUpdate(key)
	return getOneRow(txn.Run(ctx, b), b)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message. If the key doesn't exist, the proto will simply be reset.
//
// key can be either a byte slice or a string.
func (txn *Txn) GetProto(ctx context.Context, key interface{}, msg protoutil.Message) error {
	_, err := txn.GetProtoTs(ctx, key, msg)
	return err
}

// GetProtoTs retrieves the value for a key and decodes the result as a proto
// message. It additionally returns the timestamp at which the key was read.
// If the key doesn't exist, the proto will simply be reset and a zero timestamp
// will be returned. A zero timestamp will also be returned if unmarshaling
// fails.
//
// key can be either a byte slice or a string.
func (txn *Txn) GetProtoTs(
	ctx context.Context, key interface{}, msg protoutil.Message,
) (hlc.Timestamp, error) {
	r, err := txn.Get(ctx, key)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if err := r.ValueProto(msg); err != nil || r.Value == nil {
		return hlc.Timestamp{}, err
	}
	return r.Value.Timestamp, nil
}

// Put sets the value for a key
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
func (txn *Txn) Put(ctx context.Context, key, value interface{}) error {
	b := txn.NewBatch()
	b.Put(key, value)
	return getOneErr(txn.Run(ctx, b), b)
}

// CPut conditionally sets the value for a key if the existing value is equal to
// expValue. To conditionally set a value only if the key doesn't currently
// exist, pass an empty expValue.
//
// Returns a ConditionFailedError if the existing value is not equal to expValue.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
//
// An empty expValue means that the key is expected to not exist. If not empty,
// expValue needs to correspond to a Value.TagAndDataBytes() - i.e. a key's
// value without the checksum (as the checksum includes the key too).
//
// Note that, as an exception to the general rule, it's ok to send more requests
// after getting a ConditionFailedError. See comments on ConditionalPutRequest
// for more info.
func (txn *Txn) CPut(ctx context.Context, key, value interface{}, expValue []byte) error {
	b := txn.NewBatch()
	b.CPut(key, value, expValue)
	return getOneErr(txn.Run(ctx, b), b)
}

// InitPut sets the first value for a key to value. An error is reported if a
// value already exists for the key and it's not equal to the value passed in.
// If failOnTombstones is set to true, tombstones count as mismatched values
// and will cause a ConditionFailedError.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc). It is illegal to
// set value to nil.
func (txn *Txn) InitPut(ctx context.Context, key, value interface{}, failOnTombstones bool) error {
	b := txn.NewBatch()
	b.InitPut(key, value, failOnTombstones)
	return getOneErr(txn.Run(ctx, b), b)
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// The returned Result will contain a single row and Result.Err will indicate
// success or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) Inc(ctx context.Context, key interface{}, value int64) (KeyValue, error) {
	b := txn.NewBatch()
	b.Inc(key, value)
	return getOneRow(txn.Run(ctx, b), b)
}

func (txn *Txn) scan(
	ctx context.Context, begin, end interface{}, maxRows int64, isReverse, forUpdate bool,
) ([]KeyValue, error) {
	b := txn.NewBatch()
	if maxRows > 0 {
		b.Header.MaxSpanRequestKeys = maxRows
	}
	b.scan(begin, end, isReverse, forUpdate)
	r, err := getOneResult(txn.Run(ctx, b), b)
	return r.Rows, err
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) Scan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, false /* isReverse */, false /* forUpdate */)
}

// ScanForUpdate retrieves the rows between begin (inclusive) and end
// (exclusive) in ascending order. Unreplicated, exclusive locks are acquired on
// each of the returned keys.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ScanForUpdate(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, false /* isReverse */, true /* forUpdate */)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ReverseScan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, true /* isReverse */, false /* forUpdate */)
}

// ReverseScanForUpdate retrieves the rows between begin (inclusive) and end
// (exclusive) in descending order. Unreplicated, exclusive locks are acquired
// on each of the returned keys.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ReverseScanForUpdate(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, true /* isReverse */, true /* forUpdate */)
}

// Iterate performs a paginated scan and applying the function f to every page.
// The semantics of retrieval and ordering are the same as for Scan. Note that
// Txn auto-retries the transaction if necessary. Hence, the paginated data
// must not be used for side-effects before the txn has committed.
func (txn *Txn) Iterate(
	ctx context.Context, begin, end interface{}, pageSize int, f func([]KeyValue) error,
) error {
	for {
		rows, err := txn.Scan(ctx, begin, end, int64(pageSize))
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if err := f(rows); err != nil {
			return errors.Wrap(err, "running iterate callback")
		}
		if len(rows) < pageSize {
			return nil
		}
		begin = rows[len(rows)-1].Key.Next()
	}
}

// Del deletes one or more keys.
//
// key can be either a byte slice or a string.
func (txn *Txn) Del(ctx context.Context, keys ...interface{}) error {
	b := txn.NewBatch()
	b.Del(keys...)
	return getOneErr(txn.Run(ctx, b), b)
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// The returned Result will contain 0 rows and Result.Err will indicate success
// or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) DelRange(ctx context.Context, begin, end interface{}) error {
	b := txn.NewBatch()
	b.DelRange(begin, end, false)
	return getOneErr(txn.Run(ctx, b), b)
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
func (txn *Txn) Run(ctx context.Context, b *Batch) error {
	if err := b.prepare(); err != nil {
		return err
	}
	return sendAndFill(ctx, txn.Send, b)
}

func (txn *Txn) commit(ctx context.Context) error {
	// A batch with only endTxnReq is not subject to admission control, in order
	// to reduce contention by releasing locks. In multi-tenant settings, it
	// will be subject to admission control, and the zero CreateTime will give
	// it preference within the tenant.
	var ba roachpb.BatchRequest
	ba.Add(endTxnReq(true /* commit */, txn.deadline(), txn.systemConfigTrigger))
	_, pErr := txn.Send(ctx, ba)
	if pErr == nil {
		for _, t := range txn.commitTriggers {
			t(ctx)
		}
	}
	return pErr.GoError()
}

// CleanupOnError cleans up the transaction as a result of an error.
func (txn *Txn) CleanupOnError(ctx context.Context, err error) {
	if txn.typ != RootTxn {
		panic(errors.WithContextTags(errors.AssertionFailedf("CleanupOnError() called on leaf txn"), ctx))
	}

	if err == nil {
		panic(errors.WithContextTags(errors.AssertionFailedf("CleanupOnError() called with nil error"), ctx))
	}
	if replyErr := txn.rollback(ctx); replyErr != nil {
		if _, ok := replyErr.GetDetail().(*roachpb.TransactionStatusError); ok || txn.status() == roachpb.ABORTED {
			log.Eventf(ctx, "failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		} else {
			log.Warningf(ctx, "failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
	}
}

// Commit is the same as CommitOrCleanup but will not attempt to clean
// up on failure. This can be used when the caller is prepared to do proper
// cleanup.
func (txn *Txn) Commit(ctx context.Context) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.AssertionFailedf("Commit() called on leaf txn"), ctx)
	}

	return txn.commit(ctx)
}

// CommitInBatch executes the operations queued up within a batch and
// commits the transaction. Explicitly committing a transaction is
// optional, but more efficient than relying on the implicit commit
// performed when the transaction function returns without error.
// The batch must be created by this transaction.
// If the command completes successfully, the txn is considered finalized. On
// error, no attempt is made to clean up the (possibly still pending)
// transaction.
func (txn *Txn) CommitInBatch(ctx context.Context, b *Batch) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.AssertionFailedf("CommitInBatch() called on leaf txn"), ctx)
	}

	if txn != b.txn {
		return errors.Errorf("a batch b can only be committed by b.txn")
	}
	b.appendReqs(endTxnReq(true /* commit */, txn.deadline(), txn.systemConfigTrigger))
	b.initResult(1 /* calls */, 0, b.raw, nil)
	return txn.Run(ctx, b)
}

// CommitOrCleanup sends an EndTxnRequest with Commit=true.
// If that fails, an attempt to rollback is made.
// txn should not be used to send any more commands after this call.
func (txn *Txn) CommitOrCleanup(ctx context.Context) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.AssertionFailedf("CommitOrCleanup() called on leaf txn"), ctx)
	}

	err := txn.commit(ctx)
	if err != nil {
		txn.CleanupOnError(ctx, err)
	}
	return err
}

// UpdateDeadline sets the transactions deadline to the passed deadline.
// It may move the deadline to any timestamp above the current read timestamp.
// If the deadline is below the current provisional commit timestamp (write timestamp),
// then the transaction will fail with a deadline error during the commit.
// The deadline cannot be lower than txn.ReadTimestamp and we make the assumption
// the read timestamp will not change during execution, which is valid today.
func (txn *Txn) UpdateDeadline(ctx context.Context, deadline hlc.Timestamp) error {
	if txn.typ != RootTxn {
		panic(errors.WithContextTags(errors.AssertionFailedf("UpdateDeadline() called on leaf txn"), ctx))
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	readTimestamp := txn.readTimestampLocked()
	if deadline.Less(readTimestamp) {
		return errors.AssertionFailedf("deadline below read timestamp is nonsensical; "+
			"txn has would have no chance to commit. Deadline: %s. Read timestamp: %s Previous Deadline: %s.",
			deadline, readTimestamp, txn.mu.deadline)
	}
	txn.mu.deadline = new(hlc.Timestamp)
	*txn.mu.deadline = deadline
	return nil
}

// resetDeadlineLocked resets the deadline.
func (txn *Txn) resetDeadlineLocked() {
	txn.mu.deadline = nil
}

// Rollback sends an EndTxnRequest with Commit=false.
// txn is considered finalized and cannot be used to send any more commands.
func (txn *Txn) Rollback(ctx context.Context) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.AssertionFailedf("Rollback() called on leaf txn"), ctx)
	}

	return txn.rollback(ctx).GoError()
}

func (txn *Txn) rollback(ctx context.Context) *roachpb.Error {
	log.VEventf(ctx, 2, "rolling back transaction")

	// If the client has already disconnected, fall back to asynchronous cleanup
	// below. Note that this is the common path when a client disconnects in the
	// middle of an open transaction or during statement execution.
	if ctx.Err() == nil {
		// A batch with only endTxnReq is not subject to admission control, in
		// order to reduce contention by releasing locks. In multi-tenant
		// settings, it will be subject to admission control, and the zero
		// CreateTime will give it preference within the tenant.
		var ba roachpb.BatchRequest
		ba.Add(endTxnReq(false /* commit */, nil /* deadline */, false /* systemConfigTrigger */))
		_, pErr := txn.Send(ctx, ba)
		if pErr == nil {
			return nil
		}
		// If rollback errored and the ctx was canceled during rollback, assume
		// ctx cancellation caused the error and try again async below.
		if ctx.Err() == nil {
			return pErr
		}
	}

	// We don't have a client whose context we can attach to, but we do want to
	// limit how long this request is going to be around for to avoid leaking a
	// goroutine (in case of a long-lived network partition). If it gets through
	// Raft, and the intent resolver has free async task capacity, the actual
	// cleanup will be independent of this context.
	stopper := txn.db.ctx.Stopper
	ctx, cancel := stopper.WithCancelOnQuiesce(txn.db.AnnotateCtx(context.Background()))
	if err := stopper.RunAsyncTask(ctx, "async-rollback", func(ctx context.Context) {
		defer cancel()
		// A batch with only endTxnReq is not subject to admission control, in
		// order to reduce contention by releasing locks. In multi-tenant
		// settings, it will be subject to admission control, and the zero
		// CreateTime will give it preference within the tenant.
		var ba roachpb.BatchRequest
		ba.Add(endTxnReq(false /* commit */, nil /* deadline */, false /* systemConfigTrigger */))
		_ = contextutil.RunWithTimeout(ctx, "async txn rollback", asyncRollbackTimeout,
			func(ctx context.Context) error {
				if _, pErr := txn.Send(ctx, ba); pErr != nil {
					if statusErr, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); ok &&
						statusErr.Reason == roachpb.TransactionStatusError_REASON_TXN_COMMITTED {
						// A common cause of these async rollbacks failing is when they're
						// triggered by a ctx canceled while a commit is in-flight (and it's too
						// late for it to be canceled), and so the rollback finds the txn to be
						// already committed. We don't spam the logs with those.
						log.VEventf(ctx, 2, "async rollback failed: %s", pErr)
					} else {
						log.Infof(ctx, "async rollback failed: %s", pErr)
					}
				}
				return nil
			})
	}); err != nil {
		cancel()
		return roachpb.NewError(err)
	}
	return nil
}

// AddCommitTrigger adds a closure to be executed on successful commit
// of the transaction.
func (txn *Txn) AddCommitTrigger(trigger func(ctx context.Context)) {
	if txn.typ != RootTxn {
		panic(errors.AssertionFailedf("AddCommitTrigger() called on leaf txn"))
	}

	txn.commitTriggers = append(txn.commitTriggers, trigger)
}

func endTxnReq(commit bool, deadline *hlc.Timestamp, hasTrigger bool) roachpb.Request {
	req := &roachpb.EndTxnRequest{
		Commit:   commit,
		Deadline: deadline,
	}
	if hasTrigger {
		req.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
			ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
				SystemConfigSpan: true,
			},
		}
	}
	return req
}

// AutoCommitError wraps a non-retryable error coming from auto-commit.
type AutoCommitError struct {
	cause error
}

func (e *AutoCommitError) Error() string {
	return e.cause.Error()
}

// exec executes fn in the context of a distributed transaction. The closure is
// retried on retriable errors.
// If no error is returned by the closure, an attempt to commit the txn is made.
//
// When this method returns, txn might be in any state; exec does not attempt
// to clean up the transaction before returning an error. In case of
// TransactionAbortedError, txn is reset to a fresh transaction, ready to be
// used.
func (txn *Txn) exec(ctx context.Context, fn func(context.Context, *Txn) error) (err error) {
	// Run fn in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err = fn(ctx, txn)

		// Commit on success, unless the txn has already been committed by the
		// closure. We allow that, as closure might want to run 1PC transactions.
		if err == nil {
			if txn.status() != roachpb.COMMITTED {
				err = txn.Commit(ctx)
				log.Eventf(ctx, "client.Txn did AutoCommit. err: %v\n", err)
				if err != nil {
					if !errors.HasType(err, (*roachpb.TransactionRetryWithProtoRefreshError)(nil)) {
						// We can't retry, so let the caller know we tried to
						// autocommit.
						err = &AutoCommitError{cause: err}
					}
				}
			}
		}

		var retryable bool
		if errors.HasType(err, (*roachpb.UnhandledRetryableError)(nil)) {
			if txn.typ == RootTxn {
				// We sent transactional requests, so the TxnCoordSender was supposed to
				// turn retryable errors into TransactionRetryWithProtoRefreshError. Note that this
				// applies only in the case where this is the root transaction.
				log.Fatalf(ctx, "unexpected UnhandledRetryableError at the txn.exec() level: %s", err)
			}

		} else if t := (*roachpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(err, &t) {
			if !txn.IsRetryableErrMeantForTxn(*t) {
				// Make sure the txn record that err carries is for this txn.
				// If it's not, we terminate the "retryable" character of the error. We
				// might get a TransactionRetryWithProtoRefreshError if the closure ran another
				// transaction internally and let the error propagate upwards.
				return errors.Wrapf(err, "retryable error from another txn")
			}
			retryable = true
		}

		if !retryable {
			break
		}

		txn.PrepareForRetry(ctx, err)
	}

	return err
}

// PrepareForRetry needs to be called before an retry to perform some
// book-keeping.
//
// TODO(andrei): I think this is called in the wrong place. See #18170.
func (txn *Txn) PrepareForRetry(ctx context.Context, err error) {
	if txn.typ != RootTxn {
		panic(errors.WithContextTags(errors.NewAssertionErrorWithWrappedErrf(err, "PrepareForRetry() called on leaf txn"), ctx))
	}

	txn.commitTriggers = nil
	log.VEventf(ctx, 2, "automatically retrying transaction: %s because of error: %s",
		txn.DebugName(), err)
}

// IsRetryableErrMeantForTxn returns true if err is a retryable
// error meant to restart this client transaction.
func (txn *Txn) IsRetryableErrMeantForTxn(
	retryErr roachpb.TransactionRetryWithProtoRefreshError,
) bool {
	if txn.typ != RootTxn {
		panic(errors.AssertionFailedf("IsRetryableErrMeantForTxn() called on leaf txn"))
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	errTxnID := retryErr.TxnID

	// Make sure the txn record that err carries is for this txn.
	// First check if the error was meant for a previous incarnation
	// of the transaction.
	if _, ok := txn.mu.previousIDs[errTxnID]; ok {
		return true
	}
	// If not, make sure it was meant for this transaction.
	return errTxnID == txn.mu.ID
}

// Send runs the specified calls synchronously in a single batch and
// returns any errors. If the transaction is read-only or has already
// been successfully committed or aborted, a potential trailing
// EndTxn call is silently dropped, allowing the caller to always
// commit or clean-up explicitly even when that may not be required
// (or even erroneous). Returns (nil, nil) for an empty batch.
func (txn *Txn) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Fill in the GatewayNodeID on the batch if the txn knows it.
	// NOTE(andrei): It seems a bit ugly that we're filling in the batches here as
	// opposed to the point where the requests are being created, but
	// unfortunately requests are being created in many ways and this was the best
	// place I found to set this field.
	if txn.gatewayNodeID != 0 {
		ba.Header.GatewayNodeID = txn.gatewayNodeID
	}

	// Some callers have not initialized ba using a Batch constructed using
	// Txn.NewBatch. So we fallback to initializing here.
	ba.AdmissionHeader = txn.admissionHeader

	txn.mu.Lock()
	requestTxnID := txn.mu.ID
	sender := txn.mu.sender
	txn.mu.Unlock()
	br, pErr := txn.db.sendUsingSender(ctx, ba, sender)
	if pErr == nil {
		return br, nil
	}

	if retryErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryWithProtoRefreshError); ok {
		if requestTxnID != retryErr.TxnID {
			// KV should not return errors for transactions other than the one that sent
			// the request.
			log.Fatalf(ctx, "retryable error for the wrong txn. "+
				"requestTxnID: %s, retryErr.TxnID: %s. retryErr: %s",
				requestTxnID, retryErr.TxnID, retryErr)
		}
		if txn.typ == RootTxn {
			// On root senders, we bump the sender's identity upon retry errors.
			txn.mu.Lock()
			txn.handleErrIfRetryableLocked(ctx, retryErr)
			txn.mu.Unlock()
		}
	}
	return br, pErr
}

func (txn *Txn) handleErrIfRetryableLocked(ctx context.Context, err error) {
	var retryErr *roachpb.TransactionRetryWithProtoRefreshError
	if !errors.As(err, &retryErr) {
		return
	}
	txn.resetDeadlineLocked()
	txn.replaceRootSenderIfTxnAbortedLocked(ctx, retryErr, retryErr.TxnID)
}

// GetLeafTxnInputState returns the LeafTxnInputState information for this
// transaction for use with InitializeLeafTxn(), when distributing
// the state of the current transaction to multiple distributed
// transaction coordinators.
func (txn *Txn) GetLeafTxnInputState(ctx context.Context) roachpb.LeafTxnInputState {
	if txn.typ != RootTxn {
		panic(errors.WithContextTags(errors.AssertionFailedf("GetLeafTxnInputState() called on leaf txn"), ctx))
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	ts, err := txn.mu.sender.GetLeafTxnInputState(ctx, AnyTxnStatus)
	if err != nil {
		log.Fatalf(ctx, "unexpected error from GetLeafTxnInputState(AnyTxnStatus): %s", err)
	}
	return ts
}

// GetLeafTxnInputStateOrRejectClient is like GetLeafTxnInputState
// except, if the transaction is already aborted or otherwise in state
// that cannot make progress, it returns an error. If the transaction
// is aborted, the error will be a retryable one, and the transaction
// will have been prepared for another transaction attempt (so, on
// retryable errors, it acts like Send()).
func (txn *Txn) GetLeafTxnInputStateOrRejectClient(
	ctx context.Context,
) (roachpb.LeafTxnInputState, error) {
	if txn.typ != RootTxn {
		return roachpb.LeafTxnInputState{},
			errors.WithContextTags(errors.AssertionFailedf("GetLeafTxnInputStateOrRejectClient() called on leaf txn"), ctx)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	tfs, err := txn.mu.sender.GetLeafTxnInputState(ctx, OnlyPending)
	if err != nil {
		txn.handleErrIfRetryableLocked(ctx, err)
		return roachpb.LeafTxnInputState{}, err
	}
	return tfs, nil
}

// GetLeafTxnFinalState returns the LeafTxnFinalState information for this
// transaction for use with UpdateRootWithLeafFinalState(), when combining the
// impact of multiple distributed transaction coordinators that are
// all operating on the same transaction.
func (txn *Txn) GetLeafTxnFinalState(ctx context.Context) (roachpb.LeafTxnFinalState, error) {
	if txn.typ != LeafTxn {
		return roachpb.LeafTxnFinalState{},
			errors.WithContextTags(
				errors.AssertionFailedf("GetLeafTxnFinalState() called on root txn"), ctx)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	tfs, err := txn.mu.sender.GetLeafTxnFinalState(ctx, AnyTxnStatus)
	if err != nil {
		return roachpb.LeafTxnFinalState{},
			errors.WithContextTags(
				errors.NewAssertionErrorWithWrappedErrf(err,
					"unexpected error from GetLeafTxnFinalState(AnyTxnStatus)"), ctx)
	}
	return tfs, nil
}

// UpdateRootWithLeafFinalState augments this RootTxn with the supplied
// LeafTxn final state. For use with GetLeafTxnFinalState().
func (txn *Txn) UpdateRootWithLeafFinalState(
	ctx context.Context, tfs *roachpb.LeafTxnFinalState,
) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(
			errors.AssertionFailedf("UpdateRootWithLeafFinalState() called on leaf txn"), ctx)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.sender.UpdateRootWithLeafFinalState(ctx, tfs)
	return nil
}

// UpdateStateOnRemoteRetryableErr updates the txn in response to an error
// encountered when running a request through the txn. Returns a
// TransactionRetryWithProtoRefreshError on success or another error on failure.
func (txn *Txn) UpdateStateOnRemoteRetryableErr(ctx context.Context, pErr *roachpb.Error) error {
	if txn.typ != RootTxn {
		return errors.AssertionFailedf("UpdateStateOnRemoteRetryableErr() called on leaf txn")
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if pErr.TransactionRestart() == roachpb.TransactionRestart_NONE {
		log.Fatalf(ctx, "unexpected non-retryable error: %s", pErr)
	}

	// If the transaction has been reset since this request was sent,
	// ignore the error.
	// Note that in case of TransactionAbortedError, pErr.GetTxn() returns the
	// original transaction; a new transaction has not been created yet.
	origTxnID := pErr.GetTxn().ID
	if origTxnID != txn.mu.ID {
		return errors.Errorf("retryable error for an older version of txn (current: %s), err: %s",
			txn.mu.ID, pErr)
	}

	pErr = txn.mu.sender.UpdateStateOnRemoteRetryableErr(ctx, pErr)
	txn.replaceRootSenderIfTxnAbortedLocked(ctx, pErr.GetDetail().(*roachpb.TransactionRetryWithProtoRefreshError), origTxnID)

	return pErr.GoError()
}

// replaceRootSenderIfTxnAbortedLocked handles
// TransactionAbortedErrors, on which a new sender is created to
// replace the current one.
//
// origTxnID is the id of the txn that generated retryErr. Note that this can be
// different from retryErr.Transaction - the latter might be a new transaction.
func (txn *Txn) replaceRootSenderIfTxnAbortedLocked(
	ctx context.Context, retryErr *roachpb.TransactionRetryWithProtoRefreshError, origTxnID uuid.UUID,
) {
	// The proto inside the error has been prepared for use by the next
	// transaction attempt.
	newTxn := &retryErr.Transaction

	if txn.mu.ID != origTxnID {
		// The transaction has changed since the request that generated the error
		// was sent. Nothing more to do.
		log.VEventf(ctx, 2, "retriable error for old incarnation of the transaction")
		return
	}
	if !retryErr.PrevTxnAborted() {
		// We don't need a new transaction as a result of this error. Nothing more
		// to do.
		return
	}

	// The ID changed, which means that the cause was a TransactionAbortedError;
	// we've created a new Transaction that we're about to start using, so we save
	// the old transaction ID so that concurrent requests or delayed responses
	// that that throw errors know that these errors were sent to the correct
	// transaction, even once the proto is reset.
	txn.recordPreviousTxnIDLocked(txn.mu.ID)
	txn.mu.ID = newTxn.ID
	// Create a new txn sender. We need to preserve the stepping mode,
	// if any.
	// prevSteppingMode := txn.mu.sender.GetSteppingMode(ctx)
	txn.mu.sender = txn.db.factory.RootTransactionalSender(newTxn, txn.mu.userPriority)
	// txn.mu.sender.ConfigureStepping(ctx, prevSteppingMode)
}

func (txn *Txn) recordPreviousTxnIDLocked(prevTxnID uuid.UUID) {
	if txn.mu.previousIDs == nil {
		txn.mu.previousIDs = make(map[uuid.UUID]struct{})
	}
	txn.mu.previousIDs[txn.mu.ID] = struct{}{}
}

// SetFixedTimestamp makes the transaction run in an unusual way, at a "fixed
// timestamp": Timestamp and RefreshedTimestamp are set to ts, there's no clock
// uncertainty, and the txn's deadline is set to ts such that the transaction
// can't be pushed to a different timestamp.
//
// This is used to support historical queries (AS OF SYSTEM TIME queries and
// backups). This method must be called on every transaction retry (but note
// that retries should be rare for read-only queries with no clock uncertainty).
func (txn *Txn) SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) {
	if txn.typ != RootTxn {
		panic(errors.WithContextTags(
			errors.AssertionFailedf("SetFixedTimestamp() called on leaf txn"), ctx))
	}

	if ts.IsEmpty() {
		log.Fatalf(ctx, "empty timestamp is invalid for SetFixedTimestamp()")
	}
	txn.mu.sender.SetFixedTimestamp(ctx, ts)
}

// GenerateForcedRetryableError returns a TransactionRetryWithProtoRefreshError that will
// cause the txn to be retried.
//
// The transaction's epoch is bumped, simulating to an extent what the
// TxnCoordSender does on retriable errors. The transaction's timestamp is only
// bumped to the extent that txn.ReadTimestamp is racheted up to txn.WriteTimestamp.
// TODO(andrei): This method should take in an up-to-date timestamp, but
// unfortunately its callers don't currently have that handy.
func (txn *Txn) GenerateForcedRetryableError(ctx context.Context, msg string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	now := txn.db.clock.NowAsClockTimestamp()
	txn.mu.sender.ManualRestart(ctx, txn.mu.userPriority, now.ToTimestamp())
	txn.resetDeadlineLocked()
	return roachpb.NewTransactionRetryWithProtoRefreshError(
		msg,
		txn.mu.ID,
		roachpb.MakeTransaction(
			txn.debugNameLocked(),
			nil, // baseKey
			txn.mu.userPriority,
			now.ToTimestamp(),
			txn.db.clock.MaxOffset().Nanoseconds(),
		))
}

// PrepareRetryableError returns a
// TransactionRetryWithProtoRefreshError that will cause the txn to be
// retried. The current txn parameters are used. The txn remains valid
// for use.
func (txn *Txn) PrepareRetryableError(ctx context.Context, msg string) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(
			errors.AssertionFailedf("PrepareRetryableError() called on leaf txn"), ctx)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.PrepareRetryableError(ctx, msg)
}

// ManualRestart bumps the transactions epoch, and can upgrade the timestamp.
// An uninitialized timestamp can be passed to leave the timestamp alone.
//
// Used by the SQL layer which sometimes knows that a transaction will not be
// able to commit and prefers to restart early.
// It is also used after synchronizing concurrent actors using a txn when a
// retryable error is seen.
// TODO(andrei): this second use should go away once we move to a TxnAttempt
// model.
func (txn *Txn) ManualRestart(ctx context.Context, ts hlc.Timestamp) {
	if txn.typ != RootTxn {
		panic(errors.WithContextTags(
			errors.AssertionFailedf("ManualRestart() called on leaf txn"), ctx))
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.sender.ManualRestart(ctx, txn.mu.userPriority, ts)
}

// IsSerializablePushAndRefreshNotPossible returns true if the transaction is
// serializable, its timestamp has been pushed and there's no chance that
// refreshing the read spans will succeed later (thus allowing the transaction
// to commit and not be restarted). Used to detect whether the txn is guaranteed
// to get a retriable error later.
//
// Note that this method allows for false negatives: sometimes the client only
// figures out that it's been pushed when it sends an EndTxn - i.e. it's
// possible for the txn to have been pushed asynchoronously by some other
// operation (usually, but not exclusively, by a high-priority txn with
// conflicting writes).
func (txn *Txn) IsSerializablePushAndRefreshNotPossible() bool {
	return txn.mu.sender.IsSerializablePushAndRefreshNotPossible()
}

// Type returns the transaction's type.
func (txn *Txn) Type() TxnType {
	return txn.typ
}

// TestingCloneTxn returns a clone of the current txn.
// This is for use by tests only. Leaf txns should be derived
// using GetLeafTxnInitialState() and NewLeafTxn().
func (txn *Txn) TestingCloneTxn() *roachpb.Transaction {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.TestingCloneTxn()
}

func (txn *Txn) deadline() *hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.deadline
}

// Active returns true iff some commands have been performed with
// this txn already.
//
// TODO(knz): Remove this, see
// https://github.com/cockroachdb/cockroach/issues/15012
func (txn *Txn) Active() bool {
	if txn.typ != RootTxn {
		panic(errors.AssertionFailedf("Active() called on leaf txn"))
	}
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.Active()
}

// Step performs a sequencing step. Step-wise execution must be
// already enabled.
//
// In step-wise execution, reads operate at a snapshot established at
// the last step, instead of the latest write if not yet enabled.
func (txn *Txn) Step(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.Step(ctx)
}

// ConfigureStepping configures step-wise execution in the
// transaction.
func (txn *Txn) ConfigureStepping(ctx context.Context, mode SteppingMode) (prevMode SteppingMode) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.ConfigureStepping(ctx, mode)
}

// CreateSavepoint establishes a savepoint.
// This method is only valid when called on RootTxns.
func (txn *Txn) CreateSavepoint(ctx context.Context) (SavepointToken, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.CreateSavepoint(ctx)
}

// RollbackToSavepoint rolls back to the given savepoint.
// All savepoints "under" the savepoint being rolled back
// are also rolled back and their token must not be used any more.
// The token of the savepoint being rolled back remains valid
// and can be reused later (e.g. to release or roll back again).
//
// This method is only valid when called on RootTxns.
func (txn *Txn) RollbackToSavepoint(ctx context.Context, s SavepointToken) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.RollbackToSavepoint(ctx, s)
}

// ReleaseSavepoint releases the given savepoint. The savepoint
// must not have been rolled back or released already.
// All savepoints "under" the savepoint being released
// are also released and their token must not be used any more.
// This method is only valid when called on RootTxns.
func (txn *Txn) ReleaseSavepoint(ctx context.Context, s SavepointToken) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.ReleaseSavepoint(ctx, s)
}

// ManualRefresh forces a refresh of the read timestamp of a transaction to
// match that of its write timestamp. It is only recommended for transactions
// that need extremely precise control over the request ordering, like the
// transaction that merges ranges together. When combined with
// DisablePipelining, this feature allows the range merge transaction to
// prove that it will not be pushed between sending its SubsumeRequest and
// committing. This enables that request to be pushed at earlier points in
// its lifecycle.
func (txn *Txn) ManualRefresh(ctx context.Context) error {
	txn.mu.Lock()
	sender := txn.mu.sender
	txn.mu.Unlock()
	return sender.ManualRefresh(ctx)
}

// DeferCommitWait defers the transaction's commit-wait operation, passing
// responsibility of commit-waiting from the Txn to the caller of this
// method. The method returns a function which the caller must eventually
// run if the transaction completes without error. This function is safe to
// call multiple times.
//
// WARNING: failure to run the returned function could lead to consistency
// violations where a future, causally dependent transaction may fail to
// observe the writes performed by this transaction.
func (txn *Txn) DeferCommitWait(ctx context.Context) func(context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.DeferCommitWait(ctx)
}

// AdmissionHeader returns the admission header for work done in the context
// of this transaction.
func (txn *Txn) AdmissionHeader() roachpb.AdmissionHeader {
	return txn.admissionHeader
}
