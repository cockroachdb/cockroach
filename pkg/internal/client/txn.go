// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package client

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

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
	// span. This sets the SystemConfigTrigger on EndTransactionRequest.
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
}

// NewTxn returns a new txn. The typ parameter specifies whether this
// transaction is the top level (root), or one of potentially many
// distributed transactions (leaf).
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
func NewTxn(ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID, typ TxnType) *Txn {
	now := db.clock.Now()
	txn := roachpb.MakeTransaction(
		"unnamed",
		nil, // baseKey
		roachpb.NormalUserPriority,
		now,
		db.clock.MaxOffset().Nanoseconds(),
	)
	// Ensure the gateway node ID is marked as free from clock offset
	// if this is a root transaction.
	if gatewayNodeID != 0 && typ == RootTxn {
		txn.UpdateObservedTimestamp(gatewayNodeID, now)
	}
	return NewTxnWithProto(ctx, db, gatewayNodeID, typ, txn)
}

// NewTxnWithProto is like NewTxn, except it returns a new txn with the provided
// Transaction proto. This allows a client.Txn to be created with an already
// initialized proto.
func NewTxnWithProto(
	ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID, typ TxnType, proto roachpb.Transaction,
) *Txn {
	meta := roachpb.MakeTxnCoordMeta(proto)
	return NewTxnWithCoordMeta(ctx, db, gatewayNodeID, typ, meta)
}

// NewTxnWithCoordMeta is like NewTxn, except it returns a new txn with the
// provided TxnCoordMeta. This allows a client.Txn to be created with an already
// initialized proto and TxnCoordSender.
func NewTxnWithCoordMeta(
	ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID, typ TxnType, meta roachpb.TxnCoordMeta,
) *Txn {
	if db == nil {
		log.Fatalf(ctx, "attempting to create txn with nil db for Transaction: %s", meta.Txn)
	}
	if meta.Txn.Status != roachpb.PENDING {
		log.Fatalf(ctx, "can't create txn with non-PENDING proto: %s",
			meta.Txn)
	}
	meta.Txn.AssertInitialized(ctx)
	txn := &Txn{db: db, typ: typ, gatewayNodeID: gatewayNodeID}
	txn.mu.ID = meta.Txn.ID
	txn.mu.userPriority = roachpb.NormalUserPriority
	txn.mu.sender = db.factory.TransactionalSender(typ, meta, txn.mu.userPriority)
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

// IsCommitted returns true if the transaction has the committed status.
func (txn *Txn) IsCommitted() bool {
	return txn.status() == roachpb.COMMITTED
}

// SetUserPriority sets the transaction's user priority. Transactions default to
// normal user priority. The user priority must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetUserPriority(userPriority roachpb.UserPriority) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.userPriority == userPriority {
		return nil
	}

	if userPriority < roachpb.MinUserPriority || userPriority > roachpb.MaxUserPriority {
		return errors.Errorf("the given user priority %f is out of the allowed range [%f, %f]",
			userPriority, roachpb.MinUserPriority, roachpb.MaxUserPriority)
	}

	txn.mu.userPriority = userPriority
	return txn.mu.sender.SetUserPriority(userPriority)
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) InternalSetPriority(priority enginepb.TxnPriority) {
	txn.mu.Lock()
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.mu.userPriority = roachpb.UserPriority(-priority)
	if err := txn.mu.sender.SetUserPriority(txn.mu.userPriority); err != nil {
		log.Fatal(context.TODO(), err)
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

// OrigTimestamp returns the transaction's starting timestamp.
// Note a transaction can be internally pushed forward in time before
// committing so this is not guaranteed to be the commit timestamp.
// Use CommitTimestamp() when needed.
func (txn *Txn) OrigTimestamp() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.origTimestampLocked()
}

func (txn *Txn) origTimestampLocked() hlc.Timestamp {
	return txn.mu.sender.OrigTimestamp()
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

// SetSystemConfigTrigger sets the system db trigger to true on this transaction.
// This will impact the EndTransactionRequest.
func (txn *Txn) SetSystemConfigTrigger() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if err := txn.mu.sender.AnchorOnSystemConfigRange(); err != nil {
		return err
	}
	txn.systemConfigTrigger = true
	return nil
}

// DisablePipelining instructs the transaction not to pipeline requests. It
// should rarely be necessary to call this method. It is only recommended for
// transactions that need extremely precise control over the request ordering,
// like the transaction that merges ranges together.
//
// DisablePipelining must be called before any operations are performed on the
// transaction.
func (txn *Txn) DisablePipelining() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.DisablePipelining()
}

// NewBatch creates and returns a new empty batch object for use with the Txn.
func (txn *Txn) NewBatch() *Batch {
	return &Batch{txn: txn}
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error. It is not considered an error for the key to not exist.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) Get(ctx context.Context, key interface{}) (KeyValue, error) {
	b := txn.NewBatch()
	b.Get(key)
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

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue. Note that this must be an interface{}(nil), not a
// typed nil value (e.g. []byte(nil)).
//
// Returns an error if the existing value is not equal to expValue.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
func (txn *Txn) CPut(ctx context.Context, key, value interface{}, expValue *roachpb.Value) error {
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
	ctx context.Context, begin, end interface{}, maxRows int64, isReverse bool,
) ([]KeyValue, error) {
	b := txn.NewBatch()
	if maxRows > 0 {
		b.Header.MaxSpanRequestKeys = maxRows
	}
	if !isReverse {
		b.Scan(begin, end)
	} else {
		b.ReverseScan(begin, end)
	}
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
	return txn.scan(ctx, begin, end, maxRows, false)
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
	return txn.scan(ctx, begin, end, maxRows, true)
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
	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()
	if err := b.prepare(); err != nil {
		return err
	}
	return sendAndFill(ctx, txn.Send, b)
}

func (txn *Txn) commit(ctx context.Context) error {
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
	if err == nil {
		log.Fatal(ctx, "CleanupOnError called with nil error")
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
	if txn != b.txn {
		return errors.Errorf("a batch b can only be committed by b.txn")
	}
	b.appendReqs(endTxnReq(true /* commit */, txn.deadline(), txn.systemConfigTrigger))
	b.initResult(1 /* calls */, 0, b.raw, nil)
	return txn.Run(ctx, b)
}

// CommitOrCleanup sends an EndTransactionRequest with Commit=true.
// If that fails, an attempt to rollback is made.
// txn should not be used to send any more commands after this call.
func (txn *Txn) CommitOrCleanup(ctx context.Context) error {
	err := txn.commit(ctx)
	if err != nil {
		txn.CleanupOnError(ctx, err)
	}
	return err
}

// UpdateDeadlineMaybe sets the transactions deadline to the lower of the
// current one (if any) and the passed value.
//
// The deadline cannot be lower than txn.OrigTimestamp.
func (txn *Txn) UpdateDeadlineMaybe(ctx context.Context, deadline hlc.Timestamp) bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.deadline == nil || deadline.Less(*txn.mu.deadline) {
		if deadline.Less(txn.origTimestampLocked()) {
			log.Fatalf(ctx, "deadline below txn.OrigTimestamp() is nonsensical; "+
				"txn has would have no change to commit. Deadline: %s", deadline)
		}
		txn.mu.deadline = new(hlc.Timestamp)
		*txn.mu.deadline = deadline
		return true
	}
	return false
}

// resetDeadlineLocked resets the deadline.
func (txn *Txn) resetDeadlineLocked() {
	txn.mu.deadline = nil
}

// Rollback sends an EndTransactionRequest with Commit=false.
// txn is considered finalized and cannot be used to send any more commands.
func (txn *Txn) Rollback(ctx context.Context) error {
	return txn.rollback(ctx).GoError()
}

func (txn *Txn) rollback(ctx context.Context) *roachpb.Error {
	log.VEventf(ctx, 2, "rolling back transaction")

	sync := true
	if ctx.Err() != nil {
		sync = false
	}
	if sync {
		var ba roachpb.BatchRequest
		ba.Add(endTxnReq(false /* commit */, nil /* deadline */, false /* systemConfigTrigger */))
		_, pErr := txn.Send(ctx, ba)
		if pErr == nil {
			return nil
		}
		// If ctx has been canceled, assume that caused the error and try again
		// async below.
		if ctx.Err() == nil {
			return pErr
		}
	}

	// We don't have a client whose context we can attach to, but we do want to limit how
	// long this request is going to be around or it could leak a goroutine (in case of a
	// long-lived network partition).
	stopper := txn.db.ctx.Stopper
	ctx, cancel := stopper.WithCancelOnQuiesce(txn.db.AnnotateCtx(context.Background()))
	if err := stopper.RunAsyncTask(ctx, "async-rollback", func(ctx context.Context) {
		defer cancel()
		var ba roachpb.BatchRequest
		ba.Add(endTxnReq(false /* commit */, nil /* deadline */, false /* systemConfigTrigger */))
		_ = contextutil.RunWithTimeout(ctx, "async txn rollback", 3*time.Second, func(ctx context.Context) error {
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
	txn.commitTriggers = append(txn.commitTriggers, trigger)
}

// OnCurrentIncarnationFinish adds a closure to be executed when the transaction
// sender moves from state "ready" to "done" or "aborted".
// Note that, as the name suggests, this callback is not persistent across
// different underlying KV transactions. In other words, once a
// TransactionAbortedError happens, the callback is called, but then it won't be
// called again after the client restarts. This is not intended to be used by
// layers above the retries.
func (txn *Txn) OnCurrentIncarnationFinish(onFinishFn func(error)) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.sender.OnFinish(onFinishFn)
}

func endTxnReq(commit bool, deadline *hlc.Timestamp, hasTrigger bool) roachpb.Request {
	req := &roachpb.EndTransactionRequest{
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
					if _, retryable := err.(*roachpb.TransactionRetryWithProtoRefreshError); !retryable {
						// We can't retry, so let the caller know we tried to
						// autocommit.
						err = &AutoCommitError{cause: err}
					}
				}
			}
		}

		cause := errors.Cause(err)

		var retryable bool
		switch t := cause.(type) {
		case *roachpb.UnhandledRetryableError:
			if txn.typ == RootTxn {
				// We sent transactional requests, so the TxnCoordSender was supposed to
				// turn retryable errors into TransactionRetryWithProtoRefreshError. Note that this
				// applies only in the case where this is the root transaction.
				log.Fatalf(ctx, "unexpected UnhandledRetryableError at the txn.exec() level: %s", err)
			}

		case *roachpb.TransactionRetryWithProtoRefreshError:
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
	txn.commitTriggers = nil
	log.VEventf(ctx, 2, "automatically retrying transaction: %s because of error: %s",
		txn.DebugName(), err)
}

// IsRetryableErrMeantForTxn returns true if err is a retryable
// error meant to restart this client transaction.
func (txn *Txn) IsRetryableErrMeantForTxn(
	retryErr roachpb.TransactionRetryWithProtoRefreshError,
) bool {
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
// EndTransaction call is silently dropped, allowing the caller to
// always commit or clean-up explicitly even when that may not be
// required (or even erroneous). Returns (nil, nil) for an empty batch.
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
		txn.mu.Lock()
		txn.handleErrIfRetryableLocked(ctx, retryErr)
		txn.mu.Unlock()
	}
	return br, pErr
}

func (txn *Txn) handleErrIfRetryableLocked(ctx context.Context, err error) {
	retryErr, ok := err.(*roachpb.TransactionRetryWithProtoRefreshError)
	if !ok {
		return
	}
	txn.resetDeadlineLocked()
	txn.replaceSenderIfTxnAbortedLocked(ctx, retryErr, retryErr.TxnID)
}

// GetTxnCoordMeta returns the TxnCoordMeta information for this
// transaction for use with AugmentTxnCoordMeta(), when combining the
// impact of multiple distributed transaction coordinators that are
// all operating on the same transaction.
func (txn *Txn) GetTxnCoordMeta(ctx context.Context) roachpb.TxnCoordMeta {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	meta, err := txn.mu.sender.GetMeta(ctx, AnyTxnStatus)
	if err != nil {
		log.Fatalf(ctx, "unexpected error from GetMeta(AnyTxnStatus): %s", err)
	}
	return meta
}

// GetTxnCoordMetaOrRejectClient is like GetTxnCoordMeta except, if the
// transaction is already aborted or otherwise in a final state, it returns an
// error. If the transaction is aborted, the error will be a retryable one, and
// the transaction will have been prepared for another transaction attempt (so,
// on retryable errors, it acts like Send()).
func (txn *Txn) GetTxnCoordMetaOrRejectClient(ctx context.Context) (roachpb.TxnCoordMeta, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	meta, err := txn.mu.sender.GetMeta(ctx, OnlyPending)
	if err != nil {
		txn.handleErrIfRetryableLocked(ctx, err)
		return roachpb.TxnCoordMeta{}, err
	}
	return meta, nil
}

// AugmentTxnCoordMeta augments this transaction's TxnCoordMeta
// information with the supplied meta. For use with GetTxnCoordMeta().
func (txn *Txn) AugmentTxnCoordMeta(ctx context.Context, meta roachpb.TxnCoordMeta) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.sender.AugmentMeta(ctx, meta)
}

// UpdateStateOnRemoteRetryableErr updates the txn in response to an error
// encountered when running a request through the txn. Returns a
// TransactionRetryWithProtoRefreshError on success or another error on failure.
func (txn *Txn) UpdateStateOnRemoteRetryableErr(ctx context.Context, pErr *roachpb.Error) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if pErr.TransactionRestart == roachpb.TransactionRestart_NONE {
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
	txn.replaceSenderIfTxnAbortedLocked(ctx, pErr.GetDetail().(*roachpb.TransactionRetryWithProtoRefreshError), origTxnID)

	return pErr.GoError()
}

// replaceSenderIfTxnAbortedLocked handles TransactionAbortedErrors, on which a new
// sender is created to replace the current one.
//
// origTxnID is the id of the txn that generated retryErr. Note that this can be
// different from retryErr.Transaction - the latter might be a new transaction.
func (txn *Txn) replaceSenderIfTxnAbortedLocked(
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
	// Create a new txn sender.
	meta := roachpb.MakeTxnCoordMeta(*newTxn)
	txn.mu.sender = txn.db.factory.TransactionalSender(txn.typ, meta, txn.mu.userPriority)
}

func (txn *Txn) recordPreviousTxnIDLocked(prevTxnID uuid.UUID) {
	if txn.mu.previousIDs == nil {
		txn.mu.previousIDs = make(map[uuid.UUID]struct{})
	}
	txn.mu.previousIDs[txn.mu.ID] = struct{}{}
}

// SetFixedTimestamp makes the transaction run in an unusual way, at a "fixed
// timestamp": Timestamp and OrigTimestamp are set to ts, there's no clock
// uncertainty, and the txn's deadline is set to ts such that the transaction
// can't be pushed to a different timestamp.
//
// This is used to support historical queries (AS OF SYSTEM TIME queries and
// backups). This method must be called on every transaction retry (but note
// that retries should be rare for read-only queries with no clock uncertainty).
func (txn *Txn) SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) {
	txn.mu.sender.SetFixedTimestamp(ctx, ts)
}

// GenerateForcedRetryableError returns a TransactionRetryWithProtoRefreshError that will
// cause the txn to be retried.
//
// The transaction's epoch is bumped, simulating to an extent what the
// TxnCoordSender does on retriable errors. The transaction's timestamp is only
// bumped to the extent that txn.OrigTimestamp is racheted up to txn.Timestamp.
// TODO(andrei): This method should take in an up-to-date timestamp, but
// unfortunately its callers don't currently have that handy.
func (txn *Txn) GenerateForcedRetryableError(ctx context.Context, msg string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	now := txn.db.clock.Now()
	txn.mu.sender.ManualRestart(ctx, txn.mu.userPriority, now)
	txn.resetDeadlineLocked()
	return roachpb.NewTransactionRetryWithProtoRefreshError(
		msg,
		txn.mu.ID,
		roachpb.MakeTransaction(
			txn.debugNameLocked(),
			nil, // baseKey
			txn.mu.userPriority,
			now,
			txn.db.clock.MaxOffset().Nanoseconds(),
		))
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
// figures out that it's been pushed when it sends an EndTransaction - i.e. it's
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

// Serialize returns a clone of the transaction's current proto.
// This is a nuclear option; generally client code shouldn't deal with protos.
// However, this is used by DistSQL for sending the transaction over the wire
// when it creates flows.
func (txn *Txn) Serialize() *roachpb.Transaction {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.SerializeTxn()
}

func (txn *Txn) deadline() *hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.deadline
}
