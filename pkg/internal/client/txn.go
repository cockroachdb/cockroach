// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package client

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	commitTriggers []func()
	// systemConfigTrigger is set to true when modifying keys from the SystemConfig
	// span. This sets the SystemConfigTrigger on EndTransactionRequest.
	systemConfigTrigger bool
	// The txn has to be committed by this deadline. A nil value indicates no
	// deadline.
	deadline *hlc.Timestamp

	// mu holds fields that need to be synchronized for concurrent request execution.
	mu struct {
		syncutil.Mutex
		Proto roachpb.Transaction
		// UserPriority is the transaction's priority. If not set,
		// NormalUserPriority will be used.
		// TODO(andrei): Can this go away now that UserPriority and Proto.Priority
		// are initialized at the same time?
		UserPriority roachpb.UserPriority
		// active is set whenever the transaction is actively running. It will
		// be initially set when the transaction sends its first batch, but is
		// reset if the transaction is aborted.
		active bool
		// txnAnchorKey is the key at which to anchor the transaction record. If
		// unset, the first key written in the transaction will be used.
		txnAnchorKey roachpb.Key
		state        txnState
		// see IsFinalized()
		finalized bool
		// previousIDs holds the set of all previous IDs that the Txn's Proto has
		// had across transaction aborts. This allows us to determine if a given
		// response was meant for any incarnation of this transaction.
		//
		// TODO(andrei): This concept of the history of IDs seems problematic to me:
		// if we get a retryable error for a really old incarnation of a
		// transaction, a client (or Txn.Exec) probably doesn't want to retry.
		// If Txn stays multi-threaded, I think restarting a transaction should
		// become an explicit client operation that doesn't allow more requests to
		// be sent for the old incarnation, and also doesn't let results for
		// requests sent through old incarnations to be processed.
		// If Txn becomes single-threaded, then the point is moot and this can again
		// go away.
		previousIDs map[uuid.UUID]struct{}
		// sender is a stateful sender for use with transactions. A new sender is
		// created on transaction restarts (not retries).
		sender TxnSender
	}
}

// txnState represents states relating to whether Begin/EndTxn requests need to
// be sent.
type txnState int

const (
	// txnReadOnly means that the transaction never sent any writes. There's no
	// transaction record, so an EndTxn does not need to be sent.
	txnReadOnly txnState = iota
	// txnWriting means that the transaction has sent some writes (and so it also
	// sent a BeginTxn). An EndTransaction must be sent to resolve intents and/or
	// to cleanup the txn record.
	// txnWriting does not guarantee that the transaction record has been written.
	// In case the BeginTxn batch encoutered an error, it might not have been. In
	// this case, a rollback will get an error (ignored by SQL).
	txnWriting
	// txnWriteInOldEpoch means that the txn has been writing in an old epoch,
	// but then restarted with a new epoch, and there have been no writes sent
	// since then. This means that an EndTransaction(commit=false) needs to be
	// sent to clean up intents. It also means that a BeginTransaction needs to be
	// sent on the first write: the TransactionRestartError might have been
	// received by the batch with the BeginTransaction in it, in which case there
	// is no transaction record (and so it needs to be created).
	// We could be smarter about not transitioning to this state if there's ever
	// been a successful write (in which case we know that there is a txn record
	// and a BeginTransaction is not necessary) but as of May 2018 we don't do
	// that. Note that the server accepts a BeginTxn with a higher epoch if a
	// transaction record already exists.
	txnWriteInOldEpoch
	// txnError means that the txn had performed some writes and then a batch got
	// a non-retriable error. Further batches except EndTransaction(commit=false)
	// will be rejected.
	txnError
)

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
func NewTxn(db *DB, gatewayNodeID roachpb.NodeID, typ TxnType) *Txn {
	now := db.clock.Now()
	txn := roachpb.MakeTransaction(
		"unnamed",
		nil, // baseKey
		roachpb.NormalUserPriority,
		enginepb.SERIALIZABLE,
		now,
		db.clock.MaxOffset().Nanoseconds(),
	)
	// Ensure the gateway node ID is marked as free from clock offset
	// if this is a root transaction.
	if gatewayNodeID != 0 && typ == RootTxn {
		txn.UpdateObservedTimestamp(gatewayNodeID, now)
	}
	return NewTxnWithProto(db, gatewayNodeID, typ, txn)
}

// NewTxnWithProto is like NewTxn, except it returns a new txn with the provided
// Transaction proto. This allows a client.Txn to be created with an already
// initialized proto.
func NewTxnWithProto(
	db *DB, gatewayNodeID roachpb.NodeID, typ TxnType, proto roachpb.Transaction,
) *Txn {
	meta := roachpb.MakeTxnCoordMeta(proto)
	return NewTxnWithCoordMeta(db, gatewayNodeID, typ, meta)
}

// NewTxnWithCoordMeta is like NewTxn, except it returns a new txn with the
// provided TxnCoordMeta. This allows a client.Txn to be created with an already
// initialized proto and TxnCoordSender.
func NewTxnWithCoordMeta(
	db *DB, gatewayNodeID roachpb.NodeID, typ TxnType, meta roachpb.TxnCoordMeta,
) *Txn {
	if db == nil {
		log.Fatalf(context.TODO(), "attempting to create txn with nil db for Transaction: %s", meta.Txn)
	}
	meta.Txn.AssertInitialized(context.TODO())
	txn := &Txn{db: db, typ: typ, gatewayNodeID: gatewayNodeID}
	txn.mu.Proto = meta.Txn
	txn.mu.sender = db.factory.TransactionalSender(typ, meta)
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
	return txn.mu.Proto.ID
}

// IsFinalized returns true if this Txn has been finalized and should therefore
// not be used for any more KV operations.
// A Txn is considered finalized if it successfully committed or if a rollback
// was attempted (successful or not).
// Note that Commit() always leaves the transaction finalized, since it attempts
// to rollback on error.
func (txn *Txn) IsFinalized() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.finalized
}

// status returns the txn proto status field.
func (txn *Txn) status() roachpb.TransactionStatus {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.Proto.Status
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
	if txn.mu.UserPriority == userPriority {
		return nil
	}
	if txn.mu.active {
		return errors.Errorf("cannot change the user priority of a running transaction")
	}
	if userPriority < roachpb.MinUserPriority || userPriority > roachpb.MaxUserPriority {
		return errors.Errorf("the given user priority %f is out of the allowed range [%f, %f]",
			userPriority, roachpb.MinUserPriority, roachpb.MaxUserPriority)
	}
	txn.mu.UserPriority = userPriority
	txn.mu.Proto.Priority = roachpb.MakePriority(userPriority)
	return nil
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) InternalSetPriority(priority int32) {
	txn.mu.Lock()
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.mu.UserPriority = roachpb.UserPriority(-priority)
	txn.mu.Proto.Priority = roachpb.MakePriority(txn.mu.UserPriority)
	txn.mu.Unlock()
}

// UserPriority returns the transaction's user priority.
func (txn *Txn) UserPriority() roachpb.UserPriority {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.UserPriority
}

// SetDebugName sets the debug name associated with the transaction which will
// appear in log files and the web UI.
func (txn *Txn) SetDebugName(name string) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.Proto.Name == name {
		return
	}
	if txn.mu.active {
		panic("cannot change the debug name of a running transaction")
	}
	txn.mu.Proto.Name = name
}

// DebugName returns the debug name associated with the transaction.
func (txn *Txn) DebugName() string {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return fmt.Sprintf("%s (id: %s)", txn.mu.Proto.Name, txn.mu.Proto.ID)
}

// SetIsolation sets the transaction's isolation type. Transactions default to
// serializable isolation. The isolation must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetIsolation(isolation enginepb.IsolationType) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.Proto.Isolation == isolation {
		return nil
	}
	if txn.mu.active {
		return errors.Errorf("cannot change the isolation level of a running transaction")
	}
	txn.mu.Proto.Isolation = isolation
	return nil
}

// Isolation returns the transaction's isolation type.
func (txn *Txn) Isolation() enginepb.IsolationType {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.Proto.Isolation
}

// OrigTimestamp returns the transaction's starting timestamp.
// Note a transaction can be internally pushed forward in time before
// committing so this is not guaranteed to be the commit timestamp.
// Use CommitTimestamp() when needed.
func (txn *Txn) OrigTimestamp() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.Proto.OrigTimestamp
}

// CommitTimestamp returns the transaction's start timestamp.
// The start timestamp can get pushed but the use of this
// method will guarantee that if a timestamp push is needed
// the commit will fail with a retryable error.
func (txn *Txn) CommitTimestamp() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.Proto.OrigTimestampWasObserved = true
	return txn.mu.Proto.OrigTimestamp
}

// SetTxnAnchorKey sets the key at which to anchor the transaction record. The
// transaction anchor key defaults to the first key written in a transaction.
func (txn *Txn) SetTxnAnchorKey(key roachpb.Key) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.state != txnReadOnly {
		return errors.Errorf("transaction anchor key already set")
	}
	txn.mu.txnAnchorKey = key
	return nil
}

// SetSystemConfigTrigger sets the system db trigger to true on this transaction.
// This will impact the EndTransactionRequest.
//
// NOTE: The system db trigger will only execute correctly if the transaction
// record is located on the range that contains the system span. If a
// transaction is created which modifies both system *and* non-system data, it
// should be ensured that the transaction record itself is on the system span.
// This can be done by making sure a system key is the first key touched in the
// transaction.
func (txn *Txn) SetSystemConfigTrigger() error {
	if !txn.systemConfigTrigger {
		txn.systemConfigTrigger = true
		// The system-config trigger must be run on the system-config range which
		// means any transaction with the trigger set needs to be anchored to the
		// system-config range.
		return txn.SetTxnAnchorKey(keys.SystemConfigSpan.Key)
	}
	return nil
}

// Proto returns the transactions underlying protocol buffer. It is not thread-safe,
// only use if you know that no requests are executing concurrently.
//
// A thread-safe alternative would be to clone the Proto under lock and return
// this clone, but we currently have no situations where this is needed.
func (txn *Txn) Proto() *roachpb.Transaction {
	return &txn.mu.Proto
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
	r, err := txn.Get(ctx, key)
	if err != nil {
		return err
	}
	return r.ValueProto(msg)
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
func (txn *Txn) CPut(ctx context.Context, key, value, expValue interface{}) error {
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
	pErr := txn.sendEndTxnReq(ctx, true /* commit */, txn.deadline)
	if pErr == nil {
		for _, t := range txn.commitTriggers {
			t()
		}
	}
	return pErr.GoError()
}

// CleanupOnError cleans up the transaction as a result of an error.
func (txn *Txn) CleanupOnError(ctx context.Context, err error) {
	if err == nil {
		log.Fatal(ctx, "CleanupOnError called with nil error")
	}
	// This may race with a concurrent EndTxnRequests. That's fine though because
	// we're just trying to clean up and will happily log the failed Rollback error
	// if someone beat us.
	if txn.status() == roachpb.PENDING {
		if replyErr := txn.rollback(ctx); replyErr != nil {
			if _, ok := replyErr.GetDetail().(*roachpb.TransactionStatusError); ok || txn.status() == roachpb.ABORTED {
				log.Eventf(ctx, "failure aborting transaction: %s; abort caused by: %s", replyErr, err)
			} else {
				log.Warningf(ctx, "failure aborting transaction: %s; abort caused by: %s", replyErr, err)
			}
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
	b.appendReqs(endTxnReq(true /* commit */, txn.deadline, txn.systemConfigTrigger))
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
	if txn.deadline == nil || deadline.Less(*txn.deadline) {
		if deadline.Less(txn.mu.Proto.OrigTimestamp) {
			log.Fatalf(ctx, "deadline below txn.OrigTimestamp is nonsensical; "+
				"txn has would have no change to commit. Deadline: %s, txn: %s",
				deadline, txn.Proto())
		}
		txn.deadline = &deadline
		return true
	}
	return false
}

// resetDeadlineLocked resets the deadline.
func (txn *Txn) resetDeadlineLocked() {
	txn.deadline = nil
}

// Rollback sends an EndTransactionRequest with Commit=false.
// txn is considered finalized and cannot be used to send any more commands.
func (txn *Txn) Rollback(ctx context.Context) error {
	return txn.rollback(ctx).GoError()
}

// TODO(andrei): It's common for rollbacks to fail with TransactionStatusError:
// txn record not found (REASON_TXN_NOT_FOUND), in case the txn record was never
// written (e.g. if a 1PC failed). One would think that, depending on the error
// received by a 1PC batch, we'd know if a txn record was written and, if it
// wasn't, we could short-circuit the rollback. There's two tricky things about
// that, though: a) we'd need to know whether the DistSender has split what
// looked like a 1PC batch to the client and b) ambiguous errors.
func (txn *Txn) rollback(ctx context.Context) *roachpb.Error {
	log.VEventf(ctx, 2, "rolling back transaction")

	// Mark the txn as finalized. We don't allow any more requests to be sent once
	// a rollback is attempted. Also, the SQL layer likes to assert that the
	// transaction has been finalized after attempting cleanup.
	txn.mu.Lock()
	txn.mu.finalized = true
	txn.mu.Unlock()

	sync := true
	if ctx.Err() != nil {
		sync = false
	}
	if sync {
		pErr := txn.sendEndTxnReq(ctx, false /* commit */, nil)
		if pErr == nil {
			return nil
		}
		// If ctx has been canceled, assume that caused the error and try again
		// async below.
		if ctx.Err() == nil {
			return pErr
		}
	}

	stopper := txn.db.ctx.Stopper
	ctx = stopper.WithCancel(txn.db.AnnotateCtx(context.Background()))
	if err := stopper.RunAsyncTask(ctx, "async-rollback", func(ctx context.Context) {
		if err := txn.sendEndTxnReq(ctx, false /* commit */, nil); err != nil {
			log.Infof(ctx, "async rollback failed: %s", err)
		}
	}); err != nil {
		return roachpb.NewError(err)
	}
	return nil
}

// AddCommitTrigger adds a closure to be executed on successful commit
// of the transaction.
func (txn *Txn) AddCommitTrigger(trigger func()) {
	txn.commitTriggers = append(txn.commitTriggers, trigger)
}

// OnFinish adds a closure to be executed when the transaction sender
// moves from state "ready" to "done" or "aborted".
func (txn *Txn) OnFinish(onFinishFn func(error)) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.sender.OnFinish(onFinishFn)
}

// finishReadonlyLocked provides a fast-path for finishing a read-only
// transaction without going through the overhead of creating an
// EndTransactionRequest only to not send it.
//
// NB: The logic here must be kept in sync with the logic in txn.Send.
//
// TODO(andrei): Can we share this code with txn.Send?
func (txn *Txn) finishReadonlyLocked(
	ctx context.Context, commit bool, deadline *hlc.Timestamp,
) *roachpb.Error {
	txn.mu.finalized = true
	// Check that read only transactions do not violate their deadline. This can NOT
	// happen since the txn deadline is normally updated when it is about to expire
	// or expired. We will just keep the code for safety (see TestReacquireLeaseOnRestart).
	if deadline != nil && deadline.Less(txn.mu.Proto.Timestamp) {
		// NB: The returned error contains a pointer to txn.mu.Proto, but that's ok
		// because we can't have concurrent operations going on while
		// committing/aborting.
		//
		// TODO(andrei): What is happening in this case?
		//
		//     1. our txn starts at ts1
		//     2. catches uncertainty read error
		//     3. updates its timestamp
		//     4. new timestamp violates deadline
		//     5. txn retries the read
		//     6. commit fails - only thanks to this code path?
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
			"deadline exceeded before transaction finalization"), &txn.mu.Proto)
	}
	if commit {
		txn.mu.Proto.Status = roachpb.COMMITTED
	} else {
		txn.mu.Proto.Status = roachpb.ABORTED
	}
	return nil
}

func (txn *Txn) sendEndTxnReq(
	ctx context.Context, commit bool, deadline *hlc.Timestamp,
) *roachpb.Error {
	txn.mu.Lock()
	if txn.mu.state == txnReadOnly {
		defer txn.mu.Unlock()
		return txn.finishReadonlyLocked(ctx, commit, deadline)
	}

	var swallowErr bool
	if txn.mu.state == txnWriteInOldEpoch && commit {
		// If there was a write in an old epoch (and no writes in the current
		// epoch), we need to send a rollback. We'll ignore the error from it; the
		// commit is successful at this point.
		log.VEventf(ctx, 2, "old epoch write turning commit into rollback")
		commit = false
		swallowErr = true
	}
	txn.mu.Unlock()

	var ba roachpb.BatchRequest
	ba.Add(endTxnReq(commit, deadline, txn.systemConfigTrigger))
	_, pErr := txn.Send(ctx, ba)
	if swallowErr {
		return nil
	}
	return pErr
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
			if txn.status() == roachpb.ABORTED {
				log.Fatalf(ctx, "no err but aborted txn proto. txn: %+v", txn)
			}
			if txn.status() == roachpb.PENDING {
				err = txn.Commit(ctx)
				log.Eventf(ctx, "client.Txn did AutoCommit. err: %v\ntxn: %+v", err, txn.Proto())
				if err != nil {
					if _, retryable := err.(*roachpb.HandledRetryableTxnError); !retryable {
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
				// turn retryable errors into HandledRetryableTxnError. Note that this
				// applies only in the case where this is the root transaction.
				log.Fatalf(ctx, "unexpected UnhandledRetryableError at the txn.exec() level: %s", err)
			}

		case *roachpb.HandledRetryableTxnError:
			if !txn.IsRetryableErrMeantForTxn(*t) {
				// Make sure the txn record that err carries is for this txn.
				// If it's not, we terminate the "retryable" character of the error. We
				// might get a HandledRetryableTxnError if the closure ran another
				// transaction internally and let the error propagate upwards.
				return errors.Wrapf(
					err,
					"retryable error from another txn. Current txn ID: %v", txn.Proto().ID)
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
func (txn *Txn) IsRetryableErrMeantForTxn(retryErr roachpb.HandledRetryableTxnError) bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.isRetryableErrMeantForTxnLocked(retryErr)
}

func (txn *Txn) isRetryableErrMeantForTxnLocked(retryErr roachpb.HandledRetryableTxnError) bool {
	errTxnID := retryErr.TxnID

	// Make sure the txn record that err carries is for this txn.
	// First check if the error was meant for a previous incarnation
	// of the transaction.
	if _, ok := txn.mu.previousIDs[errTxnID]; ok {
		return true
	}
	// If not, make sure it was meant for this transaction.
	return errTxnID == txn.mu.Proto.ID
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
	// It doesn't make sense to use inconsistent reads in a transaction. However,
	// we still need to accept it as a parameter for this to compile.
	if ba.ReadConsistency != roachpb.CONSISTENT {
		return nil, roachpb.NewErrorf("cannot use %s ReadConsistency in txn",
			ba.ReadConsistency)
	}

	// Fill in the GatewayNodeID on the batch if the txn knows it.
	// NOTE(andrei): It seems a bit ugly that we're filling in the batches here as
	// opposed to the point where the requests are being created, but
	// unfortunately requests are being created in many ways and this was the best
	// place I found to set this field.
	if txn.gatewayNodeID != 0 {
		ba.Header.GatewayNodeID = txn.gatewayNodeID
	}

	lastIndex := len(ba.Requests) - 1
	if lastIndex < 0 {
		return nil, nil
	}

	firstWriteIdx, pErr := firstWriteIndex(ba)
	if pErr != nil {
		return nil, pErr
	}

	haveTxnWrite := firstWriteIdx != -1
	endTxnRequest, haveEndTxn := ba.Requests[lastIndex].GetInner().(*roachpb.EndTransactionRequest)

	var needBeginTxn, elideEndTxn bool
	var sender TxnSender
	lockedPrelude := func() *roachpb.Error {
		txn.mu.Lock()
		defer txn.mu.Unlock()

		if txn.mu.state == txnError {
			singleAbort := ba.IsSingleEndTransactionRequest() &&
				!ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit
			if !singleAbort {
				return roachpb.NewError(&roachpb.TxnAlreadyEncounteredErrorError{})
			}
		}

		sender = txn.mu.sender
		if txn.mu.Proto.Status != roachpb.PENDING || txn.mu.finalized {
			onlyRollback := lastIndex == 0 && haveEndTxn && !endTxnRequest.Commit
			if !onlyRollback {
				return roachpb.NewErrorf(
					"attempting to use transaction with wrong status or finalized: %s %v",
					txn.mu.Proto.Status, txn.mu.finalized)
			}
		}

		// For testing purposes, txn.UserPriority can be a negative value (see
		// roachpb.MakePriority).
		if txn.mu.UserPriority != 0 {
			ba.UserPriority = txn.mu.UserPriority
		}

		if !txn.mu.active {
			user := roachpb.MakePriority(ba.UserPriority)
			if txn.mu.Proto.Priority < user {
				txn.mu.Proto.Priority = user
			}
			txn.mu.active = true
		}

		needBeginTxn = haveTxnWrite && (txn.mu.state != txnWriting)
		// We need the EndTxn if we've ever written before or if we're writing now.
		needEndTxn := haveTxnWrite || txn.mu.state != txnReadOnly
		elideEndTxn = haveEndTxn && !needEndTxn

		// If we're not yet writing in this txn, but intend to, insert a
		// begin transaction request before the first write command and update
		// transaction state accordingly.
		if needBeginTxn {
			// We're about to send a BeginTxn, so move to the Writing state.
			txn.mu.state = txnWriting
			// From now on, all requests need to be checked against the AbortCache on
			// the server side.
			txn.mu.Proto.Writing = true

			// Set txn key based on the key of the first transactional write if
			// not already set. If the transaction already has a key (we're in a
			// restart), make sure we keep the anchor key the same.
			if len(txn.mu.Proto.Key) == 0 {
				txnAnchorKey := txn.mu.txnAnchorKey
				if len(txnAnchorKey) == 0 {
					txnAnchorKey = ba.Requests[0].GetInner().Header().Key
				}
				txn.mu.Proto.Key = txnAnchorKey
			}
			// Set the key in the begin transaction request to the txn's anchor key.
			bt := &roachpb.BeginTransactionRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: txn.mu.Proto.Key,
				},
			}
			// Inject the new request before position firstWriteIdx, taking
			// care to avoid unnecessary allocations.
			oldRequests := ba.Requests
			ba.Requests = make([]roachpb.RequestUnion, len(ba.Requests)+1)
			copy(ba.Requests, oldRequests[:firstWriteIdx])
			ba.Requests[firstWriteIdx].MustSetInner(bt)
			copy(ba.Requests[firstWriteIdx+1:], oldRequests[firstWriteIdx:])
		}

		if elideEndTxn {
			ba.Requests = ba.Requests[:lastIndex]
		}

		// Set the sequence number of each individual Request. The sequence
		// number is used for replay and reordering protection. At the Store, a
		// sequence number less than or equal to the last observed one (on a
		// given key) incurs a transaction restart (if the request is
		// transactional).
		//
		// This semantic could be adjusted in the future to provide idempotency
		// for replays and re-issues. However, a side effect of providing this
		// property is that reorder protection would no longer be provided by
		// the counter, so ordering guarantees between requests within the same
		// transaction would need to be strengthened elsewhere (e.g. by the
		// transport layer).
		for _, ru := range ba.Requests {
			txn.mu.Proto.Sequence++
			oldHeader := ru.GetInner().Header()
			oldHeader.Sequence = txn.mu.Proto.Sequence
			ru.GetInner().SetHeader(oldHeader)
		}

		// Clone the Txn's Proto so that future modifications can be made without
		// worrying about synchronization.
		newTxn := txn.mu.Proto.Clone()
		ba.Txn = &newTxn
		return nil
	}
	if pErr := lockedPrelude(); pErr != nil {
		return nil, pErr
	}

	// Send call through the DB.
	requestTxnID := ba.Txn.ID
	br, pErr := txn.db.sendUsingSender(ctx, ba, sender)

	// Lock for the entire response postlude.
	txn.mu.Lock()
	defer txn.mu.Unlock()

	// If we inserted a begin transaction request, remove it here.
	if needBeginTxn {
		if br != nil && br.Responses != nil {
			br.Responses = append(br.Responses[:firstWriteIdx], br.Responses[firstWriteIdx+1:]...)
		}
		// Handle case where inserted begin txn confused an indexed error.
		if pErr != nil && pErr.Index != nil {
			idx := pErr.Index.Index
			if idx == int32(firstWriteIdx) {
				// An error was encountered on begin txn; disallow the indexing.
				pErr.Index = nil
			} else if idx > int32(firstWriteIdx) {
				// An error was encountered after begin txn; decrement index.
				pErr.SetErrorIndex(idx - 1)
			}
		}
	}
	if haveEndTxn {
		if pErr == nil || !endTxnRequest.Commit {
			// Finalize the transaction if either we sent a successful commit
			// EndTxnRequest, or sent a rollback EndTxnRequest (regardless of
			// if it succeeded).
			txn.mu.finalized = true
		}
	}

	// If the request was part of a previous attempt, don't return any results.
	if requestTxnID != txn.mu.Proto.ID || ba.Txn.Epoch != txn.mu.Proto.Epoch {
		return nil, roachpb.NewError(&roachpb.TxnPrevAttemptError{})
	}

	if pErr != nil {
		if log.V(1) {
			log.Infof(ctx, "failed batch: %s", pErr)
		}
		var retriable bool
		switch t := pErr.GetDetail().(type) {
		case *roachpb.HandledRetryableTxnError:
			retriable = true
			retryErr := t
			if requestTxnID != retryErr.TxnID {
				// KV should not return errors for transactions other than the one that sent
				// the request.
				log.Fatalf(ctx, "retryable error for the wrong txn. "+
					"requestTxnID: %s, retryErr.TxnID: %s. retryErr: %s",
					requestTxnID, retryErr.TxnID, retryErr)
			}
			txn.updateStateOnRetryableErrLocked(ctx, retryErr)
		default:
			if errTxn := pErr.GetTxn(); txn != nil {
				txn.mu.Proto.Update(errTxn)
			}
		}
		// Note that unhandled retryable txn errors are allowed from leaf
		// transactions. We pass them up through distributed SQL flows to
		// the root transactions, at the receiver.
		if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
			retriable = true
			if txn.typ == RootTxn {
				log.Fatalf(ctx,
					"unexpected retryable error at the client.Txn level: (%T) %s",
					pErr.GetDetail(), pErr)
			}
		}

		if !retriable && txn.mu.state == txnWriting {
			txn.mu.state = txnError
		}

		return nil, pErr
	}

	if br != nil {
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(txn.mu.sender, br))
		}

		// Only successful requests can carry an updated Txn in their response
		// header. Some errors (e.g. a restart) have a Txn attached to them as
		// well; these errors have been handled above.
		txn.mu.Proto.Update(br.Txn)
	}

	if elideEndTxn {
		// Check that read only transactions do not violate their deadline. This can NOT
		// happen since the txn deadline is normally updated when it is about to expire
		// or expired. We will just keep the code for safety (see TestReacquireLeaseOnRestart).
		if endTxnRequest.Deadline != nil {
			if endTxnRequest.Deadline.Less(txn.mu.Proto.Timestamp) {
				// NB: The returned error contains a pointer to txn.mu.Proto, but
				// that's ok because we can't have concurrent operations going on while
				// committing/aborting.
				return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
					"deadline exceeded before transaction finalization"), &txn.mu.Proto)
			}
		}
		// This normally happens on the server and sent back in response
		// headers, but this transaction was optimized away. The caller may
		// still inspect the transaction struct, so we manually update it
		// here to emulate a true transaction.
		if endTxnRequest.Commit {
			txn.mu.Proto.Status = roachpb.COMMITTED
		} else {
			txn.mu.Proto.Status = roachpb.ABORTED
		}
	}
	return br, nil
}

// firstWriteIndex returns the index of the first transactional write in the
// BatchRequest. Returns -1 if the batch has not intention to write. It also
// verifies that if an EndTransactionRequest is included, then it is the last
// request in the batch.
func firstWriteIndex(ba roachpb.BatchRequest) (int, *roachpb.Error) {
	for i, ru := range ba.Requests {
		args := ru.GetInner()
		if i < len(ba.Requests)-1 /* if not last*/ {
			if _, ok := args.(*roachpb.EndTransactionRequest); ok {
				return -1, roachpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		if roachpb.IsTransactionWrite(args) {
			return i, nil
		}
	}
	return -1, nil
}

// GetTxnCoordMeta returns the TxnCoordMeta information for this
// transaction for use with AugmentTxnCoordMeta(), when combining the
// impact of multiple distributed transaction coordinators that are
// all operating on the same transaction.
func (txn *Txn) GetTxnCoordMeta() roachpb.TxnCoordMeta {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.GetMeta()
}

// GetStrippedTxnCoordMeta is like GetTxnCoordMeta, but it strips out all
// information that is unnecessary to communicate to other distributed
// transaction coordinators that are all operating on the same transaction.
func (txn *Txn) GetStrippedTxnCoordMeta() roachpb.TxnCoordMeta {
	meta := txn.GetTxnCoordMeta()
	switch txn.typ {
	case RootTxn:
		meta.Intents = nil
		meta.CommandCount = 0
		meta.RefreshReads = nil
		meta.RefreshWrites = nil
	case LeafTxn:
		meta.OutstandingWrites = nil
	}
	return meta
}

// AugmentTxnCoordMeta augments this transaction's TxnCoordMeta
// information with the supplied meta. For use with GetTxnCoordMeta().
func (txn *Txn) AugmentTxnCoordMeta(ctx context.Context, meta roachpb.TxnCoordMeta) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.Proto.Update(&meta.Txn)
	txn.mu.sender.AugmentMeta(ctx, meta)
}

// UpdateStateOnRemoteRetryableErr updates the Txn, and the
// Transaction proto inside it, in response to an error encountered
// when running a request through the txn. Returns a handled retryable
// error on success or an error on failure.
func (txn *Txn) UpdateStateOnRemoteRetryableErr(ctx context.Context, pErr *roachpb.Error) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if pErr.TransactionRestart == roachpb.TransactionRestart_NONE {
		log.Fatalf(ctx, "unexpected non-retryable error: %s", pErr)
	}

	// Emulate the processing that the TxnCoordSender would have done on
	// this error if it was experienced by the root transaction.
	newTxn := roachpb.PrepareTransactionForRetry(ctx, pErr, txn.mu.UserPriority, txn.db.clock)
	newErr := roachpb.NewHandledRetryableTxnError(pErr.Message, pErr.GetTxn().ID, newTxn)

	// If the transaction has been reset since this request was sent,
	// ignore the error.
	if newErr.TxnID != txn.mu.Proto.ID {
		return errors.Errorf("retryable error for an older version of txn (current: %s), err: %s",
			txn.mu.Proto, pErr)
	}
	txn.updateStateOnRetryableErrLocked(ctx, newErr)

	return newErr
}

// BumpEpochAfterConcurrentRetryErrorLocked bumps the transaction epoch manually
// and resets the transaction state away from txnError. This is meant to be used
// after synchronizing concurrent actors using a txn when a retryable error is
// seen.
//
// TODO(andrei): this should go away once we move to a TxnAttempt model.
func (txn *Txn) BumpEpochAfterConcurrentRetryErrorLocked() {
	// Invalidate any writes performed by any workers after the retry updated
	// the txn's proto but before we synchronized (some of these writes might
	// have been performed at the wrong epoch).
	txn.Proto().BumpEpoch()

	// The txn might have entered the txnError state after the epoch was bumped.
	// Reset the state for the retry.
	if txn.mu.state == txnError {
		txn.mu.state = txnWriteInOldEpoch
	}
}

// updateStateOnRetryableErrLocked updates the Transaction proto inside txn.
//
// This method is safe to call repeatedly for requests from the same txn epoch.
// The first such update will move the Transaction forward (either create a new
// one or increment the epoch), and subsequent calls will be no-ops.
func (txn *Txn) updateStateOnRetryableErrLocked(
	ctx context.Context, retryErr *roachpb.HandledRetryableTxnError,
) {
	txn.resetDeadlineLocked()
	newTxn := &retryErr.Transaction

	abortErr := txn.mu.Proto.ID != newTxn.ID
	if abortErr {
		// This means that the cause was a TransactionAbortedError;
		// we've created a new Transaction that we're about to start using, so we
		// save the old transaction ID so that concurrent requests or delayed
		// responses that that throw errors know that these errors were sent to
		// the correct transaction, even once the proto is reset.
		txn.recordPreviousTxnIDLocked(txn.mu.Proto.ID)

		// Overwrite the transaction proto with the one to be used for the next
		// attempt. The txn inside pErr was correctly prepared for this by
		// TxnCoordSender.
		txn.mu.Proto = *newTxn
		// We're starting a fresh transaction, with no state.
		txn.mu.state = txnReadOnly

		// Create a new txn sender.
		meta := roachpb.MakeTxnCoordMeta(*newTxn)
		txn.mu.sender = txn.db.factory.TransactionalSender(txn.typ, meta)
	} else {
		// Update the transaction proto with the one to be used for the next
		// attempt. The txn inside pErr was correctly prepared for this by
		// TxnCoordSender. However, we can't blindly replace the proto, because
		// multiple retryable errors might come back here out-of-order. Instead,
		// we rely on the associativity of Transaction.Update to sort out this
		// lack of ordering guarantee.
		txn.mu.Proto.Update(newTxn)

		// If we've been writing, we'll need to send a BeginTxn with the next
		// request.
		if txn.mu.state == txnWriting {
			txn.mu.state = txnWriteInOldEpoch
		}
	}
}

func (txn *Txn) recordPreviousTxnIDLocked(prevTxnID uuid.UUID) {
	if txn.mu.previousIDs == nil {
		txn.mu.previousIDs = make(map[uuid.UUID]struct{})
	}
	txn.mu.previousIDs[txn.mu.Proto.ID] = struct{}{}
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
	txn.mu.Lock()
	txn.mu.Proto.Timestamp = ts
	txn.mu.Proto.OrigTimestamp = ts
	txn.mu.Proto.MaxTimestamp = ts
	txn.mu.Proto.OrigTimestampWasObserved = true
	txn.mu.Unlock()
}

// GenerateForcedRetryableError returns a HandledRetryableTxnError that will
// cause the txn to be retried.
//
// The transaction's epoch is bumped, simulating to an extent what the
// TxnCoordSender does on retriable errors. The transaction's timestamp is only
// bumped to the extent that txn.OrigTimestamp is racheted up to txn.Timestamp.
// TODO(andrei): This method should take in an up-to-date timestamp, but
// unfortunately its callers don't currently have that handy.
func (txn *Txn) GenerateForcedRetryableError(msg string) error {
	txn.Proto().Restart(txn.UserPriority(), 0 /* upgradePriority */, txn.Proto().Timestamp)
	txn.resetDeadlineLocked()
	return roachpb.NewHandledRetryableTxnError(
		msg,
		txn.ID(),
		roachpb.MakeTransaction(
			txn.DebugName(),
			nil, // aseKey
			txn.UserPriority(),
			txn.Isolation(),
			txn.db.clock.Now(),
			txn.db.clock.MaxOffset().Nanoseconds(),
		))
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
	txn.mu.Lock()
	defer txn.mu.Unlock()

	origTimestamp := txn.Proto().OrigTimestamp
	origTimestamp.Forward(txn.Proto().RefreshedTimestamp)
	isTxnPushed := txn.Proto().Timestamp != origTimestamp
	// We check OrigTimestampWasObserved here because, if that's set, refreshing
	// of reads is not performed.
	return txn.Proto().Isolation == enginepb.SERIALIZABLE &&
		isTxnPushed && txn.mu.Proto.OrigTimestampWasObserved
}

// Type returns the transaction's type.
func (txn *Txn) Type() TxnType {
	return txn.typ
}
