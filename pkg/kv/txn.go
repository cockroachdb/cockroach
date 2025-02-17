// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

// MaxInternalTxnAutoRetries controls the maximum number of auto-retries a call
// to kv.DB.Txn will perform before aborting the transaction and returning an
// error. This is used to prevent infinite retry loops where a transaction has
// little chance of succeeding in the future but continues to hold locks from
// earlier epochs.
var MaxInternalTxnAutoRetries = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.transaction.internal.max_auto_retries",
	"controls the maximum number of auto-retries an internal KV transaction will "+
		"perform before aborting and returning an error. Can be set to 0 to disable any "+
		"retry attempt.",
	100,
	settings.NonNegativeInt,
)

var ErrAutoRetryLimitExhausted = errors.New("retry limit exhausted")

// IsAutoRetryLimitExhaustedError checks if the given error indicates
// that the maximum number of transaction auto-retries has been reached.
func IsAutoRetryLimitExhaustedError(err error) bool {
	return errors.Is(err, ErrAutoRetryLimitExhausted)
}

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

	// mu holds fields that need to be synchronized for concurrent request execution.
	mu struct {
		syncutil.Mutex
		ID           uuid.UUID
		debugName    string
		userPriority roachpb.UserPriority

		// sender is a stateful sender for use with transactions (usually a
		// TxnCoordSender). A new sender is created on transaction restarts (not
		// retries).
		sender TxnSender

		// The txn has to be committed by this deadline. A zero value indicates no
		// deadline.
		deadline hlc.Timestamp
	}

	// admissionHeader is used for admission control for work done in this
	// transaction. Only certain paths initialize this properly, and the
	// remaining just use the zero value. The set of code paths that initialize
	// this are expected to expand over time.
	admissionHeader kvpb.AdmissionHeader
}

// NewTxn returns a new RootTxn.
// Note: for SQL usage, prefer NewTxnWithSteppingEnabled() below.
// Note: for KV usage that should be subject to admission control, prefer
// NewTxnRootKV() below.
//
// If the transaction is used to send any operations, Commit() or Rollback()
// should eventually be called to commit/rollback the transaction (including
// stopping the heartbeat loop).
//
// gatewayNodeID: If != 0, this is the ID of the node on whose behalf this
//
//	transaction is running. Normally this is the current node, but in the case
//	of Txns created on remote nodes by DistSQL this will be the gateway.
//	If 0 is passed, then no value is going to be filled in the batches sent
//	through this txn. This will have the effect that the DistSender will fill
//	in the batch with the current node's ID.
//	If the gatewayNodeID is set and this is a root transaction, we optimize
//	away any clock uncertainty for our own node, as our clock is accessible.
//
// See also db.NewTxn().
func NewTxn(ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID) *Txn {
	return NewTxnWithAdmissionControl(
		ctx, db, gatewayNodeID, kvpb.AdmissionHeader_OTHER, admissionpb.NormalPri)
}

// NewTxnWithAdmissionControl creates a new transaction with the specified
// admission control source and priority. See NewTxn() for details.
func NewTxnWithAdmissionControl(
	ctx context.Context,
	db *DB,
	gatewayNodeID roachpb.NodeID,
	source kvpb.AdmissionHeader_Source,
	priority admissionpb.WorkPriority,
) *Txn {
	if db == nil {
		panic(errors.WithContextTags(
			errors.AssertionFailedf("attempting to create txn with nil db"), ctx))
	}

	now := db.clock.NowAsClockTimestamp()
	kvTxn := roachpb.MakeTransaction(
		"unnamed",
		nil, // baseKey
		isolation.Serializable,
		roachpb.NormalUserPriority,
		now.ToTimestamp(),
		db.clock.MaxOffset().Nanoseconds(),
		int32(db.ctx.NodeID.SQLInstanceID()),
		priority,
		false, /* omitInRangefeeds */
	)
	txn := NewTxnFromProto(ctx, db, gatewayNodeID, now, RootTxn, &kvTxn)
	txn.admissionHeader = kvpb.AdmissionHeader{
		CreateTime: db.clock.PhysicalNow(),
		Priority:   int32(priority),
		Source:     source,
	}
	return txn
}

// NewTxnWithSteppingEnabled is like NewTxn but suitable for use by SQL. Note
// that this initializes Txn.admissionHeader to specify that the source is
// FROM_SQL.
// qualityOfService is the QoSLevel level to use in admission control, whose
// value also corresponds exactly with the admissionpb.WorkPriority to use.
func NewTxnWithSteppingEnabled(
	ctx context.Context,
	db *DB,
	gatewayNodeID roachpb.NodeID,
	qualityOfService sessiondatapb.QoSLevel,
) *Txn {
	txn := NewTxnWithAdmissionControl(ctx, db, gatewayNodeID,
		kvpb.AdmissionHeader_FROM_SQL, admissionpb.WorkPriority(qualityOfService))
	_ = txn.ConfigureStepping(ctx, SteppingEnabled)
	return txn
}

// NewTxnRootKV is like NewTxn but specifically represents a transaction
// originating within KV and that is at the root of the tree of requests. For KV
// usage that should be subject to admission control. Do not use this for
// executing transactions originating in SQL. This distinction only causes this
// transaction to undergo admission control. See AdmissionHeader_Source for more
// details.
func NewTxnRootKV(ctx context.Context, db *DB, gatewayNodeID roachpb.NodeID) *Txn {
	return NewTxnWithAdmissionControl(
		ctx, db, gatewayNodeID, kvpb.AdmissionHeader_ROOT_KV, admissionpb.NormalPri)
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

// Key returns the current "anchor" key of the transaction, or nil if no such
// key has been set because the transaction has not yet acquired any locks.
func (txn *Txn) Key() roachpb.Key {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.Key()
}

// Epoch exports the txn's epoch.
func (txn *Txn) Epoch() enginepb.TxnEpoch {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.Epoch()
}

// statusLocked returns the txn proto status field.
func (txn *Txn) statusLocked() roachpb.TransactionStatus {
	return txn.mu.sender.TxnStatus()
}

// hasStatus returns true iff the transaction has the provided status.
func (txn *Txn) hasStatus(s roachpb.TransactionStatus) bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.statusLocked() == s
}

// IsCommitted returns true iff the transaction has the committed status.
func (txn *Txn) IsCommitted() bool { return txn.hasStatus(roachpb.COMMITTED) }

// IsAborted returns true iff the transaction has the aborted status.
func (txn *Txn) IsAborted() bool { return txn.hasStatus(roachpb.ABORTED) }

// IsPrepared returns true iff the transaction has the prepared status.
func (txn *Txn) IsPrepared() bool { return txn.hasStatus(roachpb.PREPARED) }

// IsOpen returns true iff the transaction is in the open state where
// it can accept further commands.
func (txn *Txn) IsOpen() bool { return txn.hasStatus(roachpb.PENDING) }

// isClientFinalized returns true if the client has issued an EndTxn request in
// an attempt to finalize the transaction.
func (txn *Txn) isClientFinalized() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.ClientFinalized()
}

// SetIsoLevel sets the transaction's isolation level. Transactions default to
// Serializable isolation. The isolation must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetIsoLevel(isoLevel isolation.Level) error {
	if txn.typ != RootTxn {
		return errors.AssertionFailedf("SetIsoLevel() called on leaf txn")
	}
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.SetIsoLevel(isoLevel)
}

// IsoLevel returns the transaction's isolation level.
func (txn *Txn) IsoLevel() isolation.Level {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.IsoLevel()
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

func (txn *Txn) SetBufferedWritesEnabled(enabled bool) {
	if txn.typ != RootTxn {
		panic(errors.AssertionFailedf("SetBufferedWritesEnabled() called on leaf txn"))
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.mu.sender.SetBufferedWritesEnabled(enabled)
}

func (txn *Txn) BufferedWritesEnabled() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	return txn.mu.sender.BufferedWritesEnabled()
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

// ReadTimestampFixed returns true if the read timestamp has been fixed
// and cannot be pushed forward.
func (txn *Txn) ReadTimestampFixed() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.ReadTimestampFixed()
}

// CommitTimestamp returns the transaction's commit timestamp.
//
// If the transaction is committed, the method returns the timestamp at
// which the transaction performed all of its writes.
//
// If the transaction is aborted, the method returns an error.
//
// If the transaction is pending and running under serializable isolation,
// the method returns the transaction's current provisional commit
// timestamp. It also fixes the transaction's read timestamp to ensure
// that the transaction cannot be pushed to a later timestamp and still
// commit. It does so by disabling read refreshes. As a result, using this
// method just once increases the likelihood that a retry error will
// bubble up to a client.
//
// If the transaction is pending and running under a weak isolation level,
// the method returns an error. Fixing the commit timestamp prematurely is
// not supported for transactions running under weak isolation levels.
func (txn *Txn) CommitTimestamp() (hlc.Timestamp, error) {
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

// GetOmitInRangefeeds returns the value of the OmitInRangefeeds attribute of
// the Transaction proto of the sender.
func (txn *Txn) GetOmitInRangefeeds() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.GetOmitInRangefeeds()
}

// SetOmitInRangefeeds sets the OmitInRangefeeds attribute to true in the
// Transaction proto of the sender.
//
// SetOmitInRangefeeds must be called before any operations are performed on the
// transaction.
func (txn *Txn) SetOmitInRangefeeds() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.sender.SetOmitInRangefeeds()
}

// NewBatch creates and returns a new empty batch object for use with the Txn.
func (txn *Txn) NewBatch() *Batch {
	return &Batch{txn: txn, AdmissionHeader: txn.AdmissionHeader()}
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error. It is not considered an error for the key to not exist.
//
//	r, err := txn.Get("a")
//	// string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) Get(ctx context.Context, key interface{}) (KeyValue, error) {
	b := txn.NewBatch()
	b.Get(key)
	return getOneRow(txn.Run(ctx, b), b)
}

// GetForUpdate retrieves the value for a key, returning the retrieved key/value
// or an error. An Exclusive lock with the supplied durability is acquired on
// the key, if it exists. It is not considered an error for the key not to
// exist.
//
//	r, err := txn.GetForUpdate("a")
//	// string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) GetForUpdate(
	ctx context.Context, key interface{}, dur kvpb.KeyLockingDurabilityType,
) (KeyValue, error) {
	b := txn.NewBatch()
	b.GetForUpdate(key, dur)
	return getOneRow(txn.Run(ctx, b), b)
}

// GetForShare retrieves the value for a key. A shared lock with the supplied
// durability is acquired on the key, if it exists. A new result will be
// appended to the batch which will contain a single row.
//
//	r, err := txn.GetForShare("a")
//	// string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) GetForShare(
	ctx context.Context, key interface{}, dur kvpb.KeyLockingDurabilityType,
) (KeyValue, error) {
	b := txn.NewBatch()
	b.GetForShare(key, dur)
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
	ctx context.Context,
	begin, end interface{},
	maxRows int64,
	isReverse bool,
	str kvpb.KeyLockingStrengthType,
	dur kvpb.KeyLockingDurabilityType,
) ([]KeyValue, error) {
	b := txn.NewBatch()
	if maxRows > 0 {
		b.Header.MaxSpanRequestKeys = maxRows
	}
	b.scan(begin, end, isReverse, str, dur)
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
	return txn.scan(ctx, begin, end, maxRows, false /* isReverse */, kvpb.NonLocking, kvpb.Invalid)
}

// ScanForUpdate retrieves the rows between begin (inclusive) and end
// (exclusive) in ascending order. Exclusive locks with the supplied durability
// are acquired on each of the returned keys.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ScanForUpdate(
	ctx context.Context, begin, end interface{}, maxRows int64, dur kvpb.KeyLockingDurabilityType,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, false /* isReverse */, kvpb.ForUpdate, dur)
}

// ScanForShare retrieves the rows between begin (inclusive) and end (exclusive)
// in ascending order. Shared locks[1] are acquired on each of the returned
// keys.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ScanForShare(
	ctx context.Context, begin, end interface{}, maxRows int64, dur kvpb.KeyLockingDurabilityType,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, false /* isReverse */, kvpb.ForShare, dur)
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
	return txn.scan(ctx, begin, end, maxRows, true /* isReverse */, kvpb.NonLocking, kvpb.Invalid)
}

// ReverseScanForUpdate retrieves the rows between begin (inclusive) and end
// (exclusive) in descending order. Exclusive locks with the supplied durability
// are acquired on each of the returned keys.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ReverseScanForUpdate(
	ctx context.Context, begin, end interface{}, maxRows int64, dur kvpb.KeyLockingDurabilityType,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, true /* isReverse */, kvpb.ForUpdate, dur)
}

// ReverseScanForShare retrieves the rows between begin (inclusive) and end
// (exclusive) in descending order. Shared locks with the supplied durability
// are acquired on each of the returned keys.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ReverseScanForShare(
	ctx context.Context, begin, end interface{}, maxRows int64, dur kvpb.KeyLockingDurabilityType,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, true /* isReverse */, kvpb.ForShare, dur)
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
// The returned []roachpb.Key will contain the keys that were actually deleted.
//
// key can be either a byte slice or a string.
func (txn *Txn) Del(ctx context.Context, keys ...interface{}) ([]roachpb.Key, error) {
	b := txn.NewBatch()
	b.Del(keys...)
	r, err := getOneResult(txn.Run(ctx, b), b)
	return r.Keys, err
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// The returned []roachpb.Key will contain the keys deleted if the returnKeys
// parameter is true, or will be nil if the parameter is false, and Result.Err
// will indicate success or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) DelRange(
	ctx context.Context, begin, end interface{}, returnKeys bool,
) ([]roachpb.Key, error) {
	b := txn.NewBatch()
	b.DelRange(begin, end, returnKeys)
	r, err := getOneResult(txn.Run(ctx, b), b)
	return r.Keys, err
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
	if err := b.validate(); err != nil {
		return err
	}
	return sendAndFill(ctx, txn.Send, b)
}

func (txn *Txn) commit(ctx context.Context) error {
	// A batch with only endTxnReq is not subject to admission control, in order
	// to reduce contention by releasing locks. In multi-tenant settings, it
	// will be subject to admission control, and the zero CreateTime will give
	// it preference within the tenant.
	et := endTxnReq(true, txn.deadline())
	ba := &kvpb.BatchRequest{Requests: et.unionArr[:]}
	_, pErr := txn.Send(ctx, ba)
	return pErr.GoError()
}

// Commit sends an EndTxnRequest with Commit=true.
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
	et := endTxnReq(true, txn.deadline())
	b.growReqs(1)
	b.reqs[len(b.reqs)-1].Value = &et.union
	b.initResult(1 /* calls */, 0, b.raw, nil)
	return txn.Run(ctx, b)
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
	txn.mu.deadline = deadline
	return nil
}

// DeadlineLikelySufficient returns true if there currently is a deadline and
// that deadline is earlier than either the ProvisionalCommitTimestamp or
// the current reading of the node's HLC clock. The second condition is a
// conservative optimization to deal with the fact that the provisional
// commit timestamp may not represent  the true commit timestamp; the
// transaction may have been pushed but not yet discovered that fact.
// Transactions that write from now on can still get pushed, versus
// transactions which are done writing where it will be less clear
// how those get pushed.
// Deadlines, in general, should not commonly be at risk of expiring near
// the current time, except in extraordinary circumstances. In cases where
// considering it helps, it helps a lot. In cases where considering it
// does not help, it does not hurt much.
func (txn *Txn) DeadlineLikelySufficient() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	// Instead of using the current HLC clock we will
	// use the current time with a fudge factor because:
	// 1) The clocks are desynchronized, so we may have
	//		been pushed above the current time.
	// 2) There is a potential to race against concurrent pushes,
	//    which a future timestamp will help against.
	// 3) If we are writing to non-blocking ranges than any
	//    push will be into the future.
	getTargetTS := func() hlc.Timestamp {
		sv := txn.db.SettingsValues()
		now := txn.db.Clock().NowAsClockTimestamp()
		maxClockOffset := txn.db.Clock().MaxOffset()
		lagTargetDuration := closedts.TargetDuration.Get(sv)
		leadTargetOverride := closedts.LeadForGlobalReadsOverride.Get(sv)
		sideTransportCloseInterval := closedts.SideTransportCloseInterval.Get(sv)
		return closedts.TargetForPolicy(now, maxClockOffset,
			lagTargetDuration, leadTargetOverride, sideTransportCloseInterval,
			roachpb.LEAD_FOR_GLOBAL_READS).Add(int64(time.Second), 0)
	}

	return !txn.mu.deadline.IsEmpty() &&
		// Avoid trying to get get the txn mutex again by directly
		// invoking ProvisionalCommitTimestamp versus calling
		// ProvisionalCommitTimestampLocked on the Txn.
		(txn.mu.deadline.Less(txn.mu.sender.ProvisionalCommitTimestamp()) ||
			// In case the transaction gets pushed and the push is not observed,
			// we cautiously also indicate that the deadline maybe expired if
			// the current HLC clock (with a fudge factor) exceeds the deadline.
			txn.mu.deadline.Less(getTargetTS()))
}

// resetDeadlineLocked resets the deadline.
func (txn *Txn) resetDeadlineLocked() {
	txn.mu.deadline = hlc.Timestamp{}
}

// Rollback sends an EndTxnRequest with Commit=false.
// txn is considered finalized and cannot be used to send any more commands.
func (txn *Txn) Rollback(ctx context.Context) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.AssertionFailedf("Rollback() called on leaf txn"), ctx)
	}

	return txn.rollback(ctx).GoError()
}

func (txn *Txn) rollback(ctx context.Context) *kvpb.Error {
	log.VEventf(ctx, 2, "rolling back transaction")

	// If the client has already disconnected, fall back to asynchronous cleanup
	// below. Note that this is the common path when a client disconnects in the
	// middle of an open transaction or during statement execution.
	if ctx.Err() == nil {
		// A batch with only endTxnReq is not subject to admission control, in
		// order to reduce contention by releasing locks. In multi-tenant
		// settings, it will be subject to admission control, and the zero
		// CreateTime will give it preference within the tenant.
		et := endTxnReq(false, hlc.Timestamp{} /* deadline */)
		ba := &kvpb.BatchRequest{Requests: et.unionArr[:]}
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
		et := endTxnReq(false, hlc.Timestamp{} /* deadline */)
		ba := &kvpb.BatchRequest{Requests: et.unionArr[:]}
		_ = timeutil.RunWithTimeout(ctx, "async txn rollback", asyncRollbackTimeout,
			func(ctx context.Context) error {
				if _, pErr := txn.Send(ctx, ba); pErr != nil {
					if statusErr, ok := pErr.GetDetail().(*kvpb.TransactionStatusError); ok &&
						statusErr.Reason == kvpb.TransactionStatusError_REASON_TXN_COMMITTED {
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
		return kvpb.NewError(err)
	}
	return nil
}

// Prepare sends an EndTxnRequest with Prepare=true. Once a transaction is
// prepared, it cannot be used to perform any more reads or writes. A prepared
// transaction can only be committed or rolled back.
func (txn *Txn) Prepare(ctx context.Context) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.AssertionFailedf("Prepare() called on leaf txn"), ctx)
	}

	et := endTxnReq(true, txn.deadline())
	et.req.Prepare = true
	ba := &kvpb.BatchRequest{Requests: et.unionArr[:]}
	_, pErr := txn.Send(ctx, ba)
	return pErr.GoError()
}

// AddCommitTrigger adds a closure to be executed on successful commit
// of the transaction.
func (txn *Txn) AddCommitTrigger(trigger func(ctx context.Context)) {
	if txn.typ != RootTxn {
		panic(errors.AssertionFailedf("AddCommitTrigger() called on leaf txn"))
	}

	txn.commitTriggers = append(txn.commitTriggers, trigger)
}

// endTxnReqAlloc is used to batch the heap allocations of an EndTxn request.
type endTxnReqAlloc struct {
	req      kvpb.EndTxnRequest
	union    kvpb.RequestUnion_EndTxn
	unionArr [1]kvpb.RequestUnion
}

func endTxnReq(commit bool, deadline hlc.Timestamp) *endTxnReqAlloc {
	alloc := new(endTxnReqAlloc)
	alloc.req.Commit = commit
	alloc.req.Deadline = deadline
	alloc.union.EndTxn = &alloc.req
	alloc.unionArr[0].Value = &alloc.union
	return alloc
}

// AutoCommitError wraps a non-retryable error coming from auto-commit.
type AutoCommitError struct {
	cause error
}

// Cause implements errors.Causer.
func (e *AutoCommitError) Cause() error {
	return e.cause
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
	retryOpts := base.DefaultRetryOptions()
	retryOpts.InitialBackoff = 20 * time.Millisecond
	retryOpts.MaxBackoff = 200 * time.Millisecond
	for r := retry.Start(retryOpts); r.Next(); {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "txn exec")
		}
		err = fn(ctx, txn)

		// Optionally inject retryable errors for testing.
		if err == nil && txn.TestingShouldRetry() {
			err = txn.GenerateForcedRetryableErr(ctx, "injected retriable error")
		}

		// Commit on success, unless the txn has already been committed or rolled
		// back by the closure. We allow that, as the closure might want to run 1PC
		// transactions or might want to rollback on certain conditions.
		if err == nil {
			if !txn.isClientFinalized() {
				err = txn.Commit(ctx)
				log.Eventf(ctx, "kv.Txn did AutoCommit. err: %v", err)
				if err != nil {
					if !errors.HasType(err, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
						// We can't retry, so let the caller know we tried to
						// autocommit.
						err = &AutoCommitError{cause: err}
					}
				}
			}
		}

		var retryable bool
		if err != nil {
			if errors.HasType(err, (*kvpb.UnhandledRetryableError)(nil)) {
				if txn.typ == RootTxn {
					// We sent transactional requests, so the TxnCoordSender was supposed to
					// turn retryable errors into TransactionRetryWithProtoRefreshError. Note that this
					// applies only in the case where this is the root transaction.
					log.Fatalf(ctx, "unexpected UnhandledRetryableError at the txn.exec() level: %s", err)
				}
			} else if t := (*kvpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(err, &t) {
				if txn.ID() != t.PrevTxnID {
					// Make sure the retryable error is meant for this level by checking
					// the transaction the error was generated for. If it's not, we
					// terminate the "retryable" character of the error. We might get a
					// TransactionRetryWithProtoRefreshError if the closure ran another
					// transaction internally and let the error propagate upwards instead
					// of handling it.
					return errors.Wrap(errors.Opaque(err), "retryable error from another txn")
				}
				if txn.isClientFinalized() {
					// We've already committed or rolled back, so we can't retry. The
					// closure should not have returned a retryable error in this case.
					return errors.NewAssertionErrorWithWrappedErrf(err,
						"client already committed or rolled back")
				}
				retryable = true
			}
		}

		if !retryable {
			break
		}

		// Determine whether the transaction has exceeded the maximum number of
		// automatic retries. We check this after each failed attempt to allow the
		// cluster setting to be changed while a transaction is stuck in a retry
		// loop.
		maxRetries := math.MaxInt64
		if txn.db.ctx.Settings != nil {
			// txn.db.ctx.Settings == nil is only expected in tests.
			maxRetries = int(MaxInternalTxnAutoRetries.Get(&txn.db.ctx.Settings.SV))
		}
		// Add 1 because r.CurrentAttempt() starts at 0.
		attempt := r.CurrentAttempt() + 1
		if attempt > maxRetries {
			// If the retries limit has been exceeded, rollback and return an error.
			rollbackErr := txn.Rollback(ctx)
			// NOTE: we don't errors.Wrap the most recent retry error because we want
			// to terminate it here. Instead, we mark the error to allow callers to
			// detect this condition and avoid automatic retries. We also include the
			// original error in the error message.
			err = errors.Mark(errors.Errorf("have retried transaction: %s %d times, most recently because of the "+
				"retryable error: %s. Terminating retry loop and returning error due to cluster setting %s (%d). "+
				"Rollback error: %v.", txn.DebugName(), attempt, err, MaxInternalTxnAutoRetries.Name(), maxRetries, rollbackErr),
				ErrAutoRetryLimitExhausted)
			log.Warningf(ctx, "%v", err)
			break
		}

		const warnEvery = 10
		if attempt%warnEvery == 0 {
			log.Warningf(ctx, "have retried transaction: %s %d times, most recently because of the "+
				"retryable error: %s. Is the transaction stuck in a retry loop?", txn.DebugName(), attempt, err)
		}

		if err := txn.PrepareForRetry(ctx); err != nil {
			return err
		}
	}

	return err
}

// PrepareForRetry needs to be called before a retry to perform some
// book-keeping and clear errors when possible.
func (txn *Txn) PrepareForRetry(ctx context.Context) error {
	// Reset commit triggers. These must be reconfigured by the client during the
	// next retry.
	txn.commitTriggers = nil

	txn.mu.Lock()
	defer txn.mu.Unlock()

	retryErr := txn.mu.sender.GetRetryableErr(ctx)
	if retryErr == nil {
		return nil
	}
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.NewAssertionErrorWithWrappedErrf(
			retryErr, "PrepareForRetry() called on leaf txn"), ctx)
	}
	if err := txn.checkRetryErrorTxnIDLocked(ctx, retryErr); err != nil {
		return err
	}

	log.VEventf(ctx, 2, "retrying transaction: %s because of a retryable error: %s",
		txn.debugNameLocked(), retryErr)
	txn.resetDeadlineLocked()

	if !retryErr.TxnMustRestartFromBeginning() {
		// If the retry error does not require the transaction to restart from
		// beginning, it will have also not caused the transaction to advance its
		// epoch. The caller has decided that it does want to restart from the
		// beginning, se we "promote" the partial retry error to a full retry by
		// manually restarting the transaction.
		const msg = "promoting partial retryable error to full transaction retry"
		log.VEventf(ctx, 2, msg)
		manualErr := txn.mu.sender.GenerateForcedRetryableErr(
			ctx, retryErr.NextTransaction.WriteTimestamp, true /* mustRestart */, msg)
		// Now replace retryErr with the error returned by ManualRestart.
		if !errors.As(manualErr, &retryErr) {
			return errors.WithContextTags(errors.NewAssertionErrorWithWrappedErrf(
				manualErr, "unexpected non-retry error during manual restart"), ctx)
		}
	}

	if !retryErr.PrevTxnAborted() {
		// If the retryable error doesn't correspond to an aborted transaction,
		// there's no need to switch out the transaction. We simply clear the
		// retryable error and proceed.
		return txn.mu.sender.ClearRetryableErr(ctx)
	}

	return txn.handleTransactionAbortedErrorLocked(ctx, retryErr)
}

// PrepareForPartialRetry is like PrepareForRetry, except that it expects the
// retryable error to not require the transaction to restart from the beginning
// (see TransactionRetryWithProtoRefreshError.TxnMustRestartFromBeginning). It
// is called once a partial retryable error has been handled and the caller is
// ready to continue using the transaction.
func (txn *Txn) PrepareForPartialRetry(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	retryErr := txn.mu.sender.GetRetryableErr(ctx)
	if retryErr == nil {
		return nil
	}
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.NewAssertionErrorWithWrappedErrf(
			retryErr, "PrepareForPartialRetry() called on leaf txn"), ctx)
	}
	if err := txn.checkRetryErrorTxnIDLocked(ctx, retryErr); err != nil {
		return err
	}
	if retryErr.TxnMustRestartFromBeginning() {
		return errors.WithContextTags(errors.NewAssertionErrorWithWrappedErrf(
			retryErr, "unexpected retryable error that must restart from beginning"), ctx)
	}

	log.VEventf(ctx, 2, "partially retrying transaction: %s because of a retryable error: %s",
		txn.debugNameLocked(), retryErr)

	return txn.mu.sender.ClearRetryableErr(ctx)
}

func (txn *Txn) checkRetryErrorTxnIDLocked(
	ctx context.Context, retryErr *kvpb.TransactionRetryWithProtoRefreshError,
) error {
	if txn.mu.ID == retryErr.PrevTxnID {
		return nil
	}
	// Sanity check that the retry error we're dealing with is for the current
	// incarnation of the transaction. Aborted transactions may be retried
	// transparently in certain cases and such incarnations come with new
	// txn IDs. However, at no point can both the old and new incarnation of a
	// transaction be active at the same time -- this would constitute a
	// programming error.
	return errors.WithContextTags(
		errors.NewAssertionErrorWithWrappedErrf(
			retryErr,
			"unexpected retryable error for old incarnation of the transaction %s; current incarnation %s",
			retryErr.PrevTxnID,
			txn.mu.ID,
		), ctx)
}

// Send runs the specified calls synchronously in a single batch and
// returns any errors. If the transaction is read-only or has already
// been successfully committed or aborted, a potential trailing
// EndTxn call is silently dropped, allowing the caller to always
// commit or clean-up explicitly even when that may not be required
// (or even erroneous). Returns (nil, nil) for an empty batch.
func (txn *Txn) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// Fill in the GatewayNodeID on the batch if the txn knows it.
	// NOTE(andrei): It seems a bit ugly that we're filling in the batches here as
	// opposed to the point where the requests are being created, but
	// unfortunately requests are being created in many ways and this was the best
	// place I found to set this field.
	if txn.gatewayNodeID != 0 {
		ba.Header.GatewayNodeID = txn.gatewayNodeID
	}

	// Requests with a bounded staleness header should use NegotiateAndSend.
	if ba.BoundedStaleness != nil {
		return nil, kvpb.NewError(errors.AssertionFailedf(
			"bounded staleness header passed to Txn.Send: %s", ba.String()))
	}

	// Some callers have not initialized ba using a Batch constructed using
	// Txn.NewBatch. So we fallback to partially overwriting here.
	if ba.AdmissionHeader.CreateTime == 0 {
		noMem := ba.AdmissionHeader.NoMemoryReservedAtSource
		ba.AdmissionHeader = txn.AdmissionHeader()
		ba.AdmissionHeader.NoMemoryReservedAtSource = noMem
	}

	txn.mu.Lock()
	requestTxnID := txn.mu.ID
	sender := txn.mu.sender
	txn.mu.Unlock()
	br, pErr := txn.db.sendUsingSender(ctx, ba, sender)

	if pErr == nil {
		// Invoking the commit triggers here ensures they run even in the case when a
		// commit request is issued manually (not via Commit).
		if et, ok := ba.GetArg(kvpb.EndTxn); ok && et.(*kvpb.EndTxnRequest).Commit {
			for _, t := range txn.commitTriggers {
				t(ctx)
			}
		}
		return br, nil
	}

	if retryErr, ok := pErr.GetDetail().(*kvpb.TransactionRetryWithProtoRefreshError); ok {
		if requestTxnID != retryErr.PrevTxnID {
			// KV should not return errors for transactions other than the one that sent
			// the request.
			log.Fatalf(ctx, "retryable error for the wrong txn. "+
				"requestTxnID: %s, retryErr.PrevTxnID: %s. retryErr: %s",
				requestTxnID, retryErr.PrevTxnID, retryErr)
		}
	}
	return br, pErr
}

// NegotiateAndSend is a specialized version of Send that is capable of
// orchestrating a bounded-staleness read through the transaction, given a
// read-only BatchRequest with a min_timestamp_bound set in its Header.
//
// Bounded-staleness orchestration consists of two phases - negotiation and
// execution. Negotiation determines the timestamp to run the query at in order
// to ensure that the read will not block on replication or on conflicting
// transactions. Execution then configures the transaction to use this timestamp
// and runs the read request in the context of the transaction.
//
// The transaction must not have been used before. If the call returns
// successfully, the transaction will have been given a fixed timestamp equal to
// the timestamp that the read-only request was evaluated at.
//
// If the read-only request hits a key or byte limit and returns a resume span,
// meaning that it was paginated and did not return all desired results, the
// transaction's timestamp will have still been fixed to a timestamp that was
// negotiated over the entire set of read spans in the provided batch. As such,
// it is safe for callers to resume reading at the bounded-staleness timestamp
// by using Send. Future calls to Send must not include a BoundedStaleness
// header, but may still specify the same routing policy.
//
// The method accepts requests with min_timestamp_bound_strict set to either
// true or false, which dictates whether a bounded staleness read whose
// min_timestamp_bound cannot be satisfied by the first replica it visits
// (subject to routing_policy) without blocking should be rejected with a
// MinTimestampBoundUnsatisfiableError or will be redirected to the leaseholder
// and permitted to block on conflicting transactions. If the flag is true,
// blocking is never permitted and callers should be prepared to handle
// MinTimestampBoundUnsatisfiableErrors. If the flag is false, blocking is
// permitted and MinTimestampBoundUnsatisfiableErrors will never be returned.
//
// The method accepts requests with either a LEASEHOLDER or a NEAREST routing
// policy, which dictates whether the request uses the leaseholder(s) of its
// target range(s) to negotiate a timestamp and perform the read or whether it
// uses the nearest replica(s) of its target range(s) to negotiate a timestamp
// and perform the read. Callers can use this flexibility to trade off increased
// staleness for reduced latency.
func (txn *Txn) NegotiateAndSend(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if err := txn.checkNegotiateAndSendPreconditions(ctx, ba); err != nil {
		return nil, kvpb.NewError(err)
	}
	if err := txn.applyDeadlineToBoundedStaleness(ctx, ba.BoundedStaleness); err != nil {
		return nil, kvpb.NewError(err)
	}

	// Attempt to hit the server-side negotiation fast-path. This fast-path
	// allows a bounded staleness read request that lands on a single range
	// to perform its negotiation phase and execution phase in a single RPC.
	//
	// The server-side negotiation fast-path provides two benefits:
	// 1. it avoids two network hops in the common-case where a bounded
	//    staleness read is targeting a single range. This in an important
	//    performance optimization for single-row point lookups.
	// 2. it provides stronger guarantees around minimizing staleness during
	//    bounded staleness reads. Bounded staleness reads that hit the
	//    server-side fast-path use their target replica's most up-to-date
	//    resolved timestamp, so they are as fresh as possible. Bounded
	//    staleness reads that miss the fast-path and perform explicit
	//    negotiation (see below) consult a cache, so they may use an
	//    out-of-date, suboptimal resolved timestamp, as long as it is fresh
	//    enough to satisfy the staleness bound of the request.
	//
	// To achieve this, we issue the batch as a non-transactional request
	// with a MinTimestampBound field set (enforced above). We send the
	// request through the TxnSenderFactory's wrapped non-transactional
	// sender, so that we not only avoid passing through our TxnCoordSender,
	// but also through the CrossRangeTxnWrapperSender. If the request spans
	// ranges, we want to hear about it.
	br, pErr := txn.DB().GetFactory().NonTransactionalSender().Send(ctx, ba)
	if pErr == nil {
		// Fix the transaction's timestamp at the result of the server-side
		// timestamp negotiation.
		if err := txn.SetFixedTimestamp(ctx, br.Timestamp); err != nil {
			return nil, kvpb.NewError(err)
		}
		// Note that we do not need to inform the TxnCoordSender about the
		// non-transactional reads that we issued on behalf of it. Now that the
		// transaction's timestamp is fixed, it won't be able to refresh anyway.
		return br, nil
	}
	if _, ok := pErr.GetDetail().(*kvpb.OpRequiresTxnError); !ok {
		return nil, pErr
	}

	// The read spans ranges, so bounded-staleness orchestration will need to be
	// performed in two distinct phases - negotiation and execution. First we'll
	// use the BoundedStalenessNegotiator to determines the timestamp to perform
	// the read at and fix the transaction's timestamp to this result. Then we'll
	// issue the request through the transaction, which will use the negotiated
	// read timestamp from the previous phase to execute the read.
	//
	// TODO(nvanbenschoten): implement this. #67554.

	return nil, kvpb.NewError(unimplemented.NewWithIssue(67554,
		"cross-range bounded staleness reads not yet implemented"))
}

// checks preconditions on BatchRequest and Txn for NegotiateAndSend.
func (txn *Txn) checkNegotiateAndSendPreconditions(
	ctx context.Context, ba *kvpb.BatchRequest,
) (err error) {
	assert := func(b bool, s string) {
		if !b {
			err = errors.CombineErrors(err,
				errors.WithContextTags(errors.AssertionFailedf(
					"%s: ba=%s, txn=%s", s, ba.String(), txn.String()), ctx),
			)
		}
	}
	if cfg := ba.BoundedStaleness; cfg == nil {
		assert(false, "bounded_staleness configuration must be set")
	} else {
		assert(!cfg.MinTimestampBound.IsEmpty(), "min_timestamp_bound must be set")
		assert(cfg.MaxTimestampBound.IsEmpty() || cfg.MinTimestampBound.Less(cfg.MaxTimestampBound),
			"max_timestamp_bound, if set, must be greater than min_timestamp_bound")
	}
	assert(ba.Timestamp.IsEmpty(), "timestamp must not be set")
	assert(ba.Txn == nil, "txn must not be set")
	assert(ba.ReadConsistency == kvpb.CONSISTENT, "read consistency must be set to CONSISTENT")
	assert(ba.IsReadOnly(), "batch must be read-only")
	assert(!ba.IsLocking(), "batch must not be locking")
	assert(txn.typ == RootTxn, "txn must be root")
	assert(!txn.ReadTimestampFixed(), "txn read timestamp must not be fixed")
	return err
}

// applyDeadlineToBoundedStaleness modifies the bounded staleness header to
// ensure that the negotiated timestamp respects the transaction deadline.
func (txn *Txn) applyDeadlineToBoundedStaleness(
	ctx context.Context, bs *kvpb.BoundedStalenessHeader,
) error {
	d := txn.deadline()
	if d.IsEmpty() {
		return nil
	}
	if d.LessEq(bs.MinTimestampBound) {
		return errors.WithContextTags(errors.AssertionFailedf(
			"transaction deadline %s equal to or below min_timestamp_bound %s",
			d, bs.MinTimestampBound), ctx)
	}
	if bs.MaxTimestampBound.IsEmpty() {
		bs.MaxTimestampBound = d
	} else {
		bs.MaxTimestampBound.Backward(d)
	}
	return nil
}

// GetLeafTxnInputState returns the LeafTxnInputState information for this
// transaction for use with NewLeafTxn(), when distributing the state of the
// current transaction to multiple distributed transaction coordinators.
//
// If the transaction is already aborted or otherwise in a state that cannot
// make progress, it returns an error. If the transaction is aborted, the error
// returned will be a retryable one. In such cases, the caller is responsible
// for handling the error before another attempt by calling PrepareForRetry. Use
// of the transaction before doing so will continue to be rejected.
func (txn *Txn) GetLeafTxnInputState(ctx context.Context) (*roachpb.LeafTxnInputState, error) {
	if txn.typ != RootTxn {
		return nil, errors.WithContextTags(
			errors.AssertionFailedf("GetLeafTxnInputState() called on leaf txn"), ctx)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.GetLeafTxnInputState(ctx)
}

// GetLeafTxnFinalState returns the LeafTxnFinalState information for this
// transaction for use with UpdateRootWithLeafFinalState(), when combining the
// impact of multiple distributed transaction coordinators that are
// all operating on the same transaction.
func (txn *Txn) GetLeafTxnFinalState(ctx context.Context) (*roachpb.LeafTxnFinalState, error) {
	if txn.typ != LeafTxn {
		return nil, errors.WithContextTags(
			errors.AssertionFailedf("GetLeafTxnFinalState() called on root txn"), ctx)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	tfs, err := txn.mu.sender.GetLeafTxnFinalState(ctx)
	if err != nil {
		return nil, errors.WithContextTags(
			errors.NewAssertionErrorWithWrappedErrf(err,
				"unexpected error from GetLeafTxnFinalState()"), ctx)
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
	return txn.mu.sender.UpdateRootWithLeafFinalState(ctx, tfs)
}

// UpdateStateOnRemoteRetryableErr updates the txn in response to an error
// encountered when running a request through the txn. Returns a
// TransactionRetryWithProtoRefreshError on success or another error on failure.
func (txn *Txn) UpdateStateOnRemoteRetryableErr(ctx context.Context, pErr *kvpb.Error) error {
	if txn.typ != RootTxn {
		return errors.AssertionFailedf("UpdateStateOnRemoteRetryableErr() called on leaf txn")
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if pErr.TransactionRestart() == kvpb.TransactionRestart_NONE {
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
	return pErr.GoError()
}

// handleTransactionAbortedErrorLocked handles performs bookkeeping to handle
// TransactionAbortedErrors before the transaction can be retried.
//
// For aborted transactions, the supplied error proto contains a new transaction
// that should be used on the next transaction attempt. The main thing to do
// here is to create a new sender associated with this transaction, and replace
// the current one with it.
func (txn *Txn) handleTransactionAbortedErrorLocked(
	ctx context.Context, retryErr *kvpb.TransactionRetryWithProtoRefreshError,
) error {
	if !retryErr.PrevTxnAborted() {
		// Sanity check we're dealing with a TransactionAbortedError.
		return errors.WithContextTags(errors.NewAssertionErrorWithWrappedErrf(
			retryErr, "cannot replace root sender if txn not aborted"), ctx)
	}

	// The transaction we had been using thus far has been aborted. The proto
	// inside the error has been prepared for use by the next transaction attempt.
	newTxn := &retryErr.NextTransaction
	txn.mu.ID = newTxn.ID
	// Create a new txn sender. We need to preserve the stepping mode, if any.
	prevSteppingMode := txn.mu.sender.GetSteppingMode(ctx)
	txn.mu.sender = txn.db.factory.RootTransactionalSender(newTxn, txn.mu.userPriority)
	txn.mu.sender.ConfigureStepping(ctx, prevSteppingMode)
	return nil
}

// SetFixedTimestamp makes the transaction run in an unusual way, at a "fixed
// timestamp": Timestamp and RefreshedTimestamp are set to ts, there's no clock
// uncertainty, and the txn's deadline is set to ts such that the transaction
// can't be pushed to a different timestamp.
//
// This is used to support historical queries (AS OF SYSTEM TIME queries and
// backups). This method must be called on every transaction retry (but note
// that retries should be rare for read-only queries with no clock uncertainty).
func (txn *Txn) SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) error {
	if txn.typ != RootTxn {
		return errors.WithContextTags(errors.AssertionFailedf(
			"SetFixedTimestamp() called on leaf txn"), ctx)
	}

	if ts.IsEmpty() {
		return errors.WithContextTags(errors.AssertionFailedf(
			"empty timestamp is invalid for SetFixedTimestamp()"), ctx)
	}
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.SetFixedTimestamp(ctx, ts)
}

// GenerateForcedRetryableErr returns a TransactionRetryWithProtoRefreshError
// that will cause the txn to be retried.
//
// The transaction's epoch is bumped, simulating to an extent what the
// TxnCoordSender does on retriable errors. The transaction's timestamp is only
// bumped to the extent that txn.ReadTimestamp is ratcheted up to txn.WriteTimestamp.
// TODO(andrei): This method should take in an up-to-date timestamp, but
// unfortunately its callers don't currently have that handy.
//
// As with other transaction retry errors, the caller must call PrepareForRetry
// before continuing to use the transaction.
func (txn *Txn) GenerateForcedRetryableErr(ctx context.Context, msg redact.RedactableString) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	now := txn.db.clock.NowAsClockTimestamp()
	return txn.mu.sender.GenerateForcedRetryableErr(ctx, now.ToTimestamp(), false /* mustRestart */, msg)
}

// RandomTxnRetryProbability is the probability that a transaction will inject a
// retryable error when either the RandomTransactionRetryFilter testing filter
// is installed or the COCKROACH_FORCE_RANDOM_TXN_RETRIES environment variable
// is set.
const RandomTxnRetryProbability = 0.1

// TestingShouldRetry returns true if we should generate a random, retryable
// error for this transaction.
func (txn *Txn) TestingShouldRetry() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.TestingShouldRetry()
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

func (txn *Txn) deadline() hlc.Timestamp {
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
func (txn *Txn) Step(ctx context.Context, allowReadTimestampStep bool) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.Step(ctx, allowReadTimestampStep)
}

// GetReadSeqNum gets the read sequence number for this transaction.
func (txn *Txn) GetReadSeqNum() enginepb.TxnSeq {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.GetReadSeqNum()
}

// SetReadSeqNum sets the read sequence number for this transaction.
func (txn *Txn) SetReadSeqNum(seq enginepb.TxnSeq) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.SetReadSeqNum(seq)
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

// CanUseSavepoint checks whether it would be valid to roll back or release
// the given savepoint in the current transaction state. It will never error.
func (txn *Txn) CanUseSavepoint(ctx context.Context, s SavepointToken) bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.sender.CanUseSavepoint(ctx, s)
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
func (txn *Txn) AdmissionHeader() kvpb.AdmissionHeader {
	h := txn.admissionHeader
	if txn.mu.sender.IsLocking() {
		h.Priority = int32(admissionpb.AdjustedPriorityWhenHoldingLocks(
			admissionpb.WorkPriority(h.Priority)))
	}
	return h
}

// OnePCNotAllowedError signifies that a request had the Require1PC flag set,
// but 1PC evaluation was not possible for one reason or another.
type OnePCNotAllowedError struct{}

var _ error = OnePCNotAllowedError{}

func (OnePCNotAllowedError) Error() string {
	return "could not commit in one phase as requested"
}
