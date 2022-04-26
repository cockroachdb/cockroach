// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// EvalContext defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
//
// ATTENTION: Some fields from this struct (particularly, but not exclusively,
// from SessionData) are also represented in execinfrapb.EvalContext. Whenever
// something that affects DistSQL execution is added, it needs to be marshaled
// through that proto too.
// TODO(andrei): remove or limit the duplication.
//
// NOTE(andrei): EvalContext is dusty; it started as a collection of fields
// needed by expression evaluation, but it has grown quite large; some of the
// things in it don't seem to belong in this low-level package (e.g. Planner).
// In the sql package it is embedded by extendedEvalContext, which adds some
// more fields from the sql package. Through that extendedEvalContext, this
// struct now generally used by planNodes.
type EvalContext struct {
	// SessionDataStack stores the session variables accessible by the correct
	// context. Each element on the stack represents the beginning of a new
	// transaction or nested transaction (savepoints).
	SessionDataStack *sessiondata.Stack
	// TxnState is a string representation of the current transactional state.
	TxnState string
	// TxnReadOnly specifies if the current transaction is read-only.
	TxnReadOnly bool
	// TxnImplicit specifies if the current transaction is implicit.
	TxnImplicit bool
	// TxnIsSingleStmt specifies the current implicit transaction consists of only
	// a single statement.
	TxnIsSingleStmt bool

	Settings *cluster.Settings
	// ClusterID is the logical cluster ID for this tenant.
	ClusterID uuid.UUID
	// ClusterName is the security string used to secure the RPC layer.
	ClusterName string
	// NodeID is either the SQL instance ID or KV Node ID, depending on
	// circumstances.
	// TODO(knz,radu): Split this into separate fields.
	NodeID *base.SQLIDContainer
	Codec  keys.SQLCodec

	// Locality contains the location of the current node as a set of user-defined
	// key/value pairs, ordered from most inclusive to least inclusive. If there
	// are no tiers, then the node's location is not known. Example:
	//
	//   [region=us,dc=east]
	//
	Locality roachpb.Locality

	Tracer *tracing.Tracer

	// The statement timestamp. May be different for every statement.
	// Used for statement_timestamp().
	StmtTimestamp time.Time
	// The transaction timestamp. Needs to stay stable for the lifetime
	// of a transaction. Used for now(), current_timestamp(),
	// transaction_timestamp() and the like.
	TxnTimestamp time.Time

	// AsOfSystemTime denotes the explicit AS OF SYSTEM TIME timestamp for the
	// query, if any. If the query is not an AS OF SYSTEM TIME query,
	// AsOfSystemTime is nil.
	// TODO(knz): we may want to support table readers at arbitrary
	// timestamps, so that each FROM clause can have its own
	// timestamp. In that case, the timestamp would not be set
	// globally for the entire txn and this field would not be needed.
	AsOfSystemTime *AsOfSystemTime

	// Placeholders relates placeholder names to their type and, later, value.
	// This pointer should always be set to the location of the PlaceholderInfo
	// in the corresponding SemaContext during normal execution. Placeholders are
	// available during Eval to permit lookup of a particular placeholder's
	// underlying datum, if available.
	Placeholders *PlaceholderInfo

	// Annotations augments the AST with extra information. This pointer should
	// always be set to the location of the Annotations in the corresponding
	// SemaContext.
	Annotations *Annotations

	// IVarContainer is used to evaluate IndexedVars. Note that the underlying
	// implementation must support the eval.IndexedVarContainer interface.
	IVarContainer IndexedVarContainer
	// iVarContainerStack is used when we swap out IVarContainers in order to
	// evaluate an intermediate expression. This keeps track of those which we
	// need to restore once we finish evaluating it.
	iVarContainerStack []IndexedVarContainer

	// Context holds the context in which the expression is evaluated.
	Context context.Context

	Planner Planner

	// Not using sql.JobExecContext type to avoid cycle dependency with sql package
	JobExecContext interface{}

	PrivilegedAccessor PrivilegedAccessor

	SessionAccessor SessionAccessor

	ClientNoticeSender ClientNoticeSender

	Sequence SequenceOperators

	Tenant TenantOperator

	// Regions stores information about regions.
	Regions RegionOperator

	JoinTokenCreator JoinTokenCreator

	PreparedStatementState PreparedStatementState

	// The transaction in which the statement is executing.
	Txn *kv.Txn
	// A handle to the database.
	DB *kv.DB

	ReCache *RegexpCache

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	PrepareOnly bool

	// SkipNormalize indicates whether expressions should be normalized
	// (false) or not (true).  It is set to true conditionally by
	// EXPLAIN(TYPES[, NORMALIZE]).
	SkipNormalize bool

	CollationEnv CollationEnvironment

	TestingKnobs eval.TestingKnobs

	Mon *mon.BytesMonitor

	// SingleDatumAggMemAccount is a memory account that all aggregate builtins
	// that store a single datum will share to account for the memory needed to
	// perform the aggregation (i.e. memory not reported by AggregateFunc.Size
	// method). This memory account exists so that such aggregate functions
	// could "batch" their reservations - otherwise, we end up a situation
	// where each aggregate function struct grows its own memory account by
	// tiny amount, yet the account reserves a lot more resulting in
	// significantly overestimating the memory usage.
	SingleDatumAggMemAccount *mon.BoundAccount

	SQLLivenessReader sqlliveness.Reader

	SQLStatsController SQLStatsController

	IndexUsageStatsController IndexUsageStatsController

	// CompactEngineSpan is used to force compaction of a span in a store.
	CompactEngineSpan CompactEngineSpanFunc

	// KVStoresIterator is used by various crdb_internal builtins to directly
	// access stores on this node.
	KVStoresIterator kvserverbase.StoresIterator
}

var _ ParseTimeContext = &EvalContext{}

// UnwrapDatum encapsulates UnwrapDatum for use in the tree.CompareContext.
func (ec *EvalContext) UnwrapDatum(d Datum) Datum {
	return UnwrapDatum(ec, d)
}

// MustGetPlaceholderValue is part of the tree.CompareContext interface.
func (ec *EvalContext) MustGetPlaceholderValue(p *Placeholder) Datum {
	e, ok := ec.Placeholders.Value(p.Idx)
	if !ok {
		panic(errors.AssertionFailedf("fail"))
	}
	out, err := eval.Expr(ec, e)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "fail"))
	}
	return out
}

// MakeTestingEvalContext returns an EvalContext that includes a MemoryMonitor.
func MakeTestingEvalContext(st *cluster.Settings) EvalContext {
	monitor := mon.NewMonitor(
		"test-monitor",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	return MakeTestingEvalContextWithMon(st, monitor)
}

// MakeTestingEvalContextWithMon returns an EvalContext with the given
// MemoryMonitor. Ownership of the memory monitor is transferred to the
// EvalContext so do not start or close the memory monitor.
func MakeTestingEvalContextWithMon(st *cluster.Settings, monitor *mon.BytesMonitor) EvalContext {
	ctx := EvalContext{
		Codec:            keys.SystemSQLCodec,
		Txn:              &kv.Txn{},
		SessionDataStack: sessiondata.NewStack(&sessiondata.SessionData{}),
		Settings:         st,
		NodeID:           base.TestingIDContainer,
	}
	monitor.Start(context.Background(), nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	ctx.Mon = monitor
	ctx.Context = context.TODO()
	now := timeutil.Now()
	ctx.SetTxnTimestamp(now)
	ctx.SetStmtTimestamp(now)
	return ctx
}

// SessionData returns the SessionData the current EvalCtx should use to eval.
func (ec *EvalContext) SessionData() *sessiondata.SessionData {
	if ec.SessionDataStack == nil {
		return nil
	}
	return ec.SessionDataStack.Top()
}

// Copy returns a deep copy of ctx.
func (ec *EvalContext) Copy() *EvalContext {
	ctxCopy := *ec
	ctxCopy.iVarContainerStack = make([]IndexedVarContainer, len(ec.iVarContainerStack), cap(ec.iVarContainerStack))
	copy(ctxCopy.iVarContainerStack, ec.iVarContainerStack)
	return &ctxCopy
}

// PushIVarContainer replaces the current IVarContainer with a different one -
// pushing the current one onto a stack to be replaced later once
// PopIVarContainer is called.
func (ec *EvalContext) PushIVarContainer(c IndexedVarContainer) {
	ec.iVarContainerStack = append(ec.iVarContainerStack, ec.IVarContainer)
	ec.IVarContainer = c
}

// PopIVarContainer discards the current IVarContainer on the EvalContext,
// replacing it with an older one.
func (ec *EvalContext) PopIVarContainer() {
	ec.IVarContainer = ec.iVarContainerStack[len(ec.iVarContainerStack)-1]
	ec.iVarContainerStack = ec.iVarContainerStack[:len(ec.iVarContainerStack)-1]
}

// QualityOfService returns the current value of session setting
// default_transaction_quality_of_service if session data is available,
// otherwise the default value (0).
func (ec *EvalContext) QualityOfService() sessiondatapb.QoSLevel {
	if ec.SessionData() == nil {
		return sessiondatapb.Normal
	}
	return ec.SessionData().DefaultTxnQualityOfService
}

// NewTestingEvalContext is a convenience version of MakeTestingEvalContext
// that returns a pointer.
func NewTestingEvalContext(st *cluster.Settings) *EvalContext {
	ctx := MakeTestingEvalContext(st)
	return &ctx
}

// Stop closes out the EvalContext and must be called once it is no longer in use.
func (ec *EvalContext) Stop(c context.Context) {
	if r := recover(); r != nil {
		ec.Mon.EmergencyStop(c)
		panic(r)
	} else {
		ec.Mon.Stop(c)
	}
}

// FmtCtx creates a FmtCtx with the given options as well as the EvalContext's session data.
func (ec *EvalContext) FmtCtx(f FmtFlags, opts ...FmtCtxOption) *FmtCtx {
	if ec.SessionData() != nil {
		opts = append(
			[]FmtCtxOption{FmtDataConversionConfig(ec.SessionData().DataConversionConfig)},
			opts...,
		)
	}
	return NewFmtCtx(
		f,
		opts...,
	)
}

// GetStmtTimestamp retrieves the current statement timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *EvalContext) GetStmtTimestamp() time.Time {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.StmtTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero statement timestamp in EvalContext"))
	}
	return ec.StmtTimestamp
}

// GetClusterTimestamp retrieves the current cluster timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *EvalContext) GetClusterTimestamp() *DDecimal {
	ts := ec.Txn.CommitTimestamp()
	if ts.IsEmpty() {
		panic(errors.AssertionFailedf("zero cluster timestamp in txn"))
	}
	return TimestampToDecimalDatum(ts)
}

// HasPlaceholders returns true if this EvalContext's placeholders have been
// assigned. Will be false during Prepare.
func (ec *EvalContext) HasPlaceholders() bool {
	return ec.Placeholders != nil
}

const regionKey = "region"

// GetLocalRegion returns the region name of the local processor
// on which we're executing.
func (ec *EvalContext) GetLocalRegion() (regionName string, ok bool) {
	return ec.Locality.Find(regionKey)
}

var (
	big10E6  = apd.NewBigInt(1e6)
	big10E10 = apd.NewBigInt(1e10)
)

// TimestampToDecimal converts the logical timestamp into a decimal
// value with the number of nanoseconds in the integer part and the
// logical counter in the decimal part.
func TimestampToDecimal(ts hlc.Timestamp) apd.Decimal {
	// Compute Walltime * 10^10 + Logical.
	// We need 10 decimals for the Logical field because its maximum
	// value is 4294967295 (2^32-1), a value with 10 decimal digits.
	var res apd.Decimal
	val := &res.Coeff
	val.SetInt64(ts.WallTime)
	val.Mul(val, big10E10)
	val.Add(val, apd.NewBigInt(int64(ts.Logical)))

	// val must be positive. If it was set to a negative value above,
	// transfer the sign to res.Negative.
	res.Negative = val.Sign() < 0
	val.Abs(val)

	// Shift 10 decimals to the right, so that the logical
	// field appears as fractional part.
	res.Exponent = -10
	return res
}

// DecimalToInexactDTimestampTZ is the inverse of TimestampToDecimal. It converts
// a decimal constructed from an hlc.Timestamp into an approximate DTimestampTZ
// containing the walltime of the hlc.Timestamp.
func DecimalToInexactDTimestampTZ(d *DDecimal) (*DTimestampTZ, error) {
	ts, err := decimalToHLC(d)
	if err != nil {
		return nil, err
	}
	return MakeDTimestampTZ(timeutil.Unix(0, ts.WallTime), time.Microsecond)
}

func decimalToHLC(d *DDecimal) (hlc.Timestamp, error) {
	var coef apd.BigInt
	coef.Set(&d.Decimal.Coeff)
	// The physical portion of the HLC is stored shifted up by 10^10, so shift
	// it down and clear out the logical component.
	coef.Div(&coef, big10E10)
	if !coef.IsInt64() {
		return hlc.Timestamp{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"timestamp value out of range: %s", d.String(),
		)
	}
	return hlc.Timestamp{WallTime: coef.Int64()}, nil
}

// DecimalToInexactDTimestamp is the inverse of TimestampToDecimal. It converts
// a decimal constructed from an hlc.Timestamp into an approximate DTimestamp
// containing the walltime of the hlc.Timestamp.
func DecimalToInexactDTimestamp(d *DDecimal) (*DTimestamp, error) {
	ts, err := decimalToHLC(d)
	if err != nil {
		return nil, err
	}
	return TimestampToInexactDTimestamp(ts), nil
}

// TimestampToDecimalDatum is the same as TimestampToDecimal, but
// returns a datum.
func TimestampToDecimalDatum(ts hlc.Timestamp) *DDecimal {
	res := TimestampToDecimal(ts)
	return &DDecimal{
		Decimal: res,
	}
}

// TimestampToInexactDTimestamp converts the logical timestamp into an
// inexact DTimestamp by dropping the logical counter and using the wall
// time at the microsecond precision.
func TimestampToInexactDTimestamp(ts hlc.Timestamp) *DTimestamp {
	return MustMakeDTimestamp(timeutil.Unix(0, ts.WallTime), time.Microsecond)
}

// GetRelativeParseTime implements ParseTimeContext.
func (ec *EvalContext) GetRelativeParseTime() time.Time {
	ret := ec.TxnTimestamp
	if ret.IsZero() {
		ret = timeutil.Now()
	}
	return ret.In(ec.GetLocation())
}

// GetTxnTimestamp retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *EvalContext) GetTxnTimestamp(precision time.Duration) *DTimestampTZ {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return MustMakeDTimestampTZ(ec.GetRelativeParseTime(), precision)
}

// GetTxnTimestampNoZone retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *EvalContext) GetTxnTimestampNoZone(precision time.Duration) *DTimestamp {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	// Move the time to UTC, but keeping the location's time.
	t := ec.GetRelativeParseTime()
	_, offsetSecs := t.Zone()
	return MustMakeDTimestamp(t.Add(time.Second*time.Duration(offsetSecs)).In(time.UTC), precision)
}

// GetTxnTime retrieves the current transaction time as per
// the evaluation context.
func (ec *EvalContext) GetTxnTime(precision time.Duration) *DTimeTZ {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return NewDTimeTZFromTime(ec.GetRelativeParseTime().Round(precision))
}

// GetTxnTimeNoZone retrieves the current transaction time as per
// the evaluation context.
func (ec *EvalContext) GetTxnTimeNoZone(precision time.Duration) *DTime {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return MakeDTime(timeofday.FromTime(ec.GetRelativeParseTime().Round(precision)))
}

// SetTxnTimestamp sets the corresponding timestamp in the EvalContext.
func (ec *EvalContext) SetTxnTimestamp(ts time.Time) {
	ec.TxnTimestamp = ts
}

// SetStmtTimestamp sets the corresponding timestamp in the EvalContext.
func (ec *EvalContext) SetStmtTimestamp(ts time.Time) {
	ec.StmtTimestamp = ts
}

// GetLocation returns the session timezone.
func (ec *EvalContext) GetLocation() *time.Location {
	return ec.SessionData().GetLocation()
}

// GetIntervalStyle returns the session interval style.
func (ec *EvalContext) GetIntervalStyle() duration.IntervalStyle {
	if ec.SessionData() == nil {
		return duration.IntervalStyle_POSTGRES
	}
	return ec.SessionData().GetIntervalStyle()
}

// GetDateStyle returns the session date style.
func (ec *EvalContext) GetDateStyle() pgdate.DateStyle {
	if ec.SessionData() == nil {
		return pgdate.DefaultDateStyle()
	}
	return ec.SessionData().GetDateStyle()
}

// Ctx returns the session's context.
func (ec *EvalContext) Ctx() context.Context {
	return ec.Context
}

// UnwrapDatum returns the base Datum type for a provided datum, stripping
// an *DOidWrapper if present. This is useful for cases like type switches,
// where type aliases should be ignored.
func UnwrapDatum(evalCtx *EvalContext, d Datum) Datum {
	d = UnwrapDOidWrapper(d)
	if p, ok := d.(*Placeholder); ok && evalCtx != nil && evalCtx.HasPlaceholders() {
		ret, err := eval.Expr(evalCtx, p)
		if err != nil {
			// If we fail to evaluate the placeholder, it's because we don't have
			// a placeholder available. Just return the placeholder and someone else
			// will handle this problem.
			return d
		}
		return ret
	}
	return d
}
