// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"
	"math"
	"math/rand"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/inspectz/inspectzpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tochar"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/ulid"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var ErrNilTxnInClusterContext = errors.New("nil txn in cluster context")

// Context defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
//
// ATTENTION: Some fields from this struct (particularly, but not exclusively,
// from SessionData) are also represented in execinfrapb.EvalContext. Whenever
// something that affects DistSQL execution is added, it needs to be marshaled
// through that proto too.
// TODO(andrei): remove or limit the duplication.
//
// NOTE(andrei): Context is dusty; it started as a collection of fields
// needed by expression evaluation, but it has grown quite large; some of the
// things in it don't seem to belong in this low-level package (e.g. Planner).
// In the sql package it is embedded by extendedEvalContext, which adds some
// more fields from the sql package. Through that extendedEvalContext, this
// struct now generally used by planNodes.
type Context struct {
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
	// TxnIsoLevel is the isolation level of the current transaction.
	TxnIsoLevel isolation.Level

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
	// The region entry in this variable is the gateway region.
	Locality roachpb.Locality

	// OriginalLocality is the initial Locality at the time the connection was
	// established. Since Locality may be overridden in some paths, this provides
	// a means of restoring the original Locality.
	OriginalLocality roachpb.Locality

	Tracer *tracing.Tracer

	// The statement timestamp. May be different for every statement.
	// Used for statement_timestamp().
	StmtTimestamp time.Time
	// The transaction timestamp. Needs to stay stable for the lifetime
	// of a transaction. Used for now(), current_timestamp(),
	// transaction_timestamp() and the like.
	TxnTimestamp time.Time

	// AsOfSystemTime denotes either the explicit (i.e. in the query text) or
	// implicit (via the txn mode) AS OF SYSTEM TIME timestamp for the query, if
	// any. If the query is not an AS OF SYSTEM TIME query, AsOfSystemTime is
	// nil.
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
	Placeholders *tree.PlaceholderInfo

	// Annotations augments the AST with extra information. This pointer should
	// always be set to the location of the Annotations in the corresponding
	// SemaContext.
	Annotations *tree.Annotations

	// IVarContainer is used to evaluate IndexedVars. Note that the underlying
	// implementation must support the eval.IndexedVarContainer interface.
	IVarContainer tree.IndexedVarContainer
	// iVarContainerStack is used when we swap out IVarContainers in order to
	// evaluate an intermediate expression. This keeps track of those which we
	// need to restore once we finish evaluating it.
	iVarContainerStack []tree.IndexedVarContainer

	Planner Planner

	StreamManagerFactory StreamManagerFactory

	// Not using sql.JobExecContext type to avoid cycle dependency with sql package
	JobExecContext interface{}

	PrivilegedAccessor PrivilegedAccessor

	SessionAccessor SessionAccessor

	ClientNoticeSender ClientNoticeSender

	Sequence SequenceOperators

	Tenant TenantOperator

	// Regions stores information about regions.
	Regions RegionOperator

	Gossip GossipOperator

	PreparedStatementState PreparedStatementState

	// The transaction in which the statement is executing.
	Txn *kv.Txn

	ReCache           *tree.RegexpCache
	ToCharFormatCache *tochar.FormatCache

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	PrepareOnly bool

	// SkipNormalize indicates whether expressions should be normalized
	// (false) or not (true).  It is set to true conditionally by
	// EXPLAIN(TYPES[, NORMALIZE]).
	SkipNormalize bool

	CollationEnv tree.CollationEnvironment

	TestingKnobs TestingKnobs

	// TestingMon is a memory monitor that should be only used in tests. In
	// production code consider using either the monitor of the planner or of
	// the flow.
	// TODO(yuzefovich): remove this.
	TestingMon *mon.BytesMonitor

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

	SchemaTelemetryController SchemaTelemetryController

	IndexUsageStatsController IndexUsageStatsController

	// CompactEngineSpan is used to force compaction of a span in a store.
	CompactEngineSpan CompactEngineSpanFunc

	// GetTableMetrics is used in crdb_internal.sstable_metrics.
	GetTableMetrics GetTableMetricsFunc

	// ScanStorageInternalKeys is used in crdb_internal.scan_storage_internal_keys.
	ScanStorageInternalKeys ScanStorageInternalKeysFunc

	// SetCompactionConcurrency is used to change the compaction concurrency of
	// a store.
	SetCompactionConcurrency SetCompactionConcurrencyFunc

	// KVStoresIterator is used by various crdb_internal builtins to directly
	// access stores on this node.
	KVStoresIterator kvserverbase.StoresIterator

	// InspectzServer is used to power various crdb_internal vtables, exposing
	// the equivalent of /inspectz but through SQL.
	InspectzServer inspectzpb.InspectzServer

	// ConsistencyChecker is to generate the results in calls to
	// crdb_internal.check_consistency.
	ConsistencyChecker ConsistencyCheckRunner

	// RangeProber is used in calls to crdb_internal.probe_ranges.
	RangeProber RangeProber

	// StmtDiagnosticsRequestInserter is used by the
	// crdb_internal.request_statement_bundle builtin to insert a statement
	// bundle request.
	StmtDiagnosticsRequestInserter StmtDiagnosticsRequestInsertFunc

	// CatalogBuiltins is used by various builtins which depend on looking up
	// catalog information. Unlike the Planner, it is available in DistSQL.
	CatalogBuiltins CatalogBuiltins

	// QueryCancelKey is the key used by the pgwire protocol to cancel the
	// query currently running in this session.
	QueryCancelKey pgwirecancel.BackendKeyData

	DescIDGenerator DescIDGenerator

	// RangeStatsFetcher is used to fetch RangeStats.
	RangeStatsFetcher RangeStatsFetcher

	// ChangefeedState stores the state (progress) of core changefeeds.
	ChangefeedState ChangefeedState

	// ParseHelper makes date parsing more efficient.
	ParseHelper pgdate.ParseHelper

	// RemoteRegions contains the slice of remote regions in a multiregion
	// database which owns a table accessed by the current SQL request.
	// This slice is only populated during the optbuild stage.
	RemoteRegions catpb.RegionNames

	// JobsProfiler is the interface for builtins to extract job specific
	// execution details that may have been aggregated during a job's lifetime.
	JobsProfiler JobsProfiler

	// RoutineSender allows nested routines in tail-call position to defer their
	// execution until control returns to the parent routine. It is only valid
	// during local execution. It may be unset.
	RoutineSender DeferredRoutineSender

	// RNGFactory, if set, provides the random number generator for the "random"
	// built-in function.
	//
	// NB: do not access this field directly - use GetRNG() instead. This field
	// is exported only for the connExecutor to pass its "external" RNGFactory.
	RNGFactory *RNGFactory

	// internalRNGFactory provides the random number generator for the "random"
	// built-in function if RNGFactory is not set. This field exists to allow
	// not setting RNGFactory on the code paths that don't need to preserve
	// usage of the same RNG within a session.
	internalRNGFactory RNGFactory

	// ULIDEntropyFactory, if set, is the entropy source for ULID generation.
	//
	// NB: do not access this field directly - use GetULIDEntropy() instead.
	// This field is exported only for the connExecutor to pass its "external"
	// ULIDEntropyFactory.
	ULIDEntropyFactory *ULIDEntropyFactory

	// internalULIDEntropyFactory is the entropy source for ULID generation if
	// ULIDEntropyFactory is not set. This field exists to allow not setting
	// ULIDEntropyFactory on the code paths that don't need to preserve usage of
	// the same RNG within a session.
	internalULIDEntropyFactory ULIDEntropyFactory

	// CidrLookup is used to look up the tag name for a given IP address.
	CidrLookup *cidr.Lookup
}

// RNGFactory is a simple wrapper to preserve the RNG throughout the session.
type RNGFactory struct {
	rng *rand.Rand
}

// GetRNG returns the RNG of the Context (which is lazily instantiated if
// necessary).
func (ec *Context) GetRNG() *rand.Rand {
	if ec.RNGFactory != nil {
		return ec.RNGFactory.getOrCreate()
	}
	return ec.internalRNGFactory.getOrCreate()
}

func (r *RNGFactory) getOrCreate() *rand.Rand {
	if r.rng == nil {
		r.rng, _ = randutil.NewPseudoRand()
	}
	return r.rng
}

// ULIDEntropyFactory is a simple wrapper to preserve the ULID entropy
// throughout the session.
type ULIDEntropyFactory struct {
	entropy ulid.MonotonicReader
}

// GetULIDEntropy returns the ULID entropy of the Context (which is lazily
// instantiated if necessary).
func (ec *Context) GetULIDEntropy() ulid.MonotonicReader {
	if ec.ULIDEntropyFactory != nil {
		return ec.ULIDEntropyFactory.getOrCreate(ec.GetRNG())
	}
	return ec.internalULIDEntropyFactory.getOrCreate(ec.GetRNG())
}

func (f *ULIDEntropyFactory) getOrCreate(rng *rand.Rand) ulid.MonotonicReader {
	if f.entropy == nil {
		f.entropy = ulid.Monotonic(rng, 0 /* inc */)
	}
	return f.entropy
}

// JobsProfiler is the interface used to fetch job specific execution details
// that may have been aggregated during a job's lifetime.
type JobsProfiler interface {
	// GenerateExecutionDetailsJSON generates a JSON blob of the job specific
	// execution details.
	GenerateExecutionDetailsJSON(ctx context.Context, evalCtx *Context, jobID jobspb.JobID) ([]byte, error)

	// RequestExecutionDetailFiles triggers the collection of execution details
	// for the specified jobID that are then persisted to `system.job_info`.
	RequestExecutionDetailFiles(ctx context.Context, jobID jobspb.JobID) error
}

// DescIDGenerator generates unique descriptor IDs.
type DescIDGenerator interface {

	// PeekNextUniqueDescID returns a lower bound to the next as-of-yet unassigned
	// unique descriptor ID in the sequence.
	PeekNextUniqueDescID(ctx context.Context) (catid.DescID, error)

	// GenerateUniqueDescID returns the next available Descriptor ID and
	// increments the counter.
	GenerateUniqueDescID(ctx context.Context) (catid.DescID, error)

	// IncrementDescID increments the descriptor ID counter by at least inc.
	// It returns the first ID in the incremented range:
	// <val> .. <val> + inc  are all available to the caller.
	IncrementDescID(ctx context.Context, inc int64) (catid.DescID, error)
}

// RangeStatsFetcher is used to fetch RangeStats.
type RangeStatsFetcher interface {

	// RangeStats fetches the stats for the ranges which contain the passed keys.
	RangeStats(ctx context.Context, keys ...roachpb.Key) ([]*kvpb.RangeStatsResponse, error)
}

var _ tree.ParseContext = &Context{}

// ConsistencyCheckRunner is an interface embedded in eval.Context used by
// crdb_internal.check_consistency.
type ConsistencyCheckRunner interface {
	CheckConsistency(
		ctx context.Context, from, to roachpb.Key, mode kvpb.ChecksumMode,
	) (*kvpb.CheckConsistencyResponse, error)
}

// RangeProber is an interface embedded in eval.Context used by
// crdb_internal.probe_ranges.
type RangeProber interface {
	RunProbe(
		ctx context.Context, desc *roachpb.RangeDescriptor, isWrite bool,
	) error
}

// UnwrapDatum encapsulates UnwrapDatum for use in the tree.CompareContext.
func (ec *Context) UnwrapDatum(ctx context.Context, d tree.Datum) tree.Datum {
	return UnwrapDatum(ctx, ec, d)
}

// MustGetPlaceholderValue is part of the tree.CompareContext interface.
func (ec *Context) MustGetPlaceholderValue(ctx context.Context, p *tree.Placeholder) tree.Datum {
	e, ok := ec.Placeholders.Value(p.Idx)
	if !ok {
		panic(errors.AssertionFailedf("fail"))
	}
	out, err := Expr(ctx, ec, e)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "fail"))
	}
	return out
}

// MakeTestingEvalContext returns an EvalContext that includes a MemoryMonitor.
func MakeTestingEvalContext(st *cluster.Settings) Context {
	monitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-monitor"),
		Settings: st,
	})
	return MakeTestingEvalContextWithMon(st, monitor)
}

// MakeTestingEvalContextWithMon returns an EvalContext with the given
// MemoryMonitor. Ownership of the memory monitor is transferred to the
// EvalContext so do not start or close the memory monitor.
func MakeTestingEvalContextWithMon(st *cluster.Settings, monitor *mon.BytesMonitor) Context {
	ctx := Context{
		Codec:            keys.SystemSQLCodec,
		Txn:              &kv.Txn{},
		SessionDataStack: sessiondata.NewStack(&sessiondata.SessionData{}),
		Settings:         st,
		NodeID:           base.TestingIDContainer,
	}
	monitor.Start(context.Background(), nil /* pool */, mon.NewStandaloneBudget(math.MaxInt64))
	ctx.TestingMon = monitor
	ctx.Planner = &fakePlannerWithMonitor{monitor: monitor}
	ctx.StreamManagerFactory = &fakeStreamManagerFactory{}
	now := timeutil.Now()
	ctx.SetTxnTimestamp(now)
	ctx.SetStmtTimestamp(now)
	return ctx
}

type fakePlannerWithMonitor struct {
	Planner
	monitor *mon.BytesMonitor
}

// Mon is part of the Planner interface.
func (p *fakePlannerWithMonitor) Mon() *mon.BytesMonitor {
	return p.monitor
}

// IsANSIDML is part of the eval.Planner interface.
func (p *fakePlannerWithMonitor) IsANSIDML() bool {
	return false
}

// EnforceHomeRegion is part of the eval.Planner interface.
func (p *fakePlannerWithMonitor) EnforceHomeRegion() bool {
	return false
}

// MaybeReallocateAnnotations is part of the eval.Planner interface.
func (p *fakePlannerWithMonitor) MaybeReallocateAnnotations(numAnnotations tree.AnnotationIdx) {
}

// Optimizer is part of the cat.Catalog interface.
func (p *fakePlannerWithMonitor) Optimizer() interface{} {
	return nil
}

// PLpgSQLFetchCursor is part of the eval.Planner interface.
func (p *fakePlannerWithMonitor) PLpgSQLFetchCursor(
	ctx context.Context, cursorStmt *tree.CursorStmt,
) (res tree.Datums, err error) {
	return nil, nil
}

// AutoCommit is part of the eval.Planner interface.
func (p *fakePlannerWithMonitor) AutoCommit() bool {
	return false
}

type fakeStreamManagerFactory struct {
	StreamManagerFactory
}

// SessionData returns the SessionData the current EvalCtx should use to eval.
func (ec *Context) SessionData() *sessiondata.SessionData {
	if ec.SessionDataStack == nil {
		return nil
	}
	return ec.SessionDataStack.Top()
}

// Copy returns a deep copy of ctx.
func (ec *Context) Copy() *Context {
	ctxCopy := *ec
	ctxCopy.iVarContainerStack = make([]tree.IndexedVarContainer, len(ec.iVarContainerStack), cap(ec.iVarContainerStack))
	copy(ctxCopy.iVarContainerStack, ec.iVarContainerStack)
	return &ctxCopy
}

// PushIVarContainer replaces the current IVarContainer with a different one -
// pushing the current one onto a stack to be replaced later once
// PopIVarContainer is called.
func (ec *Context) PushIVarContainer(c tree.IndexedVarContainer) {
	ec.iVarContainerStack = append(ec.iVarContainerStack, ec.IVarContainer)
	ec.IVarContainer = c
}

// PopIVarContainer discards the current IVarContainer on the EvalContext,
// replacing it with an older one.
func (ec *Context) PopIVarContainer() {
	ec.IVarContainer = ec.iVarContainerStack[len(ec.iVarContainerStack)-1]
	ec.iVarContainerStack = ec.iVarContainerStack[:len(ec.iVarContainerStack)-1]
}

// QualityOfService returns the current value of session setting
// default_transaction_quality_of_service if session data is available,
// otherwise the default value (0).
func (ec *Context) QualityOfService() sessiondatapb.QoSLevel {
	if ec.SessionData() == nil {
		return sessiondatapb.Normal
	}
	return ec.SessionData().DefaultTxnQualityOfService
}

// NewTestingEvalContext is a convenience version of MakeTestingEvalContext
// that returns a pointer.
func NewTestingEvalContext(st *cluster.Settings) *Context {
	ctx := MakeTestingEvalContext(st)
	return &ctx
}

// Stop closes out the EvalContext and must be called once it is no longer in use.
//
// Note: Stop is intended to gracefully handle panics. For this to work
// properly, use `defer evalctx.Stop()`, i.e. have `Stop` be called by `defer` directly.
//
// If `Stop()` is called via an intermediate function (e.g. `defer
// func() { ... evalCtx.Stop() ... }`) the panic will not be handled
// gracefully and some memory monitors may complain that they are being shut down
// with some allocated bytes remaining.
func (ec *Context) Stop(c context.Context) {
	if r := recover(); r != nil {
		ec.TestingMon.EmergencyStop(c)
		panic(r)
	} else {
		ec.TestingMon.Stop(c)
	}
}

// FmtCtx creates a FmtCtx with the given options as well as the EvalContext's session data.
func (ec *Context) FmtCtx(f tree.FmtFlags, opts ...tree.FmtCtxOption) *tree.FmtCtx {
	applyOpts := make([]tree.FmtCtxOption, 0, 2+len(opts))
	applyOpts = append(applyOpts, tree.FmtLocation(ec.GetLocation()))
	if ec.SessionData() != nil {
		applyOpts = append(
			applyOpts,
			tree.FmtDataConversionConfig(ec.SessionData().DataConversionConfig),
		)
	}
	return tree.NewFmtCtx(
		f,
		append(applyOpts, opts...)...,
	)
}

// GetStmtTimestamp retrieves the current statement timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *Context) GetStmtTimestamp() time.Time {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.StmtTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero statement timestamp in EvalContext"))
	}
	return ec.StmtTimestamp
}

// GetClusterTimestamp retrieves the current cluster timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *Context) GetClusterTimestamp() (*tree.DDecimal, error) {
	if ec.Txn == nil {
		return nil, ErrNilTxnInClusterContext
	}

	// CommitTimestamp panics for isolation levels that can operate across
	// multiple timestamps. Prevent this with a gate at the SQL level and return
	// a pgerror until we decide how this will officially behave. See #103245.
	if ec.TxnIsoLevel.ToleratesWriteSkew() {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"unsupported in %s isolation", tree.FromKVIsoLevel(ec.TxnIsoLevel).String())
	}

	ts, err := ec.Txn.CommitTimestamp()
	if err != nil {
		return nil, err
	}
	if ts.IsEmpty() {
		return nil, errors.AssertionFailedf("zero cluster timestamp in txn")
	}
	return TimestampToDecimalDatum(ts), nil
}

// HasPlaceholders returns true if this EvalContext's placeholders have been
// assigned. Will be false during Prepare.
func (ec *Context) HasPlaceholders() bool {
	return ec.Placeholders != nil
}

const regionKey = "region"

// GetLocalRegion returns the region name of the local processor
// on which we're executing.
func (ec *Context) GetLocalRegion() (regionName string, ok bool) {
	return ec.Locality.Find(regionKey)
}

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
func DecimalToInexactDTimestampTZ(d *tree.DDecimal) (*tree.DTimestampTZ, error) {
	ts, err := decimalToHLC(d)
	if err != nil {
		return nil, err
	}
	return tree.MakeDTimestampTZ(timeutil.Unix(0, ts.WallTime), time.Microsecond)
}

func decimalToHLC(d *tree.DDecimal) (hlc.Timestamp, error) {
	var floorD apd.Decimal
	if _, err := tree.DecimalCtx.Floor(&floorD, &d.Decimal); err != nil {
		return hlc.Timestamp{}, err
	}

	i, err := floorD.Int64()
	if err != nil {
		return hlc.Timestamp{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"timestamp value out of range: %s", d.String(),
		)
	}
	return hlc.Timestamp{WallTime: i}, nil
}

// DecimalToInexactDTimestamp is the inverse of TimestampToDecimal. It converts
// a decimal constructed from an hlc.Timestamp into an approximate DTimestamp
// containing the walltime of the hlc.Timestamp.
func DecimalToInexactDTimestamp(d *tree.DDecimal) (*tree.DTimestamp, error) {
	ts, err := decimalToHLC(d)
	if err != nil {
		return nil, err
	}
	return TimestampToInexactDTimestamp(ts), nil
}

// TimestampToDecimalDatum is the same as TimestampToDecimal, but
// returns a datum.
func TimestampToDecimalDatum(ts hlc.Timestamp) *tree.DDecimal {
	res := TimestampToDecimal(ts)
	return &tree.DDecimal{
		Decimal: res,
	}
}

// TimestampToInexactDTimestamp converts the logical timestamp into an
// inexact DTimestamp by dropping the logical counter and using the wall
// time at the microsecond precision.
func TimestampToInexactDTimestamp(ts hlc.Timestamp) *tree.DTimestamp {
	return tree.MustMakeDTimestamp(timeutil.Unix(0, ts.WallTime), time.Microsecond)
}

// GetRelativeParseTime implements ParseContext.
func (ec *Context) GetRelativeParseTime() time.Time {
	if ec == nil {
		return timeutil.Now()
	}
	ret := ec.TxnTimestamp
	if ret.IsZero() {
		ret = timeutil.Now()
	}
	return ret.In(ec.GetLocation())
}

// GetDateHelper implements ParseTimeContext.
func (ec *Context) GetDateHelper() *pgdate.ParseHelper {
	return &ec.ParseHelper
}

// GetTxnTimestamp retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *Context) GetTxnTimestamp(precision time.Duration) *tree.DTimestampTZ {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return tree.MustMakeDTimestampTZ(ec.GetRelativeParseTime(), precision)
}

// GetTxnTimestampNoZone retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ec *Context) GetTxnTimestampNoZone(precision time.Duration) *tree.DTimestamp {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	// Move the time to UTC, but keeping the location's time.
	t := ec.GetRelativeParseTime()
	_, offsetSecs := t.Zone()
	return tree.MustMakeDTimestamp(t.Add(time.Second*time.Duration(offsetSecs)).In(time.UTC), precision)
}

// GetTxnTime retrieves the current transaction time as per
// the evaluation context.
func (ec *Context) GetTxnTime(precision time.Duration) *tree.DTimeTZ {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return tree.NewDTimeTZFromTime(ec.GetRelativeParseTime().Round(precision))
}

// GetTxnTimeNoZone retrieves the current transaction time as per
// the evaluation context.
func (ec *Context) GetTxnTimeNoZone(precision time.Duration) *tree.DTime {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ec.PrepareOnly && ec.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return tree.MakeDTime(timeofday.FromTime(ec.GetRelativeParseTime().Round(precision)))
}

// SetTxnTimestamp sets the corresponding timestamp in the EvalContext.
func (ec *Context) SetTxnTimestamp(ts time.Time) {
	ec.TxnTimestamp = ts
}

// SetStmtTimestamp sets the corresponding timestamp in the EvalContext.
func (ec *Context) SetStmtTimestamp(ts time.Time) {
	ec.StmtTimestamp = ts
}

// GetLocation returns the session timezone.
func (ec *Context) GetLocation() *time.Location {
	if ec == nil {
		return time.UTC
	}
	return ec.SessionData().GetLocation()
}

// GetIntervalStyle returns the session interval style.
func (ec *Context) GetIntervalStyle() duration.IntervalStyle {
	if ec.SessionData() == nil {
		return duration.IntervalStyle_POSTGRES
	}
	return ec.SessionData().GetIntervalStyle()
}

// GetCollationEnv returns the collation env.
func (ec *Context) GetCollationEnv() *tree.CollationEnvironment {
	return &ec.CollationEnv
}

// GetDateStyle returns the session date style.
func (ec *Context) GetDateStyle() pgdate.DateStyle {
	if ec.SessionData() == nil {
		return pgdate.DefaultDateStyle()
	}
	return ec.SessionData().GetDateStyle()
}

// BoundedStaleness returns true if this query uses bounded staleness.
func (ec *Context) BoundedStaleness() bool {
	return ec.AsOfSystemTime != nil &&
		ec.AsOfSystemTime.BoundedStaleness
}

// ensureExpectedType will return an error if a datum does not match the
// provided type. If the expected type is AnyElement or if the datum is a Null
// type, then no error will be returned.
func ensureExpectedType(exp *types.T, d tree.Datum) error {
	if !(exp.Family() == types.AnyFamily || d.ResolvedType().Family() == types.UnknownFamily ||
		d.ResolvedType().Equivalent(exp)) {
		return errors.AssertionFailedf(
			"expected return type %q, got: %q", errors.Safe(exp), errors.Safe(d.ResolvedType()))
	}
	return nil
}

// arrayOfType returns a fresh DArray of the input type.
func arrayOfType(typ *types.T) (*tree.DArray, error) {
	if typ.Family() != types.ArrayFamily {
		return nil, errors.AssertionFailedf("array node type (%v) is not types.TArray", typ)
	}
	if err := types.CheckArrayElementType(typ.ArrayContents()); err != nil {
		return nil, err
	}
	return tree.NewDArray(typ.ArrayContents()), nil
}

// UnwrapDatum returns the base Datum type for a provided datum, stripping
// an *DOidWrapper if present. This is useful for cases like type switches,
// where type aliases should be ignored.
func UnwrapDatum(ctx context.Context, evalCtx *Context, d tree.Datum) tree.Datum {
	d = tree.UnwrapDOidWrapper(d)
	if evalCtx == nil || !evalCtx.HasPlaceholders() {
		return d
	}
	p, ok := d.(*tree.Placeholder)
	if !ok {
		return d
	}
	ret, err := Expr(ctx, evalCtx, p)
	if err != nil {
		// If we fail to evaluate the placeholder, it's because we don't have
		// a placeholder available. Just return the placeholder and someone else
		// will handle this problem.
		return d
	}
	return ret
}

// StreamManagerFactory stores methods that return the streaming managers.
type StreamManagerFactory interface {
	GetReplicationStreamManager(ctx context.Context) (ReplicationStreamManager, error)
	GetStreamIngestManager(ctx context.Context) (StreamIngestManager, error)
}

// ReplicationStreamManager represents a collection of APIs that streaming replication supports
// on the production side.
type ReplicationStreamManager interface {
	// StartReplicationStream starts a stream replication job for the specified
	// tenant on the producer side.
	StartReplicationStream(
		ctx context.Context, tenantName roachpb.TenantName, req streampb.ReplicationProducerRequest,
	) (streampb.ReplicationProducerSpec, error)

	// SetupSpanConfigsStream creates and plans a replication stream to stream the span config updates for a specific tenant.
	SetupSpanConfigsStream(ctx context.Context, tenantName roachpb.TenantName) (ValueGenerator, error)

	// HeartbeatReplicationStream sends a heartbeat to the replication stream producer, indicating
	// consumer has consumed until the given 'frontier' timestamp. This updates the producer job
	// progress and extends its life, and the new producer progress will be returned.
	// If 'frontier' is hlc.MaxTimestamp, returns the producer progress without updating it.
	HeartbeatReplicationStream(
		ctx context.Context,
		streamID streampb.StreamID,
		frontier hlc.Timestamp,
	) (streampb.StreamReplicationStatus, error)

	// StreamPartition starts streaming replication on the producer side for the partition specified
	// by opaqueSpec which contains serialized streampb.StreamPartitionSpec protocol message and
	// returns a value generator which yields events for the specified partition.
	StreamPartition(
		ctx context.Context,
		streamID streampb.StreamID,
		opaqueSpec []byte,
	) (ValueGenerator, error)

	// GetPhysicalReplicationStreamSpec gets a physical stream replication spec on the producer side.
	GetPhysicalReplicationStreamSpec(
		ctx context.Context,
		streamID streampb.StreamID,
	) (*streampb.ReplicationStreamSpec, error)

	// CompleteReplicationStream completes a replication stream job on the producer side.
	// 'successfulIngestion' indicates whether the stream ingestion finished successfully and
	// determines the fate of the producer job, succeeded or canceled.
	CompleteReplicationStream(
		ctx context.Context,
		streamID streampb.StreamID,
		successfulIngestion bool,
	) error

	DebugGetProducerStatuses(ctx context.Context) []streampb.DebugProducerStatus
	DebugGetLogicalConsumerStatuses(ctx context.Context) []*streampb.DebugLogicalConsumerStatus

	PlanLogicalReplication(
		ctx context.Context,
		req streampb.LogicalReplicationPlanRequest,
	) (*streampb.ReplicationStreamSpec, error)

	StartReplicationStreamForTables(
		ctx context.Context,
		req streampb.ReplicationProducerRequest,
	) (streampb.ReplicationProducerSpec, error)
}

// StreamIngestManager represents a collection of APIs that streaming replication supports
// on the ingestion side.
type StreamIngestManager interface {
	// GetStreamIngestionStats gets a statistics summary for a stream ingestion job.
	GetReplicationStatsAndStatus(
		ctx context.Context,
		ingestionJobID jobspb.JobID,
	) (*streampb.StreamIngestionStats, string, error)

	// RevertTenantToTimestamp reverts the given tenant to the given
	// timestamp. This is a non-transactional destructive operation that
	// should be used with care.
	RevertTenantToTimestamp(
		ctx context.Context,
		tenantName roachpb.TenantName,
		revertTo hlc.Timestamp,
	) error
}
