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

package sql

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ClusterOrganization is the organization name.
var ClusterOrganization = settings.RegisterStringSetting(
	"cluster.organization",
	"organization name",
	"",
)

// ClusterSecret is a cluster specific secret. This setting is hidden.
var ClusterSecret = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"cluster.secret",
		"cluster specific secret",
		"",
	)
	s.Hide()
	return s
}()

// traceTxnThreshold can be used to log SQL transactions that take
// longer than duration to complete. For example, traceTxnThreshold=1s
// will log the trace for any transaction that takes 1s or longer. To
// log traces for all transactions use traceTxnThreshold=1ns. Note
// that any positive duration will enable tracing and will slow down
// all execution because traces are gathered for all transactions even
// if they are not output.
var traceTxnThreshold = settings.RegisterDurationSetting(
	"sql.trace.txn.enable_threshold",
	"duration beyond which all transactions are traced (set to 0 to disable)", 0,
)

// traceSessionEventLogEnabled can be used to enable the event log
// that is normally kept for every SQL connection. The event log has a
// non-trivial performance impact and also reveals SQL statements
// which may be a privacy concern.
var traceSessionEventLogEnabled = settings.RegisterBoolSetting(
	"sql.trace.session_eventlog.enabled",
	"set to true to enable session tracing", false,
)

// OptimizerClusterMode controls the cluster default for when the cost-based optimizer is used.
var OptimizerClusterMode = settings.RegisterEnumSetting(
	"sql.defaults.optimizer",
	"default cost-based optimizer mode",
	"on",
	map[int64]string{
		int64(sessiondata.OptimizerLocal): "local",
		int64(sessiondata.OptimizerOff):   "off",
		int64(sessiondata.OptimizerOn):    "on",
	},
)

// DistSQLClusterExecMode controls the cluster default for when DistSQL is used.
var DistSQLClusterExecMode = settings.RegisterEnumSetting(
	"sql.defaults.distsql",
	"default distributed SQL execution mode",
	"auto",
	map[int64]string{
		int64(sessiondata.DistSQLOff):       "off",
		int64(sessiondata.DistSQLAuto):      "auto",
		int64(sessiondata.DistSQLOn):        "on",
		int64(sessiondata.DistSQL2Dot0Auto): "2.0-auto",
		int64(sessiondata.DistSQL2Dot0Off):  "2.0-off",
	},
)

// SerialNormalizationMode controls how the SERIAL type is interpreted in table
// definitions.
var SerialNormalizationMode = settings.RegisterEnumSetting(
	"sql.defaults.serial_normalization",
	"default handling of SERIAL in table definitions",
	"rowid",
	map[int64]string{
		int64(sessiondata.SerialUsesRowID):            "rowid",
		int64(sessiondata.SerialUsesVirtualSequences): "virtual_sequence",
		int64(sessiondata.SerialUsesSQLSequences):     "sql_sequence",
	},
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

const sqlTxnName string = "sql txn"
const metricsSampleInterval = 10 * time.Second

// Fully-qualified names for metrics.
var (
	MetaTxnBegin = metric.Metadata{
		Name:        "sql.txn.begin.count",
		Help:        "Number of SQL transaction BEGIN statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnCommit = metric.Metadata{
		Name:        "sql.txn.commit.count",
		Help:        "Number of SQL transaction COMMIT statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnAbort = metric.Metadata{
		Name:        "sql.txn.abort.count",
		Help:        "Number of SQL transaction ABORT statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRollback = metric.Metadata{
		Name:        "sql.txn.rollback.count",
		Help:        "Number of SQL transaction ROLLBACK statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSelect = metric.Metadata{
		Name:        "sql.select.count",
		Help:        "Number of SQL SELECT statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLExecLatency = metric.Metadata{
		Name:        "sql.exec.latency",
		Help:        "Latency of SQL statement execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLServiceLatency = metric.Metadata{
		Name:        "sql.service.latency",
		Help:        "Latency of SQL request execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLOpt = metric.Metadata{
		Name:        "sql.optimizer.count",
		Help:        "Number of statements which ran with the cost-based optimizer",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLOptFallback = metric.Metadata{
		Name:        "sql.optimizer.fallback.count",
		Help:        "Number of statements which the cost-based optimizer was unable to plan",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDistSQLSelect = metric.Metadata{
		Name:        "sql.distsql.select.count",
		Help:        "Number of DistSQL SELECT statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDistSQLExecLatency = metric.Metadata{
		Name:        "sql.distsql.exec.latency",
		Help:        "Latency of DistSQL statement execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaDistSQLServiceLatency = metric.Metadata{
		Name:        "sql.distsql.service.latency",
		Help:        "Latency of DistSQL request execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaUpdate = metric.Metadata{
		Name:        "sql.update.count",
		Help:        "Number of SQL UPDATE statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaInsert = metric.Metadata{
		Name:        "sql.insert.count",
		Help:        "Number of SQL INSERT statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDelete = metric.Metadata{
		Name:        "sql.delete.count",
		Help:        "Number of SQL DELETE statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDdl = metric.Metadata{
		Name:        "sql.ddl.count",
		Help:        "Number of SQL DDL statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaMisc = metric.Metadata{
		Name:        "sql.misc.count",
		Help:        "Number of other SQL statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaQuery = metric.Metadata{
		Name:        "sql.query.count",
		Help:        "Number of SQL queries",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaFailure = metric.Metadata{
		Name:        "sql.failure.count",
		Help:        "Number of statements resulting in a planning or runtime error",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
)

// NodeInfo contains metadata about the executing node and cluster.
type NodeInfo struct {
	ClusterID func() uuid.UUID
	NodeID    *base.NodeIDContainer
	AdminURL  func() *url.URL
	PGURL     func(*url.Userinfo) (*url.URL, error)
}

// An ExecutorConfig encompasses the auxiliary objects and configuration
// required to create an executor.
// All fields holding a pointer or an interface are required to create
// a Executor; the rest will have sane defaults set if omitted.
type ExecutorConfig struct {
	Settings *cluster.Settings
	NodeInfo
	AmbientCtx       log.AmbientContext
	DB               *client.DB
	Gossip           *gossip.Gossip
	DistSender       *kv.DistSender
	RPCContext       *rpc.Context
	LeaseManager     *LeaseManager
	Clock            *hlc.Clock
	DistSQLSrv       *distsqlrun.ServerImpl
	StatusServer     serverpb.StatusServer
	MetricsRecorder  *status.MetricsRecorder
	SessionRegistry  *SessionRegistry
	JobRegistry      *jobs.Registry
	VirtualSchemas   *VirtualSchemaHolder
	DistSQLPlanner   *DistSQLPlanner
	TableStatsCache  *stats.TableStatisticsCache
	ExecLogger       *log.SecondaryLogger
	AuditLogger      *log.SecondaryLogger
	InternalExecutor *InternalExecutor

	TestingKnobs              *ExecutorTestingKnobs
	SchemaChangerTestingKnobs *SchemaChangerTestingKnobs
	DistSQLRunTestingKnobs    *distsqlrun.TestingKnobs
	EvalContextTestingKnobs   tree.EvalContextTestingKnobs
	// HistogramWindowInterval is (server.Config).HistogramWindowInterval.
	HistogramWindowInterval time.Duration

	// Caches updated by DistSQL.
	RangeDescriptorCache *kv.RangeDescriptorCache
	LeaseHolderCache     *kv.LeaseHolderCache

	// ConnResultsBufferBytes is the size of the buffer in which each connection
	// accumulates results set. Results are flushed to the network when this
	// buffer overflows.
	ConnResultsBufferBytes int
}

// Organization returns the value of cluster.organization.
func (ec *ExecutorConfig) Organization() string {
	return ClusterOrganization.Get(&ec.Settings.SV)
}

var _ base.ModuleTestingKnobs = &ExecutorTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExecutorTestingKnobs) ModuleTestingKnobs() {}

// StatementFilter is the type of callback that
// ExecutorTestingKnobs.StatementFilter takes.
type StatementFilter func(context.Context, string, error)

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// CheckStmtStringChange causes Executor.execStmtGroup to verify that executed
	// statements are not modified during execution.
	CheckStmtStringChange bool

	// StatementFilter can be used to trap execution of SQL statements and
	// optionally change their results. The filter function is invoked after each
	// statement has been executed.
	StatementFilter StatementFilter

	// BeforeExecute is called by the Executor before plan execution. It is useful
	// for synchronizing statement execution, such as with parallel statemets.
	BeforeExecute func(ctx context.Context, stmt string, isParallel bool)

	// AfterExecute is like StatementFilter, but it runs in the same goroutine of the
	// statement.
	AfterExecute func(ctx context.Context, stmt string, err error)

	// DisableAutoCommit, if set, disables the auto-commit functionality of some
	// SQL statements. That functionality allows some statements to commit
	// directly when they're executed in an implicit SQL txn, without waiting for
	// the Executor to commit the implicit txn.
	// This has to be set in tests that need to abort such statements using a
	// StatementFilter; otherwise, the statement commits immediately after
	// execution so there'll be nothing left to abort by the time the filter runs.
	DisableAutoCommit bool

	// DistSQLPlannerKnobs are testing knobs for DistSQLPlanner.
	DistSQLPlannerKnobs DistSQLPlannerTestingKnobs

	// BeforeAutoCommit is called when the Executor is about to commit the KV
	// transaction after running a statement in an implicit transaction, allowing
	// tests to inject errors into that commit.
	// If an error is returned, that error will be considered the result of
	// txn.Commit(), and the txn.Commit() call will not actually be
	// made. If no error is returned, txn.Commit() is called normally.
	//
	// Note that this is not called if the SQL statement representing the implicit
	// transaction has committed the KV txn itself (e.g. if it used the 1-PC
	// optimization). This is only called when the Executor is the one doing the
	// committing.
	BeforeAutoCommit func(ctx context.Context, stmt string) error
}

// DistSQLPlannerTestingKnobs is used to control internals of the DistSQLPlanner
// for testing purposes.
type DistSQLPlannerTestingKnobs struct {
	// If OverrideSQLHealthCheck is set, we use this callback to get the health of
	// a node.
	OverrideHealthCheck func(node roachpb.NodeID, addrString string) error
}

// databaseCacheHolder is a thread-safe container for a *databaseCache.
// It also allows clients to block until the cache is updated to a desired
// state.
//
// NOTE(andrei): The way in which we handle the database cache is funky: there's
// this top-level holder, which gets updated on gossip updates. Then, each
// session gets its *databaseCache, which is updated from the holder after every
// transaction - the SystemConfig is updated and the lazily computer map of db
// names to ids is wiped. So many session are sharing and contending on a
// mutable cache, but nobody's sharing this holder. We should make up our mind
// about whether we like the sharing or not and, if we do, share the holder too.
// Also, we could use the SystemConfigDeltaFilter to limit the updates to
// databases that chaged. One of the problems with the existing architecture
// is if a transaction is completed on a session and the session remains dormant
// for a long time, the next transaction will see a rather old database cache.
type databaseCacheHolder struct {
	mu struct {
		syncutil.Mutex
		c  *databaseCache
		cv *sync.Cond
	}
}

func newDatabaseCacheHolder(c *databaseCache) *databaseCacheHolder {
	dc := &databaseCacheHolder{}
	dc.mu.c = c
	dc.mu.cv = sync.NewCond(&dc.mu.Mutex)
	return dc
}

func (dc *databaseCacheHolder) getDatabaseCache() *databaseCache {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.mu.c
}

// waitForCacheState implements the dbCacheSubscriber interface.
func (dc *databaseCacheHolder) waitForCacheState(cond func(*databaseCache) bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for done := cond(dc.mu.c); !done; done = cond(dc.mu.c) {
		dc.mu.cv.Wait()
	}
}

// databaseCacheHolder implements the dbCacheSubscriber interface.
var _ dbCacheSubscriber = &databaseCacheHolder{}

// updateSystemConfig is called whenever a new system config gossip entry is
// received.
func (dc *databaseCacheHolder) updateSystemConfig(cfg *config.SystemConfig) {
	dc.mu.Lock()
	dc.mu.c = newDatabaseCache(cfg)
	dc.mu.cv.Broadcast()
	dc.mu.Unlock()
}

// forEachRow calls the provided closure for each successful call to
// planNode.Next with planNode.Values, making sure to properly track memory
// usage.
func forEachRow(params runParams, p planNode, f func(tree.Datums) error) error {
	next, err := p.Next(params)
	for ; next; next, err = p.Next(params) {
		// If we're tracking memory, clear the previous row's memory account.
		if params.extendedEvalCtx.ActiveMemAcc != nil {
			params.extendedEvalCtx.ActiveMemAcc.Clear(params.ctx)
		}

		if err := f(p.Values()); err != nil {
			return err
		}
	}
	return err
}

// If the plan has a fast path we attempt to query that,
// otherwise we fall back to counting via plan.Next().
func countRowsAffected(params runParams, p planNode) (int, error) {
	if a, ok := p.(planNodeFastPath); ok {
		if count, res := a.FastPathResults(); res {
			if params.extendedEvalCtx.Tracing.Enabled() {
				log.VEvent(params.ctx, 2, "fast path completed")
			}
			return count, nil
		}
	}

	count := 0
	err := forEachRow(params, p, func(_ tree.Datums) error {
		count++
		return nil
	})
	return count, err
}

// shouldUseDistSQL returns true if the combination of mode and distribution
// requirement given by shouldDistributeGivenRecAndMode requires that the
// distSQL engine should be used.
// This is always true unless the mode is set to 2.0-auto or 2.0-off.
// 2.0-auto causes the distribution recommendation to control whether distsql is
// used at all, and 2.0-off causes distsql to never be used regardless of the
// recommendation.
func shouldUseDistSQL(distributePlan bool, mode sessiondata.DistSQLExecMode) bool {
	if mode == sessiondata.DistSQL2Dot0Off {
		return false
	} else if mode == sessiondata.DistSQL2Dot0Auto {
		return distributePlan
	}
	return true
}

func shouldDistributeGivenRecAndMode(
	rec distRecommendation, mode sessiondata.DistSQLExecMode,
) bool {
	switch mode {
	case sessiondata.DistSQLOff, sessiondata.DistSQL2Dot0Off:
		return false
	case sessiondata.DistSQLAuto, sessiondata.DistSQL2Dot0Auto:
		return rec == shouldDistribute
	case sessiondata.DistSQLOn, sessiondata.DistSQLAlways:
		return rec != cannotDistribute
	}
	panic(fmt.Sprintf("unhandled distsql mode %v", mode))
}

// shouldDistributePlan determines whether we should distribute the
// given logical plan, based on the session settings.
func shouldDistributePlan(
	ctx context.Context, distSQLMode sessiondata.DistSQLExecMode, dp *DistSQLPlanner, plan planNode,
) bool {
	if distSQLMode == sessiondata.DistSQLOff {
		return false
	}

	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.(*zeroNode); ok {
		return false
	}

	rec, err := dp.checkSupportForNode(plan)
	if err != nil {
		// Don't use distSQL for this request.
		log.VEventf(ctx, 1, "query not supported for distSQL: %s", err)
		return false
	}

	return shouldDistributeGivenRecAndMode(rec, distSQLMode)
}

// golangFillQueryArguments transforms Go values into datums.
// Some of the args can be datums (in which case the transformation is a no-op).
//
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func golangFillQueryArguments(args ...interface{}) tree.Datums {
	res := make(tree.Datums, len(args))
	for i, arg := range args {
		if arg == nil {
			res[i] = tree.DNull
			continue
		}

		// A type switch to handle a few explicit types with special semantics:
		// - Datums are passed along as is.
		// - Time datatypes get special representation in the database.
		var d tree.Datum
		switch t := arg.(type) {
		case tree.Datum:
			d = t
		case time.Time:
			d = tree.MakeDTimestamp(t, time.Microsecond)
		case time.Duration:
			d = &tree.DInterval{Duration: duration.Duration{Nanos: t.Nanoseconds()}}
		case bitarray.BitArray:
			d = &tree.DBitArray{BitArray: t}
		case *apd.Decimal:
			dd := &tree.DDecimal{}
			dd.Set(t)
			d = dd
		}
		if d == nil {
			// Handle all types which have an underlying type that can be stored in the
			// database.
			// Note: if this reflection becomes a performance concern in the future,
			// commonly used types could be added explicitly into the type switch above
			// for a performance gain.
			val := reflect.ValueOf(arg)
			switch val.Kind() {
			case reflect.Bool:
				d = tree.MakeDBool(tree.DBool(val.Bool()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				d = tree.NewDInt(tree.DInt(val.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				d = tree.NewDInt(tree.DInt(val.Uint()))
			case reflect.Float32, reflect.Float64:
				d = tree.NewDFloat(tree.DFloat(val.Float()))
			case reflect.String:
				d = tree.NewDString(val.String())
			case reflect.Slice:
				// Handle byte slices.
				if val.Type().Elem().Kind() == reflect.Uint8 {
					d = tree.NewDBytes(tree.DBytes(val.Bytes()))
				}
			}
			if d == nil {
				panic(fmt.Sprintf("unexpected type %T", arg))
			}
		}
		res[i] = d
	}
	return res
}

func checkResultType(typ types.T) error {
	// Compare all types that can rely on == equality.
	switch types.UnwrapType(typ) {
	case types.Unknown:
	case types.BitArray:
	case types.Bool:
	case types.Int:
	case types.Float:
	case types.Decimal:
	case types.Bytes:
	case types.String:
	case types.Date:
	case types.Time:
	case types.Timestamp:
	case types.TimestampTZ:
	case types.Interval:
	case types.JSON:
	case types.UUID:
	case types.INet:
	case types.NameArray:
	case types.Oid:
	case types.RegClass:
	case types.RegNamespace:
	case types.RegProc:
	case types.RegProcedure:
	case types.RegType:
	default:
		// Compare all types that cannot rely on == equality.
		istype := typ.FamilyEqual
		switch {
		case istype(types.FamArray):
			if istype(types.UnwrapType(typ).(types.TArray).Typ) {
				return pgerror.Unimplemented("nested arrays", "arrays cannot have arrays as element type")
			}
		case istype(types.FamCollatedString):
		case istype(types.FamTuple):
		case istype(types.FamPlaceholder):
			return errors.Errorf("could not determine data type of %s", typ)
		default:
			return errors.Errorf("unsupported result type: %s", typ)
		}
	}
	return nil
}

// EvalAsOfTimestamp evaluates and returns the timestamp from an AS OF SYSTEM
// TIME clause.
func (p *planner) EvalAsOfTimestamp(
	asOf tree.AsOfClause, max hlc.Timestamp,
) (hlc.Timestamp, error) {
	return tree.EvalAsOfTimestamp(asOf, max, &p.semaCtx, p.EvalContext())
}

// ParseHLC parses a string representation of an `hlc.Timestamp`.
func ParseHLC(s string) (hlc.Timestamp, error) {
	dec, _, err := apd.NewFromString(s)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return tree.DecimalToHLC(dec)
}

// isAsOf analyzes a statement to bypass the logic in newPlan(), since
// that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction
// should be set. The statements that will be checked are Select,
// ShowTrace (of a Select statement), and Scrub.
//
// max is a lower bound on what the transaction's timestamp will be.
// Used to check that the user didn't specify a timestamp in the future.
func (p *planner) isAsOf(stmt tree.Statement, max hlc.Timestamp) (*hlc.Timestamp, error) {
	var asOf tree.AsOfClause
	switch s := stmt.(type) {
	case *tree.Select:
		selStmt := s.Select
		var parenSel *tree.ParenSelect
		var ok bool
		for parenSel, ok = selStmt.(*tree.ParenSelect); ok; parenSel, ok = selStmt.(*tree.ParenSelect) {
			selStmt = parenSel.Select.Select
		}

		sc, ok := selStmt.(*tree.SelectClause)
		if !ok {
			return nil, nil
		}
		if sc.From == nil || sc.From.AsOf.Expr == nil {
			return nil, nil
		}

		asOf = sc.From.AsOf
	case *tree.Scrub:
		if s.AsOf.Expr == nil {
			return nil, nil
		}
		asOf = s.AsOf
	case *tree.Export:
		return p.isAsOf(s.Query, max)
	default:
		return nil, nil
	}

	ts, err := p.EvalAsOfTimestamp(asOf, max)
	return &ts, err
}

// isSavepoint returns true if stmt is a SAVEPOINT statement.
func isSavepoint(stmt Statement) bool {
	_, isSavepoint := stmt.AST.(*tree.Savepoint)
	return isSavepoint
}

// isSetTransaction returns true if stmt is a "SET TRANSACTION ..." statement.
func isSetTransaction(stmt Statement) bool {
	_, isSet := stmt.AST.(*tree.SetTransaction)
	return isSet
}

// queryPhase represents a phase during a query's execution.
type queryPhase int

const (
	// The phase before start of execution (includes parsing, building a plan).
	preparing queryPhase = 0

	// Execution phase.
	executing queryPhase = 1
)

// queryMeta stores metadata about a query. Stored as reference in
// session.mu.ActiveQueries.
type queryMeta struct {
	// The timestamp when this query began execution.
	start time.Time

	// AST of the SQL statement - converted to query string only when necessary.
	stmt tree.Statement

	// States whether this query is distributed. Note that all queries,
	// including those that are distributed, have this field set to false until
	// start of execution; only at that point can we can actually determine whether
	// this query will be distributed. Use the phase variable below
	// to determine whether this query has entered execution yet.
	isDistributed bool

	// Current phase of execution of query.
	phase queryPhase

	// Cancellation function for the context associated with this query's transaction.
	ctxCancel context.CancelFunc

	// If set, this query will not be reported as part of SHOW QUERIES. This is
	// set based on the statement implementing tree.HiddenFromShowQueries.
	hidden bool
}

// cancel cancels the query associated with this queryMeta, by closing the associated
// txn context.
func (q *queryMeta) cancel() {
	q.ctxCancel()
}

// sessionDefaults mirrors fields in Session, for restoring default
// configuration values in SET ... TO DEFAULT (or RESET ...) statements.
type sessionDefaults struct {
	applicationName string
	database        string
}

// SessionArgs contains arguments for serving a client connection.
type SessionArgs struct {
	Database        string
	User            string
	ApplicationName string
	// RemoteAddr is the client's address. This is nil iff this is an internal
	// client.
	RemoteAddr net.Addr
}

// SessionRegistry stores a set of all sessions on this node.
// Use register() and deregister() to modify this registry.
type SessionRegistry struct {
	syncutil.Mutex
	sessions map[ClusterWideID]registrySession
}

// NewSessionRegistry creates a new SessionRegistry with an empty set
// of sessions.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{sessions: make(map[ClusterWideID]registrySession)}
}

func (r *SessionRegistry) register(id ClusterWideID, s registrySession) {
	r.Lock()
	r.sessions[id] = s
	r.Unlock()
}

func (r *SessionRegistry) deregister(id ClusterWideID) {
	r.Lock()
	delete(r.sessions, id)
	r.Unlock()
}

type registrySession interface {
	user() string
	cancelQuery(queryID ClusterWideID) bool
	cancelSession()
	// serialize serializes a Session into a serverpb.Session
	// that can be served over RPC.
	serialize() serverpb.Session
}

// CancelQuery looks up the associated query in the session registry and cancels it.
func (r *SessionRegistry) CancelQuery(queryIDStr string, username string) (bool, error) {
	queryID, err := StringToClusterWideID(queryIDStr)
	if err != nil {
		return false, fmt.Errorf("query ID %s malformed: %s", queryID, err)
	}

	r.Lock()
	defer r.Unlock()

	for _, session := range r.sessions {
		if !(username == security.RootUser || username == session.user()) {
			// Skip this session.
			continue
		}

		if session.cancelQuery(queryID) {
			return true, nil
		}
	}

	return false, fmt.Errorf("query ID %s not found", queryID)
}

// CancelSession looks up the specified session in the session registry and cancels it.
func (r *SessionRegistry) CancelSession(sessionIDBytes []byte, username string) (bool, error) {
	sessionID := BytesToClusterWideID(sessionIDBytes)

	r.Lock()
	defer r.Unlock()

	for id, session := range r.sessions {
		if !(username == security.RootUser || username == session.user()) {
			// Skip this session.
			continue
		}

		if id == sessionID {
			session.cancelSession()
			return true, nil
		}
	}

	return false, fmt.Errorf("session ID %s not found", sessionID)
}

// SerializeAll returns a slice of all sessions in the registry, converted to serverpb.Sessions.
func (r *SessionRegistry) SerializeAll() []serverpb.Session {
	r.Lock()
	defer r.Unlock()

	response := make([]serverpb.Session, 0, len(r.sessions))

	for _, s := range r.sessions {
		response = append(response, s.serialize())
	}

	return response
}

func newSchemaInterface(tables *TableCollection, vt VirtualTabler) *schemaInterface {
	sc := &schemaInterface{
		physical: &CachedPhysicalAccessor{
			SchemaAccessor: UncachedPhysicalAccessor{},
			tc:             tables,
		},
	}
	sc.logical = &LogicalSchemaAccessor{
		SchemaAccessor: sc.physical,
		vt:             vt,
	}
	return sc
}

// MaxSQLBytes is the maximum length in bytes of SQL statements serialized
// into a serverpb.Session. Exported for testing.
const MaxSQLBytes = 1000

type schemaChangerCollection struct {
	schemaChangers []SchemaChanger
}

func (scc *schemaChangerCollection) queueSchemaChanger(schemaChanger SchemaChanger) {
	scc.schemaChangers = append(scc.schemaChangers, schemaChanger)
}

func (scc *schemaChangerCollection) reset() {
	scc.schemaChangers = nil
}

// execSchemaChanges releases schema leases and runs the queued
// schema changers. This needs to be run after the transaction
// scheduling the schema change has finished.
//
// The list of closures is cleared after (attempting) execution.
func (scc *schemaChangerCollection) execSchemaChanges(
	ctx context.Context, cfg *ExecutorConfig, tracing *SessionTracing,
) error {
	if len(scc.schemaChangers) == 0 {
		return nil
	}
	if fn := cfg.SchemaChangerTestingKnobs.SyncFilter; fn != nil {
		fn(TestingSchemaChangerCollection{scc})
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	var firstError error
	for _, sc := range scc.schemaChangers {
		sc.db = cfg.DB
		sc.testingKnobs = cfg.SchemaChangerTestingKnobs
		sc.distSQLPlanner = cfg.DistSQLPlanner
		sc.settings = cfg.Settings
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			evalCtx := createSchemaChangeEvalCtx(cfg.Clock.Now(), tracing)
			if err := sc.exec(ctx, true /* inSession */, &evalCtx); err != nil {
				if onError := cfg.SchemaChangerTestingKnobs.OnError; onError != nil {
					onError(err)
				}
				if shouldLogSchemaChangeError(err) {
					log.Warningf(ctx, "error executing schema change: %s", err)
				}

				if err == sqlbase.ErrDescriptorNotFound || err == ctx.Err() {
					// 1. If the descriptor is dropped while the schema change
					// is executing, the schema change is considered completed.
					// 2. If the context is canceled the schema changer quits here
					// letting the asynchronous code path complete the schema
					// change.
				} else if isPermanentSchemaChangeError(err) {
					// All constraint violations can be reported; we report it as the result
					// corresponding to the statement that enqueued this changer.
					// There's some sketchiness here: we assume there's a single result
					// per statement and we clobber the result/error of the corresponding
					// statement.
					if firstError == nil {
						firstError = err
					}
				} else {
					// retryable error.
					continue
				}
			}
			break
		}
	}
	scc.schemaChangers = nil
	return firstError
}

const panicLogOutputCutoffChars = 500

func anonymizeStmtAndConstants(stmt tree.Statement) string {
	return tree.AsStringWithFlags(stmt, tree.FmtAnonymize|tree.FmtHideConstants)
}

// AnonymizeStatementsForReporting transforms an action, SQL statements, and a value
// (usually a recovered panic) into an error that will be useful when passed to
// our error reporting as it exposes a scrubbed version of the statements.
func AnonymizeStatementsForReporting(action, sqlStmts string, r interface{}) error {
	var anonymized []string
	{
		stmts, err := parser.Parse(sqlStmts)
		if err == nil {
			for _, stmt := range stmts {
				anonymized = append(anonymized, anonymizeStmtAndConstants(stmt))
			}
		}
	}
	anonStmtsStr := strings.Join(anonymized, "; ")
	if len(anonStmtsStr) > panicLogOutputCutoffChars {
		anonStmtsStr = anonStmtsStr[:panicLogOutputCutoffChars] + " [...]"
	}

	return log.Safe(
		fmt.Sprintf("panic while %s %d statements: %s", action, len(anonymized), anonStmtsStr),
	).WithCause(r)
}

// SessionTracing holds the state used by SET TRACING {ON,OFF,LOCAL} statements in
// the context of one SQL session.
// It holds the current trace being collected (or the last trace collected, if
// tracing is not currently ongoing).
//
// SessionTracing and its interactions with the connExecutor are thread-safe;
// tracing can be turned on at any time.
type SessionTracing struct {
	// enabled is set at times when "session enabled" is active - i.e. when
	// transactions are being recorded.
	enabled bool

	// kvTracingEnabled is set at times when KV tracing is active. When
	// KV tracning is enabled, the SQL/KV interface logs individual K/V
	// operators to the current context.
	kvTracingEnabled bool

	// showResults, when set, indicates that the result rows produced by
	// the execution statement must be reported in the
	// trace. showResults can be set manually by SET TRACING = ...,
	// results
	showResults bool

	// If recording==true, recordingType indicates the type of the current
	// recording.
	recordingType tracing.RecordingType

	// ex is the connExecutor to which this SessionTracing is tied.
	ex *connExecutor

	// firstTxnSpan is the span of the first txn that was active when session
	// tracing was enabled.
	firstTxnSpan opentracing.Span

	// connSpan is the connection's span. This is recording.
	connSpan opentracing.Span

	// lastRecording will collect the recording when stopping tracing.
	lastRecording []traceRow
}

// getSessionTrace returns the session trace. If we're not currently tracing,
// this will be the last recorded trace. If we are currently tracing, we'll
// return whatever was recorded so far.
func (st *SessionTracing) getSessionTrace() ([]traceRow, error) {
	if !st.enabled {
		return st.lastRecording, nil
	}

	return generateSessionTraceVTable(st.getRecording())
}

// getRecording returns the recorded spans of the current trace.
func (st *SessionTracing) getRecording() []tracing.RecordedSpan {
	var spans []tracing.RecordedSpan
	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
	}
	return append(spans, tracing.GetRecording(st.connSpan)...)
}

// StartTracing starts "session tracing". From this moment on, everything
// happening on both the connection's context and the current txn's context (if
// any) will be traced.
// StopTracing() needs to be called to finish this trace.
//
// There's two contexts on which we must record:
// 1) If we're inside a txn, we start recording on the txn's span. We assume
// that the txn's ctx has a recordable span on it.
// 2) Regardless of whether we're in a txn or not, we need to record the
// connection's context. This context generally does not have a span, so we
// "hijack" it with one that does. Whatever happens on that context, plus
// whatever happens in future derived txn contexts, will be recorded.
//
// Args:
// kvTracingEnabled: If set, the traces will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the messages
//   are per-row.
// showResults: If set, result rows are reported in the trace.
func (st *SessionTracing) StartTracing(
	recType tracing.RecordingType, kvTracingEnabled, showResults bool,
) error {
	if st.enabled {
		// We're already tracing. Only treat as no-op if the same options
		// are requested.
		if kvTracingEnabled != st.kvTracingEnabled ||
			showResults != st.showResults ||
			recType != st.recordingType {
			var desiredOptions bytes.Buffer
			comma := ""
			if kvTracingEnabled {
				desiredOptions.WriteString("kv")
				comma = ", "
			}
			if showResults {
				fmt.Fprintf(&desiredOptions, "%sresults", comma)
				comma = ", "
			}
			recOption := "cluster"
			if recType == tracing.SingleNodeRecording {
				recOption = "local"
			}
			fmt.Fprintf(&desiredOptions, "%s%s", comma, recOption)

			return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError,
				"tracing is already started with different options").SetHintf(
				"reset with SET tracing = off; SET tracing = %s", desiredOptions.String())
		}

		return nil
	}

	// If we're inside a transaction, start recording on the txn span.
	if _, ok := st.ex.machine.CurState().(stateNoTxn); !ok {
		sp := opentracing.SpanFromContext(st.ex.state.Ctx)
		if sp == nil {
			return errors.Errorf("no txn span for SessionTracing")
		}
		tracing.StartRecording(sp, recType)
		st.firstTxnSpan = sp
	}

	st.enabled = true
	st.kvTracingEnabled = kvTracingEnabled
	st.showResults = showResults
	st.recordingType = recType

	// Now hijack the conn's ctx with one that has a recording span.

	opName := "session recording"
	var sp opentracing.Span
	connCtx := st.ex.ctxHolder.connCtx
	if parentSp := opentracing.SpanFromContext(connCtx); parentSp != nil {
		// Create a child span while recording.
		sp = parentSp.Tracer().StartSpan(
			opName,
			opentracing.ChildOf(parentSp.Context()), tracing.Recordable,
			tracing.LogTagsFromCtx(connCtx),
		)
	} else {
		// Create a root span while recording.
		sp = st.ex.server.cfg.AmbientCtx.Tracer.StartSpan(
			opName, tracing.Recordable,
			tracing.LogTagsFromCtx(connCtx),
		)
	}
	tracing.StartRecording(sp, recType)
	st.connSpan = sp

	// Hijack the connections context.
	newConnCtx := opentracing.ContextWithSpan(st.ex.ctxHolder.connCtx, sp)
	st.ex.ctxHolder.hijack(newConnCtx)

	return nil
}

// StopTracing stops the trace that was started with StartTracing().
// An error is returned if tracing was not active.
func (st *SessionTracing) StopTracing() error {
	if !st.enabled {
		// We're not currently tracing. No-op.
		return nil
	}
	st.enabled = false
	st.kvTracingEnabled = false
	st.showResults = false
	st.recordingType = tracing.NoRecording

	var spans []tracing.RecordedSpan

	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
		tracing.StopRecording(st.firstTxnSpan)
	}
	st.connSpan.Finish()
	spans = append(spans, tracing.GetRecording(st.connSpan)...)
	// NOTE: We're stopping recording on the connection's ctx only; the stopping
	// is not inherited by children. If we are inside of a txn, that span will
	// continue recording, even though nobody will collect its recording again.
	tracing.StopRecording(st.connSpan)
	st.ex.ctxHolder.unhijack()

	var err error
	st.lastRecording, err = generateSessionTraceVTable(spans)
	return err
}

// RecordingType returns which type of tracing is currently being done.
func (st *SessionTracing) RecordingType() tracing.RecordingType {
	return st.recordingType
}

// KVTracingEnabled checks whether KV tracing is currently enabled.
func (st *SessionTracing) KVTracingEnabled() bool {
	return st.kvTracingEnabled
}

// Enabled checks whether session tracing is currently enabled.
func (st *SessionTracing) Enabled() bool {
	return st.enabled
}

// TracePlanStart conditionally emits a trace message at the moment
// logical planning starts.
func (st *SessionTracing) TracePlanStart(ctx context.Context, stmtTag string) {
	if st.enabled {
		log.VEventf(ctx, 2, "planning starts: %s", stmtTag)
	}
}

// TracePlanEnd conditionally emits a trace message at the moment
// logical planning ends.
func (st *SessionTracing) TracePlanEnd(ctx context.Context, err error) {
	log.VEventfDepth(ctx, 2, 1, "planning ends")
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "planning error: %v", err)
	}
}

// TracePlanCheckStart conditionally emits a trace message at the
// moment the test of which execution engine to use starts.
func (st *SessionTracing) TracePlanCheckStart(ctx context.Context) {
	log.VEventfDepth(ctx, 2, 1, "checking distributability")
}

// TracePlanCheckEnd conditionally emits a trace message at the moment
// the engine check ends.
func (st *SessionTracing) TracePlanCheckEnd(ctx context.Context, err error, dist bool) {
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "distributability check error: %v", err)
	} else {
		log.VEventfDepth(ctx, 2, 1, "distributable plan: %v", dist)
	}
}

// TraceExecStart conditionally emits a trace message at the moment
// plan execution starts.
func (st *SessionTracing) TraceExecStart(ctx context.Context, engine string) {
	log.VEventfDepth(ctx, 2, 1, "execution starts: %s", engine)
}

// TraceExecConsume creates a context for TraceExecRowsResult below.
func (st *SessionTracing) TraceExecConsume(ctx context.Context) (context.Context, func()) {
	if st.enabled {
		consumeCtx, sp := tracing.ChildSpan(ctx, "consuming rows")
		return consumeCtx, sp.Finish
	}
	return ctx, func() {}
}

// TraceExecRowsResult conditionally emits a trace message for a single output row.
func (st *SessionTracing) TraceExecRowsResult(ctx context.Context, values tree.Datums) {
	if st.showResults {
		log.VEventfDepth(ctx, 2, 1, "output row: %s", values)
	}
}

// TraceExecEnd conditionally emits a trace message at the moment
// plan execution completes.
func (st *SessionTracing) TraceExecEnd(ctx context.Context, err error, count int) {
	log.VEventfDepth(ctx, 2, 1, "execution ends")
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "execution failed after %d rows: %v", count, err)
	} else {
		log.VEventfDepth(ctx, 2, 1, "rows affected: %d", count)
	}
}

// extractMsgFromRecord extracts the message of the event, which is either in an
// "event" or "error" field.
func extractMsgFromRecord(rec tracing.RecordedSpan_LogRecord) string {
	for _, f := range rec.Fields {
		key := f.Key
		if key == "event" {
			return f.Value
		}
		if key == "error" {
			return fmt.Sprint("error:", f.Value)
		}
	}
	return "<event missing in trace message>"
}

const (
	// span_idx    INT NOT NULL,        -- The span's index.
	traceSpanIdxCol = iota
	// message_idx INT NOT NULL,        -- The message's index within its span.
	_
	// timestamp   TIMESTAMPTZ NOT NULL,-- The message's timestamp.
	traceTimestampCol
	// duration    INTERVAL,            -- The span's duration.
	//                                  -- NULL if the span was not finished at the time
	//                                  -- the trace has been collected.
	traceDurationCol
	// operation   STRING NULL,         -- The span's operation.
	traceOpCol
	// loc         STRING NOT NULL,     -- The file name / line number prefix, if any.
	traceLocCol
	// tag         STRING NOT NULL,     -- The logging tag, if any.
	traceTagCol
	// message     STRING NOT NULL,     -- The logged message.
	traceMsgCol
	// age         INTERVAL NOT NULL    -- The age of the message.
	traceAgeCol
	// traceNumCols must be the last item in the enumeration.
	traceNumCols
)

// traceRow is the type of a single row in the session_trace vtable.
type traceRow [traceNumCols]tree.Datum

// A regular expression to split log messages.
// It has three parts:
// - the (optional) code location, with at least one forward slash and a period
//   in the file name:
//   ((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?)
// - the (optional) tag: ((?:\[(?:[^][]|\[[^]]*\])*\])?)
// - the message itself: the rest.
var logMessageRE = regexp.MustCompile(
	`(?s:^((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?) *((?:\[(?:[^][]|\[[^]]*\])*\])?) *(.*))`)

// generateSessionTraceVTable generates the rows of said table by using the log
// messages from the session's trace (i.e. the ongoing trace, if any, or the
// last one recorded).
//
// All the log messages from the current recording are returned, in
// the order in which they should be presented in the crdb_internal.session_info
// virtual table. Messages from child spans are inserted as a block in between
// messages from the parent span. Messages from sibling spans are not
// interleaved.
//
// Here's a drawing showing the order in which messages from different spans
// will be interleaved. Each box is a span; inner-boxes are child spans. The
// numbers indicate the order in which the log messages will appear in the
// virtual table.
//
// +-----------------------+
// |           1           |
// | +-------------------+ |
// | |         2         | |
// | |  +----+           | |
// | |  |    | +----+    | |
// | |  | 3  | | 4  |    | |
// | |  |    | |    |  5 | |
// | |  |    | |    | ++ | |
// | |  |    | |    |    | |
// | |  +----+ |    |    | |
// | |         +----+    | |
// | |                   | |
// | |          6        | |
// | +-------------------+ |
// |            7          |
// +-----------------------+
//
// Note that what's described above is not the order in which SHOW TRACE FOR SESSION
// displays the information: SHOW TRACE will sort by the age column.
func generateSessionTraceVTable(spans []tracing.RecordedSpan) ([]traceRow, error) {
	// Get all the log messages, in the right order.
	var allLogs []logRecordRow

	// NOTE: The spans are recorded in the order in which they are started.
	seenSpans := make(map[uint64]struct{})
	for spanIdx, span := range spans {
		if _, ok := seenSpans[span.SpanID]; ok {
			continue
		}
		spanWithIndex := spanWithIndex{
			RecordedSpan: &spans[spanIdx],
			index:        spanIdx,
		}
		msgs, err := getMessagesForSubtrace(spanWithIndex, spans, seenSpans)
		if err != nil {
			return nil, err
		}
		allLogs = append(allLogs, msgs...)
	}

	// Transform the log messages into table rows.
	// We need to populate "operation" later because it is only
	// set for the first row in each span.
	opMap := make(map[tree.DInt]*tree.DString)
	durMap := make(map[tree.DInt]*tree.DInterval)
	var res []traceRow
	var minTimestamp, zeroTime time.Time
	for _, lrr := range allLogs {
		// The "operation" column is only set for the first row in span.
		// We'll populate the rest below.
		if lrr.index == 0 {
			spanIdx := tree.DInt(lrr.span.index)
			opMap[spanIdx] = tree.NewDString(lrr.span.Operation)
			if lrr.span.Duration != 0 {
				durMap[spanIdx] = &tree.DInterval{
					Duration: duration.Duration{
						Nanos: lrr.span.Duration.Nanoseconds(),
					},
				}
			}
		}

		// We'll need the lowest timestamp to compute ages below.
		if minTimestamp == zeroTime || lrr.timestamp.Before(minTimestamp) {
			minTimestamp = lrr.timestamp
		}

		// Split the message into component parts.
		//
		// The result of FindStringSubmatchIndex is a 1D array of pairs
		// [start, end) of positions in the input string.  The first pair
		// identifies the entire match; the 2nd pair corresponds to the
		// 1st parenthetized expression in the regexp, and so on.
		loc := logMessageRE.FindStringSubmatchIndex(lrr.msg)
		if loc == nil {
			return nil, fmt.Errorf("unable to split trace message: %q", lrr.msg)
		}

		row := traceRow{
			tree.NewDInt(tree.DInt(lrr.span.index)),               // span_idx
			tree.NewDInt(tree.DInt(lrr.index)),                    // message_idx
			tree.MakeDTimestampTZ(lrr.timestamp, time.Nanosecond), // timestamp
			tree.DNull,                              // duration, will be populated below
			tree.DNull,                              // operation, will be populated below
			tree.NewDString(lrr.msg[loc[2]:loc[3]]), // location
			tree.NewDString(lrr.msg[loc[4]:loc[5]]), // tag
			tree.NewDString(lrr.msg[loc[6]:loc[7]]), // message
			tree.DNull,                              // age, will be populated below
		}
		res = append(res, row)
	}

	if len(res) == 0 {
		// Nothing to do below. Shortcut.
		return res, nil
	}

	// Populate the operation and age columns.
	for i := range res {
		spanIdx := res[i][traceSpanIdxCol]

		if opStr, ok := opMap[*(spanIdx.(*tree.DInt))]; ok {
			res[i][traceOpCol] = opStr
		}

		if dur, ok := durMap[*(spanIdx.(*tree.DInt))]; ok {
			res[i][traceDurationCol] = dur
		}

		ts := res[i][traceTimestampCol].(*tree.DTimestampTZ)
		res[i][traceAgeCol] = &tree.DInterval{
			Duration: duration.Duration{Nanos: ts.Sub(minTimestamp).Nanoseconds()},
		}
	}

	return res, nil
}

// getOrderedChildSpans returns all the spans in allSpans that are children of
// spanID. It assumes the input is ordered by start time, in which case the
// output is also ordered.
func getOrderedChildSpans(spanID uint64, allSpans []tracing.RecordedSpan) []spanWithIndex {
	children := make([]spanWithIndex, 0)
	for i := range allSpans {
		if allSpans[i].ParentSpanID == spanID {
			children = append(
				children,
				spanWithIndex{
					RecordedSpan: &allSpans[i],
					index:        i,
				})
		}
	}
	return children
}

// getMessagesForSubtrace takes a span and interleaves its log messages with
// those from its children (recursively). The order is the one defined in the
// comment on generateSessionTraceVTable().
//
// seenSpans is modified to record all the spans that are part of the subtrace
// rooted at span.
func getMessagesForSubtrace(
	span spanWithIndex, allSpans []tracing.RecordedSpan, seenSpans map[uint64]struct{},
) ([]logRecordRow, error) {
	if _, ok := seenSpans[span.SpanID]; ok {
		return nil, errors.Errorf("duplicate span %d", span.SpanID)
	}
	var allLogs []logRecordRow
	const spanStartMsgTemplate = "=== SPAN START: %s ==="

	// spanStartMsgs are metadata about the span, e.g. the operation name and tags
	// contained in the span. They are added as one log message.
	spanStartMsgs := make([]string, 0, len(span.Tags)+1)

	spanStartMsgs = append(spanStartMsgs, fmt.Sprintf(spanStartMsgTemplate, span.Operation))

	// Add recognized tags to the output.
	for name, value := range span.Tags {
		if !strings.HasPrefix(name, tracing.TagPrefix) {
			// Not a tag to be output.
			continue
		}
		spanStartMsgs = append(spanStartMsgs, fmt.Sprintf("%s: %s", name, value))
	}
	sort.Strings(spanStartMsgs[1:])

	// This message holds all the spanStartMsgs and marks the beginning of the
	// span, to indicate the start time and duration of the span.
	allLogs = append(
		allLogs,
		logRecordRow{
			timestamp: span.StartTime,
			msg:       strings.Join(spanStartMsgs, "\n"),
			span:      span,
			index:     0,
		},
	)

	seenSpans[span.SpanID] = struct{}{}
	childSpans := getOrderedChildSpans(span.SpanID, allSpans)
	var i, j int
	// Sentinel value - year 6000.
	maxTime := time.Date(6000, 0, 0, 0, 0, 0, 0, time.UTC)
	// Merge the logs with the child spans.
	for i < len(span.Logs) || j < len(childSpans) {
		logTime := maxTime
		childTime := maxTime
		if i < len(span.Logs) {
			logTime = span.Logs[i].Time
		}
		if j < len(childSpans) {
			childTime = childSpans[j].StartTime
		}

		if logTime.Before(childTime) {
			allLogs = append(allLogs,
				logRecordRow{
					timestamp: logTime,
					msg:       extractMsgFromRecord(span.Logs[i]),
					span:      span,
					// Add 1 to the index to account for the first dummy message in a
					// span.
					index: i + 1,
				})
			i++
		} else {
			// Recursively append messages from the trace rooted at the child.
			childMsgs, err := getMessagesForSubtrace(childSpans[j], allSpans, seenSpans)
			if err != nil {
				return nil, err
			}
			allLogs = append(allLogs, childMsgs...)
			j++
		}
	}
	return allLogs, nil
}

// logRecordRow is used to temporarily hold on to log messages and their
// metadata while flattening a trace.
type logRecordRow struct {
	timestamp time.Time
	msg       string
	span      spanWithIndex
	// index of the log message within its span.
	index int
}

type spanWithIndex struct {
	*tracing.RecordedSpan
	index int
}

// sessionDataMutator is the interface used by sessionVars to change the session
// state. It mostly mutates the Session's SessionData, but not exclusively (e.g.
// see curTxnReadOnly).
type sessionDataMutator struct {
	data     *sessiondata.SessionData
	defaults sessionDefaults
	settings *cluster.Settings
	// curTxnReadOnly is a value to be mutated through SET transaction_read_only = ...
	curTxnReadOnly *bool
	// applicationNamedChanged, if set, is called when the "application name"
	// variable is updated.
	applicationNameChanged func(newName string)
}

// SetApplicationName sets the application name.
func (m *sessionDataMutator) SetApplicationName(appName string) {
	m.data.ApplicationName = appName
	if m.applicationNameChanged != nil {
		m.applicationNameChanged(appName)
	}
}

func (m *sessionDataMutator) SetBytesEncodeFormat(val sessiondata.BytesEncodeFormat) {
	m.data.DataConversion.BytesEncodeFormat = val
}

func (m *sessionDataMutator) SetExtraFloatDigits(val int) {
	m.data.DataConversion.ExtraFloatDigits = val
}

func (m *sessionDataMutator) SetDatabase(dbName string) {
	m.data.Database = dbName
}

func (m *sessionDataMutator) SetDefaultIsolationLevel(iso enginepb.IsolationType) {
	m.data.DefaultIsolationLevel = iso
}

func (m *sessionDataMutator) SetDefaultReadOnly(val bool) {
	m.data.DefaultReadOnly = val
}

func (m *sessionDataMutator) SetDistSQLMode(val sessiondata.DistSQLExecMode) {
	m.data.DistSQLMode = val
}

func (m *sessionDataMutator) SetLookupJoinEnabled(val bool) {
	m.data.LookupJoinEnabled = val
}

func (m *sessionDataMutator) SetForceSplitAt(val bool) {
	m.data.ForceSplitAt = val
}

func (m *sessionDataMutator) SetZigzagJoinEnabled(val bool) {
	m.data.ZigzagJoinEnabled = val
}

func (m *sessionDataMutator) SetOptimizerMode(val sessiondata.OptimizerMode) {
	m.data.OptimizerMode = val
}

func (m *sessionDataMutator) SetSerialNormalizationMode(val sessiondata.SerialNormalizationMode) {
	m.data.SerialNormalizationMode = val
}

func (m *sessionDataMutator) SetSafeUpdates(val bool) {
	m.data.SafeUpdates = val
}

func (m *sessionDataMutator) SetSearchPath(val sessiondata.SearchPath) {
	m.data.SearchPath = val
}

func (m *sessionDataMutator) SetLocation(loc *time.Location) {
	m.data.DataConversion.Location = loc
}

func (m *sessionDataMutator) SetReadOnly(val bool) {
	*m.curTxnReadOnly = val
}

func (m *sessionDataMutator) SetStmtTimeout(timeout time.Duration) {
	m.data.StmtTimeout = timeout
}

// RecordLatestSequenceValue records that value to which the session incremented
// a sequence.
func (m *sessionDataMutator) RecordLatestSequenceVal(seqID uint32, val int64) {
	m.data.SequenceState.RecordValue(seqID, val)
}

type sqlStatsCollectorImpl struct {
	// sqlStats tracks per-application statistics for all
	// applications on each node.
	sqlStats *sqlStats
	// appStats track per-application SQL usage statistics. This is a pointer into
	// sqlStats set as the session's current app.
	appStats *appStats
	// phaseTimes tracks session-level phase times. It is copied-by-value
	// to each planner in session.newPlanner.
	phaseTimes phaseTimes
}

// sqlStatsCollectorImpl implements the sqlStatsCollector interface.
var _ sqlStatsCollector = &sqlStatsCollectorImpl{}

// newSQLStatsCollectorImpl creates an instance of sqlStatsCollectorImpl.
//
// note that phaseTimes is an array, not a slice, so this performs a copy-by-value.
func newSQLStatsCollectorImpl(
	sqlStats *sqlStats, appStats *appStats, phaseTimes *phaseTimes,
) *sqlStatsCollectorImpl {
	return &sqlStatsCollectorImpl{
		sqlStats:   sqlStats,
		appStats:   appStats,
		phaseTimes: *phaseTimes,
	}
}

// PhaseTimes is part of the sqlStatsCollector interface.
func (s *sqlStatsCollectorImpl) PhaseTimes() *phaseTimes {
	return &s.phaseTimes
}

// RecordStatement is part of the sqlStatsCollector interface.
func (s *sqlStatsCollectorImpl) RecordStatement(
	stmt Statement,
	distSQLUsed bool,
	optUsed bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
) {
	s.appStats.recordStatement(
		stmt, distSQLUsed, optUsed, automaticRetryCount, numRows, err,
		parseLat, planLat, runLat, svcLat, ovhLat)
}

// SQLStats is part of the sqlStatsCollector interface.
func (s *sqlStatsCollectorImpl) SQLStats() *sqlStats {
	return s.sqlStats
}

func (s *sqlStatsCollectorImpl) Reset(
	sqlStats *sqlStats, appStats *appStats, phaseTimes *phaseTimes,
) {
	*s = sqlStatsCollectorImpl{
		sqlStats:   sqlStats,
		appStats:   appStats,
		phaseTimes: *phaseTimes,
	}
}
