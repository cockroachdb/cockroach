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
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

const sqlTxnName string = "sql txn"
const metricsSampleInterval = 10 * time.Second

// Fully-qualified names for metrics.
var (
	MetaTxnBegin = metric.Metadata{
		Name: "sql.txn.begin.count",
		Help: "Number of SQL transaction BEGIN statements"}
	MetaTxnCommit = metric.Metadata{
		Name: "sql.txn.commit.count",
		Help: "Number of SQL transaction COMMIT statements"}
	MetaTxnAbort = metric.Metadata{
		Name: "sql.txn.abort.count",
		Help: "Number of SQL transaction ABORT statements"}
	MetaTxnRollback = metric.Metadata{
		Name: "sql.txn.rollback.count",
		Help: "Number of SQL transaction ROLLBACK statements"}
	MetaSelect = metric.Metadata{
		Name: "sql.select.count",
		Help: "Number of SQL SELECT statements"}
	MetaSQLExecLatency = metric.Metadata{
		Name: "sql.exec.latency",
		Help: "Latency in nanoseconds of SQL statement execution"}
	MetaSQLServiceLatency = metric.Metadata{
		Name: "sql.service.latency",
		Help: "Latency in nanoseconds of SQL request execution"}
	MetaDistSQLSelect = metric.Metadata{
		Name: "sql.distsql.select.count",
		Help: "Number of DistSQL SELECT statements"}
	MetaDistSQLExecLatency = metric.Metadata{
		Name: "sql.distsql.exec.latency",
		Help: "Latency in nanoseconds of DistSQL statement execution"}
	MetaDistSQLServiceLatency = metric.Metadata{
		Name: "sql.distsql.service.latency",
		Help: "Latency in nanoseconds of DistSQL request execution"}
	MetaUpdate = metric.Metadata{
		Name: "sql.update.count",
		Help: "Number of SQL UPDATE statements"}
	MetaInsert = metric.Metadata{
		Name: "sql.insert.count",
		Help: "Number of SQL INSERT statements"}
	MetaDelete = metric.Metadata{
		Name: "sql.delete.count",
		Help: "Number of SQL DELETE statements"}
	MetaDdl = metric.Metadata{
		Name: "sql.ddl.count",
		Help: "Number of SQL DDL statements"}
	MetaMisc = metric.Metadata{
		Name: "sql.misc.count",
		Help: "Number of other SQL statements"}
	MetaQuery = metric.Metadata{
		Name: "sql.query.count",
		Help: "Number of SQL queries"}
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
	SessionRegistry  *SessionRegistry
	JobRegistry      *jobs.Registry
	VirtualSchemas   *VirtualSchemaHolder
	DistSQLPlanner   *DistSQLPlanner
	ExecLogger       *log.SecondaryLogger
	AuditLogger      *log.SecondaryLogger
	InternalExecutor *InternalSQLExecutor

	TestingKnobs              *ExecutorTestingKnobs
	SchemaChangerTestingKnobs *SchemaChangerTestingKnobs
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
// databases that chaged.
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
func (dc *databaseCacheHolder) waitForCacheState(cond func(*databaseCache) (bool, error)) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for {
		done, err := cond(dc.mu.c)
		if err != nil {
			return err
		}
		if done {
			break
		}
		dc.mu.cv.Wait()
	}
	return nil
}

// databaseCacheHolder implements the dbCacheSubscriber interface.
var _ dbCacheSubscriber = &databaseCacheHolder{}

// updateSystemConfig is called whenever a new system config gossip entry is
// received.
func (dc *databaseCacheHolder) updateSystemConfig(cfg config.SystemConfig) {
	dc.mu.Lock()
	dc.mu.c = newDatabaseCache(cfg)
	dc.mu.cv.Broadcast()
	dc.mu.Unlock()
}

// getTransactionState retrieves a text representation of the given state.
func getTransactionState(txnState *txnState) string {
	state := txnState.State()
	// If the statement reading the state is in an implicit transaction, then we
	// want to tell NoTxn to the client.
	// If implicitTxn is set, the state is supposed to be AutoRetry. However, we
	// test the state too, just in case we have a state machine bug, in which case
	// we don't want this code to hide a bug.
	if txnState.implicitTxn && state == AutoRetry {
		state = NoTxn
	}
	// For the purposes of representing the states to client, make the AutoRetry
	// state look like Open.
	if state == AutoRetry {
		state = Open
	}
	return state.String()
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

// shouldUseOptimizer determines whether we should use the experimental
// optimizer for planning.
func shouldUseOptimizer(optMode sessiondata.OptimizerMode, stmt Statement) bool {
	switch optMode {
	case sessiondata.OptimizerAlways:
		// Don't try to run SET commands with the optimizer in Always mode, or
		// else we can't switch to another mode.
		if _, setVar := stmt.AST.(*tree.SetVar); !setVar {
			return true
		}

	case sessiondata.OptimizerOn:
		// Only handle a subset of the statement types (currently read-only queries).
		switch stmt.AST.(type) {
		case *tree.ParenSelect, *tree.Select, *tree.SelectClause,
			*tree.UnionClause, *tree.ValuesClause:
			return true
		}
	}
	return false
}

// shouldUseDistSQL determines whether we should use DistSQL for the
// given logical plan, based on the session settings.
func shouldUseDistSQL(
	ctx context.Context,
	distSQLMode sessiondata.DistSQLExecMode,
	dp *DistSQLPlanner,
	planner *planner,
) (bool, error) {
	if distSQLMode == sessiondata.DistSQLOff {
		return false, nil
	}

	plan := planner.curPlan.plan

	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.(*zeroNode); ok {
		return false, nil
	}

	// We don't support subqueries yet.
	if len(planner.curPlan.subqueryPlans) > 0 {
		if distSQLMode == sessiondata.DistSQLAlways {
			err := newQueryNotSupportedError("subqueries not supported yet")
			log.VEventf(ctx, 1, "query not supported for distSQL: %s", err)
			return false, err
		}
		return false, nil
	}

	// Trigger limit propagation.
	planner.setUnlimited(plan)

	distribute, err := dp.CheckSupport(plan)
	if err != nil {
		// If the distSQLMode is ALWAYS, reject anything but SET.
		if distSQLMode == sessiondata.DistSQLAlways && err != setNotSupportedError {
			return false, err
		}
		// Don't use distSQL for this request.
		log.VEventf(ctx, 1, "query not supported for distSQL: %s", err)
		return false, nil
	}

	if distSQLMode == sessiondata.DistSQLAuto && !distribute {
		log.VEventf(ctx, 1, "not distributing query")
		return false, nil
	}

	// In ON or ALWAYS mode, all supported queries are distributed.
	return true, nil
}

// golangFillQueryArguments populates the placeholder map with
// types and values from an array of Go values.
// The args can be datums, or Go basic types.
//
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
//
// TODO(andrei): make this simply return a slice of datums once the "internal
// planner" goes away.
func golangFillQueryArguments(pinfo *tree.PlaceholderInfo, args []interface{}) {
	pinfo.Clear()

	for i, arg := range args {
		k := strconv.Itoa(i + 1)
		if arg == nil {
			pinfo.SetValue(k, tree.DNull)
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
		pinfo.SetValue(k, d)
	}
}

func checkResultType(typ types.T) error {
	// Compare all types that can rely on == equality.
	switch types.UnwrapType(typ) {
	case types.Unknown:
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
func EvalAsOfTimestamp(
	evalCtx *tree.EvalContext, asOf tree.AsOfClause, max hlc.Timestamp,
) (hlc.Timestamp, error) {
	te, err := asOf.Expr.TypeCheck(nil, types.String)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	d, err := te.Eval(evalCtx)
	if err != nil {
		return hlc.Timestamp{}, err
	}

	var ts hlc.Timestamp
	var convErr error

	switch d := d.(type) {
	case *tree.DString:
		s := string(*d)
		// Allow nanosecond precision because the timestamp is only used by the
		// system and won't be returned to the user over pgwire.
		if dt, err := tree.ParseDTimestamp(s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err != nil {
			// Override the error. It would be misleading to fail with a
			// DECIMAL conversion error if a user was attempting to use a
			// timestamp string and the conversion above failed.
			convErr = errors.Errorf("AS OF SYSTEM TIME: value is neither timestamp nor decimal")
		} else {
			ts, convErr = decimalToHLC(dec)
		}
	case *tree.DInt:
		ts.WallTime = int64(*d)
	case *tree.DDecimal:
		ts, convErr = decimalToHLC(&d.Decimal)
	default:
		convErr = errors.Errorf("AS OF SYSTEM TIME: expected timestamp, got %s (%T)", d.ResolvedType(), d)
	}
	if convErr != nil {
		return ts, convErr
	}

	var zero hlc.Timestamp
	if ts == zero {
		return ts, errors.Errorf("AS OF SYSTEM TIME: zero timestamp is invalid")
	} else if max.Less(ts) {
		return ts, errors.Errorf("AS OF SYSTEM TIME: cannot specify timestamp in the future")
	}
	return ts, nil
}

func decimalToHLC(d *apd.Decimal) (hlc.Timestamp, error) {
	// Format the decimal into a string and split on `.` to extract the nanosecond
	// walltime and logical tick parts.
	// TODO(mjibson): use d.Modf() instead of converting to a string.
	s := d.Text('f')
	parts := strings.SplitN(s, ".", 2)
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return hlc.Timestamp{}, errors.Wrap(err, "AS OF SYSTEM TIME: parsing argument")
	}
	var logical int64
	if len(parts) > 1 {
		// logicalLength is the number of decimal digits expected in the
		// logical part to the right of the decimal. See the implementation of
		// cluster_logical_timestamp().
		const logicalLength = 10
		p := parts[1]
		if lp := len(p); lp > logicalLength {
			return hlc.Timestamp{}, errors.Errorf("AS OF SYSTEM TIME: logical part has too many digits")
		} else if lp < logicalLength {
			p += strings.Repeat("0", logicalLength-lp)
		}
		logical, err = strconv.ParseInt(p, 10, 32)
		if err != nil {
			return hlc.Timestamp{}, errors.Wrap(err, "AS OF SYSTEM TIME: parsing argument")
		}
	}
	return hlc.Timestamp{
		WallTime: nanos,
		Logical:  int32(logical),
	}, nil
}

// isAsOf analyzes a statement to bypass the logic in newPlan(), since
// that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction
// should be set. The statements that will be checked are Select,
// ShowTrace (of a Select statement), and Scrub.
//
// max is a lower bound on what the transaction's timestamp will be.
// Used to check that the user didn't specify a timestamp in the future.
func isAsOf(
	stmt tree.Statement, evalCtx *tree.EvalContext, max hlc.Timestamp,
) (*hlc.Timestamp, error) {
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
	case *tree.ShowTrace:
		return isAsOf(s.Statement, evalCtx, max)
	case *tree.Scrub:
		if s.AsOf.Expr == nil {
			return nil, nil
		}
		asOf = s.AsOf
	default:
		return nil, nil
	}

	ts, err := EvalAsOfTimestamp(evalCtx, asOf, max)
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
