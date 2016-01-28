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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/decimal"
)

var testingWaitForMetadata bool

// TestingWaitForMetadata causes metadata-mutating operations to wait
// for the new metadata to back-propagate through gossip.
func TestingWaitForMetadata() func() {
	testingWaitForMetadata = true
	return func() {
		testingWaitForMetadata = false
	}
}

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errStaleMetadata = errors.New("metadata is still stale")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

var plannerPool = sync.Pool{
	New: func() interface{} {
		p := &planner{}
		p.evalCtx.GetLocation = p.session.getLocation
		return p
	},
}

// Request is an SQL request to cockroach. A transaction can consist of multiple
// requests.
type Request struct {
	// User is the originating user.
	User string
	// Session settings that were returned in the last response that
	// contained them, being reflected back to the server.
	Session []byte
	// SQL statement(s) to be serially executed by the server. Multiple
	// statements are passed as a single string separated by semicolons.
	SQL string
	// Parameters referred to in the above SQL statement(s) using "?".
	Params []parser.Datum
}

// Response is the reply to an SQL request to cockroach.
type Response struct {
	// Setting that should be reflected back in all subsequent requests.
	// When not set, future requests should continue to use existing settings.
	Session []byte
	// The list of results. There is one result object per SQL statement in the
	// request.
	Results []Result
}

// Result corresponds to the execution of a single SQL statement.
type Result struct {
	PErr *roachpb.Error
	// The type of statement that the result is for.
	Type parser.StatementType
	// The tag of the statement that the result is for.
	PGTag string
	// RowsAffected will be populated if the statement type is "RowsAffected".
	RowsAffected int
	// Columns will be populated if the statement type is "Rows". It will contain
	// the names and types of the columns returned in the result set in the order
	// specified in the SQL statement. The number of columns will equal the number
	// of values in each Row.
	Columns []ResultColumn
	// Rows will be populated if the statement type is "Rows". It will contain
	// the result set of the result.
	// TODO(nvanbenschoten): Can this be streamed from the planNode?
	Rows []ResultRow
}

// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  parser.Datum

	// If set, this is an implicit column; used internally.
	hidden bool
}

// ResultRow is a collection of values representing a row in a result.
type ResultRow struct {
	Values []parser.Datum
}

// An Executor executes SQL statements.
type Executor struct {
	db       client.DB
	nodeID   roachpb.NodeID
	reCache  *parser.RegexpCache
	leaseMgr *LeaseManager

	// Transient stats.
	registry    *metric.Registry
	latency     metric.Histograms
	selectCount *metric.Counter
	txnCount    *metric.Counter
	updateCount *metric.Counter
	insertCount *metric.Counter
	deleteCount *metric.Counter
	ddlCount    *metric.Counter
	miscCount   *metric.Counter

	// System Config and mutex.
	systemConfig     config.SystemConfig
	systemConfigMu   sync.RWMutex
	systemConfigCond *sync.Cond
}

// NewExecutor creates an Executor and registers a callback on the
// system config.
func NewExecutor(db client.DB, gossip *gossip.Gossip, leaseMgr *LeaseManager, stopper *stop.Stopper) *Executor {
	registry := metric.NewRegistry()
	exec := &Executor{
		db:       db,
		reCache:  parser.NewRegexpCache(512),
		leaseMgr: leaseMgr,

		registry:    registry,
		latency:     registry.Latency("latency"),
		txnCount:    registry.Counter("transaction.count"),
		selectCount: registry.Counter("select.count"),
		updateCount: registry.Counter("update.count"),
		insertCount: registry.Counter("insert.count"),
		deleteCount: registry.Counter("delete.count"),
		ddlCount:    registry.Counter("ddl.count"),
		miscCount:   registry.Counter("misc.count"),
	}
	exec.systemConfigCond = sync.NewCond(&exec.systemConfigMu)

	gossipUpdateC := gossip.RegisterSystemConfigChannel()
	stopper.RunWorker(func() {
		for {
			select {
			case <-gossipUpdateC:
				cfg := gossip.GetSystemConfig()
				exec.updateSystemConfig(cfg)
			case <-stopper.ShouldStop():
				return
			}
		}
	})

	return exec
}

// SetNodeID sets the node ID for the SQL server. This method must be called
// before actually using the Executor.
func (e *Executor) SetNodeID(nodeID roachpb.NodeID) {
	e.nodeID = nodeID
	e.leaseMgr.nodeID = uint32(nodeID)
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (e *Executor) updateSystemConfig(cfg *config.SystemConfig) {
	e.systemConfigMu.Lock()
	e.systemConfig = *cfg
	e.systemConfigCond.Broadcast()
	e.systemConfigMu.Unlock()
}

// getSystemConfig returns a pointer to the latest system config. May be nil,
// if the gossip callback has not run.
func (e *Executor) getSystemConfig() config.SystemConfig {
	e.systemConfigMu.RLock()
	cfg := e.systemConfig
	e.systemConfigMu.RUnlock()
	return cfg
}

// Prepare returns the result types of the given statement. Args may be a
// partially populated val args map. Prepare will populate the missing val
// args. The column result types are returned (or nil if there are no results).
func (e *Executor) Prepare(user string, query string, args parser.MapArgs) ([]ResultColumn, *roachpb.Error) {
	stmt, err := parser.ParseOneTraditional(query)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	planMaker := plannerPool.Get().(*planner)
	defer plannerPool.Put(planMaker)

	*planMaker = planner{
		user: user,
		evalCtx: parser.EvalContext{
			NodeID:  e.nodeID,
			ReCache: e.reCache,
			// Copy existing GetLocation closure. See plannerPool.New() for the
			// initial setting.
			GetLocation: planMaker.evalCtx.GetLocation,
			Args:        args,
		},
		leaseMgr:     e.leaseMgr,
		systemConfig: e.getSystemConfig(),
	}

	timestamp := time.Now()
	txn := client.NewTxn(e.db)
	planMaker.setTxn(txn, timestamp)
	planMaker.evalCtx.StmtTimestamp = parser.DTimestamp{Time: timestamp}
	plan, pErr := planMaker.prepare(stmt)
	if pErr != nil {
		return nil, pErr
	}
	if plan == nil {
		return nil, nil
	}
	cols := plan.Columns()
	for _, c := range cols {
		if err := checkResultDatum(c.Typ); err != nil {
			return nil, err
		}
	}
	return cols, nil
}

// ExecuteStatements executes the given statement(s) and returns a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) ExecuteStatements(user string, session Session, stmts string, params []parser.Datum) (Response, int, error) {
	planMaker := plannerPool.Get().(*planner)
	defer plannerPool.Put(planMaker)

	*planMaker = planner{
		user: user,
		evalCtx: parser.EvalContext{
			NodeID:  e.nodeID,
			ReCache: e.reCache,
			// Copy existing GetLocation closure. See plannerPool.New() for the
			// initial setting.
			GetLocation: planMaker.evalCtx.GetLocation,
		},
		leaseMgr:     e.leaseMgr,
		systemConfig: e.getSystemConfig(),
		session:      session,
	}

	// Resume a pending transaction if present.
	if planMaker.session.Txn != nil {
		txn := client.NewTxn(e.db)
		txn.Proto = planMaker.session.Txn.Txn
		txn.UserPriority = planMaker.session.Txn.UserPriority
		if planMaker.session.MutatesSystemConfig {
			txn.SetSystemConfigTrigger()
		}
		planMaker.setTxn(txn, planMaker.session.Txn.Timestamp.GoTime())
	}

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	planMaker.params = parameters(params)
	reply := e.execStmts(stmts, planMaker)

	// Send back the session state even if there were application-level errors.
	// Add transaction to session state.
	if planMaker.txn != nil {
		// TODO(pmattis): Need to record the leases used by a transaction within
		// the transaction state and restore it when the transaction is restored.
		planMaker.releaseLeases(e.db)
		planMaker.session.Txn = &Session_Transaction{
			Txn:          planMaker.txn.Proto,
			Timestamp:    driver.Timestamp(planMaker.evalCtx.TxnTimestamp.Time),
			UserPriority: planMaker.txn.UserPriority,
		}
		planMaker.session.MutatesSystemConfig = planMaker.txn.SystemConfigTrigger()
	} else {
		planMaker.session.Txn = nil
		planMaker.session.MutatesSystemConfig = false
	}
	bytes, err := proto.Marshal(&planMaker.session)
	if err != nil {
		return Response{}, http.StatusInternalServerError, err
	}
	reply.Session = bytes

	return reply, 0, nil
}

// Execute the statement(s) in the given request and returns a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) Execute(args Request) (Response, int, error) {
	defer func(start time.Time) {
		e.latency.RecordValue(time.Now().Sub(start).Nanoseconds())
	}(time.Now())
	var session Session
	if err := proto.Unmarshal(args.Session, &session); err != nil {
		return Response{}, http.StatusBadRequest, err
	}
	return e.ExecuteStatements(args.User, session, args.SQL, args.Params)
}

// exec executes the request. Any error encountered is returned; it is
// the caller's responsibility to update the response.
func (e *Executor) execStmts(sql string, planMaker *planner) Response {
	var resp Response
	stmts, err := planMaker.parser.Parse(sql, parser.Syntax(planMaker.session.Syntax))
	if err != nil {
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		resp.Results = append(resp.Results, makeResultFromError(planMaker, roachpb.NewError(err)))
		return resp
	}
	for _, stmt := range stmts {
		result, err := e.execStmt(stmt, planMaker)
		if err != nil {
			result = makeResultFromError(planMaker, err)
		}
		// Release the leases once a transaction is complete.
		if planMaker.txn == nil {
			planMaker.releaseLeases(e.db)
			// Execute any schema changes that were scheduled.
			if len(planMaker.schemaChangers) > 0 &&
				// Disable execution in some tests.
				!disableSyncSchemaChangeExec {
				retryOpts := retry.Options{
					InitialBackoff: 20 * time.Millisecond,
					MaxBackoff:     200 * time.Millisecond,
					Multiplier:     2,
				}
				for _, sc := range planMaker.schemaChangers {
					sc.db = e.db
					for r := retry.Start(retryOpts); r.Next(); {
						if done, err := sc.IsDone(); err != nil {
							log.Warning(err)
							break
						} else if done {
							break
						}
						if pErr := sc.exec(); pErr != nil {
							if _, ok := pErr.GoError().(*roachpb.ExistingSchemaChangeLeaseError); ok {
								// Try again.
								continue
							}
							// All other errors can be reported.
							result = makeResultFromError(planMaker, pErr)
						}
						break
					}
				}
			}
		}
		resp.Results = append(resp.Results, result)
	}
	return resp
}

func (e *Executor) execStmt(stmt parser.Statement, planMaker *planner) (Result, *roachpb.Error) {
	var result Result

	e.updateStmtCounts(stmt)
	switch stmt.(type) {
	case *parser.BeginTransaction:
		if planMaker.txn != nil {
			return result, roachpb.NewError(errTransactionInProgress)
		}
		// Start a transaction here and not in planMaker to prevent begin
		// transaction from being called within an auto-transaction below.
		planMaker.setTxn(client.NewTxn(e.db), time.Now())
		planMaker.txn.SetDebugName("sql", 0)
		e.txnCount.Inc(1)
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		if planMaker.txn == nil {
			return result, roachpb.NewError(errNoTransactionInProgress)
		} else if planMaker.txn.Proto.Status == roachpb.ABORTED {
			// Reset to allow starting a new transaction.
			planMaker.resetTxn()
			return result, nil
		}
	case *parser.SetTransaction:
		if planMaker.txn == nil {
			return result, roachpb.NewError(errNoTransactionInProgress)
		}
	default:
		if planMaker.txn != nil && planMaker.txn.Proto.Status == roachpb.ABORTED {
			return result, roachpb.NewError(&roachpb.SqlTransactionAbortedError{})
		}
	}

	// Bind all the placeholder variables in the stmt to actual values.
	if err := parser.FillArgs(stmt, &planMaker.params); err != nil {
		return result, roachpb.NewError(err)
	}

	// Create a function which both makes and executes the plan, populating
	// result.
	//
	// TODO(pmattis): Should this be a separate function? Perhaps we should move
	// some of the common code back out into execStmts and have execStmt contain
	// only the body of this closure.
	f := func(timestamp time.Time, autoCommit bool) *roachpb.Error {
		planMaker.evalCtx.StmtTimestamp = parser.DTimestamp{Time: timestamp}
		plan, pErr := planMaker.makePlan(stmt, autoCommit)
		if pErr != nil {
			return pErr
		}

		result.PGTag = stmt.StatementTag()
		result.Type = stmt.StatementType()

		switch result.Type {
		case parser.RowsAffected:
			for plan.Next() {
				result.RowsAffected++
			}

		case parser.Rows:
			result.Columns = plan.Columns()
			for _, c := range result.Columns {
				if err := checkResultDatum(c.Typ); err != nil {
					return err
				}
			}

			for plan.Next() {
				// The plan.Values DTuple needs to be copied on each iteration.
				values := plan.Values()
				row := ResultRow{Values: make([]parser.Datum, 0, len(values))}
				for _, val := range values {
					if err := checkResultDatum(val); err != nil {
						return err
					}
					row.Values = append(row.Values, val)
				}
				result.Rows = append(result.Rows, row)
			}
		}

		return plan.PErr()
	}

	// If there is a pending transaction.
	if planMaker.txn != nil {
		pErr := f(time.Now(), false)
		return result, pErr
	}

	if testingWaitForMetadata {
		// We might need to verify metadata. Lock the system config so that
		// no gossip updates sneak in under us.
		// This lock does not change semantics. Even outside of tests, the
		// planner is initialized with a static systemConfig, so locking
		// the Executor's systemConfig cannot change the semantics of the
		// SQL operation being performed under lock.
		//
		// The case of a multi-request transaction is not handled here,
		// because those transactions outlive the verification callback.
		// This can be addressed when we move to a connection-oriented
		// protocol and server-side transactions.
		e.systemConfigCond.L.Lock()
		defer e.systemConfigCond.L.Unlock()
	}

	// No transaction. Run the command as a retryable block in an
	// auto-transaction.
	if pErr := e.db.Txn(func(txn *client.Txn) *roachpb.Error {
		// For transient stats, we do not report implicit transactions as part of txnCount.
		timestamp := time.Now()
		planMaker.setTxn(txn, timestamp)
		pErr := f(timestamp, true)
		planMaker.resetTxn()
		return pErr
	}); pErr != nil {
		return result, pErr
	}

	if testingWaitForMetadata {
		if verify := planMaker.testingVerifyMetadata; verify != nil {
			// In the case of a multi-statement request, avoid reusing this
			// callback.
			planMaker.testingVerifyMetadata = nil
			for i := 0; ; i++ {
				if verify(e.systemConfig) != nil {
					e.systemConfigCond.Wait()
				} else {
					if i == 0 {
						return result, roachpb.NewErrorf("expected %q to require a gossip update, but it did not", stmt)
					} else if i > 1 {
						log.Infof("%q unexpectedly required %d gossip updates", stmt, i)
					}
					break
				}
			}
		}
	}

	return result, nil
}

// updateStmtCounts updates metrics for the number of times the different types of SQL
// statements have been received by this node.
func (e *Executor) updateStmtCounts(stmt parser.Statement) {
	switch stmt.(type) {
	case *parser.Select:
		e.selectCount.Inc(1)

	case *parser.Update:
		e.updateCount.Inc(1)

	case *parser.Insert:
		e.insertCount.Inc(1)

	case *parser.Delete:
		e.deleteCount.Inc(1)

	default:
		if stmt.StatementType() == parser.DDL {
			e.ddlCount.Inc(1)
		} else {
			e.miscCount.Inc(1)
		}
	}
}

// Registry returns our metrics Registry, which must be added to another Registry so that
// the Server's Registry is aware of this one.
func (e *Executor) Registry() *metric.Registry {
	return e.registry
}

// If we hit an error and there is a pending transaction, rollback
// the transaction before returning. The client does not have to
// deal with cleaning up transaction state.
func makeResultFromError(planMaker *planner, pErr *roachpb.Error) Result {
	if planMaker.txn != nil {
		if _, ok := pErr.GoError().(*roachpb.SqlTransactionAbortedError); !ok {
			planMaker.txn.Cleanup(pErr)
		}
	}
	return Result{PErr: pErr}
}

var _ parser.Args = parameters{}

type parameters []parser.Datum

var errNamedArgument = errors.New("named arguments are not supported")

// processPositionalArgument is a helper function that processes a positional
// integer argument passed to an implementation of parser.Args. Currently, only
// positional arguments (non-negative integers) are supported, but named arguments may
// be supported in the future.
func processPositionalArgument(name string) (int64, error) {
	if len(name) == 0 {
		// This shouldn't happen unless the parser let through an invalid parameter
		// specification.
		panic(fmt.Sprintf("invalid empty parameter name"))
	}
	if ch := name[0]; ch < '0' || ch > '9' {
		// TODO(pmattis): Add support for named parameters (vs the numbered
		// parameter support below).
		return 0, errNamedArgument
	}
	return strconv.ParseInt(name, 10, 0)
}

// Arg implements the parser.Args interface.
func (p parameters) Arg(name string) (parser.Datum, bool) {
	i, err := processPositionalArgument(name)
	if err != nil {
		return nil, false
	}
	if i < 1 || int(i) > len(p) {
		return nil, false
	}
	return p[i-1], true
}

var _ parser.Args = golangParameters{}

type golangParameters []interface{}

// Arg implements the parser.Args interface.
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func (gp golangParameters) Arg(name string) (parser.Datum, bool) {
	i, err := processPositionalArgument(name)
	if err != nil {
		return nil, false
	}
	if i < 1 || int(i) > len(gp) {
		return nil, false
	}
	arg := gp[i-1]
	if arg == nil {
		return parser.DNull, true
	}

	// A type switch to handle a few explicit types with special semantics.
	switch t := arg.(type) {
	// Datums are passed along as is.
	case parser.Datum:
		return t, true
	// Time datatypes get special representation in the database.
	case time.Time:
		return parser.DTimestamp{Time: t}, true
	case time.Duration:
		return parser.DInterval{Duration: t}, true
	case decimal.Decimal:
		return parser.DDecimal{Decimal: t}, true
	}

	// Handle all types which have an underlying type that can be stored in the
	// database.
	// Note: if this reflection becomes a performance concern in the future,
	// commonly used types could be added explicitly into the type switch above
	// for a performance gain.
	val := reflect.ValueOf(arg)
	switch val.Kind() {
	case reflect.Bool:
		return parser.DBool(val.Bool()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return parser.DInt(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return parser.DInt(val.Uint()), true
	case reflect.Float32, reflect.Float64:
		return parser.DFloat(val.Float()), true
	case reflect.String:
		return parser.DString(val.String()), true
	case reflect.Slice:
		// Handle byte slices.
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return parser.DBytes(val.Bytes()), true
		}
	}

	panic(fmt.Sprintf("unexpected type %T", arg))
}

func checkResultDatum(datum parser.Datum) *roachpb.Error {
	if datum == parser.DNull {
		return nil
	}

	switch datum.(type) {
	case parser.DBool:
	case parser.DInt:
	case parser.DFloat:
	case parser.DDecimal:
	case parser.DBytes:
	case parser.DString:
	case parser.DDate:
	case parser.DTimestamp:
	case parser.DInterval:
	default:
		return roachpb.NewUErrorf("unsupported result type: %s", datum.Type())
	}
	return nil
}
