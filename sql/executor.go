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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"errors"
	"fmt"
	"net/http"
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
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
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
var errTransactionAborted = errors.New("current transaction is aborted, commands ignored until end of transaction block")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

var plannerPool = sync.Pool{
	New: func() interface{} {
		p := &planner{}
		p.evalCtx.GetLocation = p.session.getLocation
		return p
	},
}

// An Executor executes SQL statements.
type Executor struct {
	db       client.DB
	nodeID   roachpb.NodeID
	reCache  *parser.RegexpCache
	leaseMgr *LeaseManager

	// System Config and mutex.
	systemConfig     config.SystemConfig
	systemConfigMu   sync.RWMutex
	systemConfigCond *sync.Cond
}

// newExecutor creates an Executor and registers a callback on the
// system config.
func newExecutor(db client.DB, gossip *gossip.Gossip, leaseMgr *LeaseManager) *Executor {
	exec := &Executor{
		db:       db,
		reCache:  parser.NewRegexpCache(512),
		leaseMgr: leaseMgr,
	}
	exec.systemConfigCond = sync.NewCond(&exec.systemConfigMu)
	gossip.RegisterSystemConfigCallback(exec.updateSystemConfig)
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

// Execute the statement(s) in the given request and return a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) Execute(args driver.Request) (driver.Response, int, error) {
	planMaker := plannerPool.Get().(*planner)
	defer plannerPool.Put(planMaker)

	*planMaker = planner{
		user: args.GetUser(),
		evalCtx: parser.EvalContext{
			NodeID:  e.nodeID,
			ReCache: e.reCache,
			// Copy existing GetLocation closure. See plannerPool.New() for the
			// initial setting.
			GetLocation: planMaker.evalCtx.GetLocation,
		},
		leaseMgr:     e.leaseMgr,
		systemConfig: e.getSystemConfig(),
	}

	// Pick up current session state.
	if err := proto.Unmarshal(args.Session, &planMaker.session); err != nil {
		return args.CreateReply(), http.StatusBadRequest, err
	}
	// Resume a pending transaction if present.
	if planMaker.session.Txn != nil {
		txn := client.NewTxn(e.db)
		txn.Proto = planMaker.session.Txn.Txn
		if planMaker.session.MutatesSystemDB {
			txn.SetSystemDBTrigger()
		}
		planMaker.setTxn(txn, planMaker.session.Txn.Timestamp.GoTime())
	}

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	planMaker.params = parameters(args.Params)
	reply := e.execStmts(args.Sql, planMaker)

	// Send back the session state even if there were application-level errors.
	// Add transaction to session state.
	if planMaker.txn != nil {
		// TODO(pmattis): Need to record the leases used by a transaction within
		// the transaction state and restore it when the transaction is restored.
		planMaker.releaseLeases(e.db)
		planMaker.session.Txn = &Session_Transaction{Txn: planMaker.txn.Proto, Timestamp: driver.Timestamp(planMaker.evalCtx.TxnTimestamp.Time)}
		planMaker.session.MutatesSystemDB = planMaker.txn.SystemDBTrigger()
	} else {
		planMaker.session.Txn = nil
		planMaker.session.MutatesSystemDB = false
	}
	bytes, err := proto.Marshal(&planMaker.session)
	if err != nil {
		return args.CreateReply(), http.StatusInternalServerError, err
	}
	reply.Session = bytes

	return reply, 0, nil
}

// exec executes the request. Any error encountered is returned; it is
// the caller's responsibility to update the response.
func (e *Executor) execStmts(sql string, planMaker *planner) driver.Response {
	var resp driver.Response
	stmts, err := planMaker.parser.Parse(sql, parser.Syntax(planMaker.session.Syntax))
	if err != nil {
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		resp.Results = append(resp.Results, makeResultFromError(planMaker, err))
		return resp
	}
	for _, stmt := range stmts {
		result, err := e.execStmt(stmt, planMaker)
		if err != nil {
			result = makeResultFromError(planMaker, err)
		}
		resp.Results = append(resp.Results, result)
		// Release the leases once a transaction is complete.
		if planMaker.txn == nil {
			planMaker.releaseLeases(e.db)

			// The previous transaction finished executing some schema changes. Wait for
			// the schema changes to propagate to all nodes, so that once the executor
			// returns the new schema are live everywhere. This is not needed for
			// correctness but is done to make the UI experience/tests predictable.
			if err := e.waitForCompletedSchemaChangesToPropagate(planMaker); err != nil {
				log.Warning(err)
			}
		}
	}
	return resp
}

func (e *Executor) execStmt(stmt parser.Statement, planMaker *planner) (driver.Response_Result, error) {
	var result driver.Response_Result
	switch stmt.(type) {
	case *parser.BeginTransaction:
		if planMaker.txn != nil {
			return result, errTransactionInProgress
		}
		// Start a transaction here and not in planMaker to prevent begin
		// transaction from being called within an auto-transaction below.
		planMaker.setTxn(client.NewTxn(e.db), time.Now())
		planMaker.txn.SetDebugName("sql", 0)
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		if planMaker.txn == nil {
			return result, errNoTransactionInProgress
		} else if planMaker.txn.Proto.Status == roachpb.ABORTED {
			// Reset to allow starting a new transaction.
			planMaker.resetTxn()
			return result, nil
		}
	case *parser.SetTransaction:
		if planMaker.txn == nil {
			return result, errNoTransactionInProgress
		}
	default:
		if planMaker.txn != nil && planMaker.txn.Proto.Status == roachpb.ABORTED {
			return result, errTransactionAborted
		}
	}

	// Bind all the placeholder variables in the stmt to actual values.
	if err := parser.FillArgs(stmt, &planMaker.params); err != nil {
		return result, err
	}

	// Create a function which both makes and executes the plan, populating
	// result.
	//
	// TODO(pmattis): Should this be a separate function? Perhaps we should move
	// some of the common code back out into execStmts and have execStmt contain
	// only the body of this closure.
	f := func(timestamp time.Time) error {
		planMaker.evalCtx.StmtTimestamp = parser.DTimestamp{Time: timestamp}
		plan, err := planMaker.makePlan(stmt)
		if err != nil {
			return err
		}

		switch stmt.StatementType() {
		case parser.DDL:
			result.Union = &driver.Response_Result_DDL_{DDL: &driver.Response_Result_DDL{}}
		case parser.RowsAffected:
			resultRowsAffected := driver.Response_Result_RowsAffected{}
			result.Union = &resultRowsAffected
			for plan.Next() {
				resultRowsAffected.RowsAffected++
			}

		case parser.Rows:
			var resultRows driver.Response_Result_Rows
			for _, column := range plan.Columns() {
				datum, err := makeDriverDatum(column.typ)
				if err != nil {
					return err
				}

				resultRows.Columns = append(resultRows.Columns, &driver.Response_Result_Rows_Column{
					Name: column.name,
					Typ:  datum,
				})
			}

			result.Union = &driver.Response_Result_Rows_{
				Rows: &resultRows,
			}
			for plan.Next() {
				values := plan.Values()
				row := driver.Response_Result_Rows_Row{Values: make([]driver.Datum, 0, len(values))}
				for _, val := range values {
					datum, err := makeDriverDatum(val)
					if err != nil {
						return err
					}
					row.Values = append(row.Values, datum)
				}
				resultRows.Rows = append(resultRows.Rows, row)
			}
		}

		return plan.Err()
	}

	// If there is a pending transaction.
	if planMaker.txn != nil {
		err := f(time.Now())
		return result, err
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
	err := e.db.Txn(func(txn *client.Txn) error {
		timestamp := time.Now()
		planMaker.setTxn(txn, timestamp)
		err := f(timestamp)
		planMaker.resetTxn()
		return err
	})

	if testingWaitForMetadata && err == nil {
		if verify := planMaker.testingVerifyMetadata; verify != nil {
			// In the case of a multi-statement request, avoid reusing this
			// callback.
			planMaker.testingVerifyMetadata = nil
			for i := 0; ; i++ {
				if verify(e.systemConfig) != nil {
					e.systemConfigCond.Wait()
				} else {
					if i == 0 {
						err = util.Errorf("expected %q to require a gossip update, but it did not", stmt)
					} else if i > 1 {
						log.Infof("%q unexpectedly required %d gossip updates", stmt, i)
					}
					break
				}
			}
		}
	}

	return result, err
}

func (e *Executor) waitForCompletedSchemaChangesToPropagate(planMaker *planner) error {
	for _, id := range planMaker.completedSchemaChange {
		retryOpts := retry.Options{
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     200 * time.Millisecond,
			Multiplier:     2,
		}
		// Wait until there are no unexpired leases on the previous version
		// of the table.
		if _, err := e.leaseMgr.waitForOneVersion(id, retryOpts); err != nil {
			return err
		}
	}
	planMaker.completedSchemaChange = nil
	return nil
}

// If we hit an error and there is a pending transaction, rollback
// the transaction before returning. The client does not have to
// deal with cleaning up transaction state.
func makeResultFromError(planMaker *planner, err error) driver.Response_Result {
	if planMaker.txn != nil {
		if err != errTransactionAborted {
			planMaker.txn.Cleanup(err)
		}
	}
	errString := err.Error()
	return driver.Response_Result{Error: &errString}
}

var _ parser.Args = parameters{}

type parameters []driver.Datum

// Arg implements the parser.Args interface.
func (p parameters) Arg(name string) (parser.Datum, bool) {
	if len(name) == 0 {
		// This shouldn't happen unless the parser let through an invalid parameter
		// specification.
		panic(fmt.Sprintf("invalid empty parameter name"))
	}
	if ch := name[0]; ch < '0' || ch > '9' {
		// TODO(pmattis): Add support for named parameters (vs the numbered
		// parameter support below).
		return nil, false
	}
	i, err := strconv.ParseInt(name, 10, 0)
	if err != nil {
		return nil, false
	}
	if i < 1 || int(i) > len(p) {
		return nil, false
	}
	arg := p[i-1].Payload
	if arg == nil {
		return parser.DNull, true
	}
	switch t := arg.(type) {
	case *driver.Datum_BoolVal:
		return parser.DBool(t.BoolVal), true
	case *driver.Datum_IntVal:
		return parser.DInt(t.IntVal), true
	case *driver.Datum_FloatVal:
		return parser.DFloat(t.FloatVal), true
	case *driver.Datum_BytesVal:
		return parser.DBytes(t.BytesVal), true
	case *driver.Datum_StringVal:
		return parser.DString(t.StringVal), true
	case *driver.Datum_DateVal:
		return parser.DDate(t.DateVal), true
	case *driver.Datum_TimeVal:
		return parser.DTimestamp{Time: t.TimeVal.GoTime()}, true
	case *driver.Datum_IntervalVal:
		return parser.DInterval{Duration: time.Duration(t.IntervalVal)}, true
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}

func makeDriverDatum(datum parser.Datum) (driver.Datum, error) {
	if datum == parser.DNull {
		return driver.Datum{}, nil
	}

	switch vt := datum.(type) {
	case parser.DBool:
		return driver.Datum{
			Payload: &driver.Datum_BoolVal{BoolVal: bool(vt)},
		}, nil
	case parser.DInt:
		return driver.Datum{
			Payload: &driver.Datum_IntVal{IntVal: int64(vt)},
		}, nil
	case parser.DFloat:
		return driver.Datum{
			Payload: &driver.Datum_FloatVal{FloatVal: float64(vt)},
		}, nil
	case parser.DBytes:
		return driver.Datum{
			Payload: &driver.Datum_BytesVal{BytesVal: []byte(vt)},
		}, nil
	case parser.DString:
		return driver.Datum{
			Payload: &driver.Datum_StringVal{StringVal: string(vt)},
		}, nil
	case parser.DDate:
		return driver.Datum{
			Payload: &driver.Datum_DateVal{DateVal: int64(vt)},
		}, nil
	case parser.DTimestamp:
		wireTimestamp := driver.Timestamp(vt.Time)
		return driver.Datum{
			Payload: &driver.Datum_TimeVal{
				TimeVal: &wireTimestamp,
			},
		}, nil
	case parser.DInterval:
		return driver.Datum{
			Payload: &driver.Datum_IntervalVal{IntervalVal: vt.Nanoseconds()},
		}, nil
	default:
		return driver.Datum{}, fmt.Errorf("unsupported result type: %s", datum.Type())
	}
}
