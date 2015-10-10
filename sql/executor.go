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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/gogo/protobuf/proto"
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errTransactionAborted = errors.New("current transaction is aborted, commands ignored until end of transaction block")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

// An Executor executes SQL statements.
type Executor struct {
	db     client.DB
	nodeID uint32

	// System Config and mutex.
	systemConfig   *config.SystemConfig
	systemConfigMu sync.RWMutex
}

// NewExecutor creates an Executor and registers a callback on the
// system config.
func NewExecutor(db client.DB, gossip *gossip.Gossip) *Executor {
	exec := &Executor{db: db}
	gossip.RegisterSystemConfigCallback(exec.updateSystemConfig)
	return exec
}

// SetNodeID sets the node ID for the SQL server.
func (e *Executor) SetNodeID(nodeID roachpb.NodeID) {
	e.nodeID = uint32(nodeID)
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (e *Executor) updateSystemConfig(cfg *config.SystemConfig) {
	e.systemConfigMu.Lock()
	defer e.systemConfigMu.Unlock()
	e.systemConfig = cfg
}

// getSystemConfig returns a pointer to the latest system config. May be nil,
// if the gossip callback has not run.
func (e *Executor) getSystemConfig() *config.SystemConfig {
	e.systemConfigMu.RLock()
	defer e.systemConfigMu.RUnlock()
	return e.systemConfig
}

// Execute the statement(s) in the given request and return a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) Execute(args driver.Request) (driver.Response, int, error) {
	planMaker := planner{
		user: args.GetUser(),
		evalCtx: parser.EvalContext{
			NodeID: e.nodeID,
		},
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
	planMaker.evalCtx.GetLocation = planMaker.session.getLocation

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	reply := e.execStmts(args.Sql, parameters(args.Params), &planMaker)

	// Send back the session state even if there were application-level errors.
	// Add transaction to session state.
	if planMaker.txn != nil {
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
func (e *Executor) execStmts(sql string, params parameters, planMaker *planner) driver.Response {
	var resp driver.Response
	stmts, err := parser.Parse(sql, parser.Syntax(planMaker.session.Syntax))
	if err != nil {
		// A parse error occured: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		resp.Results = append(resp.Results, makeResultFromError(planMaker, err))
		return resp
	}
	for _, stmt := range stmts {
		result, err := e.execStmt(stmt, params, planMaker)
		if err != nil {
			result = makeResultFromError(planMaker, err)
		}
		resp.Results = append(resp.Results, result)
	}
	return resp
}

func (e *Executor) execStmt(stmt parser.Statement, params parameters, planMaker *planner) (driver.Response_Result, error) {
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
	if err := parser.FillArgs(stmt, params); err != nil {
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
			resultRows := &driver.Response_Result_Rows{
				Columns: plan.Columns(),
			}

			result.Union = &driver.Response_Result_Rows_{
				Rows: resultRows,
			}
			for plan.Next() {
				values := plan.Values()
				row := driver.Response_Result_Rows_Row{Values: make([]driver.Datum, 0, len(values))}
				for _, val := range values {
					if val == parser.DNull {
						row.Values = append(row.Values, driver.Datum{})
						continue
					}

					switch vt := val.(type) {
					case parser.DBool:
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_BoolVal{BoolVal: bool(vt)},
						})
					case parser.DInt:
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_IntVal{IntVal: int64(vt)},
						})
					case parser.DFloat:
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_FloatVal{FloatVal: float64(vt)},
						})
					case parser.DBytes:
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_BytesVal{BytesVal: []byte(vt)},
						})
					case parser.DString:
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_StringVal{StringVal: string(vt)},
						})
					case parser.DDate:
						wireTimestamp := driver.Timestamp(vt.Time)
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_DateVal{
								DateVal: &wireTimestamp,
							},
						})
					case parser.DTimestamp:
						wireTimestamp := driver.Timestamp(vt.Time)
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_TimeVal{
								TimeVal: &wireTimestamp,
							},
						})
					case parser.DInterval:
						row.Values = append(row.Values, driver.Datum{
							Payload: &driver.Datum_IntervalVal{IntervalVal: vt.Nanoseconds()},
						})
					default:
						return fmt.Errorf("unsupported result type: %s", val.Type())
					}
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

	// No transaction. Run the command as a retryable block in an
	// auto-transaction.
	err := e.db.Txn(func(txn *client.Txn) error {
		timestamp := time.Now()
		planMaker.setTxn(txn, timestamp)
		err := f(timestamp)
		planMaker.resetTxn()
		return err
	})
	return result, err
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

// parameters implements the parser.Args interface.
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
		return parser.DDate{Time: t.DateVal.GoTime()}, true
	case *driver.Datum_TimeVal:
		return parser.DTimestamp{Time: t.TimeVal.GoTime()}, true
	case *driver.Datum_IntervalVal:
		return parser.DInterval{Duration: time.Duration(t.IntervalVal)}, true
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}
