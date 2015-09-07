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
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

var errTransactionAborted = errors.New("current transaction is aborted, commands ignored until end of transaction block")

type server struct {
	db client.DB
}

func (s server) execute(args driver.Request) (driver.Response, int, error) {
	// Pick up current session state.
	planMaker := planner{user: args.GetUser()}
	if err := gogoproto.Unmarshal(args.Session, &planMaker.session); err != nil {
		return args.CreateReply(), http.StatusBadRequest, err
	}
	// Open a pending transaction if needed.
	if planMaker.session.Txn != nil {
		txn := client.NewTxn(s.db)
		txn.Proto = *planMaker.session.Txn
		if planMaker.session.MutatesSystemDB {
			txn.SetSystemDBTrigger()
		}
		planMaker.txn = txn
	}

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	reply := s.execStmts(args.Sql, parameters(args.Params), &planMaker)

	// Send back the session state even if there were application-level errors.
	// Add transaction to session state.
	if planMaker.txn != nil {
		planMaker.session.Txn = &planMaker.txn.Proto
		planMaker.session.MutatesSystemDB = planMaker.txn.SystemDBTrigger()
	} else {
		planMaker.session.Txn = nil
		planMaker.session.MutatesSystemDB = false
	}
	bytes, err := gogoproto.Marshal(&planMaker.session)
	if err != nil {
		return args.CreateReply(), http.StatusInternalServerError, err
	}
	reply.Session = bytes

	return reply, 0, nil
}

// exec executes the request. Any error encountered is returned; it is
// the caller's responsibility to update the response.
func (s server) execStmts(sql string, params parameters, planMaker *planner) driver.Response {
	var resp driver.Response
	stmts, err := parser.Parse(sql, parser.Syntax(planMaker.session.Syntax))
	if err != nil {
		// A parse error occured: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		resp.Results = append(resp.Results, rollbackTxnAndReturnResultWithError(planMaker, err))
		return resp
	}
	for _, stmt := range stmts {
		result, err := s.execStmt(stmt, params, planMaker)
		if err != nil {
			result = rollbackTxnAndReturnResultWithError(planMaker, err)
		}
		resp.Results = append(resp.Results, result)
	}
	return resp
}

func (s server) execStmt(stmt parser.Statement, params parameters, planMaker *planner) (driver.Result, error) {
	var result driver.Result
	if planMaker.txn == nil {
		if _, ok := stmt.(*parser.BeginTransaction); ok {
			// Start a transaction here and not in planMaker to prevent begin
			// transaction from being called within an auto-transaction below.
			planMaker.txn = client.NewTxn(s.db)
			planMaker.txn.SetDebugName("sql", 0)
		}
	} else if planMaker.txn.Proto.Status == proto.ABORTED {
		switch stmt.(type) {
		case *parser.CommitTransaction, *parser.RollbackTransaction:
			// Reset to allow starting a new transaction.
			planMaker.txn = nil
			return result, nil
		default:
			return result, errTransactionAborted
		}
	}
	// Bind all the placeholder variables in the stmt to actual values.
	if err := parser.FillArgs(stmt, params); err != nil {
		return result, err
	}
	var plan planNode
	// If there is a pending transaction.
	if planMaker.txn != nil {
		// Run in transaction planMaker.txn
		var err error
		if plan, err = planMaker.makePlan(stmt); err != nil {
			return result, err
		}
	} else {
		// No transaction. Run the command as a retryable block in an
		// auto-transaction.
		if err := s.db.Txn(func(txn *client.Txn) error {
			planMaker.txn = txn
			var err error
			plan, err = planMaker.makePlan(stmt)
			planMaker.txn = nil
			return err
		}); err != nil {
			return result, err
		}
	}
	result.Columns = plan.Columns()
	for plan.Next() {
		values := plan.Values()
		row := driver.Result_Row{Values: make([]driver.Datum, 0, len(values))}
		for _, val := range values {
			if val == parser.DNull {
				row.Values = append(row.Values, driver.Datum{})
			} else {
				switch vt := val.(type) {
				case parser.DBool:
					row.Values = append(row.Values, driver.Datum{BoolVal: (*bool)(&vt)})
				case parser.DInt:
					row.Values = append(row.Values, driver.Datum{IntVal: (*int64)(&vt)})
				case parser.DFloat:
					row.Values = append(row.Values, driver.Datum{FloatVal: (*float64)(&vt)})
				case parser.DString:
					row.Values = append(row.Values, driver.Datum{StringVal: (*string)(&vt)})
				case parser.DDate:
					row.Values = append(row.Values, driver.Datum{TimeVal: &driver.Datum_Timestamp{
						Sec:  vt.Unix(),
						Nsec: uint32(vt.Nanosecond()),
					}})
				case parser.DTimestamp:
					row.Values = append(row.Values, driver.Datum{TimeVal: &driver.Datum_Timestamp{
						Sec:  vt.Unix(),
						Nsec: uint32(vt.Nanosecond()),
					}})
				case parser.DInterval:
					s := vt.String()
					row.Values = append(row.Values, driver.Datum{StringVal: &s})
				default:
					return result, util.Errorf("unsupported datum: %T", val)
				}
			}
		}
		result.Rows = append(result.Rows, row)
	}
	if err := plan.Err(); err != nil {
		return result, err
	}
	return result, nil
}

// If we hit an error and there is a pending transaction, rollback
// the transaction before returning. The client does not have to
// deal with cleaning up transaction state.
func rollbackTxnAndReturnResultWithError(planMaker *planner, err error) driver.Result {
	if planMaker.txn != nil {
		planMaker.txn.Cleanup(err)
	}
	var errProto proto.Error
	errProto.SetResponseGoError(err)
	return driver.Result{Error: &errProto}
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
	arg := p[i-1].GetValue()
	if arg == nil {
		return parser.DNull, true
	}
	switch t := arg.(type) {
	case *bool:
		return parser.DBool(*t), true
	case *int64:
		return parser.DInt(*t), true
	case *float64:
		return parser.DFloat(*t), true
	case []byte:
		return parser.DString(t), true
	case *string:
		return parser.DString(*t), true
	case *driver.Datum_Timestamp:
		return parser.DTimestamp{Time: time.Unix((*t).Sec, int64((*t).Nsec)).UTC()}, true
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}
