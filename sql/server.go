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
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"

	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	allowedEncodings      = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}
	errNoDatabase         = errors.New("no database specified")
	errNoTable            = errors.New("no table specified")
	errEmptyDatabaseName  = errors.New("empty database name")
	errEmptyTableName     = errors.New("empty table name")
	errEmptyIndexName     = errors.New("empty index name")
	errEmptyColumnName    = errors.New("empty column name")
	errTransactionAborted = errors.New("current transaction is aborted, commands ignored until end of transaction block")
)

// A Server provides an HTTP server endpoint serving the SQL API.
// It accepts either JSON or serialized protobuf content types.
type Server struct {
	context *base.Context
	db      *client.DB
}

// NewServer allocates and returns a new Server.
func NewServer(ctx *base.Context, db *client.DB) *Server {
	return &Server{context: ctx, db: db}
}

// ServeHTTP serves the SQL API by treating the request URL path
// as the method, the request body as the arguments, and sets the
// response body as the method reply. The request body is unmarshalled
// into arguments based on the Content-Type request header. Protobuf
// and JSON-encoded requests are supported. The response body is
// encoded according to the request's Accept header, or if not
// present, in the same format as the request's incoming Content-Type
// header.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	method := r.URL.Path
	if !strings.HasPrefix(method, driver.Endpoint) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Check TLS settings.
	authenticationHook, err := security.AuthenticationHook(s.context.Insecure, r.TLS)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	method = strings.TrimPrefix(method, driver.Endpoint)
	if method != driver.Execute.String() {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Unmarshal the request.
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var args driver.Request
	if err := util.UnmarshalRequest(r, reqBody, &args, allowedEncodings); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Check request user against client certificate user.
	if err := authenticationHook(&args, true /*public*/); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Pick up current session state.
	planMaker := planner{user: args.GetUser()}
	if err := gogoproto.Unmarshal(args.Session, &planMaker.session); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Open a pending transaction if needed.
	if planMaker.session.Txn != nil {
		planMaker.txn = client.NewTxnFromProto(*s.db, *planMaker.session.Txn)
	}

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	reply := s.exec(args, &planMaker)

	// Send back the session state even if there were application-level errors.
	// Add transaction to session state.
	if planMaker.txn != nil {
		t := planMaker.txn.ToProto()
		planMaker.session.Txn = &t
	} else {
		planMaker.session.Txn = nil
	}
	bytes, err := gogoproto.Marshal(&planMaker.session)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reply.Session = bytes

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, &reply, allowedEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(body)
}

type parameters []driver.Datum

// Arg implements the Args interface
func (p parameters) Arg(i int) (parser.Datum, bool) {
	if i < 1 || i > len(p) {
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
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}

// If we hit an error and there is a pending transaction, rollback
// the transaction before returning. The client does not have to
// deal with cleaning up transaction state.
func rollbackTxnAndReturnResultWithError(planMaker *planner, err error) driver.Result {
	if planMaker.txn != nil {
		// What do we do with a rollback error? This is an internally
		// initiated rollback that the client is unaware of. Reporting it
		// will only cause confusion. Not reporting it could leave a transaction
		// pending, but that will get GCed eventually.
		_ = planMaker.txn.Rollback()
	}
	var errProto proto.Error
	errProto.SetResponseGoError(err)
	return driver.Result{Error: &errProto}
}

// exec executes the request. Any error encountered is returned; it is
// the caller's responsibility to update the response.
func (s *Server) exec(req driver.Request, planMaker *planner) driver.Response {
	var resp driver.Response
	stmts, err := parser.Parse(req.Sql, parser.Syntax(planMaker.session.Syntax))
	if err != nil {
		// A parse error occured: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		resp.Results = append(resp.Results, rollbackTxnAndReturnResultWithError(planMaker, err))
		return resp
	}
	for _, stmt := range stmts {
		result, err := s.execStmt(stmt, req, planMaker)
		if err != nil {
			result = rollbackTxnAndReturnResultWithError(planMaker, err)
		}
		resp.Results = append(resp.Results, result)
	}
	return resp
}

func (s *Server) execStmt(stmt parser.Statement, req driver.Request, planMaker *planner) (driver.Result, error) {
	var result driver.Result
	if planMaker.txn == nil {
		if _, ok := stmt.(*parser.BeginTransaction); ok {
			// Start a transaction here and not in planMaker to prevent begin
			// transaction from being called within an auto-transaction below.
			planMaker.txn = client.NewTxn(*s.db)
		}
	} else if planMaker.txn.ToProto().Status == proto.ABORTED {
		switch stmt := stmt.(type) {
		case *parser.CommitTransaction, *parser.RollbackTransaction:
			// Reset to allow starting a new transaction.
			planMaker.txn = nil
			return result, nil
		default:
			// Just have to do something with stmt to keep the compiler happy.
			_ = stmt
			return result, errTransactionAborted
		}
	}
	// Bind all the placeholder variables in the stmt to actual values.
	if err := parser.FillArgs(stmt, parameters(req.Params)); err != nil {
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
