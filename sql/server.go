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
	allowedEncodings     = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}
	errNoDatabase        = errors.New("no database specified")
	errNoTable           = errors.New("no table specified")
	errEmptyDatabaseName = errors.New("empty database name")
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
	if err := authenticationHook(&args); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Send the Request for SQL execution and set the application-level error
	// on the reply.
	reply, err := s.exec(args)
	if err != nil {
		errProto := proto.Error{}
		errProto.SetResponseGoError(err)
		reply.Error = &errProto
	}

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
		panic(fmt.Sprintf("Incorrect type %T", t))
	}
}

// exec executes the request. Any error encountered is returned; it is
// the caller's responsibility to update the response.
func (s *Server) exec(req driver.Request) (driver.Response, error) {
	var resp driver.Response

	// Pick up current session state.
	// The request user is validated in ServeHTTP. Even in insecure mode,
	// it is guaranteed not to be empty.
	planner := planner{db: s.db, user: req.GetUser()}
	if req.Session != nil {
		// TODO(tschottdorf) will have to validate the Session information (for
		// instance, whether access to the stored database is permitted).
		if err := gogoproto.Unmarshal(req.Session, &planner.session); err != nil {
			return resp, err
		}
	}
	stmts, err := parser.Parse(req.Sql)
	if err != nil {
		return resp, err
	}
	for _, stmt := range stmts {
		// Bind all the placeholder variables in the stmt to actual values.
		if err := parser.FillArgs(stmt, parameters(req.Params)); err != nil {
			return resp, err
		}
		var plan planNode
		if plan, err = planner.makePlan(stmt); err != nil {
			return resp, err
		}

		result := driver.Result{
			Columns: plan.Columns(),
		}
		for plan.Next() {
			values := plan.Values()
			row := driver.Result_Row{}
			row.Values = make([]driver.Datum, 0, len(values))
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
						return resp, util.Errorf("unsupported datum: %T", val)
					}
				}
			}
			result.Rows = append(result.Rows, row)
		}
		if err := plan.Err(); err != nil {
			return resp, err
		}

		resp.Results = append(resp.Results, result)
	}

	// Update session state.
	resp.Session, err = gogoproto.Marshal(&planner.session)
	return resp, err
}
