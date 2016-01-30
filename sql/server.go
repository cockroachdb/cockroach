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
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

var allowedEncodings = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}

// An Server provides both an HTTP and RPC server endpoint serving the SQL API.
// The HTTP endpoint accepts either JSON or serialized protobuf content types.
type Server struct {
	context *base.Context
	*Executor
}

// MakeServer creates a Server.
func MakeServer(ctx *base.Context, executor *Executor) Server {
	return Server{context: ctx, Executor: executor}
}

// ServeHTTP serves the SQL API by treating the request URL path
// as the method, the request body as the arguments, and sets the
// response body as the method reply. The request body is unmarshalled
// into arguments based on the Content-Type request header. Protobuf
// and JSON-encoded requests are supported. The response body is
// encoded according to the request's Accept header, or if not
// present, in the same format as the request's incoming Content-Type
// header.
func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	method := r.URL.Path
	if !strings.HasPrefix(method, driver.Endpoint) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Check TLS settings.
	authenticationHook, err := security.ProtoAuthHook(s.context.Insecure, r.TLS)
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
	if err := authenticationHook(&args, true /* public */); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	reply, code, err := s.Execute(requestFromProto(args))
	if err != nil {
		http.Error(w, err.Error(), code)
	}

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, protoFromResponse(reply), allowedEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	if _, err := w.Write(body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// RegisterRPC registers the SQL RPC endpoint.
func (s Server) RegisterRPC(rpcServer *rpc.Server) error {
	return rpcServer.RegisterPublic(driver.RPCMethod, s.executeCmd, &driver.Request{})
}

func (s Server) executeCmd(argsI proto.Message) (proto.Message, error) {
	args := argsI.(*driver.Request)
	reply, _, err := s.Execute(requestFromProto(*args))
	return protoFromResponse(reply), err
}

func requestFromProto(dr driver.Request) Request {
	r := Request{
		User:    dr.User,
		Session: dr.Session,
		SQL:     dr.Sql,
		Params:  make([]parser.Datum, 0, len(dr.Params)),
	}
	for _, d := range dr.Params {
		r.Params = append(r.Params, datumFromProto(d))
	}
	return r
}

func protoFromResponse(r Response) *driver.Response {
	dr := &driver.Response{
		Session: r.Session,
		Results: make([]driver.Response_Result, 0, len(r.Results)),
	}
	for _, rr := range r.Results {
		dr.Results = append(dr.Results, protoFromResult(rr))
	}
	return dr
}

func protoFromResult(r Result) driver.Response_Result {
	drr := driver.Response_Result{}
	if r.PErr != nil {
		drr.Error = proto.String(r.PErr.String())
	}
	switch r.Type {
	case parser.DDL:
		drr.Union = &driver.Response_Result_DDL_{
			DDL: &driver.Response_Result_DDL{},
		}
	case parser.RowsAffected:
		drr.Union = &driver.Response_Result_RowsAffected{
			RowsAffected: uint32(r.RowsAffected),
		}
	case parser.Rows:
		rows := &driver.Response_Result_Rows{
			Columns: make([]*driver.Response_Result_Rows_Column, 0, len(r.Columns)),
			Rows:    make([]driver.Response_Result_Rows_Row, 0, len(r.Rows)),
		}
		for _, col := range r.Columns {
			rows.Columns = append(rows.Columns, protoFromColumn(col))
		}
		for _, row := range r.Rows {
			rows.Rows = append(rows.Rows, protoFromRow(row))
		}
		drr.Union = &driver.Response_Result_Rows_{
			Rows: rows,
		}
	}
	return drr
}

func protoFromColumn(c ResultColumn) *driver.Response_Result_Rows_Column {
	return &driver.Response_Result_Rows_Column{
		Name: c.Name,
		Typ:  protoFromDatum(c.Typ),
	}
}

func protoFromRow(r ResultRow) driver.Response_Result_Rows_Row {
	rr := driver.Response_Result_Rows_Row{
		Values: make([]driver.Datum, 0, len(r.Values)),
	}
	for _, v := range r.Values {
		rr.Values = append(rr.Values, protoFromDatum(v))
	}
	return rr
}

func datumFromProto(d driver.Datum) parser.Datum {
	arg := d.Payload
	if arg == nil {
		return parser.DNull
	}
	switch t := arg.(type) {
	case *driver.Datum_BoolVal:
		return parser.DBool(t.BoolVal)
	case *driver.Datum_IntVal:
		return parser.DInt(t.IntVal)
	case *driver.Datum_FloatVal:
		return parser.DFloat(t.FloatVal)
	case *driver.Datum_DecimalVal:
		dec := new(inf.Dec)
		if _, ok := dec.SetString(t.DecimalVal); !ok {
			panic(fmt.Sprintf("could not parse string %q as decimal", t.DecimalVal))
		}
		return parser.DDecimal{Dec: dec}
	case *driver.Datum_BytesVal:
		return parser.DBytes(t.BytesVal)
	case *driver.Datum_StringVal:
		return parser.DString(t.StringVal)
	case *driver.Datum_DateVal:
		return parser.DDate(t.DateVal)
	case *driver.Datum_TimeVal:
		return parser.DTimestamp{Time: t.TimeVal.GoTime()}
	case *driver.Datum_IntervalVal:
		return parser.DInterval{Duration: time.Duration(t.IntervalVal)}
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}

func protoFromDatum(datum parser.Datum) driver.Datum {
	if datum == parser.DNull {
		return driver.Datum{}
	}

	switch vt := datum.(type) {
	case parser.DBool:
		return driver.Datum{
			Payload: &driver.Datum_BoolVal{BoolVal: bool(vt)},
		}
	case parser.DInt:
		return driver.Datum{
			Payload: &driver.Datum_IntVal{IntVal: int64(vt)},
		}
	case parser.DFloat:
		return driver.Datum{
			Payload: &driver.Datum_FloatVal{FloatVal: float64(vt)},
		}
	case parser.DDecimal:
		return driver.Datum{
			Payload: &driver.Datum_DecimalVal{DecimalVal: vt.Dec.String()},
		}
	case parser.DBytes:
		return driver.Datum{
			Payload: &driver.Datum_BytesVal{BytesVal: []byte(vt)},
		}
	case parser.DString:
		return driver.Datum{
			Payload: &driver.Datum_StringVal{StringVal: string(vt)},
		}
	case parser.DDate:
		return driver.Datum{
			Payload: &driver.Datum_DateVal{DateVal: int64(vt)},
		}
	case parser.DTimestamp:
		wireTimestamp := driver.Timestamp(vt.Time)
		return driver.Datum{
			Payload: &driver.Datum_TimeVal{
				TimeVal: &wireTimestamp,
			},
		}
	case parser.DInterval:
		return driver.Datum{
			Payload: &driver.Datum_IntervalVal{IntervalVal: vt.Nanoseconds()},
		}
	default:
		panic(util.Errorf("unsupported result type: %s", datum.Type()))
	}
}
