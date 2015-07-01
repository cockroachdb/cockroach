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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sqlserver

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
)

var allowedEncodings = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}

var allMethods = map[string]sqlwire.Method{
	sqlwire.Execute.String(): sqlwire.Execute,
}

// createArgsAndReply returns allocated request and response pairs
// according to the specified method. Note that createArgsAndReply
// only knows about public methods and explicitly returns nil for
// internal methods. Do not change this behavior without also fixing
// Server.ServeHTTP.
func createArgsAndReply(method string) (*sqlwire.Request, *sqlwire.Response) {
	if m, ok := allMethods[method]; ok {
		switch m {
		case sqlwire.Execute:
			return &sqlwire.Request{}, &sqlwire.Response{}
		}
	}
	return nil, nil
}

// A Server provides an HTTP server endpoint serving the SQL API.
// It accepts either JSON or serialized protobuf content types.
type Server struct {
	database string
	clientDB *client.DB
}

// NewServer allocates and returns a new Server.
func NewServer(db *client.DB) *Server {
	return &Server{clientDB: db}
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
	method := r.URL.Path
	if !strings.HasPrefix(method, sqlwire.Endpoint) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	method = strings.TrimPrefix(method, sqlwire.Endpoint)
	args, reply := createArgsAndReply(method)
	if args == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Unmarshal the request.
	reqBody, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := util.UnmarshalRequest(r, reqBody, args, allowedEncodings); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Send the Request for SQL execution.
	s.send(sqlwire.Call{Args: args, Reply: reply})

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, reply, allowedEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(body)
}

func echo(sql string, resp *sqlwire.Response) {
	resp.Results = []sqlwire.Result{
		{
			Columns: []string{"echo"},
			Rows: []sqlwire.Result_Row{
				{
					Values: []sqlwire.Datum{
						{
							StringVal: &sql,
						},
					},
				},
			},
		},
	}
}

// Send forwards the call for further processing.
func (s *Server) send(call sqlwire.Call) {
	req := call.Args
	resp := call.Reply
	stmt, err := parser.Parse(req.Sql)
	if err != nil {
		echo(req.Sql, resp)
		return
	}
	switch p := stmt.(type) {
	case *parser.ShowColumns:
		s.ShowColumns(p, req.Params, resp)
	case *parser.ShowDatabases:
		s.ShowDatabases(p, req.Params, resp)
	case *parser.ShowIndex:
		s.ShowIndex(p, req.Params, resp)
	case *parser.ShowTables:
		s.ShowTables(p, req.Params, resp)
	case *parser.Use:
		s.Use(p, req.Params, resp)
	default:
		echo(req.Sql, resp)
	}
}

// ShowColumns of a table
func (s *Server) ShowColumns(p *parser.ShowColumns, args []sqlwire.Datum, resp *sqlwire.Response) {
	desc, err := s.getTableDesc(p.Table)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	var rows []sqlwire.Result_Row
	for i, col := range desc.Columns {
		t := col.Type.SQLString()
		rows = append(rows, sqlwire.Result_Row{
			Values: []sqlwire.Datum{
				{StringVal: &desc.Columns[i].Name},
				{StringVal: &t},
				{BoolVal: &desc.Columns[i].Nullable},
			},
		})
	}
	// TODO(pmattis): This output doesn't match up with MySQL. Should it?
	resp.Results = []sqlwire.Result{
		{
			Columns: []string{"Field", "Type", "Null"},
			Rows:    rows,
		},
	}
}

// ShowDatabases returns all the databases.
func (s *Server) ShowDatabases(p *parser.ShowDatabases, args []sqlwire.Datum, resp *sqlwire.Response) {
	prefix := keys.MakeNameMetadataKey(structured.RootNamespaceID, "")
	sr, err := s.clientDB.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	var rows []sqlwire.Result_Row
	for _, row := range sr {
		name := string(bytes.TrimPrefix(row.Key, prefix))
		rows = append(rows, sqlwire.Result_Row{
			Values: []sqlwire.Datum{
				{StringVal: &name},
			},
		})
	}
	resp.Results = []sqlwire.Result{
		{
			Columns: []string{"Database"},
			Rows:    rows,
		},
	}
}

// ShowIndex returns all the indexes for a table.
func (s *Server) ShowIndex(p *parser.ShowIndex, args []sqlwire.Datum, resp *sqlwire.Response) {
	desc, err := s.getTableDesc(p.Table)
	if err != nil {
		resp.SetGoError(err)
		return
	}

	// TODO(pmattis): This output doesn't match up with MySQL. Should it?
	var rows []sqlwire.Result_Row

	for i, index := range desc.Indexes {
		for j, col := range index.ColumnNames {
			seq := int64(j + 1)
			c := col
			rows = append(rows, sqlwire.Result_Row{
				Values: []sqlwire.Datum{
					{StringVal: &p.Table.Name},
					{StringVal: &desc.Indexes[i].Name},
					{BoolVal: &desc.Indexes[i].Unique},
					{IntVal: &seq},
					{StringVal: &c},
				},
			})
		}
	}
	resp.Results = []sqlwire.Result{
		{
			// TODO(pmattis): This output doesn't match up with MySQL. Should it?
			Columns: []string{"Table", "Name", "Unique", "Seq", "Column"},
			Rows:    rows,
		},
	}
}

// ShowTables returns all the tables.
func (s *Server) ShowTables(p *parser.ShowTables, args []sqlwire.Datum, resp *sqlwire.Response) {
	if p.Name == "" {
		if s.database == "" {
			resp.SetGoError(errors.New("no database specified"))
			return
		}
		p.Name = s.database
	}
	dbID, err := s.lookupDatabase(p.Name)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	prefix := keys.MakeNameMetadataKey(dbID, "")
	sr, err := s.clientDB.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	var rows []sqlwire.Result_Row

	for _, row := range sr {
		name := string(bytes.TrimPrefix(row.Key, prefix))
		rows = append(rows, sqlwire.Result_Row{
			Values: []sqlwire.Datum{
				{StringVal: &name},
			},
		})
	}
	resp.Results = []sqlwire.Result{
		{
			Columns: []string{"tables"},
			Rows:    rows,
		},
	}
}

// Use sets the database being operated on.
func (s *Server) Use(p *parser.Use, args []sqlwire.Datum, resp *sqlwire.Response) {
	s.database = p.Name
}

func (s *Server) getTableDesc(table *parser.TableName) (*structured.TableDescriptor, error) {
	if err := s.normalizeTableName(table); err != nil {
		return nil, err
	}
	dbID, err := s.lookupDatabase(table.Qualifier)
	if err != nil {
		return nil, err
	}
	gr, err := s.clientDB.Get(keys.MakeNameMetadataKey(dbID, table.Name))
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, fmt.Errorf("table \"%s\" does not exist", table)
	}
	descKey := gr.ValueBytes()
	desc := structured.TableDescriptor{}
	if err := s.clientDB.GetProto(descKey, &desc); err != nil {
		return nil, err
	}
	if err := desc.Validate(); err != nil {
		return nil, err
	}
	return &desc, nil
}

func (s *Server) normalizeTableName(table *parser.TableName) error {
	if table.Qualifier == "" {
		if s.database == "" {
			return fmt.Errorf("no database specified")
		}
		table.Qualifier = s.database
	}
	if table.Name == "" {
		return fmt.Errorf("empty table name: %s", table)
	}
	return nil
}

func (s *Server) lookupDatabase(name string) (uint32, error) {
	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, name)
	gr, err := s.clientDB.Get(nameKey)
	if err != nil {
		return 0, err
	} else if !gr.Exists() {
		return 0, fmt.Errorf("database \"%s\" does not exist", name)
	}
	return uint32(gr.ValueInt()), nil
}
