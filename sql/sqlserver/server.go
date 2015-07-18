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

package sqlserver

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"

	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	allowedEncodings = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}
	allMethods       = map[string]sqlwire.Method{
		sqlwire.Execute.String(): sqlwire.Execute,
	}

	errNoDatabase        = errors.New("no database specified")
	errEmptyDatabaseName = errors.New("empty database name")
)

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
	method := r.URL.Path
	if !strings.HasPrefix(method, sqlwire.Endpoint) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Check TLS settings.
	authenticationHook, err := security.AuthenticationHook(s.context.Insecure, r.TLS)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
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

	// Check request user against client certificate user.
	if err := authenticationHook(args); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Send the Request for SQL execution and set the application-level error
	// on the reply.
	if err = s.exec(sqlwire.Call{Args: args, Reply: reply}); err != nil {
		errProto := proto.Error{}
		errProto.SetResponseGoError(err)
		reply.Error = &errProto
	}

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, reply, allowedEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(body)
}

// Send forwards the call for further processing.
func (s *Server) exec(call sqlwire.Call) error {
	req := call.Args
	resp := call.Reply
	// Pick up current session state.
	var session Session
	if req.Session != nil {
		if err := gogoproto.Unmarshal(req.Session, &session); err != nil {
			return err
		}
	}
	stmts, err := parser.Parse(req.Sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		switch p := stmt.(type) {
		case *parser.CreateDatabase:
			err = s.CreateDatabase(&session, p, req.Params, resp)
		case *parser.CreateTable:
			err = s.CreateTable(&session, p, req.Params, resp)
		case *parser.Delete:
			err = s.Delete(&session, p, req.Params, resp)
		case *parser.Insert:
			err = s.Insert(&session, p, req.Params, resp)
		case *parser.Select:
			err = s.Select(&session, p, req.Params, resp)
		case *parser.Set:
			err = s.Set(&session, p, req.Params, resp)
		case *parser.ShowColumns:
			err = s.ShowColumns(&session, p, req.Params, resp)
		case *parser.ShowDatabases:
			err = s.ShowDatabases(&session, p, req.Params, resp)
		case *parser.ShowIndex:
			err = s.ShowIndex(&session, p, req.Params, resp)
		case *parser.ShowTables:
			err = s.ShowTables(&session, p, req.Params, resp)
		case *parser.Update:
			err = s.Update(&session, p, req.Params, resp)
		default:
			err = fmt.Errorf("unknown statement type: %T", stmt)
		}
		if err != nil {
			return err
		}
	}

	// Update session state.
	resp.Session, err = gogoproto.Marshal(&session)
	return err
}

// ShowColumns of a table
func (s *Server) ShowColumns(session *Session, p *parser.ShowColumns, args []sqlwire.Datum, resp *sqlwire.Response) error {
	desc, err := s.getTableDesc(session.Database, p.Table)
	if err != nil {
		return err
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
	return nil
}

// ShowDatabases returns all the databases.
func (s *Server) ShowDatabases(session *Session, p *parser.ShowDatabases, args []sqlwire.Datum, resp *sqlwire.Response) error {
	prefix := keys.MakeNameMetadataKey(structured.RootNamespaceID, "")
	sr, err := s.db.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return err
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
	return nil
}

// ShowIndex returns all the indexes for a table.
func (s *Server) ShowIndex(session *Session, p *parser.ShowIndex, args []sqlwire.Datum, resp *sqlwire.Response) error {
	desc, err := s.getTableDesc(session.Database, p.Table)
	if err != nil {
		return err
	}

	// TODO(pmattis): This output doesn't match up with MySQL. Should it?
	var rows []sqlwire.Result_Row

	name := p.Table.Table()
	for i, index := range desc.Indexes {
		for j, col := range index.ColumnNames {
			seq := int64(j + 1)
			c := col
			rows = append(rows, sqlwire.Result_Row{
				Values: []sqlwire.Datum{
					{StringVal: &name},
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
	return nil
}

// ShowTables returns all the tables.
func (s *Server) ShowTables(session *Session, p *parser.ShowTables, args []sqlwire.Datum, resp *sqlwire.Response) error {
	if p.Name == nil {
		if session.Database == "" {
			return errNoDatabase
		}
		p.Name = append(p.Name, session.Database)
	}
	dbID, err := s.lookupDatabase(p.Name.String())
	if err != nil {
		return err
	}
	prefix := keys.MakeNameMetadataKey(dbID, "")
	sr, err := s.db.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return err
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
	return nil
}

// CreateDatabase creates a database if it doesn't exist.
func (s *Server) CreateDatabase(session *Session, p *parser.CreateDatabase, args []sqlwire.Datum, resp *sqlwire.Response) error {
	if p.Name == "" {
		return errEmptyDatabaseName
	}

	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, strings.ToLower(p.Name))
	if gr, err := s.db.Get(nameKey); err != nil {
		return err
	} else if gr.Exists() {
		if p.IfNotExists {
			return nil
		}
		return fmt.Errorf("database \"%s\" already exists", p.Name)
	}
	ir, err := s.db.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		return err
	}
	nsID := uint32(ir.ValueInt() - 1)
	// TODO(pmattis): Need to handle if-not-exists here as well.
	return s.db.CPut(nameKey, nsID, nil)
}

// CreateTable creates a table if it doesn't already exist.
func (s *Server) CreateTable(session *Session, p *parser.CreateTable, args []sqlwire.Datum, resp *sqlwire.Response) error {
	if table, err := s.normalizeTableName(session.Database, p.Table); err == nil {
		p.Table = table
	} else {
		return err
	}

	dbID, err := s.lookupDatabase(p.Table.Database())
	if err != nil {
		return err
	}

	desc, err := makeTableDesc(p)
	if err != nil {
		return err
	}
	if err := desc.AllocateIDs(); err != nil {
		return err
	}

	nameKey := keys.MakeNameMetadataKey(dbID, p.Table.Table())

	// This isn't strictly necessary as the conditional put below will fail if
	// the key already exists, but it seems good to avoid the table ID allocation
	// in most cases when the table already exists.
	if gr, getErr := s.db.Get(nameKey); getErr != nil {
		return getErr
	} else if gr.Exists() {
		if p.IfNotExists {
			return nil
		}
		return fmt.Errorf("table \"%s\" already exists", p.Table)
	}

	ir, err := s.db.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		return err
	}
	desc.ID = uint32(ir.ValueInt() - 1)

	// TODO(pmattis): Be cognizant of error messages when this is ported to the
	// server. The error currently returned below is likely going to be difficult
	// to interpret.
	// TODO(pmattis): Need to handle if-not-exists here as well.
	return s.db.Txn(func(txn *client.Txn) error {
		descKey := keys.MakeDescMetadataKey(desc.ID)
		b := &client.Batch{}
		b.CPut(nameKey, descKey, nil)
		b.Put(descKey, &desc)
		return txn.Commit(b)
	})
}

// Delete is unimplemented.
func (s *Server) Delete(session *Session, p *parser.Delete, args []sqlwire.Datum, resp *sqlwire.Response) error {
	return fmt.Errorf("TODO(pmattis): unimplemented: %T %s", p, p)
}

// Insert inserts rows into the database.
func (s *Server) Insert(session *Session, p *parser.Insert, args []sqlwire.Datum, resp *sqlwire.Response) error {
	desc, err := s.getTableDesc(session.Database, p.Table)
	if err != nil {
		return err
	}

	// Determine which columns we're inserting into.
	cols, err := s.processColumns(desc, p.Columns)
	if err != nil {
		return err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colMap := map[uint32]int{}
	for i, c := range cols {
		colMap[c.ID] = i
	}

	// Verify we have at least the columns that are part of the primary key.
	for i, id := range desc.Indexes[0].ColumnIDs {
		if _, ok := colMap[id]; !ok {
			return fmt.Errorf("missing \"%s\" primary key column", desc.Indexes[0].ColumnNames[i])
		}
	}

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	r, err := s.processInsertRows(p.Rows)
	if err != nil {
		return err
	}

	b := &client.Batch{}
	for _, row := range r {
		if len(row.Values) != len(cols) {
			return fmt.Errorf("invalid values for columns: %d != %d", len(row.Values), len(cols))
		}
		indexKey := encodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID)
		primaryKey, encodeErr := encodeIndexKey(desc.Indexes[0], colMap, cols, row.Values, indexKey)
		if encodeErr != nil {
			return encodeErr
		}
		for i, val := range row.Values {
			key := encodeColumnKey(desc, cols[i], primaryKey)
			if log.V(2) {
				log.Infof("Put %q -> %v", key, val)
			}
			// TODO(pmattis): Need to convert the value type to the column type.
			// TODO(vivek): We need a better way of storing Datum.
			if val.BoolVal != nil {
				b.Put(key, *val.BoolVal)
			} else if val.IntVal != nil {
				b.Put(key, *val.IntVal)
			} else if val.FloatVal != nil {
				b.Put(key, *val.FloatVal)
			} else if val.BytesVal != nil {
				b.Put(key, val.BytesVal)
			} else if val.StringVal != nil {
				b.Put(key, *val.StringVal)
			}
		}
	}
	return s.db.Run(b)
}

// Select selects rows from a single table.
func (s *Server) Select(session *Session, p *parser.Select, args []sqlwire.Datum, resp *sqlwire.Response) error {
	if len(p.Exprs) != 1 {
		return fmt.Errorf("TODO(pmattis): unsupported select exprs: %s", p.Exprs)
	}
	if _, ok := p.Exprs[0].(*parser.StarExpr); !ok {
		return fmt.Errorf("TODO(pmattis): unsupported select expr: %s", p.Exprs)
	}

	if len(p.From) != 1 {
		return fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From)
	}
	var desc *structured.TableDescriptor
	{
		ate, ok := p.From[0].(*parser.AliasedTableExpr)
		if !ok {
			return fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From)
		}
		table, ok := ate.Expr.(parser.QualifiedName)
		if !ok {
			return fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From)
		}
		var err error
		desc, err = s.getTableDesc(session.Database, table)
		if err != nil {
			return err
		}
	}

	// Retrieve all of the keys that start with our index key prefix.
	startKey := proto.Key(encodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID))
	endKey := startKey.PrefixEnd()
	sr, scanErr := s.db.Scan(startKey, endKey, 0)
	if scanErr != nil {
		return scanErr
	}

	// All of the columns for a particular row will be grouped together. We loop
	// over the returned key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column ID to
	// lookup the column and decode the value. All of these values go into a map
	// keyed by column name. When the index key changes we output a row
	// containing the current values.
	//
	// The TODOs here are too numerous to list. This is only performing a full
	// table scan using the primary key.

	var rows []sqlwire.Result_Row
	var primaryKey []byte
	vals := map[string]sqlwire.Datum{}
	for _, kv := range sr {
		if primaryKey != nil && !bytes.HasPrefix(kv.Key, primaryKey) {
			rows = append(rows, outputRow(desc.Columns, vals))
			vals = map[string]sqlwire.Datum{}
		}

		remaining, err := decodeIndexKey(desc, desc.Indexes[0], vals, kv.Key)
		if err != nil {
			return err
		}
		primaryKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		_, colID := encoding.DecodeUvarint(remaining)
		col, err := desc.FindColumnByID(uint32(colID))
		if err != nil {
			return err
		}
		vals[col.Name] = unmarshalValue(*col, kv)

		if log.V(2) {
			log.Infof("Scan %q -> %v", kv.Key, vals[col.Name])
		}
	}

	rows = append(rows, outputRow(desc.Columns, vals))

	resp.Results = []sqlwire.Result{
		{
			Columns: make([]string, len(desc.Columns)),
			Rows:    rows,
		},
	}
	for i, col := range desc.Columns {
		resp.Results[0].Columns[i] = col.Name
	}
	return nil
}

// Set sets session variables.
func (s *Server) Set(session *Session, p *parser.Set, args []sqlwire.Datum,
	resp *sqlwire.Response) error {
	// By using QualifiedName.String() here any variables that are keywords will
	// be double quoted.
	name := strings.ToLower(p.Name.String())
	switch name {
	case `"database"`: // Quoted: database is a reserved word
		if len(p.Values) != 1 {
			return fmt.Errorf("database: requires a single string value")
		}
		val, err := parser.EvalExpr(p.Values[0], nil)
		if err != nil {
			return err
		}
		session.Database = val.String()
	default:
		return util.Errorf("unknown variable: %s", name)
	}
	return nil
}

// Update is unimplemented.
func (s *Server) Update(session *Session, p *parser.Update, args []sqlwire.Datum,
	resp *sqlwire.Response) error {
	return fmt.Errorf("TODO(pmattis): unimplemented: %T %s", p, p)
}

func (s *Server) getTableDesc(database string, qname parser.QualifiedName) (
	*structured.TableDescriptor, error) {
	var err error
	qname, err = s.normalizeTableName(database, qname)
	if err != nil {
		return nil, err
	}
	dbID, err := s.lookupDatabase(qname.Database())
	if err != nil {
		return nil, err
	}
	gr, err := s.db.Get(keys.MakeNameMetadataKey(dbID, qname.Table()))
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, fmt.Errorf("table \"%s\" does not exist", qname)
	}
	descKey := gr.ValueBytes()
	desc := structured.TableDescriptor{}
	if err := s.db.GetProto(descKey, &desc); err != nil {
		return nil, err
	}
	if err := desc.Validate(); err != nil {
		return nil, err
	}
	return &desc, nil
}

func (s *Server) normalizeTableName(database string, qname parser.QualifiedName) (
	parser.QualifiedName, error) {
	if len(qname) == 0 {
		return nil, fmt.Errorf("empty table name: %s", qname)
	}
	if len(qname) == 1 {
		if database == "" {
			return nil, fmt.Errorf("no database specified")
		}
		qname = append(parser.QualifiedName{database}, qname[0])
	}
	return qname, nil
}

func (s *Server) lookupDatabase(name string) (uint32, error) {
	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, name)
	gr, err := s.db.Get(nameKey)
	if err != nil {
		return 0, err
	} else if !gr.Exists() {
		return 0, fmt.Errorf("database \"%s\" does not exist", name)
	}
	return uint32(gr.ValueInt()), nil
}

func (s *Server) processColumns(desc *structured.TableDescriptor,
	node parser.Columns) ([]structured.ColumnDescriptor, error) {
	if node == nil {
		return desc.Columns, nil
	}

	cols := make([]structured.ColumnDescriptor, len(node))
	for i, n := range node {
		switch nt := n.(type) {
		case *parser.StarExpr:
			return s.processColumns(desc, nil)
		case *parser.NonStarExpr:
			switch et := nt.Expr.(type) {
			case parser.QualifiedName:
				// TODO(pmattis): If et.Qualifier is not empty, verify it matches the
				// table name.
				var err error
				col, err := desc.FindColumnByName(et.Table())
				if err != nil {
					return nil, err
				}
				cols[i] = *col
			default:
				return nil, fmt.Errorf("unexpected node: %T", nt.Expr)
			}
		}
	}

	return cols, nil
}

func (s *Server) processInsertRows(node parser.SelectStatement) (rows []sqlwire.Result_Row, err error) {
	switch nt := node.(type) {
	case parser.Values:
		for _, row := range nt {
			var vals []sqlwire.Datum
			for _, val := range row {
				switch vt := val.(type) {
				case parser.StrVal:
					tmp := string(vt)
					vals = append(vals, sqlwire.Datum{StringVal: &tmp})
				case parser.NumVal:
					tmp := string(vt)
					vals = append(vals, sqlwire.Datum{StringVal: &tmp})
				case parser.ValArg:
					return rows, util.Errorf("TODO(pmattis): unsupported node: %T", val)
				case parser.BytesVal:
					tmp := string(vt)
					vals = append(vals, sqlwire.Datum{StringVal: &tmp})
				default:
					return rows, util.Errorf("TODO(pmattis): unsupported node: %T", val)
				}
			}
			rows = append(rows, sqlwire.Result_Row{Values: vals})
		}
		return rows, nil
	case *parser.Select:
		// TODO(vivek): return s.query(nt.stmt, nil)
	case *parser.Union:
		// TODO(vivek): return s.query(nt.stmt, nil)
	}
	return rows, util.Errorf("TODO(pmattis): unsupported node: %T", node)
}

// TODO(pmattis): The key encoding and decoding routines belong in either
// "keys" or "structured". Move them there when this code is moved to the
// server and no longer depends on driver.Value.

func encodeIndexKeyPrefix(tableID, indexID uint32) []byte {
	var key []byte
	key = append(key, keys.TableDataPrefix...)
	key = encoding.EncodeUvarint(key, uint64(tableID))
	key = encoding.EncodeUvarint(key, uint64(indexID))
	return key
}

func encodeIndexKey(index structured.IndexDescriptor,
	colMap map[uint32]int, cols []structured.ColumnDescriptor,
	row []sqlwire.Datum, indexKey []byte) ([]byte, error) {
	var key []byte
	key = append(key, indexKey...)

	for i, id := range index.ColumnIDs {
		j, ok := colMap[id]
		if !ok {
			return nil, fmt.Errorf("missing \"%s\" primary key column",
				index.ColumnNames[i])
		}
		// TOOD(pmattis): Need to convert the row[i] value to the type expected by
		// the column.
		var err error
		key, err = encodeTableKey(key, row[j])
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func encodeColumnKey(desc *structured.TableDescriptor,
	col structured.ColumnDescriptor, primaryKey []byte) []byte {
	var key []byte
	key = append(key, primaryKey...)
	return encoding.EncodeUvarint(key, uint64(col.ID))
}

func encodeTableKey(b []byte, v sqlwire.Datum) ([]byte, error) {
	if v.BoolVal != nil {
		if *v.BoolVal {
			return encoding.EncodeVarint(b, 1), nil
		}
		return encoding.EncodeVarint(b, 0), nil
	} else if v.IntVal != nil {
		return encoding.EncodeVarint(b, *v.IntVal), nil
	} else if v.FloatVal != nil {
		return encoding.EncodeNumericFloat(b, *v.FloatVal), nil
	} else if v.BytesVal != nil {
		return encoding.EncodeBytes(b, v.BytesVal), nil
	} else if v.StringVal != nil {
		return encoding.EncodeBytes(b, []byte(*v.StringVal)), nil
	}
	return nil, fmt.Errorf("unable to encode table key: %T", v)
}

func decodeIndexKey(desc *structured.TableDescriptor,
	index structured.IndexDescriptor, vals map[string]sqlwire.Datum, key []byte) ([]byte, error) {
	if !bytes.HasPrefix(key, keys.TableDataPrefix) {
		return nil, fmt.Errorf("%s: invalid key prefix: %q", desc.Name, key)
	}
	key = bytes.TrimPrefix(key, keys.TableDataPrefix)

	var tableID uint64
	key, tableID = encoding.DecodeUvarint(key)
	if uint32(tableID) != desc.ID {
		return nil, fmt.Errorf("%s: unexpected table ID: %d != %d", desc.Name, desc.ID, tableID)
	}

	var indexID uint64
	key, indexID = encoding.DecodeUvarint(key)
	if uint32(indexID) != index.ID {
		return nil, fmt.Errorf("%s: unexpected index ID: %d != %d", desc.Name, index.ID, indexID)
	}

	for _, id := range index.ColumnIDs {
		col, err := desc.FindColumnByID(id)
		if err != nil {
			return nil, err
		}
		switch col.Type.Kind {
		case structured.ColumnType_BIT, structured.ColumnType_INT:
			var i int64
			key, i = encoding.DecodeVarint(key)
			vals[col.Name] = sqlwire.Datum{IntVal: &i}
		case structured.ColumnType_FLOAT:
			var f float64
			key, f = encoding.DecodeNumericFloat(key)
			vals[col.Name] = sqlwire.Datum{FloatVal: &f}
		case structured.ColumnType_CHAR, structured.ColumnType_TEXT,
			structured.ColumnType_BLOB:
			var r []byte
			key, r = encoding.DecodeBytes(key, nil)
			vals[col.Name] = sqlwire.Datum{BytesVal: r}
		default:
			return nil, util.Errorf("TODO(pmattis): decoded index key: %s", col.Type.Kind)
		}
	}

	return key, nil
}

func outputRow(cols []structured.ColumnDescriptor, vals map[string]sqlwire.Datum) sqlwire.Result_Row {
	row := sqlwire.Result_Row{Values: make([]sqlwire.Datum, len(cols))}
	for i, col := range cols {
		row.Values[i] = vals[col.Name]
	}
	return row
}

func unmarshalValue(col structured.ColumnDescriptor, kv client.KeyValue) sqlwire.Datum {
	var d sqlwire.Datum
	if !kv.Exists() {
		return d
	}
	switch col.Type.Kind {
	case structured.ColumnType_BIT, structured.ColumnType_INT:
		tmp := kv.ValueInt()
		d.IntVal = &tmp
	case structured.ColumnType_FLOAT:
		tmp := math.Float64frombits(uint64(kv.ValueInt()))
		d.FloatVal = &tmp
	case structured.ColumnType_CHAR, structured.ColumnType_TEXT,
		structured.ColumnType_BLOB:
		// TODO(pmattis): The conversion to string isn't strictly necessary, but
		// makes log messages more readable right now.
		tmp := string(kv.ValueBytes())
		d.StringVal = &tmp
	}
	return d
}
