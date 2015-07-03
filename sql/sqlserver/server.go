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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
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
	// TODO(vivek): Move database to the session/transaction context.
	database string
	db       *client.DB
}

// NewServer allocates and returns a new Server.
func NewServer(db *client.DB) *Server {
	return &Server{db: db}
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
	s.exec(sqlwire.Call{Args: args, Reply: reply})

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
func (s *Server) exec(call sqlwire.Call) {
	req := call.Args
	resp := call.Reply
	stmt, err := parser.Parse(req.Sql)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	switch p := stmt.(type) {
	case *parser.CreateDatabase:
		s.CreateDatabase(p, req.Params, resp)
	case *parser.CreateTable:
		s.CreateTable(p, req.Params, resp)
	case *parser.Delete:
		s.Delete(p, req.Params, resp)
	case *parser.Insert:
		s.Insert(p, req.Params, resp)
	case *parser.Select:
		s.Select(p, req.Params, resp)
	case *parser.ShowColumns:
		s.ShowColumns(p, req.Params, resp)
	case *parser.ShowDatabases:
		s.ShowDatabases(p, req.Params, resp)
	case *parser.ShowIndex:
		s.ShowIndex(p, req.Params, resp)
	case *parser.ShowTables:
		s.ShowTables(p, req.Params, resp)
	case *parser.Update:
		s.Update(p, req.Params, resp)
	case *parser.Use:
		s.Use(p, req.Params, resp)
	default:
		resp.SetGoError(fmt.Errorf("unknown statement type: %T", stmt))
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
	sr, err := s.db.Scan(prefix, prefix.PrefixEnd(), 0)
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
	sr, err := s.db.Scan(prefix, prefix.PrefixEnd(), 0)
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

// CreateDatabase creates a database if it doesn't exist.
func (s *Server) CreateDatabase(p *parser.CreateDatabase, args []sqlwire.Datum, resp *sqlwire.Response) {
	if p.Name == "" {
		resp.SetGoError(errors.New("empty database name"))
		return
	}

	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, strings.ToLower(p.Name))
	if gr, err := s.db.Get(nameKey); err != nil {
		resp.SetGoError(err)
		return
	} else if gr.Exists() {
		if p.IfNotExists {
			return
		}
		resp.SetGoError(fmt.Errorf("database \"%s\" already exists", p.Name))
		return
	}
	ir, err := s.db.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	nsID := uint32(ir.ValueInt() - 1)
	if err := s.db.CPut(nameKey, nsID, nil); err != nil {
		// TODO(pmattis): Need to handle if-not-exists here as well.
		resp.SetGoError(err)
		return
	}
}

// CreateTable creates a table if it doesn't already exist.
func (s *Server) CreateTable(p *parser.CreateTable, args []sqlwire.Datum, resp *sqlwire.Response) {
	if err := s.normalizeTableName(p.Table); err != nil {
		resp.SetGoError(err)
		return
	}

	dbID, err := s.lookupDatabase(p.Table.Qualifier)
	if err != nil {
		resp.SetGoError(err)
		return
	}

	desc, err := makeTableDesc(p)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	if err := desc.AllocateIDs(); err != nil {
		resp.SetGoError(err)
		return
	}

	nameKey := keys.MakeNameMetadataKey(dbID, p.Table.Name)

	// This isn't strictly necessary as the conditional put below will fail if
	// the key already exists, but it seems good to avoid the table ID allocation
	// in most cases when the table already exists.
	if gr, err := s.db.Get(nameKey); err != nil {
		resp.SetGoError(err)
		return
	} else if gr.Exists() {
		if p.IfNotExists {
			return
		}
		resp.SetGoError(fmt.Errorf("table \"%s\" already exists", p.Table))
		return
	}

	ir, err := s.db.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		resp.SetGoError(err)
		return
	}
	desc.ID = uint32(ir.ValueInt() - 1)

	// TODO(pmattis): Be cognizant of error messages when this is ported to the
	// server. The error currently returned below is likely going to be difficult
	// to interpret.
	err = s.db.Txn(func(txn *client.Txn) error {
		descKey := keys.MakeDescMetadataKey(desc.ID)
		b := &client.Batch{}
		b.CPut(nameKey, descKey, nil)
		b.Put(descKey, &desc)
		return txn.Commit(b)
	})
	if err != nil {
		// TODO(pmattis): Need to handle if-not-exists here as well.
		resp.SetGoError(err)
		return
	}
}

// Delete is unimplemented.
func (s *Server) Delete(p *parser.Delete, args []sqlwire.Datum, resp *sqlwire.Response) {
	resp.SetGoError(fmt.Errorf("TODO(pmattis): unimplemented: %T %s", p, p))
}

// Insert inserts rows into the database.
func (s *Server) Insert(p *parser.Insert, args []sqlwire.Datum, resp *sqlwire.Response) {

	desc, err := s.getTableDesc(p.Table)
	if err != nil {
		resp.SetGoError(err)
		return
	}

	// Determine which columns we're inserting into.
	cols, err := s.processColumns(desc, p.Columns)
	if err != nil {
		resp.SetGoError(err)
		return
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
			resp.SetGoError(fmt.Errorf("missing \"%s\" primary key column",
				desc.Indexes[0].ColumnNames[i]))
			return
		}
	}

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	r, err := s.processInsertRows(p.Rows)
	if err != nil {
		resp.SetGoError(err)
		return
	}

	b := &client.Batch{}
	for _, row := range r {
		if len(row.Values) != len(cols) {
			resp.SetGoError(fmt.Errorf("invalid values for columns: %d != %d", len(row.Values), len(cols)))
			return
		}
		indexKey := encodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID)
		primaryKey, err := encodeIndexKey(desc.Indexes[0], colMap, cols, row.Values, indexKey)
		if err != nil {
			resp.SetGoError(err)
			return
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
			} else if val.UintVal != nil {
				b.Put(key, *val.UintVal)
			} else if val.FloatVal != nil {
				b.Put(key, *val.FloatVal)
			} else if val.BytesVal != nil {
				b.Put(key, val.BytesVal)
			} else if val.StringVal != nil {
				b.Put(key, *val.StringVal)
			}
		}
	}
	if err := s.db.Run(b); err != nil {
		resp.SetGoError(err)
		return
	}
}

// Select selects rows from a single table.
func (s *Server) Select(p *parser.Select, args []sqlwire.Datum, resp *sqlwire.Response) {

	if len(p.Exprs) != 1 {
		resp.SetGoError(fmt.Errorf("TODO(pmattis): unsupported select exprs: %s", p.Exprs))
		return
	}
	if _, ok := p.Exprs[0].(*parser.StarExpr); !ok {
		resp.SetGoError(fmt.Errorf("TODO(pmattis): unsupported select expr: %s", p.Exprs))
		return
	}

	if len(p.From) != 1 {
		resp.SetGoError(fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From))
		return
	}
	var desc *structured.TableDescriptor
	{
		ate, ok := p.From[0].(*parser.AliasedTableExpr)
		if !ok {
			resp.SetGoError(fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From))
			return
		}
		table, ok := ate.Expr.(*parser.TableName)
		if !ok {
			resp.SetGoError(fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From))
			return
		}
		var err error
		desc, err = s.getTableDesc(table)
		if err != nil {
			resp.SetGoError(err)
			return
		}
	}

	// Retrieve all of the keys that start with our index key prefix.
	startKey := proto.Key(encodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID))
	endKey := startKey.PrefixEnd()
	sr, err := s.db.Scan(startKey, endKey, 0)
	if err != nil {
		resp.SetGoError(err)
		return
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
			resp.SetGoError(err)
			return
		}
		primaryKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		_, colID := encoding.DecodeUvarint(remaining)
		if err != nil {
			resp.SetGoError(err)
			return
		}
		col, err := desc.FindColumnByID(uint32(colID))
		if err != nil {
			resp.SetGoError(err)
			return
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
}

// Update is unimplemented.
func (s *Server) Update(p *parser.Update, args []sqlwire.Datum, resp *sqlwire.Response) {

	resp.SetGoError(fmt.Errorf("TODO(pmattis): unimplemented: %T %s", p, p))
}

func (s *Server) getTableDesc(table *parser.TableName) (*structured.TableDescriptor, error) {
	if err := s.normalizeTableName(table); err != nil {
		return nil, err
	}
	dbID, err := s.lookupDatabase(table.Qualifier)
	if err != nil {
		return nil, err
	}
	gr, err := s.db.Get(keys.MakeNameMetadataKey(dbID, table.Name))
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, fmt.Errorf("table \"%s\" does not exist", table)
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
			case *parser.ColName:
				// TODO(pmattis): If et.Qualifier is not empty, verify it matches the
				// table name.
				var err error
				col, err := desc.FindColumnByName(et.Name)
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

func (s *Server) processInsertRows(node parser.InsertRows) (rows []sqlwire.Result_Row, err error) {
	switch nt := node.(type) {
	case parser.Values:
		for _, row := range nt {
			switch rt := row.(type) {
			case parser.ValTuple:
				var vals []sqlwire.Datum
				for _, val := range rt {
					switch vt := val.(type) {
					case parser.StrVal:
						tmp := string(vt)
						vals = append(vals, sqlwire.Datum{StringVal: &tmp})
					case parser.NumVal:
						tmp := string(vt)
						vals = append(vals, sqlwire.Datum{StringVal: &tmp})
					case parser.ValArg:
						return rows, fmt.Errorf("TODO(pmattis): unsupported node: %T", val)
					case parser.BytesVal:
						tmp := string(vt)
						vals = append(vals, sqlwire.Datum{StringVal: &tmp})
					default:
						return rows, fmt.Errorf("TODO(pmattis): unsupported node: %T", val)
					}
				}
				rows = append(rows, sqlwire.Result_Row{Values: vals})
			case *parser.Subquery:
				return rows, fmt.Errorf("TODO(pmattis): unsupported node: %T", row)
			}
		}
		return rows, nil
	case *parser.Select:
		// TODO(vivek): return s.query(nt.stmt, nil)
	case *parser.Union:
		// TODO(vivek): return s.query(nt.stmt, nil)
	}
	return rows, fmt.Errorf("TODO(pmattis): unsupported node: %T", node)
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
	} else if v.UintVal != nil {
		return encoding.EncodeUvarint(b, *v.UintVal), nil
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
		case structured.ColumnType_CHAR, structured.ColumnType_BINARY,
			structured.ColumnType_TEXT, structured.ColumnType_BLOB:
			var r []byte
			key, r = encoding.DecodeBytes(key, nil)
			vals[col.Name] = sqlwire.Datum{BytesVal: r}
		default:
			return nil, fmt.Errorf("TODO(pmattis): decoded index key: %s", col.Type.Kind)
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
	case structured.ColumnType_CHAR, structured.ColumnType_BINARY,
		structured.ColumnType_TEXT, structured.ColumnType_BLOB:
		// TODO(pmattis): The conversion to string isn't strictly necessary, but
		// makes log messages more readable right now.
		tmp := string(kv.ValueBytes())
		d.StringVal = &tmp
	}
	return d
}
