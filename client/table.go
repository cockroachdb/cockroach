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

package client

import (
	"bytes"
	"encoding"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	roachencoding "github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TODO(pmattis):
//
// - This file contains the experimental Cockroach table-based interface. The
//   API will eventually be dispersed into {batch,db,txn}.go, but are collected
//   here during initial development. Much of the implementation will
//   eventually wind up on the server using new table-based requests to perform
//   operations.
//
// - Enhance DelRange to handle model types? Or add a DelStructRange?
//
// - Naming? PutStruct vs StructPut vs TablePut?
//
// - Need appropriate locking for the DB.experimentalModels map.
//
// - Allow usage of `map[string]interface{}` in place of a struct. Probably
//   need table schemas first so we know which columns exist.
//
// - Add support for namespaces. Currently namespace ID 0 is hardcoded.

func lowerStrings(s []string) []string {
	for i := range s {
		s[i] = strings.ToLower(s[i])
	}
	return s
}

// column holds information about a particular column and the field that column
// is mapped to.
type column struct {
	proto.ColumnDescriptor
	field reflect.StructField
}

// model holds information about a particular type that has been bound to a
// table using DB.BindModel.
type model struct {
	name             string // Table name.
	desc             *proto.TableDescriptor
	columnsByName    map[string]*column
	columnsByID      map[uint32]*column
	primaryKey       []*column // The columns that compose the primary key.
	otherColumnNames []string  // All non-primary key columns.
}

// encodeTableKey encodes a single element of a table key, appending the
// encoded value to b.
func encodeTableKey(b []byte, v reflect.Value) ([]byte, error) {
	switch t := v.Interface().(type) {
	case []byte:
		return roachencoding.EncodeBytes(b, t), nil
	case string:
		return roachencoding.EncodeBytes(b, []byte(t)), nil
	}

	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			return roachencoding.EncodeVarint(b, 1), nil
		}
		return roachencoding.EncodeVarint(b, 0), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return roachencoding.EncodeVarint(b, v.Int()), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return roachencoding.EncodeUvarint(b, v.Uint()), nil

	case reflect.Float32, reflect.Float64:
		return roachencoding.EncodeNumericFloat(b, v.Float()), nil

	case reflect.String:
		return roachencoding.EncodeBytes(b, []byte(v.String())), nil
	}

	return nil, fmt.Errorf("unable to encode key: %s", v)
}

// decodeTableKey decodes a single element of a table key from b, returning the
// remaining (not yet decoded) bytes.
func decodeTableKey(b []byte, v reflect.Value) ([]byte, error) {
	switch t := v.Addr().Interface().(type) {
	case *[]byte:
		b, *t = roachencoding.DecodeBytes(b, nil)
		return b, nil
	case *string:
		var r []byte
		b, r = roachencoding.DecodeBytes(b, nil)
		*t = string(r)
		return b, nil
	}

	switch v.Kind() {
	case reflect.Bool:
		var i int64
		b, i = roachencoding.DecodeVarint(b)
		v.SetBool(i != 0)
		return b, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var i int64
		b, i = roachencoding.DecodeVarint(b)
		v.SetInt(i)
		return b, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		var i uint64
		b, i = roachencoding.DecodeUvarint(b)
		v.SetUint(i)
		return b, nil

	case reflect.Float32, reflect.Float64:
		var f float64
		b, f = roachencoding.DecodeNumericFloat(b)
		v.SetFloat(f)
		return b, nil

	case reflect.String:
		var r []byte
		b, r = roachencoding.DecodeBytes(b, nil)
		v.SetString(string(r))
		return b, nil
	}

	return nil, fmt.Errorf("unable to decode key: %s", v)
}

// encodePrimaryKey encodes a primary key for the table using the model object
// v. It returns the encoded primary key.
func (m *model) encodePrimaryKey(v reflect.Value) ([]byte, error) {
	var key []byte
	key = append(key, keys.TableDataPrefix...)
	key = roachencoding.EncodeUvarint(key, uint64(m.desc.ID))

	for _, col := range m.primaryKey {
		var err error
		key, err = encodeTableKey(key, v.FieldByIndex(col.field.Index))
		if err != nil {
			return nil, err
		}
	}

	return key, nil
}

// decodePrimaryKey decodes a primary key for the table into the model object
// v. It returns the remaining (undecoded) bytes.
func (m *model) decodePrimaryKey(key []byte, v reflect.Value) ([]byte, error) {
	if !bytes.HasPrefix(key, keys.TableDataPrefix) {
		return nil, fmt.Errorf("%s: invalid key prefix: %q", m.name, key)
	}
	key = bytes.TrimPrefix(key, keys.TableDataPrefix)

	var tableID uint64
	key, tableID = roachencoding.DecodeUvarint(key)
	if uint32(tableID) != m.desc.ID {
		return nil, fmt.Errorf("%s: unexpected table ID: %d != %d", m.name, m.desc.ID, tableID)
	}

	for _, col := range m.primaryKey {
		var err error
		key, err = decodeTableKey(key, v.FieldByIndex(col.field.Index))
		if err != nil {
			return nil, err
		}
	}

	return key, nil
}

// encodeColumnKey encodes the column and appends it to primaryKey.
func (m *model) encodeColumnKey(primaryKey []byte, colID uint32) []byte {
	var key []byte
	key = append(key, primaryKey...)
	return roachencoding.EncodeUvarint(key, uint64(colID))
}

// CreateTable creates a table from the specified schema. Table creation will
// fail if the table name is already in use.
func (db *DB) CreateTable(schema proto.TableSchema) error {
	desc := proto.TableDescFromSchema(schema)
	if err := proto.ValidateTableDesc(desc); err != nil {
		return err
	}

	nameKey := keys.MakeNameMetadataKey(0, desc.Name)

	// This isn't strictly necessary as the conditional put below will fail if
	// the key already exists, but it seems good to avoid the table ID allocation
	// in most cases when the table already exists.
	if gr, err := db.Get(nameKey); err != nil {
		return err
	} else if gr.Exists() {
		return fmt.Errorf("table \"%s\" already exists", desc.Name)
	}

	ir, err := db.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		return err
	}
	desc.ID = uint32(ir.ValueInt() - 1)

	// TODO(pmattis): Be cognizant of error messages when this is ported to the
	// server. The error currently returned below is likely going to be difficult
	// to interpret.
	return db.Txn(func(txn *Txn) error {
		descKey := keys.MakeDescMetadataKey(desc.ID)
		b := &Batch{}
		b.CPut(nameKey, descKey, nil)
		b.Put(descKey, &desc)
		return txn.Commit(b)
	})
}

// DescribeTable retrieves the table schema for the specified table.
func (db *DB) DescribeTable(name string) (*proto.TableSchema, error) {
	desc, err := db.getTableDesc(name)
	if err != nil {
		return nil, err
	}
	schema := proto.TableSchemaFromDesc(*desc)
	return &schema, nil
}

// RenameTable renames a table.
func (db *DB) RenameTable(oldName, newName string) error {
	// TODO(pmattis): Should we allow both the old and new name to exist
	// simultaneously for a period of time? The thought is to allow an
	// application to access the table via either name while the application is
	// being upgraded. Alternatively, instead of a rename table operation perhaps
	// there should be a link table operation which adds a "hard link" to the
	// table. Similar to a file, a table would not be removed until all of the
	// hard links are removed.

	return db.Txn(func(txn *Txn) error {
		oldNameKey := keys.MakeNameMetadataKey(0, strings.ToLower(oldName))
		gr, err := txn.Get(oldNameKey)
		if err != nil {
			return err
		}
		if !gr.Exists() {
			return fmt.Errorf("unable to find table \"%s\"", oldName)
		}
		descKey := gr.ValueBytes()
		desc := proto.TableDescriptor{}
		if err := txn.GetProto(descKey, &desc); err != nil {
			return err
		}
		desc.Name = strings.ToLower(newName)
		if err := proto.ValidateTableDesc(desc); err != nil {
			return err
		}
		newNameKey := keys.MakeNameMetadataKey(0, desc.Name)
		b := &Batch{}
		b.Put(descKey, &desc)
		// If the new name already exists the conditional put will fail causing the
		// transaction to fail.
		b.CPut(newNameKey, descKey, nil)
		b.Del(oldNameKey)
		return txn.Commit(b)
	})
}

// DeleteTable deletes the specified table.
func (db *DB) DeleteTable(name string) error {
	nameKey := keys.MakeNameMetadataKey(0, strings.ToLower(name))
	gr, err := db.Get(nameKey)
	if err != nil {
		return err
	}
	if !gr.Exists() {
		return fmt.Errorf("unable to find table \"%s\"", name)
	}
	descKey := gr.ValueBytes()
	desc := proto.TableDescriptor{}
	if err := db.GetProto(descKey, &desc); err != nil {
		return err
	}

	panic("TODO(pmattis): delete all of the tables rows")
	// return db.Del(descKey)
}

// ListTables lists the tables.
func (db *DB) ListTables() ([]string, error) {
	tableNamePrefix := keys.MakeNameMetadataKey(0, "")
	rows, err := db.Scan(tableNamePrefix, tableNamePrefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	tableNames := make([]string, len(rows))
	for i, row := range rows {
		tableNames[i] = string(bytes.TrimPrefix(row.Key, tableNamePrefix))
	}
	return tableNames, nil
}

// BindModel binds the supplied interface with the named table. You must bind
// the model for any type you wish to perform operations on. It is an error to
// bind the same model type more than once and a single model type can only be
// bound to a single table. The primaryKey arguments specify the columns that
// make up the primary key.
func (db *DB) BindModel(name string, obj interface{}) error {
	t := deref(reflect.TypeOf(obj))
	if db.experimentalModels == nil {
		db.experimentalModels = make(map[reflect.Type]*model)
	}
	if _, ok := db.experimentalModels[t]; ok {
		return fmt.Errorf("%s: model '%T' already defined", name, obj)
	}
	fields, err := getDBFields(t)
	if err != nil {
		return err
	}

	desc, err := db.getTableDesc(name)
	if err != nil {
		return err
	}

	columnsByName := map[string]*column{}
	columnsByID := map[uint32]*column{}
	for _, col := range desc.Columns {
		f, ok := fields[col.Name]
		if !ok {
			continue
		}
		c := &column{
			ColumnDescriptor: col,
			field:            f,
		}
		columnsByName[c.Name] = c
		columnsByID[c.ID] = c
	}

	var primaryKey []*column
	isPrimaryKey := make(map[string]struct{})
	for _, colID := range desc.Indexes[0].ColumnIDs {
		col := columnsByID[colID]
		primaryKey = append(primaryKey, col)
		isPrimaryKey[col.Name] = struct{}{}
	}

	var otherColumnNames []string
	for _, col := range desc.Columns {
		if _, ok := isPrimaryKey[col.Name]; ok {
			if _, ok2 := columnsByName[col.Name]; !ok2 {
				return fmt.Errorf("primary key column \"%s\" not mapped", col.Name)
			}
		}
		otherColumnNames = append(otherColumnNames, col.Name)
	}

	m := &model{
		name:             name,
		desc:             desc,
		columnsByName:    columnsByName,
		columnsByID:      columnsByID,
		primaryKey:       primaryKey,
		otherColumnNames: otherColumnNames,
	}
	db.experimentalModels[t] = m

	// TODO(pmattis): Check that all of the primary key columns are compatible
	// with {encode,decode}PrimaryKey.
	return nil
}

func (db *DB) getTableDesc(name string) (*proto.TableDescriptor, error) {
	gr, err := db.Get(keys.MakeNameMetadataKey(0, strings.ToLower(name)))
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, fmt.Errorf("unable to find table \"%s\"", name)
	}
	descKey := gr.ValueBytes()
	desc := proto.TableDescriptor{}
	if err := db.GetProto(descKey, &desc); err != nil {
		return nil, err
	}
	if err := proto.ValidateTableDesc(desc); err != nil {
		return nil, err
	}
	return &desc, nil
}

func (db *DB) getModel(t reflect.Type, mustBePointer bool) (*model, error) {
	// mustBePointer is an assertion requested by the caller that t is a pointer
	// type. It is used by {Get,Inc}Struct to verify that those methods were
	// passed pointers and not structures.
	if mustBePointer && t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("pointer type required: '%s'", t)
	}
	t = deref(t)
	if model, ok := db.experimentalModels[t]; ok {
		return model, nil
	}
	return nil, fmt.Errorf("unable to find model for '%s'", t)
}

// GetStruct ...
func (db *DB) GetStruct(obj interface{}, columns ...string) error {
	b := db.NewBatch()
	b.GetStruct(obj, columns...)
	_, err := runOneResult(db, b)
	return err
}

// PutStruct ...
func (db *DB) PutStruct(obj interface{}, columns ...string) error {
	b := db.NewBatch()
	b.PutStruct(obj, columns...)
	_, err := runOneResult(db, b)
	return err
}

// IncStruct ...
func (db *DB) IncStruct(obj interface{}, value int64, column string) error {
	b := db.NewBatch()
	b.IncStruct(obj, value, column)
	_, err := runOneResult(db, b)
	return err
}

// ScanStruct ...
func (db *DB) ScanStruct(dest, start, end interface{}, maxRows int64, columns ...string) error {
	b := db.NewBatch()
	b.ScanStruct(dest, start, end, maxRows, columns...)
	_, err := runOneResult(db, b)
	return err
}

// DelStruct ...
func (db *DB) DelStruct(obj interface{}, columns ...string) error {
	b := db.NewBatch()
	b.DelStruct(obj, columns...)
	_, err := runOneResult(db, b)
	return err
}

// GetStruct ...
func (txn *Txn) GetStruct(obj interface{}, columns ...string) error {
	b := txn.NewBatch()
	b.GetStruct(obj, columns...)
	_, err := runOneResult(txn, b)
	return err
}

// PutStruct ...
func (txn *Txn) PutStruct(obj interface{}, columns ...string) error {
	b := txn.NewBatch()
	b.PutStruct(obj, columns...)
	_, err := runOneResult(txn, b)
	return err
}

// IncStruct ...
func (txn *Txn) IncStruct(obj interface{}, value int64, column string) error {
	b := txn.NewBatch()
	b.IncStruct(obj, value, column)
	_, err := runOneResult(txn, b)
	return err
}

// ScanStruct ...
func (txn *Txn) ScanStruct(dest, start, end interface{}, maxRows int64, columns ...string) error {
	b := txn.NewBatch()
	b.ScanStruct(dest, start, end, maxRows, columns...)
	_, err := runOneResult(txn, b)
	return err
}

// DelStruct ...
func (txn *Txn) DelStruct(obj interface{}, columns ...string) error {
	b := txn.NewBatch()
	b.DelStruct(obj, columns...)
	_, err := runOneResult(txn, b)
	return err
}

// GetStruct retrieves the specified columns in the structured table identified
// by obj. The primary key columns within obj are used to identify which row to
// retrieve. The obj type must have previously been bound to a table using
// BindModel. If columns is empty all of the columns are retrieved. Obj must be
// a pointer to the model type.
func (b *Batch) GetStruct(obj interface{}, columns ...string) {
	v := reflect.ValueOf(obj)
	m, err := b.DB.getModel(v.Type(), true)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	v = reflect.Indirect(v)

	primaryKey, err := m.encodePrimaryKey(v)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}

	if len(columns) == 0 {
		columns = m.otherColumnNames
	} else {
		lowerStrings(columns)
	}

	var calls []proto.Call
	for _, colName := range columns {
		col, ok := m.columnsByName[colName]
		if !ok {
			b.initResult(0, 0, fmt.Errorf("%s: unable to find column %s", m.name, colName))
			return
		}

		key := m.encodeColumnKey(primaryKey, col.ID)
		if log.V(2) {
			log.Infof("Get %q", key)
		}
		c := proto.GetCall(proto.Key(key))
		c.Post = func() error {
			reply := c.Reply.(*proto.GetResponse)
			return unmarshalTableValue(reply.Value, v.FieldByIndex(col.field.Index))
		}
		calls = append(calls, c)
	}

	b.calls = append(b.calls, calls...)
	b.initResult(len(calls), len(calls), nil)
}

// PutStruct sets the specified columns in the structured table identified by
// obj. The primary key columns within obj are used to identify which row to
// modify. The obj type must have previously been bound to a table using
// BindModel. If columns is empty all of the columns are set.
func (b *Batch) PutStruct(obj interface{}, columns ...string) {
	v := reflect.Indirect(reflect.ValueOf(obj))
	m, err := b.DB.getModel(v.Type(), false)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}

	primaryKey, err := m.encodePrimaryKey(v)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}

	if len(columns) == 0 {
		columns = m.otherColumnNames
	} else {
		lowerStrings(columns)
	}

	var calls []proto.Call
	for _, colName := range columns {
		col, ok := m.columnsByName[colName]
		if !ok {
			b.initResult(0, 0, fmt.Errorf("%s: unable to find column %s", m.name, colName))
			return
		}

		key := m.encodeColumnKey(primaryKey, col.ID)
		value := v.FieldByIndex(col.field.Index)
		if log.V(2) {
			log.Infof("Put %q -> %v", key, value.Interface())
		}

		v, err := marshalTableValue(value)
		if err != nil {
			b.initResult(0, 0, err)
			return
		}

		calls = append(calls, proto.PutCall(key, v))
	}

	b.calls = append(b.calls, calls...)
	b.initResult(len(calls), len(calls), nil)
}

// IncStruct increments the specified column in the structured table identify
// by obj. The primary key columns within obj are used to identify which row to
// modify. The obj type must have previously been bound to a table using
// BindModel.
func (b *Batch) IncStruct(obj interface{}, value int64, column string) {
	v := reflect.ValueOf(obj)
	m, err := b.DB.getModel(v.Type(), true)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	v = reflect.Indirect(v)

	primaryKey, err := m.encodePrimaryKey(v)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}

	col, ok := m.columnsByName[strings.ToLower(column)]
	if !ok {
		b.initResult(0, 0, fmt.Errorf("%s: unable to find column %s", m.name, column))
		return
	}

	key := m.encodeColumnKey(primaryKey, col.ID)
	if log.V(2) {
		log.Infof("Inc %q", key)
	}
	c := proto.IncrementCall(proto.Key(key), value)
	c.Post = func() error {
		reply := c.Reply.(*proto.IncrementResponse)
		// TODO(pmattis): This isn't very efficient. Should be able to pass the
		// integer value directly instead of encoding it into a []byte.
		pv := &proto.Value{}
		pv.SetInteger(reply.NewValue)
		return unmarshalTableValue(pv, v.FieldByIndex(col.field.Index))
	}

	b.calls = append(b.calls, c)
	b.initResult(1, 1, nil)
}

// ScanStruct scans the specified columns from the structured table identified
// by the destination slice. The slice element type, start and end key types
// must be identical. The primary key columns within start and end are used to
// identify which rows to scan. The type must have previously been bound to a
// table using BindModel. If columns is empty all of the columns in the table
// are scanned.
func (b *Batch) ScanStruct(dest, start, end interface{}, maxRows int64, columns ...string) {
	sliceV := reflect.ValueOf(dest)
	if sliceV.Kind() != reflect.Ptr {
		b.initResult(0, 0, fmt.Errorf("dest must be a pointer to a slice: %T", dest))
		return
	}
	sliceV = sliceV.Elem()
	if sliceV.Kind() != reflect.Slice {
		b.initResult(0, 0, fmt.Errorf("dest must be a pointer to a slice: %T", dest))
		return
	}

	modelT := sliceV.Type().Elem()
	// Are we returning a slice of structs or pointers to structs?
	ptrResults := modelT.Kind() == reflect.Ptr
	if ptrResults {
		modelT = modelT.Elem()
	}

	m, err := b.DB.getModel(modelT, false)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}

	var scanColIDs map[uint32]bool
	if len(columns) > 0 {
		lowerStrings(columns)
		scanColIDs = make(map[uint32]bool, len(columns))
		for _, colName := range columns {
			col, ok := m.columnsByName[colName]
			if !ok {
				b.initResult(0, 0, fmt.Errorf("%s: unable to find column %s", m.name, colName))
				return
			}
			scanColIDs[col.ID] = true
		}
	}

	startV := reflect.Indirect(reflect.ValueOf(start))
	if modelT != startV.Type() {
		b.initResult(0, 0, fmt.Errorf("incompatible start key type: %s != %s", modelT, startV.Type()))
		return
	}

	endV := reflect.Indirect(reflect.ValueOf(end))
	if modelT != endV.Type() {
		b.initResult(0, 0, fmt.Errorf("incompatible end key type: %s != %s", modelT, endV.Type()))
		return
	}

	startKey, err := m.encodePrimaryKey(startV)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	endKey, err := m.encodePrimaryKey(endV)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	if log.V(2) {
		log.Infof("Scan %q %q", startKey, endKey)
	}

	c := proto.ScanCall(proto.Key(startKey), proto.Key(endKey), maxRows)
	c.Post = func() error {
		reply := c.Reply.(*proto.ScanResponse)
		if len(reply.Rows) == 0 {
			return nil
		}

		var primaryKey []byte
		resultPtr := reflect.New(modelT)
		result := resultPtr.Elem()
		zero := reflect.Zero(result.Type())

		for _, row := range reply.Rows {
			if primaryKey != nil && !bytes.HasPrefix(row.Key, primaryKey) {
				if ptrResults {
					sliceV = reflect.Append(sliceV, resultPtr)
					resultPtr = reflect.New(modelT)
					result = resultPtr.Elem()
				} else {
					sliceV = reflect.Append(sliceV, result)
					result.Set(zero)
				}
				_, err := m.decodePrimaryKey(primaryKey, result)
				if err != nil {
					return err
				}
			}

			remaining, err := m.decodePrimaryKey([]byte(row.Key), result)
			if err != nil {
				return err
			}
			primaryKey = []byte(row.Key[:len(row.Key)-len(remaining)])

			_, colID := roachencoding.DecodeUvarint(remaining)
			if err != nil {
				return err
			}
			if scanColIDs != nil && !scanColIDs[uint32(colID)] {
				continue
			}
			col, ok := m.columnsByID[uint32(colID)]
			if !ok {
				return fmt.Errorf("%s: unable to find column %d", m.name, colID)
			}
			if err := unmarshalTableValue(&row.Value, result.FieldByIndex(col.field.Index)); err != nil {
				return err
			}
		}

		if ptrResults {
			sliceV = reflect.Append(sliceV, resultPtr)
		} else {
			sliceV = reflect.Append(sliceV, result)
		}
		reflect.ValueOf(dest).Elem().Set(sliceV)
		return nil
	}

	b.calls = append(b.calls, c)
	b.initResult(1, 0, nil)
}

// DelStruct deletes the specified columns from the structured table identified
// by obj. The primary key columns within obj are used to identify which row to
// modify. The obj type must have previously been bound to a table using
// BindModel. If columns is empty the entire row is deleted.
//
// TODO(pmattis): If "obj" is a pointer, should we clear the columns in "obj"
// that are being deleted?
func (b *Batch) DelStruct(obj interface{}, columns ...string) {
	v := reflect.Indirect(reflect.ValueOf(obj))
	m, err := b.DB.getModel(v.Type(), false)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}

	primaryKey, err := m.encodePrimaryKey(v)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}

	if len(columns) == 0 {
		columns = m.otherColumnNames
	} else {
		lowerStrings(columns)
	}

	var calls []proto.Call
	for _, colName := range columns {
		col, ok := m.columnsByName[colName]
		if !ok {
			b.initResult(0, 0, fmt.Errorf("%s: unable to find field %s", m.name, colName))
			return
		}
		key := m.encodeColumnKey(primaryKey, col.ID)
		if log.V(2) {
			log.Infof("Del %q", key)
		}
		calls = append(calls, proto.DeleteCall(key))
	}

	b.calls = append(b.calls, calls...)
	b.initResult(len(calls), len(calls), nil)
}

// marshalTableValue returns a proto.Value initialized from the source
// reflect.Value, returning an error if the types are not compatible.
func marshalTableValue(v reflect.Value) (proto.Value, error) {
	var r proto.Value
	switch t := v.Interface().(type) {
	case nil:
		return r, nil

	case string:
		r.Bytes = []byte(t)
		return r, nil

	case []byte:
		r.Bytes = t
		return r, nil

	case gogoproto.Message:
		var err error
		r.Bytes, err = gogoproto.Marshal(t)
		return r, err

	case encoding.BinaryMarshaler:
		var err error
		r.Bytes, err = t.MarshalBinary()
		return r, err
	}

	switch v.Kind() {
	case reflect.Bool:
		i := int64(0)
		if v.Bool() {
			i = 1
		}
		r.SetInteger(i)
		return r, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		r.SetInteger(v.Int())
		return r, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		r.SetInteger(int64(v.Uint()))
		return r, nil

	case reflect.Float32, reflect.Float64:
		r.SetInteger(int64(math.Float64bits(v.Float())))
		return r, nil

	case reflect.String:
		r.Bytes = []byte(v.String())
		return r, nil
	}

	return r, fmt.Errorf("unable to marshal value: %s", v)
}

// unmarshalTableValue sets the destination reflect.Value contents from the
// source proto.Value, returning an error if the types are not compatible.
func unmarshalTableValue(src *proto.Value, dest reflect.Value) error {
	if src == nil {
		dest.Set(reflect.Zero(dest.Type()))
		return nil
	}

	switch d := dest.Addr().Interface().(type) {
	case *string:
		if src.Bytes != nil {
			*d = string(src.Bytes)
		} else {
			*d = ""
		}
		return nil

	case *[]byte:
		if src.Bytes != nil {
			*d = src.Bytes
		} else {
			*d = nil
		}
		return nil

	case *gogoproto.Message:
		panic("TODO(pmattis): unimplemented")

	case *encoding.BinaryMarshaler:
		panic("TODO(pmattis): unimplemented")
	}

	switch dest.Kind() {
	case reflect.Bool:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetBool(i != 0)
		return nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetInt(i)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetUint(uint64(i))
		return nil

	case reflect.Float32, reflect.Float64:
		i, err := src.GetInteger()
		if err != nil {
			return err
		}
		dest.SetFloat(math.Float64frombits(uint64(i)))
		return nil

	case reflect.String:
		if src == nil || src.Bytes == nil {
			dest.SetString("")
			return nil
		}
		dest.SetString(string(src.Bytes))
		return nil
	}

	return fmt.Errorf("unable to unmarshal value: %s", dest.Type())
}
