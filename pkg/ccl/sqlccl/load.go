// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"bufio"
	"bytes"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Load converts r into SSTables and backup descriptors. database is the name
// of the database into which the SSTables will eventually be written. uri
// is the storage location. ts is the time at which the MVCC data will
// be set. loadChunkBytes is the size at which to create a new SSTable
// (which will translate into a new range during restore); set to 0 to use
// the zone's default range max / 2.
func Load(
	ctx context.Context,
	db *gosql.DB,
	r io.Reader,
	database, uri string,
	ts hlc.Timestamp,
	loadChunkBytes int64,
) (BackupDescriptor, error) {
	if loadChunkBytes == 0 {
		loadChunkBytes = config.DefaultZoneConfig().RangeMaxBytes / 2
	}

	// TODO(dan): Handle traditional vs modern.
	syntax := parser.Traditional
	parse := parser.Parser{}
	evalCtx := parser.EvalContext{}

	dir, err := storageccl.ExportStorageFromURI(ctx, uri)
	if err != nil {
		return BackupDescriptor{}, errors.Wrap(err, "export storage from URI")
	}
	defer func() {
		if err := dir.Close(); err != nil {
			log.Errorf(ctx, "close storage: %s", err)
		}
	}()

	var dbDescBytes []byte
	if err := db.QueryRow(`
		SELECT
			d.descriptor
		FROM system.namespace n INNER JOIN system.descriptor d ON n.id = d.id
		WHERE n.parentID = $1
		AND n.name = $2`,
		keys.RootNamespaceID,
		database,
	).Scan(&dbDescBytes); err != nil {
		return BackupDescriptor{}, errors.Wrap(err, "fetch database descriptor")
	}
	var dbDescWrapper sqlbase.Descriptor
	if err := dbDescWrapper.Unmarshal(dbDescBytes); err != nil {
		return BackupDescriptor{}, errors.Wrap(err, "unmarshal database descriptor")
	}
	dbDesc := dbDescWrapper.GetDatabase()

	privs := dbDesc.GetPrivileges()

	tableDescs := make(map[string]*sqlbase.TableDescriptor)

	var currentCmd bytes.Buffer
	scanner := bufio.NewReader(r)
	var ri sql.RowInserter
	var defaultExprs []parser.TypedExpr
	var cols []sqlbase.ColumnDescriptor
	var tableDesc *sqlbase.TableDescriptor
	var tableName string
	var prevKey roachpb.Key
	var kvs []engine.MVCCKeyValue
	var kvBytes int64
	backup := BackupDescriptor{
		Descriptors: []sqlbase.Descriptor{
			{Union: &sqlbase.Descriptor_Database{Database: dbDesc}},
		},
	}
	for {
		line, err := scanner.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return BackupDescriptor{}, errors.Wrap(err, "read line")
		}
		currentCmd.WriteString(line)
		if !isEndOfStatement(syntax, currentCmd.String()) {
			currentCmd.WriteByte('\n')
			continue
		}
		cmd := currentCmd.String()
		currentCmd.Reset()
		stmt, err := parser.ParseOne(cmd, syntax)
		if err != nil {
			return BackupDescriptor{}, errors.Wrapf(err, "parsing: %q", cmd)
		}
		switch s := stmt.(type) {
		case *parser.CreateTable:
			if tableDesc != nil {
				if err := writeSST(ctx, &backup, dir, kvs, ts); err != nil {
					return BackupDescriptor{}, errors.Wrap(err, "writeSST")
				}
				kvs = kvs[:0]
				kvBytes = 0
			}

			// TODO(mjibson): error for now on FKs and CHECK constraints
			// TODO(mjibson): differentiate between qualified (with database) and unqualified (without database) table names

			tableName = s.Table.String()
			tableDesc = tableDescs[tableName]
			if tableDesc != nil {
				return BackupDescriptor{}, errors.Errorf("duplicate CREATE TABLE for %s", tableName)
			}

			affected := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
			// A nil txn is safe because it is only used by sql.MakeTableDesc, which
			// only uses txn for resolving FKs and interleaved tables, neither of which
			// are present here.
			var txn *client.Txn
			desc, err := sql.MakeTableDesc(ctx, txn, sql.NilVirtualTabler, nil, s, dbDesc.ID, 0 /* table ID */, privs, affected, dbDesc.Name)
			if err != nil {
				return BackupDescriptor{}, errors.Wrap(err, "make table desc")
			}

			tableDesc = &desc
			tableDescs[tableName] = tableDesc
			backup.Descriptors = append(backup.Descriptors, sqlbase.Descriptor{
				Union: &sqlbase.Descriptor_Table{Table: tableDesc},
			})

			ri, err = sql.MakeRowInserter(nil, tableDesc, nil, tableDesc.Columns, true)
			if err != nil {
				return BackupDescriptor{}, errors.Wrap(err, "make row inserter")
			}
			cols, defaultExprs, err = sql.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &parse, &evalCtx)
			if err != nil {
				return BackupDescriptor{}, errors.Wrap(err, "process default columns")
			}

		case *parser.Insert:
			name := parser.AsString(s.Table)
			if tableDesc == nil {
				return BackupDescriptor{}, errors.Errorf("expected previous CREATE TABLE %s statement", name)
			}
			if parser.ReNormalizeName(name) != parser.ReNormalizeName(tableName) {
				return BackupDescriptor{}, errors.Errorf("unexpected INSERT for table %s after CREATE TABLE %s", name, tableName)
			}
			outOfOrder := false
			err := insertStmtToKVs(ctx, tableDesc, defaultExprs, cols, evalCtx, ri, s, func(kv roachpb.KeyValue) {
				if outOfOrder || prevKey.Compare(kv.Key) >= 0 {
					outOfOrder = true
					return
				}
				prevKey = kv.Key
				kvBytes += int64(len(kv.Key) + len(kv.Value.RawBytes))
				kvs = append(kvs, engine.MVCCKeyValue{
					Key:   engine.MVCCKey{Key: kv.Key, Timestamp: kv.Value.Timestamp},
					Value: kv.Value.RawBytes,
				})
			})
			if err != nil {
				return BackupDescriptor{}, errors.Wrapf(err, "insertStmtToKVs")
			}
			if outOfOrder {
				return BackupDescriptor{}, errors.Errorf("out of order row: %s", cmd)
			}

			if kvBytes > loadChunkBytes {
				if err := writeSST(ctx, &backup, dir, kvs, ts); err != nil {
					return BackupDescriptor{}, errors.Wrap(err, "writeSST")
				}
				kvs = kvs[:0]
				kvBytes = 0
			}

		default:
			return BackupDescriptor{}, errors.Errorf("unsupported load statement: %q", stmt)
		}
	}

	if tableDesc != nil {
		if err := writeSST(ctx, &backup, dir, kvs, ts); err != nil {
			return BackupDescriptor{}, errors.Wrap(err, "writeSST")
		}
	}

	descBuf, err := backup.Marshal()
	if err != nil {
		return BackupDescriptor{}, errors.Wrap(err, "marshal backup descriptor")
	}
	descFile, err := dir.PutFile(ctx, BackupDescriptorName)
	if err != nil {
		return BackupDescriptor{}, errors.Wrap(err, "creating backup descriptor file")
	}
	defer descFile.Cleanup()
	if err = ioutil.WriteFile(descFile.LocalFile(), descBuf, 0600); err != nil {
		return BackupDescriptor{}, errors.Wrap(err, "write backup descriptor")
	}
	if err := descFile.Finish(); err != nil {
		return BackupDescriptor{}, errors.Wrap(err, "uploading backup descriptor")
	}

	return backup, nil
}

func insertStmtToKVs(
	ctx context.Context,
	tableDesc *sqlbase.TableDescriptor,
	defaultExprs []parser.TypedExpr,
	cols []sqlbase.ColumnDescriptor,
	evalCtx parser.EvalContext,
	ri sql.RowInserter,
	stmt *parser.Insert,
	f func(roachpb.KeyValue),
) error {
	if stmt.OnConflict != nil {
		return errors.Errorf("load insert: ON CONFLICT not supported: %q", stmt)
	}
	if parser.HasReturningClause(stmt.Returning) {
		return errors.Errorf("load insert: RETURNING not supported: %q", stmt)
	}
	if len(stmt.Columns) > 0 {
		if len(stmt.Columns) != len(cols) {
			return errors.Errorf("load insert: wrong number of columns: %q", stmt)
		}
		for i, col := range tableDesc.Columns {
			if stmt.Columns[i].String() != col.Name {
				return errors.Errorf("load insert: unexpected column order: %q", stmt)
			}
		}
	}
	if stmt.Rows.Limit != nil {
		return errors.Errorf("load insert: LIMIT not supported: %q", stmt)
	}
	if stmt.Rows.OrderBy != nil {
		return errors.Errorf("load insert: ORDER BY not supported: %q", stmt)
	}
	values, ok := stmt.Rows.Select.(*parser.ValuesClause)
	if !ok {
		return errors.Errorf("load insert: expected VALUES clause: %q", stmt)
	}

	b := inserter(f)
	for _, tuple := range values.Tuples {
		row := make([]parser.Datum, len(tuple.Exprs))
		for i, expr := range tuple.Exprs {
			if expr == parser.DNull {
				row[i] = parser.DNull
				continue
			}
			c, ok := expr.(parser.Constant)
			if !ok {
				return errors.Errorf("unsupported expr: %q", expr)
			}
			var err error
			row[i], err = c.ResolveAsType(nil, tableDesc.Columns[i].Type.ToDatumType())
			if err != nil {
				return err
			}
		}
		row, err := sql.GenerateInsertRow(defaultExprs, ri.InsertColIDtoRowIndex, cols, evalCtx, tableDesc, row)
		if err != nil {
			return errors.Wrapf(err, "process insert %q", row)
		}
		if err := ri.InsertRow(ctx, b, row, true); err != nil {
			return errors.Wrapf(err, "insert %q", row)
		}
	}
	return nil
}

type inserter func(roachpb.KeyValue)

func (i inserter) CPut(key, value, expValue interface{}) {
	panic("unimplemented")
}

func (i inserter) Put(key, value interface{}) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// isEndOfStatement returns true if stmt ends with a semicolon.
func isEndOfStatement(syntax parser.Syntax, stmt string) bool {
	sc := parser.MakeScanner(stmt, syntax)
	var last int
	sc.Tokens(func(t int) {
		last = t
	})
	return last == ';'
}

func writeSST(
	ctx context.Context,
	backup *BackupDescriptor,
	base storageccl.ExportStorage,
	kvs []engine.MVCCKeyValue,
	ts hlc.Timestamp,
) error {
	filename := fmt.Sprintf("load-%d.sst", rand.Int63())
	log.Info(ctx, "writesst ", filename)
	sstFile, err := base.PutFile(ctx, filename)
	if err != nil {
		return err
	}
	defer sstFile.Cleanup()

	sst := engine.MakeRocksDBSstFileWriter()
	if err := sst.Open(sstFile.LocalFile()); err != nil {
		return err
	}
	for _, kv := range kvs {
		kv.Key.Timestamp = ts
		if err := sst.Add(kv); err != nil {
			return err
		}
	}
	if err := sst.Close(); err != nil {
		return err
	}

	if err := sstFile.Finish(); err != nil {
		return err
	}

	backup.Files = append(backup.Files, BackupDescriptor_File{
		Span: roachpb.Span{
			Key: kvs[0].Key.Key,
			// The EndKey is exclusive, so use PrefixEnd to get the first key
			// greater than the last key in the sst.
			EndKey: kvs[len(kvs)-1].Key.Key.PrefixEnd(),
		},
		Path: filename,
	})
	backup.DataSize += sst.DataSize
	return nil
}
