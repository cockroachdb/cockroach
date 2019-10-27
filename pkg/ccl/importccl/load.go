// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
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
	tempPrefix string,
) (backupccl.BackupDescriptor, error) {
	if loadChunkBytes == 0 {
		loadChunkBytes = *zonepb.DefaultZoneConfig().RangeMaxBytes / 2
	}

	var txCtx transform.ExprTransformContext
	curTime := timeutil.Unix(0, ts.WallTime)
	evalCtx := &tree.EvalContext{}
	evalCtx.SetTxnTimestamp(curTime)
	evalCtx.SetStmtTimestamp(curTime)

	conf, err := cloud.ExternalStorageConfFromURI(uri)
	if err != nil {
		return backupccl.BackupDescriptor{}, err
	}
	dir, err := cloud.MakeExternalStorage(ctx, conf, cluster.NoSettings)
	if err != nil {
		return backupccl.BackupDescriptor{}, errors.Wrap(err, "export storage from URI")
	}
	defer dir.Close()

	var dbDescBytes []byte
	if err := db.QueryRow(`
		SELECT
			d.descriptor
		FROM system.namespace n INNER JOIN system.descriptor d ON n.id = d.id
		WHERE n."parentID" = $1
		AND n.name = $2`,
		keys.RootNamespaceID,
		database,
	).Scan(&dbDescBytes); err != nil {
		return backupccl.BackupDescriptor{}, errors.Wrap(err, "fetch database descriptor")
	}
	var dbDescWrapper sqlbase.Descriptor
	if err := protoutil.Unmarshal(dbDescBytes, &dbDescWrapper); err != nil {
		return backupccl.BackupDescriptor{}, errors.Wrap(err, "unmarshal database descriptor")
	}
	dbDesc := dbDescWrapper.GetDatabase()

	privs := dbDesc.GetPrivileges()

	tableDescs := make(map[string]*sqlbase.ImmutableTableDescriptor)

	var currentCmd bytes.Buffer
	scanner := bufio.NewReader(r)
	var ri row.Inserter
	var defaultExprs []tree.TypedExpr
	var cols []sqlbase.ColumnDescriptor
	var tableDesc *sqlbase.ImmutableTableDescriptor
	var tableName string
	var prevKey roachpb.Key
	var kvs []engine.MVCCKeyValue
	var kvBytes int64
	backup := backupccl.BackupDescriptor{
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
			return backupccl.BackupDescriptor{}, errors.Wrap(err, "read line")
		}
		currentCmd.WriteString(line)
		if !parser.EndsInSemicolon(currentCmd.String()) {
			currentCmd.WriteByte('\n')
			continue
		}
		cmd := currentCmd.String()
		currentCmd.Reset()
		stmt, err := parser.ParseOne(cmd)
		if err != nil {
			return backupccl.BackupDescriptor{}, errors.Wrapf(err, "parsing: %q", cmd)
		}
		switch s := stmt.AST.(type) {
		case *tree.CreateTable:
			if tableDesc != nil {
				if err := writeSST(ctx, &backup, dir, tempPrefix, kvs, ts); err != nil {
					return backupccl.BackupDescriptor{}, errors.Wrap(err, "writeSST")
				}
				kvs = kvs[:0]
				kvBytes = 0
			}

			// TODO(mjibson): error for now on FKs and CHECK constraints
			// TODO(mjibson): differentiate between qualified (with database) and unqualified (without database) table names

			tableName = s.Table.String()
			tableDesc = tableDescs[tableName]
			if tableDesc != nil {
				return backupccl.BackupDescriptor{}, errors.Errorf("duplicate CREATE TABLE for %s", tableName)
			}

			// Using test cluster settings means that we'll generate a backup using
			// the latest cluster version available in this binary. This will be safe
			// once we verify the cluster version during restore.
			//
			// TODO(benesch): ensure backups from too-old or too-new nodes are
			// rejected during restore.
			st := cluster.MakeTestingClusterSettings()

			affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
			// A nil txn is safe because it is only used by sql.MakeTableDesc, which
			// only uses txn for resolving FKs and interleaved tables, neither of which
			// are present here. Ditto for the schema accessor.
			var txn *client.Txn
			// At this point the CREATE statements in the loaded SQL do not
			// use the SERIAL type so we need not process SERIAL types here.
			desc, err := sql.MakeTableDesc(ctx, txn, nil /* vt */, st, s, dbDesc.ID, 0, /* table ID */
				ts, privs, affected, nil, evalCtx, false /* temporary */)
			if err != nil {
				return backupccl.BackupDescriptor{}, errors.Wrap(err, "make table desc")
			}

			tableDesc = sqlbase.NewImmutableTableDescriptor(*desc.TableDesc())
			tableDescs[tableName] = tableDesc
			backup.Descriptors = append(backup.Descriptors, sqlbase.Descriptor{
				Union: &sqlbase.Descriptor_Table{Table: desc.TableDesc()},
			})

			for i := range tableDesc.Columns {
				col := &tableDesc.Columns[i]
				if col.IsComputed() {
					return backupccl.BackupDescriptor{}, errors.Errorf("computed columns are not allowed")
				}
			}

			ri, err = row.MakeInserter(nil, tableDesc, nil, tableDesc.Columns,
				true, &sqlbase.DatumAlloc{})
			if err != nil {
				return backupccl.BackupDescriptor{}, errors.Wrap(err, "make row inserter")
			}
			cols, defaultExprs, err =
				sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &txCtx, evalCtx)
			if err != nil {
				return backupccl.BackupDescriptor{}, errors.Wrap(err, "process default columns")
			}

		case *tree.Insert:
			name := tree.AsString(s.Table)
			if tableDesc == nil {
				return backupccl.BackupDescriptor{}, errors.Errorf("expected previous CREATE TABLE %s statement", name)
			}
			if name != tableName {
				return backupccl.BackupDescriptor{}, errors.Errorf("unexpected INSERT for table %s after CREATE TABLE %s", name, tableName)
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
				return backupccl.BackupDescriptor{}, errors.Wrapf(err, "insertStmtToKVs")
			}
			if outOfOrder {
				return backupccl.BackupDescriptor{}, errors.Errorf("out of order row: %s", cmd)
			}

			if kvBytes > loadChunkBytes {
				if err := writeSST(ctx, &backup, dir, tempPrefix, kvs, ts); err != nil {
					return backupccl.BackupDescriptor{}, errors.Wrap(err, "writeSST")
				}
				kvs = kvs[:0]
				kvBytes = 0
			}

		default:
			return backupccl.BackupDescriptor{}, errors.Errorf("unsupported load statement: %q", stmt)
		}
	}

	if tableDesc != nil {
		if err := writeSST(ctx, &backup, dir, tempPrefix, kvs, ts); err != nil {
			return backupccl.BackupDescriptor{}, errors.Wrap(err, "writeSST")
		}
	}

	descBuf, err := protoutil.Marshal(&backup)
	if err != nil {
		return backupccl.BackupDescriptor{}, errors.Wrap(err, "marshal backup descriptor")
	}
	if err := dir.WriteFile(ctx, backupccl.BackupDescriptorName, bytes.NewReader(descBuf)); err != nil {
		return backupccl.BackupDescriptor{}, errors.Wrap(err, "uploading backup descriptor")
	}

	return backup, nil
}

func insertStmtToKVs(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	defaultExprs []tree.TypedExpr,
	cols []sqlbase.ColumnDescriptor,
	evalCtx *tree.EvalContext,
	ri row.Inserter,
	stmt *tree.Insert,
	f func(roachpb.KeyValue),
) error {
	if stmt.OnConflict != nil {
		return errors.Errorf("load insert: ON CONFLICT not supported: %q", stmt)
	}
	if tree.HasReturningClause(stmt.Returning) {
		return errors.Errorf("load insert: RETURNING not supported: %q", stmt)
	}
	if len(stmt.Columns) > 0 {
		if len(stmt.Columns) != len(cols) {
			return errors.Errorf("load insert: wrong number of columns: %q", stmt)
		}
		for i := range tableDesc.Columns {
			if stmt.Columns[i].String() != tableDesc.Columns[i].Name {
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
	values, ok := stmt.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return errors.Errorf("load insert: expected VALUES clause: %q", stmt)
	}

	b := row.KVInserter(f)
	computedIVarContainer := sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    tableDesc.Columns,
	}
	for _, tuple := range values.Rows {
		insertRow := make([]tree.Datum, len(tuple))
		for i, expr := range tuple {
			if expr == tree.DNull {
				insertRow[i] = tree.DNull
				continue
			}
			c, ok := expr.(tree.Constant)
			if !ok {
				return errors.Errorf("unsupported expr: %q", expr)
			}
			var err error
			insertRow[i], err = c.ResolveAsType(nil, &tableDesc.Columns[i].Type)
			if err != nil {
				return err
			}
		}

		// We have disallowed computed exprs.
		var computeExprs []tree.TypedExpr
		var computedCols []sqlbase.ColumnDescriptor

		insertRow, err := row.GenerateInsertRow(
			defaultExprs, computeExprs, cols, computedCols, evalCtx, tableDesc, insertRow, &computedIVarContainer,
		)
		if err != nil {
			return errors.Wrapf(err, "process insert %q", insertRow)
		}
		// TODO(bram): Is the checking of FKs here required? If not, turning them
		// off may provide a speed boost.
		if err := ri.InsertRow(ctx, b, insertRow, true, row.CheckFKs, false /* traceKV */); err != nil {
			return errors.Wrapf(err, "insert %q", insertRow)
		}
	}
	return nil
}

func writeSST(
	ctx context.Context,
	backup *backupccl.BackupDescriptor,
	base cloud.ExternalStorage,
	tempPrefix string,
	kvs []engine.MVCCKeyValue,
	ts hlc.Timestamp,
) error {
	if len(kvs) == 0 {
		return nil
	}

	filename := fmt.Sprintf("load-%d.sst", rand.Int63())
	log.Info(ctx, "writesst ", filename)

	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	defer sst.Close()
	for _, kv := range kvs {
		kv.Key.Timestamp = ts
		if err := sst.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	sstContents, err := sst.Finish()
	if err != nil {
		return err
	}

	if err := base.WriteFile(ctx, filename, bytes.NewReader(sstContents)); err != nil {
		return err
	}

	backup.Files = append(backup.Files, backupccl.BackupDescriptor_File{
		Span: roachpb.Span{
			Key: kvs[0].Key.Key,
			// The EndKey is exclusive, so use PrefixEnd to get the first key
			// greater than the last key in the sst.
			EndKey: kvs[len(kvs)-1].Key.Key.PrefixEnd(),
		},
		Path: filename,
	})
	backup.EntryCounts.DataSize += sst.DataSize
	return nil
}
