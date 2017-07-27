// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// LoadCSV converts CSV files into enterprise backup format.
func LoadCSV(
	ctx context.Context,
	table string,
	dataFiles []string,
	dest string,
	comma, comment rune,
	nullif *string,
	sstMaxSize int64,
) (csvCount, kvCount, sstCount int64, err error) {
	if table == "" {
		return 0, 0, 0, errors.New("no table specified")
	}
	if dest == "" {
		return 0, 0, 0, errors.New("no destination specified")
	}
	if comma == 0 {
		comma = ','
	}
	if len(dataFiles) == 0 {
		dataFiles = []string{fmt.Sprintf("%s.dat", table)}
	}
	tableDefStr, err := ioutil.ReadFile(table)
	if err != nil {
		return 0, 0, 0, err
	}
	stmt, err := parser.ParseOne(string(tableDefStr))
	if err != nil {
		return 0, 0, 0, err
	}
	create, ok := stmt.(*parser.CreateTable)
	if !ok {
		return 0, 0, 0, errors.New("expected CREATE TABLE statement in table file")
	}
	if create.IfNotExists {
		return 0, 0, 0, errors.New("unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return 0, 0, 0, errors.New("interleaved not supported")
	}
	if create.AsSource != nil {
		return 0, 0, 0, errors.New("CREATE AS not supported")
	}
	// TODO(mjibson): error on FKs

	const (
		// We need to choose arbitrary database and table IDs. These aren't important,
		// but they do match what would happen when creating a new database and
		// table on an empty cluster.
		parentID = keys.MaxReservedDescID + 1
		id       = parentID + 1
	)
	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		sql.NilVirtualTabler,
		nil, /* SearchPath */
		create,
		parentID,
		id,
		sqlbase.NewDefaultPrivilegeDescriptor(),
		nil, /* affected */
		"",  /* sessionDB */
		nil, /* EvalContext */
	)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "creating table descriptor")
	}

	rocksdbDest, err := ioutil.TempDir("", "cockroach-csv-rocksdb")
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err := os.RemoveAll(rocksdbDest); err != nil {
			log.Infof(ctx, "could not remove temp directory %s: %s", rocksdbDest, err)
		}
	}()

	// Some channels are buffered because reads happen in bursts, so having lots
	// of pre-computed data improves overall performance.
	const chanSize = 10000

	group, gCtx := errgroup.WithContext(ctx)
	recordCh := make(chan csvRecord, chanSize)
	kvCh := make(chan roachpb.KeyValue, chanSize)
	contentCh := make(chan sstContent)
	group.Go(func() error {
		defer close(recordCh)
		var err error
		csvCount, err = readCSV(gCtx, comma, comment, len(tableDesc.VisibleColumns()), dataFiles, recordCh)
		return err
	})
	group.Go(func() error {
		defer close(kvCh)
		return groupWorkers(gCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, recordCh, kvCh, nullif, &tableDesc)
		})
	})
	group.Go(func() error {
		defer close(contentCh)
		var err error
		kvCount, err = writeRocksDB(gCtx, kvCh, rocksdbDest, sstMaxSize, contentCh)
		return err
	})
	group.Go(func() error {
		var err error
		sstCount, err = makeBackup(gCtx, parentID, &tableDesc, dest, contentCh)
		return err
	})
	return csvCount, kvCount, sstCount, group.Wait()
}

// groupWorkers creates num worker go routines in an error group.
func groupWorkers(ctx context.Context, num int, f func(context.Context) error) error {
	group, ctx := errgroup.WithContext(ctx)
	for i := 0; i < num; i++ {
		group.Go(func() error {
			return f(ctx)
		})
	}
	return group.Wait()
}

// readCSV sends records on ch from CSV listed by dataFiles. comma, if
// non-zero, specifies the field separator. comment, if non-zero, specifies
// the comment character. It returns the number of rows read.
func readCSV(
	ctx context.Context,
	comma, comment rune,
	expectedCols int,
	dataFiles []string,
	recordCh chan<- csvRecord,
) (int64, error) {
	expectedColsExtra := expectedCols + 1
	done := ctx.Done()
	var count int64
	for _, dataFile := range dataFiles {
		select {
		case <-done:
			return 0, ctx.Err()
		default:
		}
		err := func() error {
			f, err := os.Open(dataFile)
			if err != nil {
				return err
			}
			defer f.Close()
			cr := csv.NewReader(f)
			cr.Comma = comma
			cr.FieldsPerRecord = -1
			cr.LazyQuotes = true
			cr.Comment = comment
			for i := 1; ; i++ {
				record, err := cr.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrapf(err, "row %d: reading CSV record", i)
				}
				if len(record) == expectedCols {
					// Expected number of columns.
				} else if len(record) == expectedColsExtra && record[expectedCols] == "" {
					// Line has the optional trailing comma, ignore the empty field.
					record = record[:expectedCols]
				} else {
					return errors.Errorf("row %d: expected %d fields, got %d", i, expectedCols, len(record))
				}
				cr := csvRecord{
					r:    record,
					file: dataFile,
					row:  i,
				}
				select {
				case <-done:
					return ctx.Err()
				case recordCh <- cr:
					count++
				}
			}
			return nil
		}()
		if err != nil {
			return 0, errors.Wrapf(err, dataFile)
		}
	}
	return count, nil
}

type csvRecord struct {
	r    []string
	file string
	row  int
}

// convertRecord converts CSV records KV pairs and sends them on the kvCh chan.
func convertRecord(
	ctx context.Context,
	recordCh <-chan csvRecord,
	kvCh chan<- roachpb.KeyValue,
	nullif *string,
	tableDesc *sqlbase.TableDescriptor,
) error {
	done := ctx.Done()

	visibleCols := tableDesc.VisibleColumns()
	for _, col := range visibleCols {
		if col.DefaultExpr != nil {
			return errors.Errorf("column %q: DEFAULT expression unsupported", col.Name)
		}
	}
	keyDatums := make(sqlbase.EncDatumRow, len(tableDesc.PrimaryIndex.ColumnIDs))
	// keyDatumIdx maps ColumnIDs to indexes in keyDatums.
	keyDatumIdx := make(map[sqlbase.ColumnID]int)
	for _, id := range tableDesc.PrimaryIndex.ColumnIDs {
		for _, col := range visibleCols {
			if col.ID == id {
				keyDatumIdx[id] = len(keyDatumIdx)
				break
			}
		}
	}

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, tableDesc, nil, /* fkTables */
		tableDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return errors.Wrap(err, "make row inserter")
	}

	parse := parser.Parser{}
	evalCtx := parser.EvalContext{}
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &parse, &evalCtx)
	if err != nil {
		return errors.Wrap(err, "process default columns")
	}

	datums := make([]parser.Datum, len(visibleCols))
	var insertErr error
	for record := range recordCh {
		for i, r := range record.r {
			if nullif != nil && r == *nullif {
				datums[i] = parser.DNull
			} else {
				datums[i], err = parser.ParseStringAs(visibleCols[i].Type.ToDatumType(), r, time.UTC)
				if err != nil {
					return errors.Wrapf(err, "%s: row %d: parse %q as %s", record.file, record.row, visibleCols[i].Name, visibleCols[i].Type.SQLString())
				}
			}
			if idx, ok := keyDatumIdx[visibleCols[i].ID]; ok {
				keyDatums[idx] = sqlbase.DatumToEncDatum(visibleCols[i].Type, datums[i])
			}
		}

		row, err := sql.GenerateInsertRow(defaultExprs, ri.InsertColIDtoRowIndex, cols, evalCtx, tableDesc, datums)
		if err != nil {
			return errors.Wrapf(err, "generate insert row: %s: row %d", record.file, record.row)
		}
		insertErr = nil
		if err := ri.InsertRow(ctx, inserter(func(kv roachpb.KeyValue) {
			select {
			case kvCh <- kv:
			case <-done:
				insertErr = ctx.Err()
			}
		}), row, true /* ignoreConflicts */, false /* traceKV */); err != nil {
			return errors.Wrapf(err, "insert row: %s: row %d", record.file, record.row)
		}
		if insertErr != nil {
			return insertErr
		}
	}
	return nil
}

type sstContent struct {
	data []byte
	size int64
	span roachpb.Span
}

// writeRocksDB writes kvs to a RocksDB instance that is created at
// rocksdbDir. After kvs is closed, sst files are created of size maxSize
// and sent on contents. It returns the number of KV pairs created.
func writeRocksDB(
	ctx context.Context,
	kvCh <-chan roachpb.KeyValue,
	rocksdbDir string,
	sstMaxSize int64,
	contentCh chan<- sstContent,
) (int64, error) {
	const batchMaxSize = 1024 * 50

	cache := engine.NewRocksDBCache(0)
	defer cache.Release()
	r, err := engine.NewRocksDB(engine.RocksDBConfig{
		Dir:          rocksdbDir,
		MaxSizeBytes: 0,
		MaxOpenFiles: 1024,
	}, cache)
	if err != nil {
		return 0, errors.Wrap(err, "create rocksdb instance")
	}
	defer r.Close()
	b := r.NewBatch()
	var mk engine.MVCCKey
	for kv := range kvCh {
		mk.Key = kv.Key

		// We need to detect duplicate primary/unique keys. This means we can't
		// call .Put with the same keys, because it will overwrite a previous
		// key. Calling .Get before each .Put is very slow. Instead, give each key
		// a unique timestamp. When we iterate in order, this timestamp will be set
		// to a static value, which will cause duplicate keys to error during sst.Add.
		mk.Timestamp.WallTime++

		if err := b.Put(mk, kv.Value.RawBytes); err != nil {
			return 0, err
		}
		if len(b.Repr()) > batchMaxSize {
			if err := b.Commit(false /* sync */); err != nil {
				return 0, err
			}
			b = r.NewBatch()
		}
	}
	if len(b.Repr()) > 0 {
		if err := b.Commit(false /* sync */); err != nil {
			return 0, err
		}
	}
	it := r.NewIterator(false /* prefix */)
	defer it.Close()
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return 0, err
	}
	defer sst.Close()

	writeSST := func(key, endKey roachpb.Key) error {
		data, err := sst.Finish()
		if err != nil {
			return err
		}
		sc := sstContent{
			data: data,
			size: sst.DataSize,
			span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
		}
		select {
		case contentCh <- sc:
		case <-ctx.Done():
			return ctx.Err()
		}
		sst.Close()
		return nil
	}

	var kv engine.MVCCKeyValue
	var firstKey, lastKey roachpb.Key
	ts := timeutil.Now().UnixNano()
	var count int64
	for it.Seek(engine.MVCCKey{}); ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return 0, err
		} else if !ok {
			break
		}
		count++

		// Save the first key for the span.
		if firstKey == nil {
			firstKey = append([]byte(nil), it.UnsafeKey().Key...)

			// Ensure the first key doesn't match the last key of the previous SST.
			if firstKey.Equal(lastKey) {
				return 0, errors.Errorf("duplicate key: %s", firstKey)
			}
		}

		kv.Key = it.UnsafeKey()
		kv.Key.Timestamp.WallTime = ts
		kv.Value = it.UnsafeValue()

		if err := sst.Add(kv); err != nil {
			return 0, errors.Wrapf(err, "SST creation error at %s; this can happen when a primary or unique index has duplicate keys", kv.Key.Key)
		}
		if sst.DataSize > sstMaxSize {
			if err := writeSST(firstKey, kv.Key.Key.Next()); err != nil {
				return 0, err
			}
			firstKey = nil
			lastKey = append([]byte(nil), kv.Key.Key...)

			sst, err = engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return 0, err
			}
			defer sst.Close()
		}
	}
	if sst.DataSize > 0 {
		if err := writeSST(firstKey, kv.Key.Key.Next()); err != nil {
			return 0, err
		}
	}
	return count, nil
}

// makeBackup writes sst files from contents to destDir and creates a backup
// descriptor. It returns the number of SST files written.
func makeBackup(
	ctx context.Context,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	destDir string,
	contentCh <-chan sstContent,
) (int64, error) {
	backupDesc := BackupDescriptor{
		FormatVersion: BackupFormatInitialVersion,
	}

	i := 0
	for sst := range contentCh {
		backupDesc.EntryCounts.DataSize += sst.size
		checksum, err := storageccl.SHA512ChecksumData(sst.data)
		if err != nil {
			return 0, err
		}
		i++
		name := fmt.Sprintf("%d.sst", i)
		path := filepath.Join(destDir, name)
		if err := ioutil.WriteFile(path, sst.data, 0666); err != nil {
			return 0, err
		}

		backupDesc.Files = append(backupDesc.Files, BackupDescriptor_File{
			Path:   name,
			Span:   sst.span,
			Sha512: checksum,
		})
	}
	if len(backupDesc.Files) == 0 {
		return 0, errors.New("no files in backup")
	}

	sort.Sort(backupFileDescriptors(backupDesc.Files))
	backupDesc.Spans = []roachpb.Span{
		{
			Key:    backupDesc.Files[0].Span.Key,
			EndKey: backupDesc.Files[len(backupDesc.Files)-1].Span.EndKey,
		},
	}
	backupDesc.Descriptors = []sqlbase.Descriptor{
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{
			Name: "csv",
			ID:   parentID,
		}),
		*sqlbase.WrapDescriptor(tableDesc),
	}
	descBuf, err := backupDesc.Marshal()
	if err != nil {
		return 0, err
	}
	err = ioutil.WriteFile(filepath.Join(destDir, BackupDescriptorName), descBuf, 0666)
	return int64(len(backupDesc.Files)), err
}

func loadPlanHook(
	stmt parser.Statement, p sql.PlanHookState,
) (func(context.Context) ([]parser.Datums, error), sqlbase.ResultColumns, error) {
	loadStmt, ok := stmt.(*parser.Load)
	if !ok {
		return nil, nil, nil
	}

	// TODO(dan): This entire method is a placeholder to get the distsql
	// plumbing worked out while mjibson works on the new processors and router.
	// Currently, it "uses" distsql to compute an int and this method returns
	// it.
	header := sqlbase.ResultColumns{
		{Name: "start_key", Typ: parser.TypeBytes},
		{Name: "end_key", Typ: parser.TypeBytes},
		{Name: "path", Typ: parser.TypeString},
		{Name: "sha512", Typ: parser.TypeBytes},
		{Name: "data_size", Typ: parser.TypeInt},
	}
	fn := func(ctx context.Context) ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, loadStmt.StatementTag())
		defer tracing.FinishSpan(span)

		resp, err := p.ExecCfg().StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return nil, err
		}
		var nodes []roachpb.NodeDescriptor
		for _, node := range resp.Nodes {
			nodes = append(nodes, node.Desc)
		}

		result, err := sql.LoadCSVDistributed(
			ctx, p.ExecCfg().DB, p.EvalContext(), nodes, p.DistSQLPlanner())
		ret := []parser.Datums{{
			parser.DNull,
			parser.DNull,
			parser.DNull,
			parser.DNull,
			parser.NewDInt(parser.DInt(result)),
		}}
		return ret, nil
	}
	return fn, header, nil
}

func init() {
	sql.AddPlanHook(loadPlanHook)
}
