// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	importOptionDelimiter = "delimiter"
	importOptionComment   = "comment"
	importOptionLocal     = "local"
	importOptionNullIf    = "nullif"
	importOptionTransform = "transform"
	importOptionSSTSize   = "sstsize"
)

var importOptionExpectValues = map[string]bool{
	importOptionDelimiter: true,
	importOptionComment:   true,
	importOptionLocal:     false,
	importOptionNullIf:    true,
	importOptionTransform: true,
	importOptionSSTSize:   true,
	restoreOptIntoDB:      true,
}

// LoadCSV converts CSV files into enterprise backup format.
func LoadCSV(
	ctx context.Context,
	table string,
	dataFiles []string,
	dest string,
	comma, comment rune,
	nullif *string,
	sstMaxSize int64,
	tempDir string,
) (csvCount, kvCount, sstCount int64, err error) {
	if table == "" {
		return 0, 0, 0, errors.New("no table specified")
	}
	table, err = storageccl.MakeLocalStorageURI(table)
	if err != nil {
		return 0, 0, 0, err
	}
	if dest == "" {
		return 0, 0, 0, errors.New("no destination specified")
	}
	if len(dataFiles) == 0 {
		dataFiles = []string{fmt.Sprintf("%s.dat", table)}
	}
	createTable, err := readCreateTableFromStore(ctx, table, cluster.NoSettings)
	if err != nil {
		return 0, 0, 0, err
	}

	var parentID = defaultCSVParentID
	walltime := timeutil.Now().UnixNano()

	// Using test cluster settings means that we'll generate a backup using
	// the latest cluster version available in this binary. This will be safe
	// once we verify the cluster version during restore.
	//
	// TODO(benesch): ensure backups from too-old or too-new nodes are
	// rejected during restore.
	st := cluster.MakeTestingClusterSettings()

	tableDesc, err := makeSimpleTableDescriptor(
		ctx, st, createTable, parentID, defaultCSVTableID, walltime)
	if err != nil {
		return 0, 0, 0, err
	}

	for i, f := range dataFiles {
		dataFiles[i], err = storageccl.MakeLocalStorageURI(f)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	// TODO(mjibson): allow users to optionally specify a full URI to an export store.
	dest, err = storageccl.MakeLocalStorageURI(dest)
	if err != nil {
		return 0, 0, 0, err
	}

	rocksdbDir, err := ioutil.TempDir(tempDir, "cockroach-csv-rocksdb")
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err := os.RemoveAll(rocksdbDir); err != nil {
			log.Infof(ctx, "could not remove temp directory %s: %s", rocksdbDir, err)
		}
	}()

	fileLimit, err := server.SetOpenFileLimitForOneStore()
	if err != nil {
		return 0, 0, 0, err
	}

	cache := engine.NewRocksDBCache(0)
	defer cache.Release()
	r, err := engine.NewRocksDB(engine.RocksDBConfig{
		Settings:     cluster.MakeTestingClusterSettings(),
		Dir:          rocksdbDir,
		MaxSizeBytes: 0,
		MaxOpenFiles: fileLimit,
	}, cache)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "create rocksdb instance")
	}
	defer r.Close()

	return doLocalCSVTransform(
		ctx, nil, parentID, tableDesc, dest, dataFiles, comma, comment, nullif, sstMaxSize, r, walltime, nil,
	)
}

func doLocalCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	dest string,
	dataFiles []string,
	comma, comment rune,
	nullif *string,
	sstMaxSize int64,
	tempEngine engine.Engine,
	walltime int64,
	execCfg *sql.ExecutorConfig,
) (csvCount, kvCount, sstCount int64, err error) {
	// Some channels are buffered because reads happen in bursts, so having lots
	// of pre-computed data improves overall performance. If this value is too
	// high, it will decrease the accuracy of the progress estimation because
	// of the hidden buffered work to be done.
	const chanSize = 1000

	recordCh := make(chan csvRecord, chanSize)
	kvCh := make(chan []roachpb.KeyValue, chanSize)
	contentCh := make(chan sstContent)
	var backupDesc *BackupDescriptor
	conf, err := storageccl.ExportStorageConfFromURI(dest)
	if err != nil {
		return 0, 0, 0, err
	}
	var st *cluster.Settings
	if execCfg != nil {
		st = execCfg.Settings
	}
	es, err := storageccl.MakeExportStorage(ctx, conf, st)
	if err != nil {
		return 0, 0, 0, err
	}
	defer es.Close()

	var readProgressFn, writeProgressFn func(float32) error
	if job != nil {
		// These consts determine how much of the total progress the read csv and
		// write sst groups take overall. 50% each is an approximation but kind of
		// accurate based on my testing.
		const (
			readPct  = 0.5
			writePct = 1.0 - readPct
		)
		// Both read and write progress funcs register their progress as 50% of total progress.
		readProgressFn = func(pct float32) error {
			return job.Progressed(ctx, jobs.FractionUpdater(pct*readPct))
		}
		writeProgressFn = func(pct float32) error {
			return job.Progressed(ctx, jobs.FractionUpdater(readPct+pct*writePct))
		}
	}

	// The first group reads the CSVs, converts them into KVs, and writes all
	// KVs into a single RocksDB instance.
	group, gCtx := errgroup.WithContext(ctx)
	store := engine.NewRocksDBMultiMap(tempEngine)
	group.Go(func() error {
		defer close(recordCh)
		var err error
		csvCount, err = readCSV(gCtx, comma, comment, len(tableDesc.VisibleColumns()), dataFiles, recordCh, readProgressFn, st)
		return err
	})
	group.Go(func() error {
		defer close(kvCh)
		return groupWorkers(gCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, recordCh, kvCh, nullif, tableDesc)
		})
	})
	group.Go(func() error {
		var err error
		kvCount, err = writeRocksDB(gCtx, kvCh, store.NewBatchWriter())
		return err
	})
	if err := group.Wait(); err != nil {
		return 0, 0, 0, err
	}

	// The second group iterates over the KVs in the RocksDB instance in sorted
	// order, chunks them up into SST files, and writes them to storage.
	group, gCtx = errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(contentCh)
		return makeSSTs(gCtx, store.NewIterator(), sstMaxSize, contentCh, walltime, kvCount, writeProgressFn)
	})
	group.Go(func() error {
		var err error
		backupDesc, err = makeBackup(gCtx, contentCh, walltime, es)
		return err
	})
	if err := group.Wait(); err != nil {
		return 0, 0, 0, err
	}
	err = finalizeCSVBackup(ctx, backupDesc, parentID, tableDesc, es, execCfg)
	sstCount = int64(len(backupDesc.Files))

	return csvCount, kvCount, sstCount, err
}

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID sqlbase.ID = keys.MaxReservedDescID + 1
	defaultCSVTableID  sqlbase.ID = defaultCSVParentID + 1
)

func readCreateTableFromStore(
	ctx context.Context, filename string, settings *cluster.Settings,
) (*tree.CreateTable, error) {
	store, err := exportStorageFromURI(ctx, filename, settings)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmt, err := parser.ParseOne(string(tableDefStr))
	if err != nil {
		return nil, err
	}
	create, ok := stmt.(*tree.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return create, nil
}

// makeSimpleTableDescriptor creates a TableDescriptor from a CreateTable parse
// node without the full machinery. Many parts of the syntax are unsupported
// (see the implementation and TestMakeSimpleTableDescriptorErrors for details),
// but this is enough for our csv IMPORT and for some unit tests.
func makeSimpleTableDescriptor(
	ctx context.Context,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID,
	tableID sqlbase.ID,
	walltime int64,
) (*sqlbase.TableDescriptor, error) {
	sql.HoistConstraints(create)
	if create.IfNotExists {
		return nil, errors.New("unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, errors.New("interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, errors.New("CREATE AS not supported")
	}
	for _, def := range create.Defs {
		switch def := def.(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.DefaultExpr.Expr != nil {
				return nil, errors.Errorf("DEFAULT expressions not supported: %s", tree.AsString(def))
			}
		case *tree.ForeignKeyConstraintTableDef:
			return nil, errors.Errorf("foreign keys not supported: %s", tree.AsString(def))
		default:
			return nil, errors.Errorf("unsupported table definition: %s", tree.AsString(def))
		}
	}
	semaCtx := tree.SemaContext{}
	evalCtx := tree.EvalContext{}
	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		sql.NilVirtualTabler,
		st,
		create,
		parentID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultPrivilegeDescriptor(),
		nil, /* affected */
		"",  /* sessionDB */
		&semaCtx,
		&evalCtx,
	)
	if err != nil {
		return nil, err
	}

	return &tableDesc, nil
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
// the comment character. It returns the number of rows read. progressFn, if
// not nil, is periodically invoked with a percentage of the total progress
// of reading through all of the files. This percentage attempts to use
// the Size() method of ExportStorage to determine how many bytes must be
// read of the CSV files, and reports the percent of bytes read among all
// dataFiles. If any Size() fails for any file, then progress is reported
// only after each file has been read.
func readCSV(
	ctx context.Context,
	comma, comment rune,
	expectedCols int,
	dataFiles []string,
	recordCh chan<- csvRecord,
	progressFn func(float32) error,
	settings *cluster.Settings,
) (int64, error) {
	const batchSize = 500
	expectedColsExtra := expectedCols + 1
	done := ctx.Done()
	var count int64
	if comma == 0 {
		comma = ','
	}

	var totalBytes, readBytes int64
	// Attempt to fetch total number of bytes for all files.
	for _, dataFile := range dataFiles {
		conf, err := storageccl.ExportStorageConfFromURI(dataFile)
		if err != nil {
			return 0, err
		}
		es, err := storageccl.MakeExportStorage(ctx, conf, settings)
		if err != nil {
			return 0, err
		}
		sz, err := es.Size(ctx, "")
		es.Close()
		if sz <= 0 {
			// Don't log dataFile here because it could leak auth information.
			log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			totalBytes = 0
			break
		}
		totalBytes += sz
	}
	updateFromFiles := progressFn != nil && totalBytes == 0
	updateFromBytes := progressFn != nil && totalBytes > 0

	for dataFileI, dataFile := range dataFiles {
		select {
		case <-done:
			return 0, ctx.Err()
		default:
		}

		err := func() error {
			conf, err := storageccl.ExportStorageConfFromURI(dataFile)
			if err != nil {
				return err
			}
			es, err := storageccl.MakeExportStorage(ctx, conf, settings)
			if err != nil {
				return err
			}
			defer es.Close()
			f, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			bc := byteCounter{r: f}
			cr := csv.NewReader(&bc)
			cr.Comma = comma
			cr.FieldsPerRecord = -1
			cr.LazyQuotes = true
			cr.Comment = comment

			batch := csvRecord{
				file:      dataFile,
				rowOffset: 1,
				r:         make([][]string, 0, batchSize),
			}

			for i := 1; ; i++ {
				record, err := cr.Read()
				if err == io.EOF || len(batch.r) >= batchSize {
					// if the batch isn't empty, we need to flush it.
					if len(batch.r) > 0 {
						select {
						case <-done:
							return ctx.Err()
						case recordCh <- batch:
							count += int64(len(batch.r))
						}
					}
					const fiftyMiB = 50 << 20
					if updateFromBytes && (err == io.EOF || bc.n > fiftyMiB) {
						readBytes += bc.n
						bc.n = 0
						if err := progressFn(float32(readBytes) / float32(totalBytes)); err != nil {
							return err
						}
					}
					if err == io.EOF {
						break
					}
					batch.rowOffset = i
					batch.r = make([][]string, 0, batchSize)
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
				batch.r = append(batch.r, record)
			}
			return nil
		}()
		if err != nil {
			return 0, errors.Wrap(err, dataFile)
		}
		if updateFromFiles {
			if err := progressFn(float32(dataFileI+1) / float32(len(dataFiles))); err != nil {
				return 0, err
			}
		}
	}
	return count, nil
}

type byteCounter struct {
	r io.Reader
	n int64
}

func (b *byteCounter) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}

type csvRecord struct {
	r         [][]string
	file      string
	rowOffset int
}

// convertRecord converts CSV records KV pairs and sends them on the kvCh chan.
func convertRecord(
	ctx context.Context,
	recordCh <-chan csvRecord,
	kvCh chan<- []roachpb.KeyValue,
	nullif *string,
	tableDesc *sqlbase.TableDescriptor,
) error {
	done := ctx.Done()

	const kvBatchSize = 1000
	padding := 2 * (len(tableDesc.Indexes) + len(tableDesc.Families))
	visibleCols := tableDesc.VisibleColumns()

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, tableDesc, nil, /* fkTables */
		tableDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return errors.Wrap(err, "make row inserter")
	}

	var txCtx transform.ExprTransformContext
	evalCtx := tree.EvalContext{SessionData: sessiondata.SessionData{Location: time.UTC}}
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &txCtx, &evalCtx)
	if err != nil {
		return errors.Wrap(err, "process default columns")
	}

	datums := make([]tree.Datum, len(visibleCols))
	kvBatch := make([]roachpb.KeyValue, 0, kvBatchSize+padding)

	for batch := range recordCh {
		for batchIdx, record := range batch.r {
			rowNum := batch.rowOffset + batchIdx
			for i, v := range record {
				col := visibleCols[i]
				if nullif != nil && v == *nullif {
					datums[i] = tree.DNull
				} else {
					datums[i], err = parser.ParseStringAs(col.Type.ToDatumType(), v, &evalCtx)
					if err != nil {
						return errors.Wrapf(err, "%s: row %d: parse %q as %s", batch.file, rowNum, col.Name, col.Type.SQLString())
					}
				}
			}

			row, err := sql.GenerateInsertRow(defaultExprs, ri.InsertColIDtoRowIndex, cols, evalCtx, tableDesc, datums)
			if err != nil {
				return errors.Wrapf(err, "generate insert row: %s: row %d", batch.file, rowNum)
			}
			// TODO(bram): Is the checking of FKs here required? If not, turning them
			// off may provide a speed boost.
			if err := ri.InsertRow(
				ctx,
				inserter(func(kv roachpb.KeyValue) {
					kvBatch = append(kvBatch, kv)
				}),
				row,
				true, /* ignoreConflicts */
				sqlbase.CheckFKs,
				false, /* traceKV */
			); err != nil {
				return errors.Wrapf(err, "insert row: %s: row %d", batch.file, rowNum)
			}
			if len(kvBatch) >= kvBatchSize {
				select {
				case kvCh <- kvBatch:
				case <-done:
					return ctx.Err()
				}
				kvBatch = make([]roachpb.KeyValue, 0, kvBatchSize+padding)
			}
		}
	}
	select {
	case kvCh <- kvBatch:
	case <-done:
		return ctx.Err()
	}
	return nil
}

type sstContent struct {
	data []byte
	size int64
	span roachpb.Span
}

const errSSTCreationMaybeDuplicateTemplate = "SST creation error at %s; this can happen when a primary or unique index has duplicate keys"

// writeRocksDB writes kvs to a RocksDB instance that is created at
// rocksdbDir. It returns the number of KV pairs written.
func writeRocksDB(
	ctx context.Context, kvCh <-chan []roachpb.KeyValue, writer engine.SortedDiskMapBatchWriter,
) (int64, error) {
	var count int64
	for kvBatch := range kvCh {
		count += int64(len(kvBatch))
		for _, kv := range kvBatch {
			if err := writer.Put(kv.Key, kv.Value.RawBytes); err != nil {
				return 0, err
			}
		}
	}
	if err := writer.Close(ctx); err != nil {
		return 0, err
	}
	return count, nil
}

// makeSSTs creates SST files in memory of size maxSize and sent on
// contentCh. progressFn, if not nil, is periodically invoked with the
// percentage of KVs that have been written to SSTs and sent on contentCh.
func makeSSTs(
	ctx context.Context,
	it engine.SortedDiskMapIterator,
	sstMaxSize int64,
	contentCh chan<- sstContent,
	walltime int64,
	totalKVs int64,
	progressFn func(float32) error,
) error {
	defer it.Close()

	if totalKVs == 0 {
		return nil
	}

	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	defer sst.Close()

	var writtenKVs int64
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
		if progressFn != nil {
			if err := progressFn(float32(writtenKVs) / float32(totalKVs)); err != nil {
				return err
			}
		}
		return nil
	}

	var kv engine.MVCCKeyValue
	kv.Key.Timestamp.WallTime = walltime
	// firstKey is always the first key of the span. lastKey, if nil, means the
	// current SST hasn't yet filled up. Once the SST has filled up, lastKey is
	// set to the key at which to stop adding KVs. We have to do this because
	// all column families for a row must be in one SST and the SST may have
	// filled up with only some of the KVs from the column families being added.
	var firstKey, lastKey roachpb.Key

	it.Rewind()
	if ok, err := it.Valid(); err != nil {
		return err
	} else if !ok {
		return errors.New("could not get first key")
	}
	firstKey = it.Key()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		writtenKVs++

		kv.Key.Key = it.UnsafeKey()
		kv.Value = it.UnsafeValue()
		if lastKey != nil {
			if kv.Key.Key.Compare(lastKey) >= 0 {
				if err := writeSST(firstKey, lastKey); err != nil {
					return err
				}
				firstKey = it.Key()
				lastKey = nil

				sst, err = engine.MakeRocksDBSstFileWriter()
				if err != nil {
					return err
				}
				defer sst.Close()
			}
		}
		if err := sst.Add(kv); err != nil {
			return errors.Wrapf(err, errSSTCreationMaybeDuplicateTemplate, kv.Key.Key)
		}
		if sst.DataSize > sstMaxSize && lastKey == nil {
			// When we would like to split the file, proceed until we aren't in the
			// middle of a row. Start by finding the next safe split key.
			lastKey, err = keys.EnsureSafeSplitKey(kv.Key.Key)
			if err != nil {
				return err
			}
			lastKey = lastKey.PrefixEnd()
		}
	}
	if sst.DataSize > 0 {
		if err := writeSST(firstKey, kv.Key.Key.Next()); err != nil {
			return err
		}
	}
	return nil
}

// makeBackup writes SST files from contents to es and creates a backup
// descriptor populated with the written SST files.
func makeBackup(
	ctx context.Context, contentCh <-chan sstContent, walltime int64, es storageccl.ExportStorage,
) (*BackupDescriptor, error) {
	backupDesc := BackupDescriptor{
		FormatVersion: BackupFormatInitialVersion,
		EndTime:       hlc.Timestamp{WallTime: walltime},
	}
	i := 0
	for sst := range contentCh {
		backupDesc.EntryCounts.DataSize += sst.size
		checksum, err := storageccl.SHA512ChecksumData(sst.data)
		if err != nil {
			return nil, err
		}
		i++
		name := fmt.Sprintf("%d.sst", i)
		if err := es.WriteFile(ctx, name, bytes.NewReader(sst.data)); err != nil {
			return nil, err
		}

		backupDesc.Files = append(backupDesc.Files, BackupDescriptor_File{
			Path:   name,
			Span:   sst.span,
			Sha512: checksum,
		})
	}
	return &backupDesc, nil
}

const csvDatabaseName = "csv"

func finalizeCSVBackup(
	ctx context.Context,
	backupDesc *BackupDescriptor,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	es storageccl.ExportStorage,
	execCfg *sql.ExecutorConfig,
) error {
	sort.Sort(backupFileDescriptors(backupDesc.Files))
	backupDesc.Spans = []roachpb.Span{tableDesc.TableSpan()}
	backupDesc.Descriptors = []sqlbase.Descriptor{
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{
			Name: csvDatabaseName,
			ID:   parentID,
		}),
		*sqlbase.WrapDescriptor(tableDesc),
	}
	backupDesc.FormatVersion = BackupFormatInitialVersion
	backupDesc.BuildInfo = build.GetInfo()
	if execCfg != nil {
		backupDesc.NodeID = execCfg.NodeID.Get()
		backupDesc.ClusterID = execCfg.ClusterID()
	}
	descBuf, err := protoutil.Marshal(backupDesc)
	if err != nil {
		return err
	}
	return es.WriteFile(ctx, BackupDescriptorName, bytes.NewReader(descBuf))
}

func importJobDescription(
	orig *tree.Import, defs tree.TableDefs, files []string, opts map[string]string,
) (string, error) {
	stmt := *orig
	stmt.CreateFile = nil
	stmt.CreateDefs = defs
	stmt.Files = nil
	for _, file := range files {
		clean, err := storageccl.SanitizeExportStorageURI(file)
		if err != nil {
			return "", err
		}
		stmt.Files = append(stmt.Files, tree.NewDString(clean))
	}
	stmt.Options = nil
	for k, v := range opts {
		switch k {
		case importOptionTransform:
			clean, err := storageccl.SanitizeExportStorageURI(v)
			if err != nil {
				return "", err
			}
			v = clean
		}
		opt := tree.KVOption{Key: tree.Name(k)}
		if importOptionExpectValues[k] {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	return tree.AsStringWithFlags(&stmt, tree.FmtAlwaysQualifyTableNames), nil
}

const importCSVEnabledSetting = "experimental.importcsv.enabled"

var importCSVEnabled = settings.RegisterBoolSetting(
	importCSVEnabledSetting,
	"enable experimental IMPORT CSV statement",
	false,
)

func importPlanHook(
	stmt tree.Statement, p sql.PlanHookState,
) (func(context.Context, chan<- tree.Datums) error, sqlbase.ResultColumns, error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return nil, nil, nil
	}

	filesFn, err := p.TypeAsStringArray(importStmt.Files, "IMPORT")
	if err != nil {
		return nil, nil, err
	}

	var createFileFn func() (string, error)
	if importStmt.CreateDefs == nil {
		createFileFn, err = p.TypeAsString(importStmt.CreateFile, "IMPORT")
		if err != nil {
			return nil, nil, err
		}
	}

	if importStmt.FileFormat != "CSV" {
		// not possible with current parser rules.
		return nil, nil, errors.Errorf("unsupported import format: %q", importStmt.FileFormat)
	}

	optsFn, err := p.TypeAsStringOpts(importStmt.Options, importOptionExpectValues)
	if err != nil {
		return nil, nil, err
	}
	// TODO(dan): This entire method is a placeholder to get the distsql
	// plumbing worked out while mjibson works on the new processors and router.
	// Currently, it "uses" distsql to compute an int and this method returns
	// it.
	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer tracing.FinishSpan(span)

		walltime := timeutil.Now().UnixNano()

		if !importCSVEnabled.Get(&p.ExecCfg().Settings.SV) {
			return errors.Errorf(
				`IMPORT is an experimental feature and is disabled by default; `+
					`enable by executing: SET CLUSTER SETTING %s = true`,
				importCSVEnabledSetting,
			)
		}

		if err := p.RequireSuperUser("IMPORT"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("IMPORT cannot be used inside a transaction")
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		files, err := filesFn()
		if err != nil {
			return err
		}

		parentID := defaultCSVParentID
		transform := opts[importOptionTransform]
		var targetDB string
		var transformStorage storageccl.ExportStorage
		if transform == "" {
			if override, ok := opts[restoreOptIntoDB]; !ok {
				if session := p.SessionData().Database; session != "" {
					targetDB = session
				} else {
					return errors.Errorf("must specify target database with %q option", restoreOptIntoDB)
				}
			} else {
				targetDB = override
			}
			// Check if database exists right now. It might not after the import is done,
			// but it's better to fail fast than wait until restore.
			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				id, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(0, targetDB))
				if err != nil {
					return err
				}
				if id.Value == nil {
					return errors.Errorf("database does not exist: %q", targetDB)
				}
				parentID = sqlbase.ID(id.ValueInt())
				return nil
			}); err != nil {
				return err
			}
		} else {
			if _, ok := opts[restoreOptIntoDB]; ok {
				return errors.Errorf("cannot specify both %s and %s", importOptionTransform, restoreOptIntoDB)
			}
			transformStorage, err = exportStorageFromURI(ctx, transform, p.ExecCfg().Settings)
			if err != nil {
				return err
			}
			defer transformStorage.Close()
		}

		var comma rune
		if override, ok := opts[importOptionDelimiter]; ok {
			comma, err = util.GetSingleRune(override)
			if err != nil {
				return errors.Wrap(err, "invalid comma value")
			}
		}

		var comment rune
		if override, ok := opts[importOptionComment]; ok {
			comment, err = util.GetSingleRune(override)
			if err != nil {
				return errors.Wrap(err, "invalid comment value")
			}
		}

		var nullif *string
		if override, ok := opts[importOptionNullIf]; ok {
			nullif = &override
		}

		sstSize := config.DefaultZoneConfig().RangeMaxBytes / 2
		if override, ok := opts[importOptionSSTSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			sstSize = sz
		}

		var create *tree.CreateTable
		if importStmt.CreateDefs != nil {
			normName := tree.NormalizableTableName{TableNameReference: &importStmt.Table}
			create = &tree.CreateTable{Table: normName, Defs: importStmt.CreateDefs}
		} else {
			filename, err := createFileFn()
			if err != nil {
				return err
			}
			create, err = readCreateTableFromStore(ctx, filename, p.ExecCfg().Settings)
			if err != nil {
				return err
			}
			if named, parsed := importStmt.Table.String(), create.Table.String(); parsed != named {
				return errors.Errorf("importing table %q, but file specifies a schema for table %q", named, parsed)
			}
		}

		tableDesc, err := makeSimpleTableDescriptor(
			ctx, p.ExecCfg().Settings, create, parentID, defaultCSVTableID, walltime)
		if err != nil {
			return err
		}

		jobDesc, err := importJobDescription(importStmt, create.Defs, files, opts)
		if err != nil {
			return err
		}

		if transform != "" {
			// Delay writing the BACKUP-CHECKPOINT file until as late as possible.
			if err := verifyUsableExportTarget(ctx, transformStorage, transform); err != nil {
				return err
			}
		} else {
			// Verification steps have passed, generate a new table ID if we're
			// restoring. We do this last because we want to avoid calling
			// GenerateUniqueDescID if there's any kind of error above.
			// Reserving a table ID now means we can avoid the rekey work during restore.
			tableDesc.ID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
			if err != nil {
				return err
			}
		}

		// NB: the post-conversion RESTORE will create and maintain its own job.
		// This job is thus only for tracking the conversion, and will be Finished()
		// before the restore starts.
		job := p.ExecCfg().JobRegistry.NewJob(jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details: jobs.ImportDetails{
				Tables: []jobs.ImportDetails_Table{{
					Desc:       tableDesc,
					URIs:       files,
					BackupPath: transform,
				}},
			},
		})
		if err := job.Created(ctx); err != nil {
			return err
		}
		if err := job.Started(ctx); err != nil {
			return err
		}

		var importErr error
		if _, local := opts[importOptionLocal]; !local {
			importErr = doDistributedCSVTransform(
				ctx, job, files, p, parentID, tableDesc, transform,
				comma, comment, nullif, walltime,
				sstSize,
			)
		} else {
			if transform == "" {
				return errors.Errorf("%s option required for local import", importOptionTransform)
			}
			_, _, _, importErr = doLocalCSVTransform(
				ctx, job, parentID, tableDesc, transform, files,
				comma, comment, nullif, sstSize,
				p.ExecCfg().DistSQLSrv.TempStorage,
				walltime, p.ExecCfg(),
			)
		}
		if transform != "" {
			// Always attempt to cleanup the checkpoint even if the import failed.
			if err := transformStorage.Delete(ctx, BackupDescriptorCheckpointName); err != nil {
				log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
			}
		}
		if importErr != nil {
			if err := job.Failed(ctx, importErr, jobs.NoopFn); err != nil {
				return err
			}
			if err, ok := errors.Cause(importErr).(*jobs.InvalidStatusError); ok && err.Status() == jobs.StatusCanceled {
				importErr = errors.Errorf("job %s", err.Status())
			}
		} else {
			if err := job.Succeeded(ctx, jobs.NoopFn); err != nil {
				return err
			}
		}
		if importErr != nil {
			return importErr
		}

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(0)),
			tree.NewDInt(tree.DInt(0)),
			tree.NewDInt(tree.DInt(0)),
			tree.NewDInt(tree.DInt(0)),
		}
		return nil
	}
	return fn, restoreHeader, nil
}

func doDistributedCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	files []string,
	p sql.PlanHookState,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	temp string,
	comma, comment rune,
	nullif *string,
	walltime int64,
	sstSize int64,
) error {
	evalCtx := p.ExtendedEvalContext()

	// TODO(dan): Filter out unhealthy nodes.
	resp, err := p.ExecCfg().StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}
	var nodes []roachpb.NodeDescriptor
	for _, node := range resp.Nodes {
		nodes = append(nodes, node.Desc)
	}

	ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_STRING},
		{SemanticType: sqlbase.ColumnType_INT},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
	})
	rows := sqlbase.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	if err := p.DistLoader().LoadCSV(
		ctx,
		job,
		p.ExecCfg().DB,
		evalCtx,
		p.ExecCfg().NodeID.Get(),
		nodes,
		sql.NewRowResultWriter(tree.Rows, rows),
		tableDesc,
		files,
		temp,
		comma, comment,
		nullif,
		walltime,
		sstSize,
	); err != nil {
		// If the job was canceled, any of the distsql processors could have been
		// the first to encounter the .Progress error. This error's string is sent
		// through distsql back here, so we can't examine the err type in this case
		// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
		// job progress to coerce out the correct error type. If the update succeeds
		// then return the original error, otherwise return this error instead so
		// it can be cleaned up at a higher level.
		if err := job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
			d := details.(*jobs.Payload_Import).Import
			return d.Tables[0].Completed()
		}); err != nil {
			return err
		}
		return err
	}

	if temp == "" {
		err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			return restoreTableDescs(ctx, txn, nil, []*sqlbase.TableDescriptor{tableDesc}, job.Record.Username)
		})
		return errors.Wrap(err, "creating table descriptor")
	}

	backupDesc := BackupDescriptor{
		EndTime: hlc.Timestamp{WallTime: walltime},
	}
	n := rows.Len()
	for i := 0; i < n; i++ {
		row := rows.At(i)
		name := row[0].(*tree.DString)
		size := row[1].(*tree.DInt)
		checksum := row[2].(*tree.DBytes)
		spanStart := row[3].(*tree.DBytes)
		spanEnd := row[4].(*tree.DBytes)
		backupDesc.EntryCounts.DataSize += int64(*size)
		backupDesc.Files = append(backupDesc.Files, BackupDescriptor_File{
			Path: string(*name),
			Span: roachpb.Span{
				Key:    roachpb.Key(*spanStart),
				EndKey: roachpb.Key(*spanEnd),
			},
			Sha512: []byte(*checksum),
		})
	}

	dest, err := storageccl.ExportStorageConfFromURI(temp)
	if err != nil {
		return err
	}
	es, err := storageccl.MakeExportStorage(ctx, dest, p.ExecCfg().Settings)
	if err != nil {
		return err
	}
	defer es.Close()

	return finalizeCSVBackup(ctx, &backupDesc, parentID, tableDesc, es, p.ExecCfg())
}

var csvOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newReadCSVProcessor(
	flowCtx *distsqlrun.FlowCtx, spec distsqlrun.ReadCSVSpec, output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	cp := &readCSVProcessor{
		flowCtx:    flowCtx,
		csvOptions: spec.Options,
		sampleSize: spec.SampleSize,
		tableDesc:  spec.TableDesc,
		uri:        spec.Uri,
		output:     output,
		settings:   flowCtx.Settings,
		registry:   flowCtx.JobRegistry,
		progress:   spec.Progress,
	}
	if err := cp.out.Init(&distsqlrun.PostProcessSpec{}, csvOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return cp, nil
}

type readCSVProcessor struct {
	flowCtx    *distsqlrun.FlowCtx
	csvOptions roachpb.CSVOptions
	sampleSize int32
	tableDesc  sqlbase.TableDescriptor
	uri        []string
	out        distsqlrun.ProcOutputHelper
	output     distsqlrun.RowReceiver
	settings   *cluster.Settings
	registry   *jobs.Registry
	progress   distsqlrun.JobProgress
}

var _ distsqlrun.Processor = &readCSVProcessor{}

func (cp *readCSVProcessor) OutputTypes() []sqlbase.ColumnType {
	return csvOutputTypes
}

func (cp *readCSVProcessor) Run(wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(cp.flowCtx.Ctx, "readCSVProcessor")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	group, gCtx := errgroup.WithContext(ctx)
	done := gCtx.Done()
	recordCh := make(chan csvRecord)
	kvCh := make(chan []roachpb.KeyValue)
	sampleCh := make(chan sqlbase.EncDatumRow)

	// Read CSV into CSV records
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "readcsv")
		defer tracing.FinishSpan(span)
		defer close(recordCh)

		job, err := cp.registry.LoadJob(gCtx, cp.progress.JobID)
		if err != nil {
			return err
		}

		progFn := func(pct float32) error {
			return job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
				d := details.(*jobs.Payload_Import).Import
				slotpct := pct * cp.progress.Contribution
				if len(d.Tables[0].SamplingProgress) > 0 {
					d.Tables[0].SamplingProgress[cp.progress.Slot] = slotpct
				} else {
					d.Tables[0].ReadProgress[cp.progress.Slot] = slotpct
				}
				return d.Tables[0].Completed()
			})
		}

		_, err = readCSV(sCtx, cp.csvOptions.Comma, cp.csvOptions.Comment,
			len(cp.tableDesc.VisibleColumns()), cp.uri, recordCh, progFn, cp.settings)
		return err
	})
	// Convert CSV records to KVs
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "convertcsv")
		defer tracing.FinishSpan(span)

		defer close(kvCh)
		return groupWorkers(sCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, recordCh, kvCh, cp.csvOptions.Nullif, &cp.tableDesc)
		})
	})
	// Sample KVs
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "samplecsv")
		defer tracing.FinishSpan(span)

		defer close(sampleCh)
		var fn sampleFunc
		if cp.sampleSize == 0 {
			fn = sampleAll
		} else {
			sr := sampleRate{
				rnd:        rand.New(rand.NewSource(rand.Int63())),
				sampleSize: float64(cp.sampleSize),
			}
			fn = sr.sample
		}
		typeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
		for kvBatch := range kvCh {
			for _, kv := range kvBatch {
				if fn(kv) {
					row := sqlbase.EncDatumRow{
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Key))),
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Value.RawBytes))),
					}
					select {
					case <-done:
						return sCtx.Err()
					case sampleCh <- row:
					}
				}
			}
		}
		return nil
	})
	// Send sampled KVs to dist sql
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "sendcsvkv")
		defer tracing.FinishSpan(span)

		for row := range sampleCh {
			cs, err := cp.out.EmitRow(sCtx, row)
			if err != nil {
				return err
			}
			if cs != distsqlrun.NeedMoreRows {
				return errors.New("unexpected closure of consumer")
			}
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		distsqlrun.DrainAndClose(ctx, cp.output, err)
		return
	}

	cp.out.Close()
}

type sampleFunc func(roachpb.KeyValue) bool

// sampleRate is a sampleFunc that samples a row with a probability of the
// row's size / the sample size.
type sampleRate struct {
	rnd        *rand.Rand
	sampleSize float64
}

func (s sampleRate) sample(kv roachpb.KeyValue) bool {
	sz := float64(len(kv.Key) + len(kv.Value.RawBytes))
	prob := sz / s.sampleSize
	return prob > s.rnd.Float64()
}

func sampleAll(kv roachpb.KeyValue) bool {
	return true
}

var sstOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_STRING},
	{SemanticType: sqlbase.ColumnType_INT},
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newSSTWriterProcessor(
	flowCtx *distsqlrun.FlowCtx,
	spec distsqlrun.SSTWriterSpec,
	input distsqlrun.RowSource,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	sp := &sstWriter{
		flowCtx:     flowCtx,
		spec:        spec,
		input:       input,
		output:      output,
		tempStorage: flowCtx.TempStorage,
		settings:    flowCtx.Settings,
		registry:    flowCtx.JobRegistry,
		progress:    spec.Progress,
		db:          flowCtx.EvalCtx.Txn.DB(),
	}
	if err := sp.out.Init(&distsqlrun.PostProcessSpec{}, sstOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return sp, nil
}

type sstWriter struct {
	flowCtx     *distsqlrun.FlowCtx
	spec        distsqlrun.SSTWriterSpec
	input       distsqlrun.RowSource
	out         distsqlrun.ProcOutputHelper
	output      distsqlrun.RowReceiver
	tempStorage engine.Engine
	settings    *cluster.Settings
	registry    *jobs.Registry
	progress    distsqlrun.JobProgress
	db          *client.DB
}

var _ distsqlrun.Processor = &sstWriter{}

func (sp *sstWriter) OutputTypes() []sqlbase.ColumnType {
	return sstOutputTypes
}

func (sp *sstWriter) Run(wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(sp.flowCtx.Ctx, "sstWriter")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	err := func() error {
		job, err := sp.registry.LoadJob(ctx, sp.progress.JobID)
		if err != nil {
			return err
		}

		// Sort incoming KVs, which will be from multiple spans, into a single
		// RocksDB instance.
		types := sp.input.OutputTypes()
		input := distsqlrun.MakeNoMetadataRowSource(sp.input, sp.output)
		alloc := &sqlbase.DatumAlloc{}
		store := engine.NewRocksDBMultiMap(sp.tempStorage)
		defer store.Close(ctx)
		batch := store.NewBatchWriter()
		var key, val []byte
		for {
			row, err := input.NextRow()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
			if len(row) != 2 {
				return errors.Errorf("expected 2 datums, got %d", len(row))
			}
			for i, ed := range row {
				if err := ed.EnsureDecoded(&types[i], alloc); err != nil {
					return err
				}
				datum := ed.Datum.(*tree.DBytes)
				b := []byte(*datum)
				switch i {
				case 0:
					key = b
				case 1:
					val = b
				}
			}
			if err := batch.Put(key, val); err != nil {
				return err
			}
		}
		if err := batch.Close(ctx); err != nil {
			return err
		}

		// Fetch all the keys in each span and write them to storage.
		iter := store.NewIterator()
		iter.Rewind()
		maxSize := storageccl.MaxImportBatchSize(sp.settings)
		for i, span := range sp.spec.Spans {
			// Since we sampled the CSVs, it is possible for an SST to end up larger
			// than the max raft command size. Split them up into correctly sized chunks.
			for chunk := 0; ; chunk++ {
				data, firstKey, lastKeyInclusive, more, err := extractSSTSpan(iter, span.End, sp.spec.WalltimeNanos, maxSize)
				if err != nil {
					return err
				}
				// Empty span.
				if data == nil {
					break
				}
				lastKeyExclusive := roachpb.Key(lastKeyInclusive).Next()

				var checksum []byte
				name := span.Name
				if chunk > 0 {
					name = fmt.Sprintf("%d-%s", chunk, name)
				}

				if sp.spec.Destination == "" {
					end := span.End
					if more {
						end = lastKeyExclusive
					}
					if err := sp.db.AdminSplit(ctx, end, end); err != nil {
						return err
					}

					log.VEventf(ctx, 1, "scattering key %s", roachpb.PrettyPrintKey(nil, end))
					scatterReq := &roachpb.AdminScatterRequest{
						Span: roachpb.Span{
							Key:    end,
							EndKey: roachpb.Key(end).Next(),
						},
					}
					if _, pErr := client.SendWrapped(ctx, sp.db.GetSender(), scatterReq); pErr != nil {
						// TODO(dan): Unfortunately, Scatter is still too unreliable to
						// fail the IMPORT when Scatter fails. I'm uncomfortable that
						// this could break entirely and not start failing the tests,
						// but on the bright side, it doesn't affect correctness, only
						// throughput.
						log.Errorf(ctx, "failed to scatter span %s: %s", roachpb.PrettyPrintKey(nil, end), pErr)
					}
					if err := storageccl.AddSSTable(ctx, sp.db, firstKey, lastKeyExclusive, data); err != nil {
						return err
					}
				} else {
					checksum, err = storageccl.SHA512ChecksumData(data)
					if err != nil {
						return err
					}
					conf, err := storageccl.ExportStorageConfFromURI(sp.spec.Destination)
					if err != nil {
						return err
					}
					es, err := storageccl.MakeExportStorage(ctx, conf, sp.settings)
					if err != nil {
						return err
					}
					err = es.WriteFile(ctx, name, bytes.NewReader(data))
					es.Close()
					if err != nil {
						return err
					}
				}

				if err := job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
					d := details.(*jobs.Payload_Import).Import
					d.Tables[0].WriteProgress[sp.progress.Slot] = float32(i+1) / float32(len(sp.spec.Spans)) * sp.progress.Contribution
					return d.Tables[0].Completed()
				}); err != nil {
					return err
				}

				row := sqlbase.EncDatumRow{
					sqlbase.DatumToEncDatum(
						sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
						tree.NewDString(name),
					),
					sqlbase.DatumToEncDatum(
						sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
						tree.NewDInt(tree.DInt(len(data))),
					),
					sqlbase.DatumToEncDatum(
						sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
						tree.NewDBytes(tree.DBytes(checksum)),
					),
					sqlbase.DatumToEncDatum(
						sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
						tree.NewDBytes(tree.DBytes(firstKey)),
					),
					sqlbase.DatumToEncDatum(
						sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
						tree.NewDBytes(tree.DBytes(lastKeyExclusive)),
					),
				}
				cs, err := sp.out.EmitRow(ctx, row)
				if err != nil {
					return err
				}
				if cs != distsqlrun.NeedMoreRows {
					return errors.New("unexpected closure of consumer")
				}
				if !more {
					break
				}
			}
		}
		return nil
	}()
	distsqlrun.DrainAndClose(ctx, sp.output, err, sp.input)
}

// extractSSTSpan creates an SST from the iterator, excluding keys >= end.
func extractSSTSpan(
	iter engine.SortedDiskMapIterator, end []byte, walltimeNanos int64, maxSize int64,
) (data, firstKey, lastKey []byte, more bool, err error) {
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return nil, nil, nil, false, err
	}
	defer sst.Close()
	var kv engine.MVCCKeyValue
	kv.Key.Timestamp.WallTime = walltimeNanos
	any := false
	for {
		if ok, err := iter.Valid(); err != nil {
			return nil, nil, nil, false, err
		} else if !ok {
			break
		}
		kv.Key.Key = iter.UnsafeKey()
		if kv.Key.Key.Compare(end) >= 0 {
			// If we are at the end, break the loop and return the data. There is no
			// need to back up one key, because the iterator is pointing at the start
			// of the next block already, and Next won't be called until after the key
			// has been extracted again during the next call to this function.
			break
		}
		kv.Value = iter.UnsafeValue()
		if firstKey == nil {
			firstKey = iter.Key()
		}
		any = true
		if err := sst.Add(kv); err != nil {
			return nil, nil, nil, false, errors.Wrapf(err, errSSTCreationMaybeDuplicateTemplate, kv.Key.Key)
		}
		lastKey = append(lastKey[:0], kv.Key.Key...)

		iter.Next()

		if sst.DataSize > maxSize {
			more = true
			break
		}
	}
	if !any {
		return nil, nil, nil, false, nil
	}
	data, err = sst.Finish()
	return data, firstKey, lastKey, more, err
}

func init() {
	sql.AddPlanHook(importPlanHook)
	distsqlrun.NewReadCSVProcessor = newReadCSVProcessor
	distsqlrun.NewSSTWriterProcessor = newSSTWriterProcessor
}
